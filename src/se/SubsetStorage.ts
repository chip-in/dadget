import * as http from "http";
import * as parser from "mongo-parse";
import * as hash from "object-hash";
import * as ReadWriteLock from "rwlock";
import * as URL from "url";
import * as EJSON from "../util/Ejson";

import { Proxy, ResourceNode, ServiceEngine, Subscriber } from "@chip-in/resource-node";
import { CORE_NODE, EXPORT_LIMIT_NUM, MAX_EXPORT_NUM, MAX_STRING_LENGTH, SPLIT_IN_SUBSET_DB } from "../Config";
import { CacheDb } from "../db/container/CacheDb";
import { PersistentDb } from "../db/container/PersistentDb";
import { JournalDb } from "../db/JournalDb";
import { SubsetDb } from "../db/SubsetDb";
import { SystemDb } from "../db/SystemDb";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { LOG_MESSAGES } from "../LogMessages";
import { DadgetError } from "../util/DadgetError";
import { Logger } from "../util/Logger";
import { LogicalOperator } from "../util/LogicalOperator";
import { ProxyHelper } from "../util/ProxyHelper";
import { Util } from "../util/Util";
import { CLIENT_VERSION, CountResult, CsnMode, default as Dadget, QueryResult } from "./Dadget";
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry";

const MAX_RESPONSE_SIZE_OF_JOURNALS = 10485760;

class UpdateProcessor extends Subscriber {

  private logger: Logger;
  private lock: ReadWriteLock;

  constructor(
    protected storage: SubsetStorage,
    protected database: string,
    protected subsetName: string,
    protected subsetDefinition: SubsetDef) {

    super();
    this.logger = Logger.getLogger("UpdateProcessor", storage.getDbName());
    this.lock = new ReadWriteLock();
  }

  onReceive(msg: string) {
    const transaction = EJSON.parse(msg) as TransactionObject;
    console.log("UpdateProcessor received: ", this.storage.getOption().subset, transaction.csn);
    this.procTransaction(transaction);
  }

  /**
   * トランザクションの処理を行う
   */
  procTransaction(transaction: TransactionObject) {
    this.logger.info(LOG_MESSAGES.PROCTRANSACTION, [transaction.type], [transaction.csn]);

    if (transaction.protectedCsn) {
      if (this.storage.getJournalDb().getProtectedCsn() < transaction.protectedCsn) {
        this.logger.info(LOG_MESSAGES.CHECKPOINT_PROTECTEDCSN, [], [transaction.protectedCsn]);
        this.storage.getJournalDb().setProtectedCsn(transaction.protectedCsn);
        this.storage.getJournalDb().deleteBeforeCsn(transaction.protectedCsn)
          .catch((err) => {
            this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [200]);
          });
      }
    }

    console.log("UpdateProcessor waiting writeLock1");
    this.lock.writeLock((_release1) => {
      console.log("UpdateProcessor got writeLock1");
      const release1 = () => {
        console.log("UpdateProcessor released writeLock1");
        _release1();
        if (this.storage.notifyListener) {
          this.storage.notifyListener.procNotify(transaction);
        }
      };
      console.log("UpdateProcessor waiting writeLock2");
      this.storage.getLock().writeLock((_release2) => {
        console.log("UpdateProcessor got writeLock2");
        const release2 = () => {
          console.log("UpdateProcessor released writeLock2");
          _release2();
        };
        this.storage.getSystemDb().getCsn()
          .then((csn) => {
            if (!this.storage.getReady()) {
              if (csn > 0 && csn === transaction.csn) {
                return this.storage.getJournalDb().findByCsn(csn)
                  .then((journal) => {
                    if (journal && journal.digest === transaction.digest) {
                      this.storage.setReady(transaction);
                      release2();
                      release1();
                      return;
                    } else {
                      return this.adjustData(transaction.csn)
                        .then(() => { release2(); })
                        .then(() => { release1(); });
                    }
                  });
              } else if (csn > transaction.csn) {
                return this.rollbackSubsetDb(transaction.csn, true)
                  .catch((e) => {
                    this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [216]);
                    return this.resetData(transaction.csn, true);
                  })
                  .then(() => { this.storage.setReady(); })
                  .then(() => { release2(); })
                  .then(() => { release1(); });
              } else {
                return this.adjustData(transaction.csn)
                  .then(() => { release2(); })
                  .then(() => { release1(); });
              }
            } else if (csn > transaction.csn && transaction.type === TransactionType.FORCE_ROLLBACK) {
              return this.fetchJournal(transaction.csn)
                .then((fetchJournal) => {
                  if (transaction.csn === 0 || !fetchJournal) { return this.resetData(transaction.csn, true) }
                  return this.rollbackSubsetDb(transaction.csn, true)
                    .catch((e) => {
                      this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [201]);
                      return this.resetData(transaction.csn, true);
                    });
                })
                .then(() => { release2(); })
                .then(() => { release1(); });
            } else if (csn >= transaction.csn) {
              release2();
              release1();
              return;
            } else {
              const doQueuedQuery = (csn: number) => {
                let promise = Promise.resolve();
                for (const query of this.storage.pullQueryWaitingList(csn)) {
                  this.logger.info(LOG_MESSAGES.QUEUED_QUERY, [], [csn]);
                  promise = promise.then(() => query());
                }
                return promise;
              };
              let promise = Promise.resolve();
              const journals = new Map<number, TransactionObject>();
              if (csn + 1 < transaction.csn) {
                promise = promise.then(() => this.fetchJournals(csn, transaction.csn - 1, (fetchJournal) => {
                  journals.set(fetchJournal.csn, fetchJournal);
                  return Promise.resolve();
                }));
              }
              for (let i = csn + 1; i < transaction.csn; i++) {
                // csnが飛んでいた場合はジャーナル取得を行い、そちらから更新
                const _csn = i;
                promise = promise.then(() => this.adjustData(_csn, journals));
                promise = promise.then(() => doQueuedQuery(_csn));
              }
              promise = promise.then(() => this.updateSubsetDbWithTx(transaction));
              promise = promise.then(() => doQueuedQuery(transaction.csn));
              return promise.then(() => {
                release2();
                release1();
              });
            }
          })
          .catch((e) => {
            this.logger.error(LOG_MESSAGES.ERROR_MSG, [e.toString()], [202]);
            release2();
            release1();
          });
      });
    });
  }

  /**
   * トランザクションを進める
   */
  proceedTransaction(to_csn: number) {
    this.logger.info(LOG_MESSAGES.PROCEED_TRANSACTION, [], [to_csn]);
    this.storage.getSystemDb().getCsn()
      .then((csn) => {
        if (to_csn > csn) {
          this.fetchJournals(csn + 1, to_csn, (fetchJournal) => {
            this.procTransaction(fetchJournal);
            return Promise.resolve();
          })
        }
      });
  }

  private async updateSubsetDbWithTx(transaction: TransactionObject): Promise<void> {
    let session = await this.storage.getSystemDb().startTransaction();
    try {
      await this.updateSubsetDb(transaction, session);
      await this.storage.getSystemDb().commitTransaction(session);
    } catch (err) {
      await this.storage.getSystemDb().abortTransaction(session);
      throw err;
    }
  }

  private async updateSubsetDb(transaction: TransactionObject, session?: any): Promise<void> {
    if (transaction.csn > 1) {
      const journal = await this.storage.getJournalDb().findByCsn(transaction.csn - 1, session);
      if (!journal) {
        throw new Error("journal not found, csn:" + (transaction.csn - 1)
          + ", database:" + this.database
          + ", subset:" + this.subsetName);
      }
      if (journal.digest !== transaction.beforeDigest) {
        throw new Error("beforeDigest mismatch, csn:" + transaction.csn
          + ", database:" + this.database
          + ", subset:" + this.subsetName
          + ", journal digest:" + journal.digest
          + ", transaction beforeDigest:" + transaction.beforeDigest);
      }
    }

    this.logger.info(LOG_MESSAGES.UPDATE_SUBSET_DB, [], [transaction.csn]);
    try {
      try {
        const type = transaction.type;
        if ((type === TransactionType.INSERT || type === TransactionType.RESTORE) && transaction.new) {
          const newObj = TransactionRequest.getNew(transaction);
          const obj = { ...newObj, _id: transaction.target, csn: transaction.csn };
          await this.storage.getSubsetDb().insert(obj, session, true);
        } else if (type === TransactionType.UPDATE && transaction.before) {
          const updateObj = TransactionRequest.applyOperator(transaction);
          updateObj.csn = transaction.csn;
          await this.storage.getSubsetDb().update(transaction.target, updateObj, session, true);
        } else if (type === TransactionType.UPSERT || type === TransactionType.REPLACE) {
          const updateObj = TransactionRequest.applyOperator(transaction);
          updateObj.csn = transaction.csn;
          await this.storage.getSubsetDb().update(transaction.target, updateObj, session, true);
        } else if (type === TransactionType.DELETE && transaction.before) {
          await this.storage.getSubsetDb().deleteById(transaction.target, session, true);
        } else if (type === TransactionType.TRUNCATE) {
          await this.storage.getSubsetDb().deleteAll(undefined, true);
        } else if (type === TransactionType.BEGIN || type === TransactionType.BEGIN_IMPORT) {
          this.storage.committedCsn = transaction.committedCsn;
        } else if (type === TransactionType.END || type === TransactionType.END_IMPORT) {
          this.storage.committedCsn = undefined;
        } else if (type === TransactionType.ABORT || type === TransactionType.ABORT_IMPORT) {
          if (transaction.committedCsn === undefined) { throw new Error("committedCsn required"); }
          this.storage.committedCsn = undefined;
          const committedCsn = transaction.committedCsn;
          try {
            await this.rollbackSubsetDb(committedCsn, false);
          } catch (e) {
            this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [203]);
            await this.resetData(committedCsn, false);
          }
        } else if (type === TransactionType.FORCE_ROLLBACK) {
          const committedCsn = transaction.csn;
          if (committedCsn === 0) {
            await this.resetData0();
          } else {
            try {
              await this.rollbackSubsetDb(committedCsn, true);
            } catch (e) {
              this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [204]);
              await this.resetData(committedCsn, true);
            }
          }
        } else if (type === TransactionType.BEGIN_RESTORE) {
          this.storage.getJournalDb().setProtectedCsn(transaction.csn);
          this.storage.committedCsn = transaction.committedCsn;
        } else if (type === TransactionType.END_RESTORE || type === TransactionType.ABORT_RESTORE) {
          this.storage.committedCsn = undefined;
        } else if (type !== TransactionType.NONE) {
          throw new Error("Unsupported type: " + type);
        }
      } catch (e) {
        // ContextManagerで受付済みのトランザクションなので、ログを出力して処理は正常系動作で通す
        this.logger.warn(LOG_MESSAGES.UPDATE_SUBSET_ERROR, [e.toString()]);
      }
      await this.storage.getJournalDb().insert(transaction, session);
      await this.storage.getSystemDb().updateCsn(transaction.csn, session);
    } catch (err) {
      throw err;
    }
  }

  private async rollbackSubsetDb(csn: number, withJournal: boolean): Promise<void> {
    this.logger.warn(LOG_MESSAGES.ROLLBACK_TRANSACTIONS, [], [csn]);
    // Csn of the range is not csn + 1 for keeping last journal
    const firstJournalCsn = withJournal ? csn : csn + 1;
    const transactions = await this.storage.getJournalDb().findByCsnRange(firstJournalCsn, Number.MAX_VALUE, undefined);
    transactions.sort((a, b) => b.csn - a.csn);
    if (transactions.length === 0 || transactions[transactions.length - 1].csn !== firstJournalCsn) {
      throw new Error("Lack of transactions");
    }
    try {
      let committedCsn: number | undefined;
      for (const transaction of transactions) {
        if (transaction.csn === csn) { continue; }
        if (committedCsn !== undefined && committedCsn < transaction.csn) { continue; }
        const type = transaction.type;
        if (type === TransactionType.INSERT || type === TransactionType.RESTORE) {
          await this.storage.getSubsetDb().deleteById(transaction.target);
        } else if (type === TransactionType.UPDATE && transaction.before) {
          const before = TransactionRequest.getBefore(transaction);
          await this.storage.getSubsetDb().update(transaction.target, before);
        } else if (type === TransactionType.UPSERT || type === TransactionType.REPLACE) {
          if (transaction.before) {
            const before = TransactionRequest.getBefore(transaction);
            await this.storage.getSubsetDb().update(transaction.target, before);
          } else {
            await this.storage.getSubsetDb().deleteById(transaction.target);
          }
        } else if (type === TransactionType.DELETE && transaction.before) {
          const before = TransactionRequest.getBefore(transaction);
          await this.storage.getSubsetDb().insert(before);
        } else if (type === TransactionType.TRUNCATE) {
          throw new Error("Cannot roll back TRUNCATE");
        } else if (type === TransactionType.BEGIN) {
        } else if (type === TransactionType.END) {
        } else if (type === TransactionType.ABORT || type === TransactionType.ABORT_IMPORT) {
          if (transaction.committedCsn === undefined) { throw new Error("committedCsn required"); }
          committedCsn = transaction.committedCsn;
          if (transaction.committedCsn < csn) {
            await this.rollforwardSubsetDb(transaction.committedCsn, csn);
          }
        } else if (type === TransactionType.BEGIN_IMPORT) {
        } else if (type === TransactionType.END_IMPORT) {
        } else if (type === TransactionType.BEGIN_RESTORE) {
        } else if (type === TransactionType.END_RESTORE) {
        } else if (type === TransactionType.ABORT_RESTORE) {
        } else if (type !== TransactionType.NONE && type !== TransactionType.FORCE_ROLLBACK) {
          throw new Error("Unsupported type: " + type);
        }
      }
      if (withJournal) {
        await this.storage.getJournalDb().deleteAfterCsn(csn);
        await this.storage.getSystemDb().updateCsn(csn);
      }
    } catch (err) {
      throw err;
    }
  }

  private async rollforwardSubsetDb(fromCsn: number, toCsn: number): Promise<void> {
    this.logger.warn(LOG_MESSAGES.ROLLFORWARD_TRANSACTIONS, [], [fromCsn, toCsn]);
    const transactions = await this.storage.getJournalDb().findByCsnRange(fromCsn + 1, toCsn, undefined);
    transactions.sort((a, b) => a.csn - b.csn);
    if (transactions.length !== toCsn - fromCsn) {
      throw new Error("Lack of rollforward transactions");
    }
    for (const transaction of transactions) {
      await this.updateSubsetDb(transaction);
    }
  }

  fetchJournal(csn: number, journals?: Map<number, TransactionObject>): Promise<TransactionObject | null> {
    if (journals) {
      const journal = journals.get(csn);
      if (journal) {
        return Promise.resolve(journal);
      }
    }
    return Util.fetchJournal(csn, this.database, this.storage.getNode(), this.storage.getOption().subscribe);
  }

  fetchJournals(
    fromCsn: number,
    toCsn: number,
    callback: (obj: TransactionObject) => Promise<void>,
  ): Promise<void> {
    return Util.fetchJournals(fromCsn, toCsn, this.database, this.storage.getNode(), callback, this.storage.getOption().subscribe);
  }

  private adjustData(csn: number, journals?: Map<number, TransactionObject>): Promise<void> {
    this.logger.warn(LOG_MESSAGES.ADJUST_DATA, [], [csn]);
    return Promise.resolve()
      .then(() => {
        if (csn === 0 || this.storage.getSystemDb().isNew()) {
          return this.resetData(csn, true);
        } else {
          return this.storage.getJournalDb().getLastJournal()
            .then((journal) => {
              return this.fetchJournal(journal.csn, journals)
                .then((fetchJournal) => {
                  if (!fetchJournal || fetchJournal.digest !== journal.digest) {
                    // rollback incorrect journals
                    const loopData = {
                      csn: journal.csn,
                    };
                    return Util.promiseWhile<{ csn: number }>(
                      loopData,
                      (loopData) => {
                        return loopData.csn !== 0;
                      },
                      (loopData) => {
                        const prevCsn = loopData.csn - 1;
                        return this.rollbackSubsetDb(prevCsn, true)
                          .then(() => this.storage.getJournalDb().findByCsn(prevCsn))
                          .then((journal) => {
                            if (!journal) { throw new Error("journal not found: " + prevCsn); }
                            return this.fetchJournal(journal.csn, journals)
                              .then((fetchJournal) => {
                                if (fetchJournal && fetchJournal.digest === journal.digest) {
                                  return { ...loopData, csn: 0 };
                                } else {
                                  return { ...loopData, csn: prevCsn };
                                }
                              });
                          });
                      },
                    )
                      .then(() => {
                        return this.adjustData(csn);
                      });
                  } else {
                    const callback = (fetchJournal: TransactionObject) => {
                      const subsetTransaction = UpdateListener.convertTransactionForSubset(this.subsetDefinition, fetchJournal);
                      return this.updateSubsetDb(subsetTransaction);
                    };
                    if (journal.csn + 1 === csn) {
                      return this.fetchJournal(csn, journals)
                        .then((j) => {
                          if (!j) { throw new Error("journal not found: " + csn); }
                          return callback(j);
                        })
                        .then(() => this.storage.getSystemDb().updateCsn(csn))
                        .then(() => { this.storage.setReady(); });
                    } else {
                      return this.fetchJournals(journal.csn + 1, csn, callback)
                        .then(() => this.storage.getSystemDb().updateCsn(csn))
                        .then(() => { this.storage.setReady(); });
                    }
                  }
                });
            })
            .catch((e) => {
              this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [205]);
              return this.resetData(csn, true);
            });
        }
      });
  }

  private async resetData(csn: number, withJournal: boolean): Promise<void> {
    if (csn === 0) { return this.resetData0(); }
    this.logger.warn(LOG_MESSAGES.RESET_DATA, [], [csn]);
    this.storage.pause();
    const query = this.subsetDefinition.query ? this.subsetDefinition.query : {};
    try {
      const fetchJournal = await this.fetchJournal(csn);
      if (!fetchJournal) { throw new Error("journal not found: " + csn); }
      const subsetTransaction = UpdateListener.convertTransactionForSubset(this.subsetDefinition, fetchJournal);
      const result = await Dadget._query(this.storage.getNode(), this.database, query, undefined, undefined, undefined, csn, "strict", { _id: 1 });
      if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
      await this.storage.getSubsetDb().deleteAll();
      await Util.promiseWhile<{ ids: object[]; }>(
        { ids: [...result.resultSet] },
        (whileData) => {
          return whileData.ids.length !== 0;
        },
        async (whileData) => {
          const idMap = new Map();
          const ids = [];
          for (let i = 0; i < MAX_EXPORT_NUM; i++) {
            const row = whileData.ids.shift();
            if (row) {
              const id = (row as any)._id;
              idMap.set(id, id);
              ids.push(id);
            }
          }
          const rowData = await Dadget._query(this.storage.getNode(), this.database, { _id: { $in: ids } }, undefined, EXPORT_LIMIT_NUM, undefined, csn, "strict");
          if (rowData.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
          if (rowData.resultSet.length === 0) { return whileData; }
          for (const data of rowData.resultSet) {
            idMap.delete((data as any)._id);
          }
          for (const id_1 of idMap.keys()) {
            whileData.ids.push({ _id: id_1 });
          }
          await this.storage.getSubsetDb().insertMany(rowData.resultSet);
          return whileData;
        });
      if (withJournal) {
        await this.storage.getJournalDb().deleteAll();
        await this.storage.getJournalDb().insert(subsetTransaction);
        await this.storage.getSystemDb().updateCsn(csn);
        await this.storage.getSystemDb().updateQueryHash();
      }
      this.storage.setReady(withJournal ? subsetTransaction : undefined);
    } catch (e) {
      this.logger.error(LOG_MESSAGES.ERROR_MSG, [e.toString()], [206]);
      throw e;
    }
  }

  private async resetData0(): Promise<void> {
    this.logger.warn(LOG_MESSAGES.RESET_DATA0);
    this.storage.pause();
    try {
      await Promise.resolve();
      await this.storage.getJournalDb().deleteAll();
      await this.storage.getSubsetDb().deleteAll();
      await this.storage.getSystemDb().updateCsn(0);
      await this.storage.getSystemDb().updateQueryHash();
      this.storage.committedCsn = undefined;
      this.storage.setReady();
    } catch (e) {
      this.logger.error(LOG_MESSAGES.ERROR_MSG, [e.toString()], [207]);
      throw e;
    }
  }
}

/**
 * 更新リスナー(UpdateListener)
 *
 * 更新リスナーは、コンテキストマネージャが発信する更新情報（トランザクションオブジェクト）を受信して更新トランザクションをサブセットへのトランザクションに変換し更新レシーバに転送する。
 */
class UpdateListener extends Subscriber {

  private logger: Logger;

  constructor(
    protected storage: SubsetStorage,
    protected database: string,
    protected subsetDefinition: SubsetDef,
    protected updateProcessor: UpdateProcessor,
    protected exported: boolean,
  ) {
    super();
    this.logger = Logger.getLogger("UpdateListener", storage.getDbName());
    this.logger.debug(LOG_MESSAGES.CREATED, ["UpdateListener"]);
  }

  onReceive(transctionJSON: string) {
    const transaction = EJSON.parse(transctionJSON) as TransactionObject;
    this.logger.info(LOG_MESSAGES.RECEIVED_TYPE_CSN, [transaction.type], [transaction.csn]);
    const subsetTransaction = UpdateListener.convertTransactionForSubset(this.subsetDefinition, transaction);
    if (this.exported) {
      this.storage.getNode().publish(CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.storage.getOption().subset), EJSON.stringify(subsetTransaction)
      )
        .catch((err) => {
          this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [215]);
          if (process) { process.exit(1); }
        });
    }
    this.updateProcessor.procTransaction(subsetTransaction);
  }

  static convertTransactionForSubset(subsetDefinition: SubsetDef, transaction: TransactionObject): TransactionObject {
    // サブセット用のトランザクション内容に変換

    if (transaction.type === TransactionType.FORCE_ROLLBACK ||
      transaction.type === TransactionType.TRUNCATE ||
      transaction.type === TransactionType.BEGIN ||
      transaction.type === TransactionType.END ||
      transaction.type === TransactionType.ABORT ||
      transaction.type === TransactionType.BEGIN_IMPORT ||
      transaction.type === TransactionType.END_IMPORT ||
      transaction.type === TransactionType.ABORT_IMPORT ||
      transaction.type === TransactionType.BEGIN_RESTORE ||
      transaction.type === TransactionType.END_RESTORE ||
      transaction.type === TransactionType.ABORT_RESTORE ||
      transaction.type === TransactionType.NONE) {
      return transaction;
    }
    if (!subsetDefinition.query) { return transaction; }
    const query = parser.parse(subsetDefinition.query);

    if (!transaction.before && transaction.new) {
      const newObj = TransactionRequest.getNew(transaction);
      if (query.matches(newObj, false)) {
        // insert to inner -> INSERT
        return transaction;
      } else {
        // insert to outer -> NONE
        return { ...transaction, type: TransactionType.NONE, new: undefined };
      }
    }

    if (transaction.type === TransactionType.DELETE) {
      const before = TransactionRequest.getBefore(transaction);
      if (query.matches(before, false)) {
        // delete from inner -> DELETE
        return transaction;
      } else {
        // delete from out -> NONE
        return { ...transaction, type: TransactionType.NONE, before: undefined };
      }
    }

    if (transaction.before) {
      const updateObj = TransactionRequest.applyOperator(transaction);
      const before = TransactionRequest.getBefore(transaction);
      if (query.matches(before, false)) {
        if (query.matches(updateObj, false)) {
          // update from inner to inner -> UPDATE
          return transaction;
        } else {
          // update from inner to outer -> DELETE
          return { ...transaction, type: TransactionType.DELETE, operator: undefined };
        }
      } else {
        if (query.matches(updateObj, false)) {
          // update from outer to inner -> INSERT
          return { ...transaction, type: TransactionType.INSERT, new: updateObj, before: undefined, operator: undefined };
        } else {
          // update from outer to outer -> NONE
          return { ...transaction, type: TransactionType.NONE, before: undefined, operator: undefined };
        }
      }
    }

    throw new Error("Bad transaction data:" + JSON.stringify(transaction));
  }
}

class SubsetUpdatorProxy extends Proxy {

  private logger: Logger;

  constructor(protected storage: SubsetStorage) {
    super();
    this.logger = Logger.getLogger("SubsetUpdatorProxy", storage.getDbName());
    this.logger.debug(LOG_MESSAGES.CREATED, ["SubsetUpdatorProxy"]);
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) { throw new Error("url is required."); }
    if (!req.method) { throw new Error("method is required."); }
    const url = URL.parse(req.url);
    if (url.pathname == null) { throw new Error("pathname is required."); }
    const method = req.method.toUpperCase();
    this.logger.debug(LOG_MESSAGES.ON_RECEIVE, [method, url.pathname]);
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTION) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (request) => {
        const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTION, [], [csn]);
        return this.getTransactionJournal(csn);
      });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTION_OLD) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (request) => {
        const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTION, [], [csn]);
        return this.getTransactionJournal(csn);
      });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTIONS) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (request) => {
        const fromCsn = ProxyHelper.validateNumberRequired(request.fromCsn, "fromCsn");
        const toCsn = ProxyHelper.validateNumberRequired(request.toCsn, "toCsn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTIONS, [], [fromCsn, toCsn]);
        return this.getTransactionJournals(fromCsn, toCsn);
      });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTIONS_OLD) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (request) => {
        const fromCsn = ProxyHelper.validateNumberRequired(request.fromCsn, "fromCsn");
        const toCsn = ProxyHelper.validateNumberRequired(request.toCsn, "toCsn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTIONS, [], [fromCsn, toCsn]);
        return this.getTransactionJournals(fromCsn, toCsn);
      });
    } else {
      this.logger.warn(LOG_MESSAGES.SERVER_COMMAND_NOT_FOUND, [method, url.pathname]);
      return ProxyHelper.procError(req, res);
    }
  }

  getTransactionJournal(csn: number): Promise<object> {
    return this.storage.getJournalDb().findByCsn(csn)
      .then((journal) => {
        if (journal) {
          delete (journal as any)._id;
          return {
            status: "OK",
            journal: EJSON.serialize(journal),
          };
        } else {
          return {
            status: "NG",
          };
        }
      });
  }

  getTransactionJournals(fromCsn: number, toCsn: number): Promise<object> {
    const loopData = {
      csn: fromCsn,
      size: 0,
    };
    const journals: string[] = [];
    return Util.promiseWhile<{ csn: number, size: number }>(
      loopData,
      (loopData) => {
        return loopData.csn <= toCsn && loopData.size <= MAX_RESPONSE_SIZE_OF_JOURNALS;
      },
      (loopData) => {
        return this.storage.getJournalDb().findByCsn(loopData.csn)
          .then((journal) => {
            if (!journal) { throw new Error("journal not found: " + loopData.csn); }
            delete (journal as any)._id;
            const journalStr = EJSON.stringify(journal);
            journals.push(journalStr);
            return { csn: loopData.csn + 1, size: loopData.size + journalStr.length };
          });
      },
    )
      .then(() => ({
        status: "OK",
        journals,
      }));
  }
}

/**
 * サブセットストレージコンフィグレーションパラメータ
 */
export class SubsetStorageConfigDef {

  /**
   * データベース名
   */
  readonly database: string;

  /**
   * サブセット名
   */
  readonly subset: string;

  /**
   * 上位サブセット名
   */
  readonly subscribe?: string;

  /**
   * true の場合はクエリハンドラを "loadBalancing" モードで登録し、外部にサービスを公開する。 false の場合はクエリハンドラを "localOnly" モードで登録する
   */
  readonly exported?: boolean;

  /**
   * サブセットストレージのタイプで persistent か cache のいずれかである
   */
  readonly type: "persistent" | "cache";

  /**
   * エクスポート処理での一回あたりの最大処理数
   */
  readonly exportMaxLines?: number;
}

/**
 * サブセットストレージ(SubsetStorage)
 *
 * サブセットストレージは、クエリハンドラと更新レシーバの機能を提供するサービスエンジンである。
 */
export class SubsetStorage extends ServiceEngine implements Proxy {

  public bootOrder = 50;
  public committedCsn?: number;
  private logger: Logger;
  private option: SubsetStorageConfigDef;
  private node: ResourceNode;
  private database: string;
  private subsetName: string;
  private subsetDefinition: SubsetDef;
  private type: string;
  private subsetDb: SubsetDb;
  private journalDb: JournalDb;
  private systemDb: SystemDb;
  private mountHandle: string;
  private mounted = false;
  private lock: ReadWriteLock;
  private queryWaitingList: { [csn: number]: (() => Promise<any>)[] } = {};
  private subscriberKey: string | null;
  private readyFlag: boolean = false;
  private readyQueue: any[] = [];
  private updateProcessor: UpdateProcessor;
  private updateListener: UpdateListener;
  private updateListenerKey?: string;
  private updateListenerMountHandle?: string;
  public notifyListener?: { procNotify: (transaction: TransactionObject) => void };

  constructor(option: SubsetStorageConfigDef) {
    super(option);
    if (typeof option.exported === "undefined") {
      option = { ...option, exported: true };
    }
    this.option = option;
    this.database = option.database;
    this.subsetName = option.subset;
    this.logger = Logger.getLogger("SubsetStorage", this.getDbName());
    this.logger.debug(LOG_MESSAGES.CREATED, ["SubsetStorage"]);
    this.lock = new ReadWriteLock();
  }

  getOption(): SubsetStorageConfigDef {
    return this.option;
  }

  getNode(): ResourceNode {
    return this.node;
  }

  getSubsetDb(): SubsetDb {
    return this.subsetDb;
  }

  getJournalDb(): JournalDb {
    return this.journalDb;
  }

  getSystemDb(): SystemDb {
    return this.systemDb;
  }

  getLock(): ReadWriteLock {
    return this.lock;
  }

  getDbName(): string {
    return this.database + SPLIT_IN_SUBSET_DB + this.subsetName;
  }

  getType(): string {
    return this.type;
  }

  getReady(): boolean {
    return this.readyFlag;
  }

  waitReady(): Promise<void> {
    if (this.readyFlag) {
      return Promise.resolve();
    } else {
      return new Promise<void>((resolve, reject) => {
        this.readyQueue.push(resolve);
      });
    }
  }

  isWhole(): boolean {
    return !this.subsetDefinition.query;
  }

  pause(): void {
    this.readyFlag = false;
  }

  setReady(transaction?: TransactionObject): void {
    if (transaction) {
      this.committedCsn = transaction.committedCsn;
    }
    if (this.readyFlag) {
      return;
    }
    this.readyFlag = true;
    setTimeout(() => {
      while (this.readyQueue.length) {
        this.readyQueue.pop()();
      }
    });

    if (!this.mounted) {
      // Rest サービスを登録する。
      this.mounted = true;
      const mountingMode = this.option.exported ? "loadBalancing" : "localOnly";
      this.logger.info(LOG_MESSAGES.MOUNTING_MODE, [mountingMode]);
      this.node.mount(CORE_NODE.PATH_SUBSET
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), mountingMode, this)
        .then((value) => {
          this.mountHandle = value;
        })
        .catch((err) => {
          this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [210]);
          if (process) { process.exit(1); }
        });
    }
  }

  pullQueryWaitingList(csn: number): (() => Promise<any>)[] {
    const list = this.queryWaitingList[csn];
    if (list) {
      delete this.queryWaitingList[csn];
      return list;
    }
    return [];
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug(LOG_MESSAGES.STARTING, ["SubsetStorage"]);

    if (!this.database) {
      throw new DadgetError(ERROR.E2401, ["Database name is missing."]);
    }
    if (this.database.match(/--/)) {
      throw new DadgetError(ERROR.E2401, ["Database name can not contain '--'."]);
    }

    if (!this.subsetName) {
      throw new DadgetError(ERROR.E2401, ["Subset name is missing."]);
    }
    if (this.subsetName.match(/--/)) {
      throw new DadgetError(ERROR.E2401, ["Subset name can not contain '--'."]);
    }

    // サブセットの定義を取得する
    const seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length !== 1) {
      throw new DadgetError(ERROR.E2401, ["DatabaseRegistry is missing, or there are multiple ones."]);
    }
    const registry = seList[0] as DatabaseRegistry;
    const metaData = registry.getMetadata();
    this.subsetDefinition = metaData.subsets[this.subsetName];
    const queryHash = hash.MD5(this.subsetDefinition.query || {});

    this.type = this.option.type.toLowerCase();
    if (this.type !== "persistent" && this.type !== "cache") {
      throw new DadgetError(ERROR.E2401, [`SubsetStorage type ${this.type} is not supported.`]);
    }

    // ストレージを準備
    const dbName = this.getDbName();
    if (this.type === "cache") {
      this.subsetDb = new SubsetDb(new CacheDb(dbName), this.subsetName, metaData.indexes || []);
      this.journalDb = new JournalDb(new CacheDb(dbName));
      this.systemDb = new SystemDb(new CacheDb(dbName));
    } else if (this.type === "persistent") {
      this.subsetDb = new SubsetDb(new PersistentDb(dbName), this.subsetName, metaData.indexes || []);
      this.journalDb = new JournalDb(new PersistentDb(dbName));
      this.systemDb = new SystemDb(new PersistentDb(dbName));
    }

    this.updateProcessor = new UpdateProcessor(this, this.database, this.subsetName, this.subsetDefinition);
    this.updateListener = new UpdateListener(this, this.database, this.subsetDefinition, this.updateProcessor, !!this.option.exported);
    let promise = this.subsetDb.start();
    promise = promise.then(() => this.journalDb.start());
    promise = promise.then(() => this.systemDb.start());
    promise = promise.then(() => {
      return this.systemDb.checkQueryHash(queryHash)
        .then((result) => {
          if (result) {
            this.logger.warn(LOG_MESSAGES.SUBSET_QUERY_WARN);
          }
        });
    });
    if (this.option.exported) {
      promise = promise.then(() => this.connectUpdateListener());
      promise = promise.then(() => this.subscribeUpdateProcessor());
    } else {
      // Local update mode
      promise = promise.then(() => this.subscribeUpdateListener());
    }
    promise = promise.then(() => new Promise<void>((resolve) => setTimeout(resolve, 1)));
    promise = promise.then(() => { this.logger.debug(LOG_MESSAGES.STARTED, ["SubsetStorage"]); });
    return promise;
  }

  connectUpdateListener() {
    this.node.mount(CORE_NODE.PATH_SUBSET_UPDATOR
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName), "singletonMaster", new SubsetUpdatorProxy(this), {
      onDisconnect: () => {
        this.logger.error(LOG_MESSAGES.DISCONNECTED, ["Updator"]);
        if (process) { process.exit(1); }
      },
      onRemount: (mountHandle: string) => {
        this.logger.info(LOG_MESSAGES.REMOUNTED, ["Updator"]);
        this.updateListenerMountHandle = mountHandle;
        this.subscribeUpdateListener()
          .then(() => {
            if (this.subscriberKey) { this.node.unsubscribe(this.subscriberKey).catch(e => console.warn(e)); }
          });
      },
    })
      .then((mountHandle) => {
        // マスターを取得した場合のみ実行される
        this.logger.info(LOG_MESSAGES.CONNECTED, ["Updator"]);
        if (this.updateListenerMountHandle === "stopped") {
          setTimeout(() => { this.node.unmount(mountHandle).catch(e => console.warn(e)); }, 1);
          return;
        }
        this.updateListenerMountHandle = mountHandle;
        this.subscribeUpdateListener()
          .then(() => {
            if (this.subscriberKey) { this.node.unsubscribe(this.subscriberKey).catch(e => console.warn(e)); }
          });
      })
      .catch((err) => {
        this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [211]);
        if (process) { process.exit(1); }
      });
  }

  subscribeUpdateListener(): Promise<void> {
    if (this.option.subscribe) {
      return this.node.subscribe(
        CORE_NODE.PATH_SUBSET_TRANSACTION
          .replace(/:database\b/g, this.database)
          .replace(/:subset\b/g, this.option.subscribe), this.updateListener)
        .then((key) => { this.updateListenerKey = key; })
        .catch((err) => {
          this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [212]);
          if (process) { process.exit(1); }
        });
    } else {
      return this.node.subscribe(
        CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), this.updateListener)
        .then((key) => { this.updateListenerKey = key; })
        .catch((err) => {
          this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [213]);
          if (process) { process.exit(1); }
        });
    }
  }

  subscribeUpdateProcessor(): Promise<void> {
    return this.node.subscribe(
      CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), this.updateProcessor)
      .then((key) => { this.subscriberKey = key; })
      .catch((err) => {
        this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [214]);
        if (process) { process.exit(1); }
      });
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
      .then(() => {
        if (this.updateListenerMountHandle) { return this.node.unmount(this.updateListenerMountHandle).catch(e => console.warn(e)); }
        this.updateListenerMountHandle = "stopped";
      })
      .then(() => {
        if (this.updateListenerKey) { return this.node.unsubscribe(this.updateListenerKey).catch(e => console.warn(e)); }
      })
      .then(() => {
        if (this.mountHandle) { return this.node.unmount(this.mountHandle).catch(e => console.warn(e)); }
      })
      .then(() => {
        if (this.subscriberKey) { return this.node.unsubscribe(this.subscriberKey).catch(e => console.warn(e)); }
      });
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) { throw new Error("url is required."); }
    if (!req.method) { throw new Error("method is required."); }
    const url = URL.parse(req.url);
    if (url.pathname == null) { throw new Error("pathname is required."); }
    const method = req.method.toUpperCase();
    this.logger.debug(LOG_MESSAGES.ON_RECEIVE, [method, url.pathname]);
    const procQuery = (request: any) => {
      const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
      const query = EJSON.parse(request.query);
      const sort = request.sort ? EJSON.parse(request.sort) : undefined;
      const limit = ProxyHelper.validateNumber(request.limit, "limit");
      const offset = ProxyHelper.validateNumber(request.offset, "offset");
      const projection = request.projection ? EJSON.parse(request.projection) : undefined;
      if (request.version && Number(request.version) > CLIENT_VERSION) throw new DadgetError(ERROR.E3002);
      return this.query(csn, query, sort, limit, request.csnMode, projection, offset)
        .then((result) => {
          let total = 0;
          let count = 0;
          let length = result.resultSet.length;
          for (const obj of result.resultSet) {
            total += JSON.stringify(obj).length + 1;
            count += 1;
            if (limit === EXPORT_LIMIT_NUM) {
              if (total > MAX_STRING_LENGTH) {
                result.resultSet = result.resultSet.slice(0, Math.max(1, count - 1));
                return { status: "OK", result };
              }
            } else {
              if ((total / count) * length > MAX_STRING_LENGTH) {
                this.logger.warn(LOG_MESSAGES.TOO_LARGE_RESPONSE, [JSON.stringify(query)]);
                let ids: any[] = [];
                for (const obj of result.resultSet) {
                  ids.push({ _id: (obj as any)._id });
                }
                result.resultSet = ids;
                return { status: "HUGE", result };
              }
            }
          }
          return { status: "OK", result };
        });
    };
    const procCount = (request: any) => {
      const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
      const query = EJSON.parse(request.query);
      return this.count(csn, query, request.csnMode)
        .then((result) => {
          return { status: "OK", result };
        });
    };
    const procWait = (request: any) => {
      const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
      return this.wait(csn)
        .then((_) => {
          return { status: "OK" };
        });
    };
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_QUERY) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, procQuery);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_QUERY_OLD) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, procQuery);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_COUNT) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, procCount);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_COUNT_OLD) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, procCount);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_WAIT) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, procWait);
    } else {
      this.logger.warn(LOG_MESSAGES.SERVER_COMMAND_NOT_FOUND, [method, url.pathname]);
      return ProxyHelper.procError(req, res);
    }
  }

  count(csn: number, query: object, csnMode?: CsnMode): Promise<CountResult> {
    if (!this.readyFlag) {
      return Promise.resolve({ csn, resultCount: 0, restQuery: query, csnMode });
    }
    const innerQuery = LogicalOperator.getInsideOfCache(query, this.subsetDefinition.query);
    if (!innerQuery) {
      return Promise.resolve({ csn, resultCount: 0, restQuery: query, csnMode });
    }
    const restQuery = LogicalOperator.getOutsideOfCache(query, this.subsetDefinition.query);
    this.logger.info(LOG_MESSAGES.COUNT_CSN, [csnMode || ""], [csn]);
    this.logger.info(LOG_MESSAGES.COUNT, [JSON.stringify(query)]);
    let release: () => void;
    const promise = new Promise<void>((resolve, reject) => {
      this.getLock().readLock((unlock) => {
        this.logger.debug(LOG_MESSAGES.GET_READLOCK);
        release = () => {
          this.logger.debug(LOG_MESSAGES.RELEASE_READLOCK);
          unlock();
        };
        resolve();
      });
    });

    const protectedCsn = this.getJournalDb().getProtectedCsn();

    return promise
      .then(() => this.getSystemDb().getCsn())
      .then((currentCsn) => {
        if (csn === 0 || (csn < currentCsn && csnMode === "latest")) {
          csn = Math.max(csn, this.committedCsn || currentCsn);
        }
        if (csn === currentCsn) {
          return this.getSubsetDb().count(innerQuery, true)
            .then((result) => {
              release();
              return { csn: currentCsn, resultCount: result, restQuery };
            });
        } else if (csn < protectedCsn) {
          throw new DadgetError(ERROR.E2402, [csn, protectedCsn]);
        } else if (csn < currentCsn) {
          this.logger.info(LOG_MESSAGES.ROLLBACK_TRANSACTIONS2, [], [csn, currentCsn]);
          // rollback transactions
          return this.getJournalDb().findByCsnRange(csn + 1, currentCsn)
            .then((transactions) => {
              if (transactions.length !== currentCsn - csn) {
                release();
                this.logger.info(LOG_MESSAGES.INSUFFICIENT_ROLLBACK_TRANSACTIONS);
                return { csn, resultCount: 0, restQuery: query };
              }
              return this.getSubsetDb().count({ $and: [innerQuery, { csn: { $lte: csn } }] }, true)
                .then((result) => {
                  release();
                  const resultSet = SubsetStorage.rollbackAndFind([], transactions, innerQuery);
                  console.log("rollback count:", result, resultSet.length);
                  return { csn, resultCount: result + resultSet.length, restQuery };
                });
            });
        } else {
          this.logger.warn(LOG_MESSAGES.WAIT_FOR_TRANSACTIONS, [], [csn, currentCsn]);
          // wait for transaction journals
          this.updateProcessor.proceedTransaction(csn);
          return new Promise<CountResult>((resolve, reject) => {
            if (!this.queryWaitingList[csn]) { this.queryWaitingList[csn] = []; }
            this.queryWaitingList[csn].push(() => {
              return this.getSubsetDb().count(innerQuery, true)
                .then((result) => {
                  resolve({ csn, resultCount: result, restQuery });
                }).catch((reason) => reject(reason));
            });
            release();
          });
        }
      }).catch((e) => {
        this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [208]);
        release();
        return Promise.reject(e);
      });
  }

  query(csn: number, query: object, sort?: object, limit?: number, csnMode?: CsnMode, projection?: object, offset?: number): Promise<QueryResult> {
    if (!this.readyFlag) {
      return Promise.resolve({ csn, resultSet: [], restQuery: query, csnMode });
    }
    const innerQuery = LogicalOperator.getInsideOfCache(query, this.subsetDefinition.query);
    if (!innerQuery) {
      return Promise.resolve({ csn, resultSet: [], restQuery: query, csnMode });
    }
    const restQuery = LogicalOperator.getOutsideOfCache(query, this.subsetDefinition.query);
    if (restQuery && offset) {
      return Promise.resolve({ csn, resultSet: [], restQuery: query, csnMode });
    }
    if (limit && limit < 0) { limit = this.option.exportMaxLines; }
    this.logger.info(LOG_MESSAGES.QUERY_CSN, [csnMode || ""], [csn]);
    this.logger.info(LOG_MESSAGES.QUERY, [JSON.stringify(query)]);
    let release: () => void;
    const promise = new Promise<void>((resolve, reject) => {
      this.getLock().readLock((unlock) => {
        this.logger.debug(LOG_MESSAGES.GET_READLOCK);
        release = () => {
          this.logger.debug(LOG_MESSAGES.RELEASE_READLOCK);
          unlock();
        };
        resolve();
      });
    });

    const protectedCsn = this.getJournalDb().getProtectedCsn();

    return promise
      .then(() => this.getSystemDb().getCsn())
      .then((currentCsn) => {
        if (csn === 0 || (csn < currentCsn && csnMode === "latest")) {
          csn = Math.max(csn, this.committedCsn || currentCsn);
        }
        if (csn === currentCsn) {
          return this.getSubsetDb().find(innerQuery, sort, limit, projection, offset, true)
            .then((result) => {
              release();
              return { csn: currentCsn, resultSet: result, restQuery };
            });
        } else if (csn < protectedCsn) {
          throw new DadgetError(ERROR.E2402, [csn, protectedCsn]);
        } else if (csn < currentCsn) {
          this.logger.info(LOG_MESSAGES.ROLLBACK_TRANSACTIONS2, [], [csn, currentCsn]);
          // rollback transactions
          return this.getJournalDb().findByCsnRange(csn + 1, currentCsn)
            .then((transactions) => {
              if (transactions.length !== currentCsn - csn) {
                release();
                this.logger.info(LOG_MESSAGES.INSUFFICIENT_ROLLBACK_TRANSACTIONS);
                return { csn, resultSet: [], restQuery: query };
              }
              const _offset = offset ? offset : 0;
              const maxLimit = limit ? limit + _offset : undefined;
              const possibleLimit = maxLimit ? maxLimit + transactions.length : undefined;
              return this.getSubsetDb().find(innerQuery, sort, possibleLimit, undefined, undefined, true)
                .then((result) => {
                  release();
                  const resultSet = SubsetStorage.rollbackAndFind(result, transactions, innerQuery, sort, limit, offset)
                    .map((val) => Util.project(val, projection));
                  return { csn, resultSet, restQuery };
                });
            });
        } else {
          this.logger.warn(LOG_MESSAGES.WAIT_FOR_TRANSACTIONS, [], [csn, currentCsn]);
          // wait for transaction journals
          this.updateProcessor.proceedTransaction(csn);
          return new Promise<QueryResult>((resolve, reject) => {
            if (!this.queryWaitingList[csn]) { this.queryWaitingList[csn] = []; }
            this.queryWaitingList[csn].push(() => {
              return this.getSubsetDb().find(innerQuery, sort, limit, projection, offset, true)
                .then((result) => {
                  resolve({ csn, resultSet: result, restQuery });
                }).catch((reason) => reject(reason));
            });
            release();
          });
        }
      }).catch((e) => {
        this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [209]);
        release();
        return Promise.reject(e);
      });
  }

  wait(csn: number): Promise<void> {
    return this.getSystemDb().getCsn()
      .then((currentCsn) => {
        if (csn <= currentCsn) {
          return;
        } else {
          this.logger.warn(LOG_MESSAGES.WAIT_FOR_TRANSACTIONS, [], [csn, currentCsn]);
          // wait for transaction journals
          setTimeout(() => {
            this.updateProcessor.proceedTransaction(csn);
          }, 5000);
          return new Promise<void>((resolve, reject) => {
            if (!this.queryWaitingList[csn]) { this.queryWaitingList[csn] = []; }
            this.queryWaitingList[csn].push(() => {
              resolve();
              return Promise.resolve();
            });
          });
        }
      }).catch((e) => {
        this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [217]);
        return Promise.reject(e);
      });
  }

  static rollbackAndFind(orgList: any[], transactions: TransactionObject[], query: object, sort?: object, limit?: number, offset?: number): any[] {
    transactions.sort((a, b) => b.csn - a.csn);
    console.log("rollbackAndFind");

    const dataMap: any = {};
    orgList.forEach((val) => {
      dataMap[val._id] = val;
    });

    let committedCsn: number | undefined;
    transactions.forEach((trans) => {
      if (committedCsn !== undefined && committedCsn < trans.csn) { return; }
      if (trans.type === TransactionType.ABORT || trans.type === TransactionType.ABORT_IMPORT) {
        if (trans.committedCsn === undefined) { throw new Error("committedCsn required"); }
        committedCsn = trans.committedCsn;
        return;
      }
      if (trans.before) {
        dataMap[trans.target] = TransactionRequest.getBefore(trans);
      } else if (trans.type === TransactionType.INSERT || trans.type === TransactionType.RESTORE) {
        delete dataMap[trans.target];
      }
    });

    const dataList = [];
    for (const _id of Object.keys(dataMap)) {
      dataList.push(dataMap[_id]);
    }
    let list = Util.mongoSearch(dataList, query, sort) as object[];
    if (offset) {
      if (limit && limit > 0) {
        list = list.slice(offset, offset + limit);
      } else {
        list = list.slice(offset);
      }
    } else {
      if (limit && limit > 0) {
        list = list.slice(0, limit);
      }
    }
    return list;
  }
}
