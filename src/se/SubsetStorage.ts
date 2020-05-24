import * as http from "http";
import * as parser from "mongo-parse";
import * as hash from "object-hash";
import * as ReadWriteLock from "rwlock";
import * as URL from "url";
import * as EJSON from "../util/Ejson";

import { Proxy, ResourceNode, ServiceEngine, Subscriber } from "@chip-in/resource-node";
import { CORE_NODE } from "../Config";
import { CacheDb } from "../db/container/CacheDb";
import { PersistentDb } from "../db/container/PersistentDb";
import { JournalDb } from "../db/JournalDb";
import { SubsetDb } from "../db/SubsetDb";
import { SystemDb } from "../db/SystemDb";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import { LogicalOperator } from "../util/LogicalOperator";
import { ProxyHelper } from "../util/ProxyHelper";
import { Util } from "../util/Util";
import { CountResult, CsnMode, default as Dadget, QueryResult } from "./Dadget";
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry";

const MAX_RESPONSE_SIZE_OF_JOURNALS = 10485760;

class UpdateProcessor extends Subscriber {

  private lock: ReadWriteLock;

  constructor(
    protected storage: SubsetStorage,
    protected database: string,
    protected subsetName: string,
    protected subsetDefinition: SubsetDef) {

    super();
    this.logger.category = "UpdateProcessor";
    this.lock = new ReadWriteLock();
  }

  onReceive(msg: string) {
    const transaction = EJSON.parse(msg) as TransactionObject;
    console.log("UpdateProcessor received: ", this.storage.getOption().subset, transaction.csn);
    this.procTransaction(transaction);
  }

  procTransaction(transaction: TransactionObject) {
    this.logger.info("procTransaction:", transaction.type, transaction.csn);

    if (transaction.protectedCsn) {
      if (this.storage.getJournalDb().getProtectedCsn() < transaction.protectedCsn) {
        this.logger.info("CHECKPOINT protectedCsn: " + transaction.protectedCsn);
        this.storage.getJournalDb().setProtectedCsn(transaction.protectedCsn);
        this.storage.getJournalDb().deleteBeforeCsn(transaction.protectedCsn)
          .catch((err) => {
            this.logger.error(err.toString());
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
                      this.storage.setReady();
                      release2();
                      release1();
                      return;
                    } else {
                      return this.adjustData(transaction.csn)
                        .then(() => { release2(); })
                        .then(() => { release1(); });
                    }
                  });
              } else {
                return this.adjustData(transaction.csn)
                  .then(() => { release2(); })
                  .then(() => { release1(); });
              }
            } else if (csn > transaction.csn && transaction.type === TransactionType.ROLLBACK) {
              return this.fetchJournal(transaction.csn)
                .then((fetchJournal) => {
                  if (!fetchJournal) { throw new Error("can not rollback because journal isn't found: " + transaction.csn); }
                  return this.rollbackSubsetDb(transaction.csn)
                    .catch((e) => {
                      this.logger.warn(e.toString());
                      return this.resetData(transaction.csn);
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
                  this.logger.info("do queued query: " + csn);
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
              promise = this.updateSubsetDb(promise, transaction);
              promise = promise.then(() => doQueuedQuery(transaction.csn));
              return promise.then(() => {
                release2();
                release1();
              });
            }
          })
          .catch((e) => {
            this.logger.error(e.toString());
            release2();
            release1();
          });
      });
    });
  }

  private updateSubsetDb(promise: Promise<void>, transaction: TransactionObject): Promise<void> {
    if (transaction.csn > 1) {
      promise = promise.then(() => this.storage.getJournalDb().findByCsn(transaction.csn - 1))
        .then((journal) => {
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
        });
    }

    this.logger.info("update subset db csn:", transaction.csn);
    if (transaction.type === TransactionType.INSERT && transaction.new) {
      const obj = Object.assign({ _id: transaction.target, csn: transaction.csn }, transaction.new);
      promise = promise.then(() => this.storage.getSubsetDb().insert(obj));
    } else if (transaction.type === TransactionType.UPDATE && transaction.before) {
      const updateObj = TransactionRequest.applyOperator(transaction);
      promise = promise.then(() => this.storage.getSubsetDb().update(transaction.target, updateObj));
    } else if (transaction.type === TransactionType.DELETE && transaction.before) {
      promise = promise.then(() => this.storage.getSubsetDb().deleteById(transaction.target));
    } else if (transaction.type === TransactionType.NONE) {
    }
    promise = promise.then(() => this.storage.getJournalDb().insert(transaction));
    promise = promise.then(() => this.storage.getSystemDb().updateCsn(transaction.csn));
    return promise;
  }

  private rollbackSubsetDb(csn: number): Promise<void> {
    this.logger.warn("ROLLBACK transactions, csn:", csn);
    return this.storage.getJournalDb().findByCsnRange(csn, Number.MAX_VALUE)
      .then((transactions) => {
        transactions.sort((a, b) => b.csn - a.csn);
        if (transactions.length === 0 || transactions[transactions.length - 1].csn !== csn) {
          throw new Error("Lack of transactions");
        }
        let promise = Promise.resolve();
        transactions.forEach((trans) => {
          if (trans.csn === csn) { return; }
          if (trans.type === TransactionType.INSERT) {
            promise = promise.then(() => this.storage.getSubsetDb().deleteById(trans.target));
          } else if (trans.type === TransactionType.UPDATE && trans.before) {
            promise = promise.then(() => this.storage.getSubsetDb().update(trans.target, trans.before as object));
          } else if (trans.type === TransactionType.DELETE && trans.before) {
            promise = promise.then(() => this.storage.getSubsetDb().insert(trans.before as object));
          }
          promise = promise.then(() => this.storage.getJournalDb().deleteAfterCsn(trans.csn - 1));
          promise = promise.then(() => this.storage.getSystemDb().updateCsn(trans.csn - 1));
        });
        return promise;
      });
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
    this.logger.warn("adjustData:" + csn);
    return Promise.resolve()
      .then(() => {
        if (this.storage.getSystemDb().isNew()) {
          return this.resetData(csn);
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
                        const nextCsn = loopData.csn - 1;
                        return this.rollbackSubsetDb(nextCsn)
                          .then(() => this.storage.getJournalDb().findByCsn(nextCsn))
                          .then((journal) => {
                            if (!journal) { throw new Error("journal not found: " + nextCsn); }
                            return this.fetchJournal(journal.csn, journals)
                              .then((fetchJournal) => {
                                if (fetchJournal && fetchJournal.digest === journal.digest) {
                                  return { ...loopData, csn: 0 };
                                } else {
                                  return { ...loopData, csn: nextCsn };
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
                      return this.updateSubsetDb(Promise.resolve(), subsetTransaction);
                    };
                    if (journal.csn + 1 === csn) {
                      return this.fetchJournal(csn, journals)
                        .then(callback)
                        .then(() => { this.storage.setReady(); });
                    } else {
                      return this.fetchJournals(journal.csn + 1, csn, callback)
                        .then(() => { this.storage.setReady(); });
                    }
                  }
                });
            })
            .catch((e) => {
              this.logger.warn(e.toString());
              return this.resetData(csn);
            });
        }
      });
  }

  private resetData(csn: number): Promise<void> {
    if (csn === 0) { return this.resetData0(); }
    this.logger.warn("resetData:", csn);
    const query = this.subsetDefinition.query ? this.subsetDefinition.query : {};
    return this.fetchJournal(csn)
      .then((fetchJournal) => {
        if (!fetchJournal) { throw new Error("journal not found: " + csn); }
        const subsetTransaction = UpdateListener.convertTransactionForSubset(this.subsetDefinition, fetchJournal);
        return Dadget._query(this.storage.getNode(), this.database, query, undefined, undefined, undefined, csn, "strict")
          .then((result) => {
            if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
            return Promise.resolve()
              .then(() => this.storage.getJournalDb().deleteAll())
              .then(() => this.storage.getJournalDb().insert(subsetTransaction))
              .then(() => this.storage.getSubsetDb().deleteAll())
              .then(() => this.storage.getSubsetDb().insertMany(result.resultSet))
              .then(() => this.storage.getSystemDb().updateCsn(result.csn))
              .then(() => this.storage.getSystemDb().updateQueryHash())
              .then(() => {
                this.storage.setReady();
              });
          });
      })
      .catch((e) => {
        this.logger.error("Error:", e.toString());
        throw e;
      });
  }

  private resetData0(): Promise<void> {
    this.logger.warn("resetData0");
    return Promise.resolve()
      .then(() => this.storage.getJournalDb().deleteAll())
      .then(() => this.storage.getSubsetDb().deleteAll())
      .then(() => this.storage.getSystemDb().updateCsn(0))
      .then(() => this.storage.getSystemDb().updateQueryHash())
      .then(() => {
        this.storage.setReady();
      })
      .catch((e) => {
        this.logger.error("Error:", e.toString());
        throw e;
      });
  }
}

/**
 * 更新マネージャ(UpdateManager)
 *
 * 更新マネージャは、コンテキストマネージャが発信する更新情報（トランザクションオブジェクト）を受信して更新トランザクションをサブセットへのトランザクションに変換し更新レシーバに転送する。
 */
class UpdateListener extends Subscriber {

  constructor(
    protected storage: SubsetStorage,
    protected database: string,
    protected subsetDefinition: SubsetDef,
    protected updateProcessor: UpdateProcessor,
    protected exported: boolean,
  ) {
    super();
    this.logger.category = "UpdateListener";
    this.logger.debug("UpdateListener is created");
  }

  onReceive(transctionJSON: string) {
    const transaction = EJSON.parse(transctionJSON) as TransactionObject;
    this.logger.info("received:", transaction.type, transaction.csn);
    const subsetTransaction = UpdateListener.convertTransactionForSubset(this.subsetDefinition, transaction);
    if (this.exported) {
      this.storage.getNode().publish(CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.storage.getOption().subset), EJSON.stringify(subsetTransaction));
    }
    this.updateProcessor.procTransaction(subsetTransaction);
  }

  static convertTransactionForSubset(subsetDefinition: SubsetDef, transaction: TransactionObject): TransactionObject {
    // サブセット用のトランザクション内容に変換

    if (transaction.type === TransactionType.ROLLBACK || transaction.type === TransactionType.NONE) {
      return transaction;
    }
    if (!subsetDefinition.query) { return transaction; }
    const query = parser.parse(subsetDefinition.query);

    if (transaction.type === TransactionType.INSERT && transaction.new) {
      if (query.matches(transaction.new, false)) {
        // insert to inner -> INSERT
        return transaction;
      } else {
        // insert to outer -> NONE
        return { ...transaction, type: TransactionType.NONE, new: undefined };
      }
    }

    if (transaction.type === TransactionType.UPDATE && transaction.before) {
      const updateObj = TransactionRequest.applyOperator(transaction);
      if (query.matches(transaction.before, false)) {
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

    if (transaction.type === TransactionType.DELETE && transaction.before) {
      if (query.matches(transaction.before, false)) {
        // delete from inner -> DELETE
        return transaction;
      } else {
        // delete from out -> NONE
        return { ...transaction, type: TransactionType.NONE, before: undefined };
      }
    }
    throw new Error("Bad transaction data:" + JSON.stringify(transaction));
  }
}

class SubsetUpdatorProxy extends Proxy {

  constructor(protected storage: SubsetStorage) {
    super();
    this.logger.category = "SubsetUpdatorProxy";
    this.logger.debug("SubsetUpdatorProxy is created");
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) { throw new Error("url is required."); }
    if (!req.method) { throw new Error("method is required."); }
    const url = URL.parse(req.url);
    if (url.pathname == null) { throw new Error("pathname is required."); }
    const method = req.method.toUpperCase();
    this.logger.debug(method, url.pathname);
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTION) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (request) => {
        const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
        this.logger.info(CORE_NODE.PATH_GET_TRANSACTION, "csn:", csn);
        return this.getTransactionJournal(csn);
      });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTIONS) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (request) => {
        const fromCsn = ProxyHelper.validateNumberRequired(request.fromCsn, "fromCsn");
        const toCsn = ProxyHelper.validateNumberRequired(request.toCsn, "toCsn");
        this.logger.info(CORE_NODE.PATH_GET_TRANSACTIONS, "from:", fromCsn, "to:", toCsn);
        return this.getTransactionJournals(fromCsn, toCsn);
      });
    } else {
      this.logger.warn("server command not found!:", method, url.pathname);
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
            journal,
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
            const journalStr = JSON.stringify(journal);
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
}

/**
 * サブセットストレージ(SubsetStorage)
 *
 * サブセットストレージは、クエリハンドラと更新レシーバの機能を提供するサービスエンジンである。
 */
export class SubsetStorage extends ServiceEngine implements Proxy {

  public bootOrder = 50;
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
  private lock: ReadWriteLock;
  private queryWaitingList: { [csn: number]: Array<() => Promise<any>> } = {};
  private subscriberKey: string | null;
  private readyFlag: boolean = false;
  private updateProcessor: UpdateProcessor;
  private updateListener: UpdateListener;
  private updateListenerKey?: string;
  private updateListenerMountHandle?: string;
  notifyListener?: { procNotify: (transaction: TransactionObject) => void };

  constructor(option: SubsetStorageConfigDef) {
    super(option);
    if (typeof option.exported === "undefined") {
      option = { ...option, exported: true };
    }
    this.logger.debug(JSON.stringify(option));
    this.option = option;
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
    return this.database + "--" + this.subsetName;
  }

  getType(): string {
    return this.type;
  }

  getReady(): boolean {
    return this.readyFlag;
  }

  setReady(): void {
    if (this.readyFlag) {
      return;
    }
    this.readyFlag = true;

    // Rest サービスを登録する。
    const mountingMode = this.option.exported ? "loadBalancing" : "localOnly";
    this.logger.info("mountingMode:", mountingMode);
    this.node.mount(CORE_NODE.PATH_SUBSET
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName), mountingMode, this)
      .then((value) => {
        this.mountHandle = value;
      });
  }

  pullQueryWaitingList(csn: number): Array<() => Promise<any>> {
    const list = this.queryWaitingList[csn];
    if (list) {
      delete this.queryWaitingList[csn];
      return list;
    }
    return [];
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug("SubsetStorage is starting");

    if (!this.option.database) {
      throw new DadgetError(ERROR.E2401, ["Database name is missing."]);
    }
    if (this.option.database.match(/--/)) {
      throw new DadgetError(ERROR.E2401, ["Database name can not contain '--'."]);
    }
    this.database = this.option.database;

    if (!this.option.subset) {
      throw new DadgetError(ERROR.E2401, ["Subset name is missing."]);
    }
    if (this.option.subset.match(/--/)) {
      throw new DadgetError(ERROR.E2401, ["Subset name can not contain '--'."]);
    }
    this.subsetName = this.option.subset;
    this.logger.info("subsetName:", this.subsetName);
    console.dir(this.option);

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
    promise = promise.then(() => { this.logger.debug("SubsetStorage is starting"); });
    promise = promise.then(() => this.journalDb.start());
    promise = promise.then(() => this.systemDb.start());
    promise = promise.then(() => {
      return this.systemDb.checkQueryHash(queryHash)
        .then((result) => {
          if (result) {
            this.logger.warn("Subset Storage requires resetting because the query hash has been changed.");
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
    promise = promise.then(() => { this.logger.debug("SubsetStorage is started"); });
    return promise;
  }

  connectUpdateListener() {
    this.node.mount(CORE_NODE.PATH_SUBSET_UPDATOR
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName), "singletonMaster", new SubsetUpdatorProxy(this), {
      onDisconnect: () => {
        this.logger.info("updator is disconnected");
        this.subscribeUpdateProcessor()
          .then(() => {
            if (this.updateListenerKey) { this.node.unsubscribe(this.updateListenerKey); }
            this.updateListenerKey = undefined;
          });
      },
      onRemount: (mountHandle: string) => {
        this.logger.info("updator is remounted");
        this.updateListenerMountHandle = mountHandle;
        this.subscribeUpdateListener()
          .then(() => {
            if (this.subscriberKey) { this.node.unsubscribe(this.subscriberKey); }
          });
      },
    })
      .then((mountHandle) => {
        // マスターを取得した場合のみ実行される
        this.logger.info("updator is connected");
        if (this.updateListenerMountHandle === "stopped") {
          setTimeout(() => { this.node.unmount(mountHandle); }, 1);
          return;
        }
        this.updateListenerMountHandle = mountHandle;
        this.subscribeUpdateListener()
          .then(() => {
            if (this.subscriberKey) { this.node.unsubscribe(this.subscriberKey); }
          });
      });
  }

  subscribeUpdateListener(): Promise<void> {
    if (this.option.subscribe) {
      return this.node.subscribe(
        CORE_NODE.PATH_SUBSET_TRANSACTION
          .replace(/:database\b/g, this.database)
          .replace(/:subset\b/g, this.option.subscribe), this.updateListener)
        .then((key) => { this.updateListenerKey = key; });
    } else {
      return this.node.subscribe(
        CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), this.updateListener)
        .then((key) => { this.updateListenerKey = key; });
    }
  }

  subscribeUpdateProcessor(): Promise<void> {
    return this.node.subscribe(
      CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), this.updateProcessor)
      .then((key) => { this.subscriberKey = key; });
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
      .then(() => {
        if (this.updateListenerMountHandle) { return this.node.unmount(this.updateListenerMountHandle).catch(); }
        this.updateListenerMountHandle = "stopped";
      })
      .then(() => {
        if (this.updateListenerKey) { return this.node.unsubscribe(this.updateListenerKey); }
      })
      .then(() => {
        if (this.mountHandle) { return this.node.unmount(this.mountHandle).catch(); }
      })
      .then(() => {
        if (this.subscriberKey) { return this.node.unsubscribe(this.subscriberKey).catch(); }
      });
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) { throw new Error("url is required."); }
    if (!req.method) { throw new Error("method is required."); }
    const url = URL.parse(req.url);
    if (url.pathname == null) { throw new Error("pathname is required."); }
    const method = req.method.toUpperCase();
    this.logger.debug(method, url.pathname);
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_QUERY) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (request) => {
        this.logger.debug(CORE_NODE.PATH_QUERY);
        const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
        const query = EJSON.parse(request.query);
        const sort = request.sort ? EJSON.parse(request.sort) : undefined;
        const limit = ProxyHelper.validateNumber(request.limit, "limit");
        const offset = ProxyHelper.validateNumber(request.offset, "offset");
        const projection = request.projection ? EJSON.parse(request.projection) : undefined;
        return this.query(csn, query, sort, limit, request.csnMode, projection, offset)
          .then((result) => {
            return { status: "OK", result };
          });
      });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_COUNT) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (request) => {
        this.logger.debug(CORE_NODE.PATH_COUNT);
        const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
        const query = EJSON.parse(request.query);
        return this.count(csn, query, request.csnMode)
          .then((result) => {
            return { status: "OK", result };
          });
      });
    } else {
      this.logger.warn("server command not found!:", url.pathname);
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
    let release: () => void;
    const promise = new Promise<void>((resolve, reject) => {
      this.getLock().readLock((unlock) => {
        this.logger.debug("get readLock");
        release = () => {
          this.logger.debug("release readLock");
          unlock();
        };
        resolve();
      });
    });

    const protectedCsn = this.getJournalDb().getProtectedCsn();

    return promise
      .then(() => this.getSystemDb().getCsn())
      .then((currentCsn) => {
        if (csn === 0 || csn === currentCsn || (csn < currentCsn && csnMode === "latest")) {
          return this.getSubsetDb().count(innerQuery)
            .then((result) => {
              release();
              return { csn: currentCsn, resultCount: result, restQuery };
            });
        } else if (csn < protectedCsn) {
          throw new DadgetError(ERROR.E2402, [csn, protectedCsn]);
        } else if (csn < currentCsn) {
          this.logger.info("rollback transactions", csn, currentCsn);
          // rollback transactions
          return this.getJournalDb().findByCsnRange(csn + 1, currentCsn)
            .then((transactions) => {
              if (transactions.length !== currentCsn - csn) {
                release();
                this.logger.info("not enough rollback transactions");
                return { csn, resultCount: 0, restQuery: query };
              }
              return this.getSubsetDb().count({ $and: [innerQuery, { csn: { $lte: csn } }] })
                .then((result) => {
                  release();
                  const resultSet = SubsetStorage.rollbackAndFind([], transactions, innerQuery);
                  console.log("rollback count:", result, resultSet.length);
                  return { csn, resultCount: result + resultSet.length, restQuery };
                });
            });
        } else {
          this.logger.info("wait for transactions", csn, currentCsn);
          // wait for transactions
          return new Promise<CountResult>((resolve, reject) => {
            if (!this.queryWaitingList[csn]) { this.queryWaitingList[csn] = []; }
            this.queryWaitingList[csn].push(() => {
              return this.getSubsetDb().count(innerQuery)
                .then((result) => {
                  resolve({ csn, resultCount: result, restQuery });
                });
            });
            release();
          });
        }
      }).catch((e) => {
        this.logger.warn("SubsetStorage query error: " + e.toString());
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
    let release: () => void;
    const promise = new Promise<void>((resolve, reject) => {
      this.getLock().readLock((unlock) => {
        this.logger.debug("get readLock");
        release = () => {
          this.logger.debug("release readLock");
          unlock();
        };
        resolve();
      });
    });

    const protectedCsn = this.getJournalDb().getProtectedCsn();

    return promise
      .then(() => this.getSystemDb().getCsn())
      .then((currentCsn) => {
        if (csn === 0 || csn === currentCsn || (csn < currentCsn && csnMode === "latest")) {
          return this.getSubsetDb().find(innerQuery, sort, limit, projection, offset)
            .then((result) => {
              release();
              return { csn: currentCsn, resultSet: result, restQuery };
            });
        } else if (csn < protectedCsn) {
          throw new DadgetError(ERROR.E2402, [csn, protectedCsn]);
        } else if (csn < currentCsn) {
          this.logger.info("rollback transactions", csn, currentCsn);
          // rollback transactions
          return this.getJournalDb().findByCsnRange(csn + 1, currentCsn)
            .then((transactions) => {
              if (transactions.length !== currentCsn - csn) {
                release();
                this.logger.info("not enough rollback transactions");
                return { csn, resultSet: [], restQuery: query };
              }
              const _offset = offset ? offset : 0;
              const maxLimit = limit ? limit + _offset : undefined;
              const possibleLimit = maxLimit ? maxLimit + transactions.length : undefined;
              return this.getSubsetDb().find(innerQuery, sort, possibleLimit)
                .then((result) => {
                  release();
                  const resultSet = SubsetStorage.rollbackAndFind(result, transactions, innerQuery, sort, limit, offset)
                    .map((val) => Util.project(val, projection));
                  return { csn, resultSet, restQuery };
                });
            });
        } else {
          this.logger.info("wait for transactions", csn, currentCsn);
          // wait for transactions
          return new Promise<QueryResult>((resolve, reject) => {
            if (!this.queryWaitingList[csn]) { this.queryWaitingList[csn] = []; }
            this.queryWaitingList[csn].push(() => {
              return this.getSubsetDb().find(innerQuery, sort, limit, projection, offset)
                .then((result) => {
                  resolve({ csn, resultSet: result, restQuery });
                });
            });
            release();
          });
        }
      }).catch((e) => {
        this.logger.warn("SubsetStorage query error: " + e.toString());
        release();
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

    transactions.forEach((trans) => {
      if (trans.before) {
        dataMap[trans.target] = trans.before;
      } else if (trans.type === TransactionType.INSERT) {
        delete dataMap[trans.target];
      }
    });

    const dataList = [];
    for (const _id of Object.keys(dataMap)) {
      dataList.push(dataMap[_id]);
    }
    let list = Util.mongoSearch(dataList, query, sort) as object[];
    if (offset) {
      if (limit) {
        list = list.slice(offset, offset + limit);
      } else {
        list = list.slice(offset);
      }
    } else {
      if (limit) {
        list = list.slice(0, limit);
      }
    }
    return list;
  }
}
