import { Proxy, ResourceNode, ServiceEngine, Subscriber } from "@chip-in/resource-node";
import * as AsyncLock from "async-lock";
import { serialize } from "bson";
import * as http from "http";
import * as URL from "url";
import { CORE_NODE, MAX_OBJECT_SIZE } from "../Config";
import { PersistentDb } from "../db/container/PersistentDb";
import { JournalDb } from "../db/JournalDb";
import { SystemDb } from "../db/SystemDb";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { LOG_MESSAGES } from "../LogMessages";
import { DadgetError, UniqueError } from "../util/DadgetError";
import * as EJSON from "../util/Ejson";
import { Logger } from "../util/Logger";
import { ProxyHelper } from "../util/ProxyHelper";
import { Util } from "../util/Util";
import Dadget, { CLIENT_VERSION, CsnMode, QueryResult } from "./Dadget";
import { DatabaseRegistry, IndexDef } from "./DatabaseRegistry";
import { UniqueCache } from "./UniqueCache";
import { SubsetStorage } from "./SubsetStorage";

const KEEP_TIME_AFTER_CONTEXT_MANAGER_MASTER_ACQUIRED_MS = 3000; // 3000ms
const KEEP_TIME_AFTER_SENDING_ROLLBACK_MS = 1000; // 1000ms
const CHECK_POINT_CHECK_PERIOD_MS = 10 * 60 * 1000;  // 10 minutes
const CHECK_POINT_DELETE_PERIOD_MS = 72 * 60 * 60 * 1000; // 72 hours
const MAX_RESPONSE_SIZE_OF_JOURNALS = 10485760;
const ATOMIC_OPERATION_FIRST_LOCK_TIME = 20 * 1000;  // 20 sec
const ATOMIC_OPERATION_MAX_LOCK_TIME = 10 * 60 * 1000;  // 10 minutes
const MASTER_LOCK = "master";
const TRANSACTION_LOCK = "transaction";
const PUBLISH_LOCK = "publish";

/**
 * コンテキストマネージャコンフィグレーションパラメータ
 */
export class ContextManagerConfigDef {

  /**
   * データベース名
   */
  database: string;
}

class TransactionJournalSubscriber extends Subscriber {
  private logger: Logger;

  constructor(protected context: ContextManager) {
    super();
    this.logger = Logger.getLogger("TransJournalSubscriber", context.getDatabase());
    this.logger.debug(LOG_MESSAGES.CREATED, ["TransactionJournalSubscriber"]);
  }

  /**
   * Receive a MQTT message
   */
  onReceive(msg: string) {
    const transaction: TransactionObject = EJSON.parse(msg);
    this.logger.info(LOG_MESSAGES.MSG_RECEIVED, [transaction.type], [transaction.csn]);

    if (transaction.protectedCsn) {
      const protectedCsn = transaction.protectedCsn;
      if (this.context.getJournalDb().getProtectedCsn() < protectedCsn) {
        this.logger.info(LOG_MESSAGES.CHECKPOINT_RECEIVED, [], [protectedCsn]);
        this.context.getJournalDb().setProtectedCsn(protectedCsn);
        setTimeout(() => {
          this.context.getJournalDb().deleteBeforeCsn(protectedCsn)
            .catch((err) => {
              this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [100]);
            });
        });
      }
    }

    if (this.context.getDigestMap().has(transaction.csn) &&
      this.context.getDigestMap().get(transaction.csn) === transaction.digest) {
      this.context.getDigestMap().delete(transaction.csn);
      return;
    }

    this.context.getLock().acquire(TRANSACTION_LOCK, () => {
      if (transaction.type === TransactionType.FORCE_ROLLBACK) {
        this.logger.warn(LOG_MESSAGES.ROLLBACK, [], [transaction.csn]);
        const protectedCsn = this.context.getJournalDb().getProtectedCsn();
        this.context.getJournalDb().setProtectedCsn(Math.min(protectedCsn, transaction.csn));
        return this.context.getJournalDb().findByCsn(transaction.csn)
          .then((tr) => {
            return (transaction.csn > 0 ? this.context.getJournalDb().deleteAfterCsn(transaction.csn) : this.context.getJournalDb().deleteAll())
              .then(() => {
                this.context.resetUniqueCache(transaction.csn);
                if (transaction.csn === 0 || tr && tr.digest === transaction.digest) {
                  return this.context.getSystemDb().updateCsn(transaction.csn);
                } else {
                  return this.adjustData(transaction.csn);
                }
              });
          })
          .catch((err) => {
            this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [101]);
          });
      } else {
        // Assume this node is a replication.
        return this.context.getSystemDb().getCsn()
          .then((csn) => {
            if (csn < transaction.csn - 1) {
              return this.adjustData(transaction.csn - 1);
            }
          })
          .then(() => {
            return Promise.all([
              this.context.getJournalDb().findByCsn(transaction.csn),
              this.context.getJournalDb().findByCsn(transaction.csn - 1),
            ]);
          })
          .then(([savedTransaction, preTransaction]) => {
            if (!savedTransaction) {
              if (preTransaction && preTransaction.digest === transaction.beforeDigest) {
                return this.context.getJournalDb().insert(transaction)
                  .then(() => {
                    this.context.updateUniqueCache(transaction);
                    return this.context.getSystemDb().updateCsn(transaction.csn);
                  });
              }
            }
          })
          .catch((err) => {
            this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString(), [102]]);
          });
      }
    });
  }

  fetchJournal(csn: number): Promise<TransactionObject | null> {
    return Util.fetchJournal(csn, this.context.getDatabase(), this.context.getNode());
  }

  async adjustData(csn: number): Promise<void> {
    this.logger.warn(LOG_MESSAGES.ADJUST_DATA, [], [csn]);
    let csnUpdated = false;
    const loopData = { csn };
    try {
      await Util.promiseWhile<{ csn: number; }>(
        loopData,
        (loopData) => {
          return loopData.csn !== 0;
        },
        async (loopData) => {
          const journal = await this.context.getJournalDb().findByCsn(loopData.csn);
          const fetchJournal = await this.fetchJournal(loopData.csn);
          if (!fetchJournal) { return { ...loopData, csn: 0 }; }
          if (!csnUpdated) {
            await this.context.getSystemDb().updateCsn(csn);
            csnUpdated = true;
          }
          if (!journal) {
            return this.context.getJournalDb().insert(fetchJournal)
              .then(() => ({ ...loopData, csn: loopData.csn - 1 }));
          }
          if (fetchJournal.digest === journal.digest) {
            return { ...loopData, csn: 0 };
          } else {
            return this.context.getJournalDb().replace(journal, fetchJournal)
              .then(() => ({ ...loopData, csn: loopData.csn - 1 }));
          }
        });
    } catch (err) {
      this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [103]);
      throw err;
    }
  }
}

class ContextManagementServer extends Proxy {
  private logger: Logger;
  private lastBeforeObj?: { _id?: string, csn?: number };
  private lastCheckPointTime: number = 0;
  private checkPointTimer?: any;
  private atomicLockId?: string = undefined;
  private atomicTimer?: any;
  private queueWaitingList: (() => void)[] = [];
  private pubDataList: string[] = [];

  constructor(protected context: ContextManager) {
    super();
    this.logger = Logger.getLogger("ContextManagementServer", context.getDatabase());
    this.logger.debug(LOG_MESSAGES.CREATED, ["ContextManagementServer"]);
  }

  resetLastBeforeObj() {
    this.lastBeforeObj = undefined;
  }

  private notifyAllWaitingList() {
    const queue = this.queueWaitingList;
    this.queueWaitingList = [];
    setTimeout(() => {
      for (const task of queue) { task(); }
    }, 0);
  }

  /**
   * Receive a HTTP request
   */
  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) { throw new Error("url is required."); }
    if (!req.method) { throw new Error("method is required."); }
    const url = URL.parse(req.url);
    if (url.pathname == null) { throw new Error("pathname is required."); }
    const method = req.method.toUpperCase();
    this.logger.debug(LOG_MESSAGES.ON_RECEIVE, [method, url.pathname]);
    const time = Date.now();
    const sleep = (waitMS: number) => {
      if (waitMS < 0) {
        return Promise.resolve();
      }
      return new Promise<void>(resolve => {
        setTimeout(() => {
          resolve()
        }, waitMS)
      })
    }
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_EXEC) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        const csn = ProxyHelper.validateNumberRequired(data.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_EXEC, [], [csn]);
        if (data.version && Number(data.version) > CLIENT_VERSION) throw new DadgetError(ERROR.E3002);
        return this.exec(csn, data.request, data.atomicId, data.options)
          .then((result) => Promise.any([sleep(570 * 1000 - (Date.now() - time)), this.context._waitSubsetStorage(result)]))
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_EXEC_MANY) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        const csn = ProxyHelper.validateNumberRequired(data.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_EXEC_MANY, [], [csn]);
        if (data.version && Number(data.version) > CLIENT_VERSION) throw new DadgetError(ERROR.E3002);
        return this.execMany(csn, data.requests, data.atomicId, data.options)
          .then((result) => Promise.any([sleep(570 * 1000 - (Date.now() - time)), this.context._waitSubsetStorage(result)]))
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_UPDATE_MANY) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_UPDATE_MANY, [JSON.stringify(data.query), JSON.stringify(data.operator)], []);
        if (data.version && Number(data.version) > CLIENT_VERSION) throw new DadgetError(ERROR.E3002);
        return this.updateMany(data.query, data.operator, data.atomicId)
          .then((result) => Promise.any([sleep(570 * 1000 - (Date.now() - time)), this.context._waitSubsetStorage(result)]))
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTION) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        const csn = ProxyHelper.validateNumberRequired(data.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTION, [], [csn]);
        return this.getTransactionJournal(csn)
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTION_OLD) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (data) => {
        const csn = ProxyHelper.validateNumberRequired(data.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTION, [], [csn]);
        return this.getTransactionJournal(csn)
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTIONS) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        const fromCsn = ProxyHelper.validateNumberRequired(data.fromCsn, "fromCsn");
        const toCsn = ProxyHelper.validateNumberRequired(data.toCsn, "toCsn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTIONS, [], [fromCsn, toCsn]);
        return this.getTransactionJournals(fromCsn, toCsn)
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_TRANSACTIONS_OLD) && method === "GET") {
      return ProxyHelper.procGet(req, res, this.logger, (data) => {
        const fromCsn = ProxyHelper.validateNumberRequired(data.fromCsn, "fromCsn");
        const toCsn = ProxyHelper.validateNumberRequired(data.toCsn, "toCsn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_TRANSACTIONS, [], [fromCsn, toCsn]);
        return this.getTransactionJournals(fromCsn, toCsn)
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_UPDATE_DATA) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        const csn = ProxyHelper.validateNumberRequired(data.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_UPDATE_DATA, [], [csn]);
        return this.getUpdateData(csn)
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_GET_LATEST_CSN) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (_data) => {
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_GET_LATEST_CSN);
        return this.getCommittedCsn()
          .then((csn) => ({ status: "OK", csn }))
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else {
      this.logger.warn(LOG_MESSAGES.SERVER_COMMAND_NOT_FOUND, [method, url.pathname]);
      return ProxyHelper.procError(req, res);
    }
  }

  exec(postulatedCsn: number, request: TransactionRequest, atomicId?: string, options?: ExecOptions): Promise<object> {
    const err = this.checkTransactionRequest(request, options);
    if (err) {
      return Promise.resolve({
        status: "NG",
        reason: new DadgetError(ERROR.E2002, [err]),
      });
    }

    if (request.type === TransactionType.CHECK) {
      if (this.atomicLockId === atomicId) {
        this.setTransactionTimeout(false);
        return this.context.getSystemDb().getCsn()
          .then((csn) => {
            return Promise.resolve({
              status: "OK",
              csn,
            });
          });
      } else {
        return Promise.resolve({
          status: "NG",
          reason: "transaction mismatch",
        });
      }
    }

    if (this.context.getLock().isBusy(MASTER_LOCK) || this.atomicLockId && this.atomicLockId !== atomicId) {
      return new Promise<object>((resolve, reject) => {
        this.logger.info(LOG_MESSAGES.QUEUE_WAITING);
        this.queueWaitingList.push(() => {
          this.exec(postulatedCsn, request, atomicId, options)
            .then((result) => resolve(result)).catch((reason) => reject(reason));
        });
      });
    }

    return new Promise((resolve, reject) => {
      this.context.getLock().acquire(MASTER_LOCK, () => {
        if (this.atomicLockId ? this.atomicLockId !== atomicId : atomicId) {
          if (request.type !== TransactionType.BEGIN &&
            request.type !== TransactionType.BEGIN_IMPORT &&
            request.type !== TransactionType.BEGIN_RESTORE) {
            reject(new DadgetError(ERROR.E2009));
            return;
          }
        }

        return this.context.getLock().acquire(TRANSACTION_LOCK, () => {
          return this.procTransaction(postulatedCsn, request, atomicId, options);
        }).then(({ transaction, newCsn, updateObject }) => {
          this.checkProtectedCsn();
          if (updateObject) {
            if (!updateObject._id) { updateObject._id = transaction.target; }
            updateObject.csn = newCsn;
          }
          if (request.type !== TransactionType.BEGIN &&
            request.type !== TransactionType.END) {
            this.lastBeforeObj = updateObject;
          }
          resolve({
            status: "OK",
            csn: newCsn,
            updateObject,
          });
        }, (reason) => {
          if (ExecOptions.continueOnError(options, reason)) {
            return this.context.getSystemDb().getCsn()
              .then((csn) => {
                resolve({
                  status: "OK",
                  csn,
                  updateObject: null
                });
              })
          }
          let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason]);
          if (cause.code === ERROR.E1105.code) { cause = new DadgetError(ERROR.E2004, [cause]); }
          cause.convertInsertsToString();
          this.logger.warn(LOG_MESSAGES.ERROR_CAUSE, [cause.toString()]);
          resolve({
            status: "NG",
            reason: cause,
          });
        });
      }).then(() => this.notifyAllWaitingList());
    });
  }

  execMany(postulatedCsn: number, requests: TransactionRequest[], atomicId?: string, options?: ExecOptions): Promise<object> {
    for (const request of requests) {
      const err = this.checkTransactionRequest(request, options);
      if (err) {
        return Promise.resolve({
          status: "NG",
          reason: new DadgetError(ERROR.E2002, [err]),
        });
      }
    }

    for (const request of requests) {
      if (request.type === TransactionType.BEGIN ||
        request.type === TransactionType.BEGIN_IMPORT ||
        request.type === TransactionType.BEGIN_RESTORE ||
        request.type === TransactionType.END ||
        request.type === TransactionType.END_IMPORT ||
        request.type === TransactionType.END_RESTORE ||
        request.type === TransactionType.ABORT ||
        request.type === TransactionType.ABORT_RESTORE ||
        request.type === TransactionType.ABORT_IMPORT) {
        return Promise.resolve({
          status: "NG",
          reason: new DadgetError(ERROR.E2008, [request.type]),
        });
      }
    }

    if (this.context.getLock().isBusy(MASTER_LOCK) || this.atomicLockId && this.atomicLockId !== atomicId) {
      return new Promise<object>((resolve, reject) => {
        this.logger.info(LOG_MESSAGES.QUEUE_WAITING);
        this.queueWaitingList.push(() => {
          this.execMany(postulatedCsn, requests, atomicId, options)
            .then((result) => resolve(result)).catch((reason) => reject(reason));
        });
      });
    }

    return new Promise((resolve, reject) => {
      this.context.getLock().acquire(MASTER_LOCK, () => {
        let _newCsn: number;
        return this.context.getLock().acquire(TRANSACTION_LOCK, () => {
          if (this.atomicLockId) {
            return Util.promiseEach<TransactionRequest>(
              requests,
              (request) => {
                return this.procTransaction(postulatedCsn, request, atomicId, options)
                  .then(({ newCsn }) => {
                    _newCsn = newCsn;
                  })
                  .catch((e) => {
                    if (!ExecOptions.continueOnError(options, e)) throw e;
                  });
              },
            );
          } else {
            atomicId = Dadget.uuidGen();
            return this.procTransaction(0, { type: TransactionType.BEGIN, target: "" }, atomicId)
              .then(({ newCsn }) => {
                _newCsn = newCsn;
                return Util.promiseEach<TransactionRequest>(
                  requests,
                  (request) => {
                    return this.procTransaction(postulatedCsn, request, atomicId, options)
                      .then(({ newCsn }) => {
                        _newCsn = newCsn;
                      })
                      .catch((e) => {
                        if (!ExecOptions.continueOnError(options, e)) throw e;
                      });
                  },
                );
              })
              .catch((reason) => {
                return this.procTransaction(0, { type: TransactionType.ABORT, target: "" }, atomicId)
                  .then(({ newCsn }) => {
                    _newCsn = newCsn;
                  })
                  .then(() => { throw reason; });
              })
              .then(() => {
                return this.procTransaction(0, { type: TransactionType.END, target: "" }, atomicId)
                  .then(({ newCsn }) => {
                    _newCsn = newCsn;
                  });
              });
          }
        }).then(() => {
          this.checkProtectedCsn();
          this.lastBeforeObj = undefined;
          resolve({
            status: "OK",
            csn: _newCsn,
          });
        }, (reason) => {
          let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason]);
          if (cause.code === ERROR.E1105.code) { cause = new DadgetError(ERROR.E2004, [cause]); }
          cause.convertInsertsToString();
          this.logger.warn(LOG_MESSAGES.ERROR_CAUSE, [cause.toString()]);
          resolve({
            status: "NG",
            csn: _newCsn,
            reason: cause,
          });
        }).then(() => this.notifyAllWaitingList());
      });
    });
  }

  _query(query: object, csn: number, projection?: object): Promise<QueryResult> {
    const node = this.context.getNode();
    const database = this.context.getDatabase();
    const subsetStorage = (node.searchServiceEngine("SubsetStorage", { database }) as SubsetStorage[]).find((v) => v.isWhole());
    let promise = subsetStorage ?
      subsetStorage.waitReady().then(() => subsetStorage.query(csn, query, undefined, undefined, undefined, projection)) :
      Dadget._query(node, database, query, undefined, undefined, undefined, csn, undefined, projection);
    return promise;
  }

  async _updateMany(resultSet: object[], csn: number, operator: object, atomicId?: string): Promise<number> {
    const idList = resultSet.map((row) => (row as any)._id);
    let count = 0;
    while (idList.length > 0) {
      const ids = idList.splice(0, 2);
      const result = await this._query({ _id: { $in: ids } }, csn);
      if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
      if (result.resultSet.length !== ids.length) { throw new Error(`Update object not found ${result.resultSet.length}, ${ids.length}`); }
      for (const obj of result.resultSet) {
        const request = new TransactionRequest();
        request.type = TransactionType.UPDATE;
        request.target = (obj as any)._id;
        request.before = obj;
        request.operator = operator;
        count++;
        await this.procTransaction(result.csn, request, atomicId);
      }
    }
    return count;
  }

  updateMany(query: object, operator: object, atomicId?: string): Promise<object> {
    if (this.context.getLock().isBusy(MASTER_LOCK) || this.atomicLockId && this.atomicLockId !== atomicId) {
      return new Promise<object>((resolve, reject) => {
        this.queueWaitingList.push(() => {
          this.updateMany(query, operator, atomicId)
            .then((result) => resolve(result)).catch((reason) => reject(reason));
        });
      });
    }

    return new Promise((resolve, reject) => {
      this.context.getLock().acquire(MASTER_LOCK, () => {
        let count = 0;
        return this.context.getLock().acquire(TRANSACTION_LOCK, () => {
          return this.context.getSystemDb().getCsn()
            .then((currentCsn) => {
              return this._query(query, currentCsn, { _id: 1 })
                .then((result) => {
                  if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
                  if (this.atomicLockId) {
                    return this._updateMany(result.resultSet, currentCsn, operator, atomicId).then((_count) => {
                      count = _count;
                    });
                  } else {
                    atomicId = Dadget.uuidGen();
                    return this.procTransaction(0, { type: TransactionType.BEGIN, target: "" }, atomicId)
                      .then(() => this._updateMany(result.resultSet, currentCsn, operator, atomicId))
                      .catch((reason) => {
                        return this.procTransaction(0, { type: TransactionType.ABORT, target: "" }, atomicId)
                          .then(() => { throw reason; });
                      })
                      .then((_count) => {
                        count = _count;
                        return this.procTransaction(0, { type: TransactionType.END, target: "" }, atomicId).then(() => { });
                      });
                  }
                });
            });
        }).then(() => {
          this.checkProtectedCsn();
          this.lastBeforeObj = undefined;
          resolve({
            status: "OK",
            csn: this.context.getSystemDb()._getCsn(),
            count,
          });
        }, (reason) => {
          let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason]);
          if (cause.code === ERROR.E1105.code) { cause = new DadgetError(ERROR.E2004, [cause]); }
          cause.convertInsertsToString();
          this.logger.warn(LOG_MESSAGES.ERROR_CAUSE, [cause.toString()]);
          resolve({
            status: "NG",
            csn: this.context.getSystemDb()._getCsn(),
            reason: cause,
          });
        }).then(() => this.notifyAllWaitingList());
      });
    });
  }

  private async procTransaction(postulatedCsn: number, request: TransactionRequest, atomicId?: string, options?: ExecOptions) {
    const _request = { ...request, datetime: new Date() };
    if (this.lastBeforeObj && request.before && this.lastBeforeObj._id === request.target) {
      const before = TransactionRequest.getBefore(request);
      const objDiff = Util.diff(this.lastBeforeObj, before);
      if (objDiff) {
        this.logger.error(LOG_MESSAGES.REQUEST_BEFORE_HAS_MISMATCH, [JSON.stringify(objDiff)]);
        throw new DadgetError(ERROR.E2005, [JSON.stringify(request)]);
      } else {
        this.logger.debug(LOG_MESSAGES.LASTBEFOREOBJ_CHECK_PASSED);
      }
    }
    await this.context.getJournalDb().checkConsistent(postulatedCsn, _request);
    const currentCsn = await this.context.getSystemDb().getCsn();
    const updateObject: { _id?: string, csn?: number } | undefined = await this.context.checkUniqueConstraint(currentCsn, _request, options);
    let newCsn = await this.context.getSystemDb().getCsn() + 1;
    const lastDigest = await this.context.getJournalDb().getLastDigest();
    if (request.type === TransactionType.FORCE_ROLLBACK) { newCsn = 0; }
    this.logger.info(LOG_MESSAGES.EXEC_NEWCSN, [], [newCsn]);
    const transaction: TransactionObject = { ..._request, csn: newCsn, beforeDigest: lastDigest };
    transaction.digest = TransactionObject.calcDigest(transaction);
    transaction.protectedCsn = this.context.getJournalDb().getProtectedCsn();
    this.procTransactionCtrl(transaction, newCsn, atomicId);
    const session = await this.context.getSystemDb().startTransaction();
    try {
      await this.context.getJournalDb().insert(transaction, session);
      await this.context.getSystemDb().updateCsn(newCsn, session);
      await this.context.getSystemDb().commitTransaction(session);
    } catch (err) {
      await this.context.getSystemDb().abortTransaction(session);
      throw err;
    }
    const pubData = EJSON.stringify(transaction);
    this.context.updateUniqueCache(transaction);
    if (request.type !== TransactionType.FORCE_ROLLBACK && transaction.digest) {
      this.context.getDigestMap().set(transaction.csn, transaction.digest);
    }
    this.pubDataList.push(pubData);
    if (!this.context.getLock().isBusy(PUBLISH_LOCK)) {
      this.context.getLock().acquire(PUBLISH_LOCK, () => {
        return Util.promiseWhile<string[]>(
          this.pubDataList,
          (data) => {
            return data.length !== 0;
          },
          async (data) => {
            const pubData = data.shift();
            if (!pubData) { throw new Error("empty data error"); }
            try {
              await this.context.getNode().publish(
                CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase()), pubData);
            } catch (err) {
              this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [108]);
              process.exit(1);
            }
            return this.pubDataList;
          });
      });
    }
    return { transaction, newCsn, updateObject };
  }

  private setTransactionTimeout(first: boolean) {
    if (this.atomicTimer) { clearTimeout(this.atomicTimer); }
    this.atomicTimer = setTimeout(() => {
      this.atomicTimer = undefined;
      this.logger.warn(LOG_MESSAGES.TRANSACTION_TIMEOUT);
      this.procTransaction(0, { type: TransactionType.ABORT, target: "" }, this.atomicLockId)
        .then(() => this.notifyAllWaitingList())
        .catch(err => {
          this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [109]);
        });
    }, first ? ATOMIC_OPERATION_FIRST_LOCK_TIME : ATOMIC_OPERATION_MAX_LOCK_TIME);
  }

  private clearTransactionTimeout() {
    if (this.atomicTimer) { clearTimeout(this.atomicTimer); }
    this.atomicTimer = undefined;
  }

  private procTransactionCtrl(transaction: TransactionObject, newCsn: number, atomicId?: string) {
    if (this.atomicLockId) {
      if (this.atomicLockId !== atomicId) {
        throw new DadgetError(ERROR.E2007);
      } else {
        this.setTransactionTimeout(false);
      }
    }

    if (this.context.committedCsn !== undefined) {
      transaction.committedCsn = this.context.committedCsn;
    }

    if (transaction.type === TransactionType.BEGIN ||
      transaction.type === TransactionType.BEGIN_IMPORT ||
      transaction.type === TransactionType.BEGIN_RESTORE) {
      if (!atomicId) { throw new DadgetError(ERROR.E2011, ["atomicId"]); }
      this.atomicLockId = atomicId;
      this.context.committedCsn = undefined;
      delete transaction.committedCsn;
      this.setTransactionTimeout(true);
    } else if (atomicId && this.atomicLockId !== atomicId) {
      throw new DadgetError(ERROR.E2010);
    }

    if (transaction.type === TransactionType.BEGIN || transaction.type === TransactionType.BEGIN_IMPORT) {
      // BEGIN_RESTOREの場合はcommittedCsnにロールバックできないので設定しない
      this.context.committedCsn = newCsn - 1;
      transaction.committedCsn = this.context.committedCsn;
    }

    if (transaction.type === TransactionType.END ||
      transaction.type === TransactionType.END_IMPORT ||
      transaction.type === TransactionType.END_RESTORE ||
      transaction.type === TransactionType.ABORT_RESTORE) {
      if (!atomicId) { throw new DadgetError(ERROR.E2011, ["atomicId"]); }
      this.atomicLockId = undefined;
      this.context.committedCsn = undefined;
      delete transaction.committedCsn;
      this.clearTransactionTimeout();
    }

    if (transaction.type === TransactionType.ABORT || transaction.type === TransactionType.ABORT_IMPORT) {
      if (!atomicId) { throw new DadgetError(ERROR.E2011, ["atomicId"]); }
      if (this.context.committedCsn === undefined) {
        // リストア時にタイムアウトでABORTが呼び出される場合があるので、エラーにしない
        // throw new DadgetError(ERROR.E2009);
      }
      transaction.committedCsn = this.context.committedCsn;
      this.atomicLockId = undefined;
      this.context.committedCsn = undefined;
      this.clearTransactionTimeout();
    }
  }

  private checkTransactionRequest(request: TransactionRequest, options?: ExecOptions) {
    let err: string | null = null;
    if (request.type === TransactionType.INSERT || request.type === TransactionType.RESTORE) {
      if (!request.target) { err = "target is required for INSERT"; }
      if (request.before) { err = "before is not required for INSERT"; }
      if (request.operator && !options?.upsertOnUniqueError) { err = "operator is not required for INSERT"; }
      if (!request.new) { err = "new is required for INSERT"; }
    } else if (request.type === TransactionType.UPDATE) {
      if (!request.target) { err = "target is required for UPDATE"; }
      if (!request.operator) { err = "operator is required for UPDATE"; }
      if (request.new) { err = "new is not required for UPDATE"; }
    } else if (request.type === TransactionType.UPSERT || request.type === TransactionType.REPLACE) {
      if (!request.target) { err = "target is required for UPSERT and REPLACE"; }
      if (!request.new) { err = "new is required for UPSERT and REPLACE"; }
    } else if (request.type === TransactionType.DELETE) {
      if (!request.target) { err = "target is required for DELETE"; }
      if (request.operator) { err = "operator is not required for DELETE"; }
      if (request.new) { err = "new is not required for DELETE"; }
    } else if (request.type === TransactionType.TRUNCATE) {
      this.logger.warn(LOG_MESSAGES.EXEC_TRUNCATE);
    } else if (request.type === TransactionType.BEGIN) {
      this.logger.info(LOG_MESSAGES.EXEC_BEGIN);
    } else if (request.type === TransactionType.END) {
      this.logger.info(LOG_MESSAGES.EXEC_END);
    } else if (request.type === TransactionType.ABORT) {
      this.logger.info(LOG_MESSAGES.EXEC_ABORT);
    } else if (request.type === TransactionType.BEGIN_IMPORT) {
      this.logger.info(LOG_MESSAGES.EXEC_BEGIN_IMPORT);
    } else if (request.type === TransactionType.END_IMPORT) {
      this.logger.info(LOG_MESSAGES.EXEC_END_IMPORT);
    } else if (request.type === TransactionType.ABORT_IMPORT) {
      this.logger.warn(LOG_MESSAGES.EXEC_ABORT_IMPORT);
    } else if (request.type === TransactionType.BEGIN_RESTORE) {
      this.logger.info(LOG_MESSAGES.EXEC_BEGIN_RESTORE);
    } else if (request.type === TransactionType.END_RESTORE) {
      this.logger.info(LOG_MESSAGES.EXEC_END_RESTORE);
    } else if (request.type === TransactionType.ABORT_RESTORE) {
      this.logger.warn(LOG_MESSAGES.EXEC_ABORT_RESTORE);
    } else if (request.type === TransactionType.FORCE_ROLLBACK) {
      this.logger.warn(LOG_MESSAGES.EXEC_FORCE_ROLLBACK);
    } else if (request.type === TransactionType.CHECK) {
    } else {
      err = "type is not found in a transaction: " + request.type;
    }
    return err;
  }

  private checkProtectedCsn(): void {
    if (Date.now() - this.lastCheckPointTime <= CHECK_POINT_CHECK_PERIOD_MS) { return; }
    if (this.checkPointTimer) { clearTimeout(this.checkPointTimer); }
    this.checkPointTimer = setTimeout(() => {
      this.checkPointTimer = undefined;
      this.lastCheckPointTime = Date.now();
      const time = new Date();
      time.setMilliseconds(-CHECK_POINT_DELETE_PERIOD_MS);
      this.context.getJournalDb().getBeforeCheckPointTime(time)
        .then((journal) => {
          const deletableLastCsn = journal ? journal.csn : 0;
          return this.context.getJournalDb().getOneAfterCsn(deletableLastCsn)
            .then((protectedCsnJournal) => {
              if (protectedCsnJournal) {
                if (protectedCsnJournal.committedCsn !== undefined) {
                  return protectedCsnJournal.committedCsn;
                }
                return protectedCsnJournal.csn;
              } else {
                return this.context.getSystemDb().getCsn();
              }
            });
        })
        .then((protectedCsn) => {
          if (--protectedCsn < 0) protectedCsn = 0;
          this.logger.info(LOG_MESSAGES.CHECKPOINT_PROTECTEDCSN, [], [protectedCsn]);
          this.context.getJournalDb().setProtectedCsn(protectedCsn);
          this.context.getJournalDb().deleteBeforeCsn(protectedCsn)
            .catch((err) => {
              this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [104]);
            });
        });
    }, 60 * 1000);
  }

  getTransactionJournal(csn: number): Promise<object> {
    return this.context.getJournalDb().findByCsn(csn)
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
        return this.context.getJournalDb().findByCsn(loopData.csn)
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

  async getUpdateData(csn: number): Promise<object> {
    const journal = await this.context.getJournalDb().findByCsn(csn);
    if (!journal) { return { status: "NG" }; }
    if (journal.type == TransactionType.END) {
      const journal = await this.context.getJournalDb().findByCsn(csn - 1);
      if (!journal) { return { status: "NG" }; }
      const committedCsn = journal.committedCsn!;
      const journals = await this.context.getJournalDb().findByCsnRange(committedCsn + 2, csn - 1);
      if (journals.length < csn - committedCsn - 2) { return { status: "NG" }; }
      const rows = [];
      for (const journal of journals.reverse()) {
        const row: TransactionUpdateDetail = {
          type: journal.type,
          target: journal.target,
          csn: journal.csn,
        };
        if ((journal.new || journal.before) && journal.type != TransactionType.DELETE) {
          const data = TransactionRequest.applyOperator(journal);
          if (data) {
            if (!data._id && journal.target) data._id = journal.target;
            data.csn = journal.csn;
            row.data = data;
          }
        }
        rows.push(row);
      }
      return {
        status: "OK",
        list: rows,
      };
    } else {
      const row: TransactionUpdateDetail = {
        type: journal.type,
        target: journal.target,
        csn: journal.csn,
      };
      if ((journal.new || journal.before) && journal.type != TransactionType.DELETE) {
        const data = TransactionRequest.applyOperator(journal);
        if (data) {
          if (!data._id && journal.target) data._id = journal.target;
          data.csn = journal.csn;
          row.data = data;
        }
      }
      return {
        status: "OK",
        list: [row],
      };
    }
  }

  async getCommittedCsn(): Promise<number> {
    let csn = await this.context.getSystemDb().getCsn();
    if (csn == 0) return 0;
    let journal = await this.context.getJournalDb().findByCsn(csn);
    if (!journal) return 0;
    if (journal.committedCsn !== undefined) return journal.committedCsn;
    return csn;
  }
}

/**
 * コンテキストマネージャ(ContextManager)
 *
 * コンテキストマネージャは、逆接続プロキシの Rest API で exec メソッドを提供する。
 */
export class ContextManager extends ServiceEngine {

  public bootOrder = 20;
  public committedCsn?: number;
  private logger: Logger;
  private option: ContextManagerConfigDef;
  private node: ResourceNode;
  private database: string;
  private journalDb: JournalDb;
  private systemDb: SystemDb;
  private subscriber: TransactionJournalSubscriber;
  private server: ContextManagementServer;
  private mountHandle?: string;
  private lock: AsyncLock;
  private subscriberKey: string | null;
  private uniqueIndexes: IndexDef[];
  private digestMap: Map<number, string> = new Map();

  constructor(option: ContextManagerConfigDef) {
    super(option);
    this.logger = Logger.getLogger("ContextManager", option.database);
    this.logger.debug(LOG_MESSAGES.CREATED, ["ContextManager"]);
    this.option = option;
    this.lock = new AsyncLock();
  }

  getNode(): ResourceNode {
    return this.node;
  }

  getDatabase(): string {
    return this.database;
  }

  getJournalDb(): JournalDb {
    return this.journalDb;
  }

  getSystemDb(): SystemDb {
    return this.systemDb;
  }

  getLock(): AsyncLock {
    return this.lock;
  }

  getMountHandle(): string | undefined {
    return this.mountHandle;
  }

  getDigestMap() {
    return this.digestMap;
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug(LOG_MESSAGES.STARTING, ["ContextManager"]);

    if (!this.option.database) {
      throw new DadgetError(ERROR.E2001, ["Database name is missing."]);
    }
    if (this.option.database.match(/--/)) {
      throw new DadgetError(ERROR.E2001, ["Database name can not contain '--'."]);
    }
    if (this.option.database.match(/__/)) {
      throw new DadgetError(ERROR.E2001, ["Database name can not contain '__'."]);
    }
    if (this.option.database.match(/==/)) {
      throw new DadgetError(ERROR.E2001, ["Database name can not contain '=='."]);
    }
    this.database = this.option.database;

    // サブセットの定義を取得する
    const seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length !== 1) {
      throw new DadgetError(ERROR.E2001, ["DatabaseRegistry is missing, or there are multiple ones."]);
    }
    const registry = seList[0] as DatabaseRegistry;
    const metaData = registry.getMetadata();
    this.uniqueIndexes = (metaData.indexes || []).filter((val) => val.property && val.property.unique);

    // ストレージを準備
    this.journalDb = new JournalDb(new PersistentDb(this.database));
    this.systemDb = new SystemDb(new PersistentDb(this.database));
    let promise = Promise.all([this.journalDb.start(), this.systemDb.start()]).then((_) => { });

    // スレーブ動作で同期するのためのサブスクライバを登録
    this.subscriber = new TransactionJournalSubscriber(this);
    promise = promise.then(() => {
      setTimeout(() => {
        node.subscribe(CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), this.subscriber)
          .then((key) => { this.subscriberKey = key; });
      }, KEEP_TIME_AFTER_CONTEXT_MANAGER_MASTER_ACQUIRED_MS + 1000);
    });
    promise = promise.then(() => {
      // コンテキストマネージャのRestサービスを登録
      this.server = new ContextManagementServer(this);
      this.connect();
    });

    promise = promise.then(() => {
      this.logger.debug(LOG_MESSAGES.STARTED, ["ContextManager"]);
    });
    return promise;
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
      .then(() => {
        if (this.mountHandle) { return this.node.unmount(this.mountHandle).catch(e => console.warn(e)); }
      })
      .then(() => {
        if (this.subscriberKey) { return this.node.unsubscribe(this.subscriberKey).catch(e => console.warn(e)); }
      });
  }

  connect() {
    this.node.mount(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database), "singletonMaster", this.server, {
      onDisconnect: () => {
        this.logger.info(LOG_MESSAGES.DISCONNECTED, ["ContextManagementServer"]);
        this.mountHandle = undefined;
        this.server.resetLastBeforeObj();
      },
      onRemount: (mountHandle: string) => {
        this.logger.info(LOG_MESSAGES.REMOUNTED, ["ContextManagementServer"]);
        this.server.resetLastBeforeObj();
        this.procAfterContextManagementServerConnect(mountHandle);
      },
    })
      .then((mountHandle) => {
        // マスターを取得した場合のみ実行される
        this.logger.info(LOG_MESSAGES.CONNECTED, ["ContextManagementServer"]);
        this.procAfterContextManagementServerConnect(mountHandle);
      })
      .catch((err) => {
        this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [107]);
        process.exit(1);
      });
  }

  private procAfterContextManagementServerConnect(mountHandle: string) {
    this.mountHandle = mountHandle;
    this.getLock().acquire(MASTER_LOCK, () => {
      return new Promise<void>((resolve) => {
        setTimeout(resolve, KEEP_TIME_AFTER_CONTEXT_MANAGER_MASTER_ACQUIRED_MS);
      })
        .then(() => {
          return this.systemDb.getCsn()
            .then((csn) => {
              return this.journalDb.findByCsn(csn)
                .then((tr) => {
                  const protectedCsn = this.getJournalDb().getProtectedCsn();
                  this.getJournalDb().setProtectedCsn(Math.min(protectedCsn, csn));

                  if (tr && tr.committedCsn !== undefined && ![
                    TransactionType.ABORT,
                    TransactionType.ABORT_IMPORT,
                    TransactionType.ABORT_RESTORE].includes(tr.type)) {
                    csn = tr.committedCsn;
                  }
                  this.committedCsn = undefined;

                  const transaction = new TransactionObject();
                  if (tr) {
                    transaction.digest = tr.digest;
                  }
                  transaction.csn = csn;
                  transaction.type = TransactionType.FORCE_ROLLBACK;
                  // As this is a master, other replications must be rollback.
                  this.getNode().publish(
                    CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.getDatabase())
                    , EJSON.stringify(transaction)
                  ).catch((err) => {
                    this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [105]);
                  });
                });
            })
            .then(() => {
              return new Promise<void>((resolve) => {
                setTimeout(resolve, KEEP_TIME_AFTER_SENDING_ROLLBACK_MS);
              });
            });
        })
        .catch((err) => {
          this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [106]);
          throw err;
        });
    });
  }

  checkUniqueConstraint(csn: number, request: TransactionRequest, options?: ExecOptions): Promise<object | undefined> {
    if (request.type === TransactionType.TRUNCATE) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.BEGIN) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.END) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.ABORT) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.BEGIN_IMPORT) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.END_IMPORT) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.ABORT_IMPORT) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.BEGIN_RESTORE) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.END_RESTORE) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.ABORT_RESTORE) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.FORCE_ROLLBACK) { return Promise.resolve(undefined); }
    if (request.type === TransactionType.RESTORE && request.new) {
      const newObj = TransactionRequest.getNew(request);
      if (newObj.hasOwnProperty("_id") || newObj.hasOwnProperty("csn")) {
        throw new DadgetError(ERROR.E2002, ["new object must not contain _id or csn"]);
      }
      return Promise.resolve(newObj);
    }
    if (request.type === TransactionType.INSERT && request.new) {
      const newObj = TransactionRequest.getNew(request);
      if (newObj.hasOwnProperty("_id") || newObj.hasOwnProperty("csn")) {
        throw new DadgetError(ERROR.E2002, ["new object must not contain _id or csn"]);
      }
      if ((typeof request.new === "string" ? request.new : serialize(request.new)).length >= MAX_OBJECT_SIZE) {
        throw new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]);
      }
      if (this.uniqueIndexes.length !== 1 &&
        (options?.upsertOnUniqueError || options?.replaceOnUniqueError)) {
        throw new DadgetError(ERROR.E2012);
      }
      return this._checkUniqueConstraint(csn, newObj)
        .then(() => Promise.resolve(newObj))
        .catch((error) => {
          if (error instanceof UniqueError && (options?.upsertOnUniqueError || options?.replaceOnUniqueError)) {
            const beforeObj = error.obj as any;
            request.target = beforeObj._id;
            request.before = beforeObj;
            request.type = options?.upsertOnUniqueError ? TransactionType.UPSERT : TransactionType.REPLACE;
            const updateObj = TransactionRequest.applyOperator(request, beforeObj, newObj);
            if (serialize(updateObj).length >= MAX_OBJECT_SIZE) {
              throw new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]);
            }
            return this._checkUniqueConstraint(csn, updateObj, request.target, beforeObj)
              .then(() => Promise.resolve(updateObj));
          } else {
            throw error;
          }
        });
    } else if (request.type === TransactionType.UPDATE) {
      return (request.before ? Promise.resolve() : this._find(csn, request.target)
        .then((result) => {
          if (!result) {
            throw new DadgetError(ERROR.E1104);
          } else {
            request.before = result;
          }
        }))
        .then(() => {
          const before = TransactionRequest.getBefore(request);
          const newObj = TransactionRequest.applyOperator(request, before);
          if (serialize(newObj).length >= MAX_OBJECT_SIZE) {
            throw new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]);
          }
          return this._checkUniqueConstraint(csn, newObj, request.target, before)
            .then(() => Promise.resolve(newObj));
        })
    } else if ((request.type === TransactionType.UPSERT || request.type === TransactionType.REPLACE) && request.new) {
      const newObj = TransactionRequest.getNew(request);
      if (newObj.hasOwnProperty("_id") || newObj.hasOwnProperty("csn")) {
        throw new DadgetError(ERROR.E2002, ["new object must not contain _id or csn"]);
      }
      if ((typeof request.new === "string" ? request.new : serialize(request.new)).length >= MAX_OBJECT_SIZE) {
        throw new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]);
      }
      return this._find(csn, request.target)
        .then((beforeObj) => {
          if (beforeObj) {
            request.before = beforeObj;
            const updateObj = TransactionRequest.applyOperator(request, beforeObj, newObj);
            if (serialize(updateObj).length >= MAX_OBJECT_SIZE) {
              throw new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]);
            }
            return [updateObj, newObj];
          } else {
            return [newObj, undefined];
          }
        }).then(([obj, before]) => {
          return this._checkUniqueConstraint(csn, obj!, request.target, before)
            .then(() => Promise.resolve(obj));
        })
    } else if (request.type === TransactionType.DELETE) {
      return (request.before ? Promise.resolve() : this._find(csn, request.target)
        .then((result) => {
          if (!result) {
            throw new DadgetError(ERROR.E1104);
          } else {
            request.before = result;
          }
        }))
        .then(() => {
          const before = TransactionRequest.getBefore(request);
          return Promise.resolve(before);
        })
    } else {
      throw new Error("checkConsistent error");
    }
  }

  private _find(csn: number, id?: string): Promise<object | null> {
    return Dadget._query(this.getNode(), this.database, { _id: id }, undefined, undefined, undefined, csn, "strict")
      .then((result) => {
        if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
        if (result.resultSet.length === 0) { return null; }
        return result.resultSet[0];
      });
  }

  private _checkUniqueConstraint(csn: number, obj: { [field: string]: any }, exceptId?: string, before?: { [field: string]: any }): Promise<void> {
    const loopData = { count: 0 };
    return Util.promiseWhile<{ count: number }>(
      loopData,
      (loopData) => {
        return loopData.count < this.uniqueIndexes.length;
      },
      (loopData) => {
        const indexDef = this.uniqueIndexes[loopData.count++];
        const condition: { [field: string]: any }[] = [];
        for (const field in indexDef.index) {
          if (!indexDef.index.hasOwnProperty(field)) { continue; }
          const val = obj[field];
          if (val === undefined || val === null) {
            if (indexDef.required) {
              throw new DadgetError(ERROR.E2013, [field]);
            } else {
              return Promise.resolve(loopData);
            }
          } else {
            condition.push({ [field]: val });
          }
        }
        if (Object.keys(condition).length == 0) { return Promise.resolve(loopData); }
        if (exceptId) {
          condition.push({ _id: { $ne: exceptId } });
        }

        const fields = Object.keys(indexDef.index);
        const val = UniqueCache._convertKey(fields, obj);;
        const beforeVal = UniqueCache._convertKey(fields, before);;
        if (!val || val === beforeVal) { return Promise.resolve(loopData); }
        const seList = this.getNode().searchServiceEngine("UniqueCache", { database: this.database, field: fields.join(',') });
        if (seList.length >= 1) {
          const se = seList[0] as UniqueCache;
          return se.has(val, csn)
            .then((result) => {
              if (!result) { return loopData; }
              // 検索不可の場合もtrueを返すので、再検索が必要
              return Dadget._query(this.getNode(), this.database, { $and: condition }, undefined, undefined, undefined, csn, "strict")
                .then((result) => {
                  if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
                  if (result.resultSet.length === 0) { return loopData; }
                  throw new UniqueError("duplicate data error: " + JSON.stringify(condition), result.resultSet[0]);
                });
            });
        }

        return Dadget._query(this.getNode(), this.database, { $and: condition }, undefined, undefined, undefined, csn, "strict")
          .then((result) => {
            if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
            if (result.resultSet.length === 0) { return loopData; }
            throw new UniqueError("duplicate data error: " + JSON.stringify(condition), result.resultSet[0]);
          });
      },
    ).then(() => { return; });
  }

  updateUniqueCache(transaction: TransactionObject) {
    const seList = this.getNode().searchServiceEngine("UniqueCache", { database: this.getDatabase() }) as UniqueCache[];
    for (const se of seList) {
      se.procTransaction(transaction);
    }
  }

  resetUniqueCache(csn: number) {
    const seList = this.getNode().searchServiceEngine("UniqueCache", { database: this.getDatabase() }) as UniqueCache[];
    for (const se of seList) {
      se.resetData(csn, true);
    }
  }
  _waitSubsetStorage(result: any): Promise<any> {
    if (result.csn) {
      return Dadget._wait(this.getNode(), this.database, result.csn).then(() => result)
    }
    return Promise.resolve(result);
  }
}

export class ExecOptions {
  /**
   * ユニーク制約エラーの場合にスキップして処理を継続する。（_idは不可）
   */
  continueOnUniqueError?: boolean;

  /**
   * ユニーク制約エラーの場合にupsertを実行する
   */
  upsertOnUniqueError?: boolean;

  /**
  * ユニーク制約エラーの場合にreplaceを実行する
  */
  replaceOnUniqueError?: boolean;

  static continueOnError(options: ExecOptions | undefined, e: any) {
    if (options?.continueOnUniqueError) {
      if (e instanceof UniqueError) return true;
    }
    return false;
  }
}

export class TransactionUpdateDetail {
  type: TransactionType;
  target: string;
  csn: number;
  data?: object;
}