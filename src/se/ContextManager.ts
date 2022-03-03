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
import Dadget from "./Dadget";
import { DatabaseRegistry, IndexDef } from "./DatabaseRegistry";

const KEEP_TIME_AFTER_CONTEXT_MANAGER_MASTER_ACQUIRED_MS = 3000; // 3000ms
const KEEP_TIME_AFTER_SENDING_ROLLBACK_MS = 1000; // 1000ms
const CHECK_POINT_CHECK_PERIOD_MS = 10 * 60 * 1000;  // 10 minutes
const CHECK_POINT_DELETE_PERIOD_MS = 72 * 60 * 60 * 1000; // 72 hours
const MAX_RESPONSE_SIZE_OF_JOURNALS = 10485760;
export const ATOMIC_OPERATION_MAX_LOCK_TIME = 10 * 60 * 1000;  // 10 minutes
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
                if (transaction.csn === 0 || tr && tr.digest === transaction.digest) {
                  return this.context.getSystemDb().updateCsn(transaction.csn);
                } else {
                  return this.adjustData(transaction.csn);
                }
              })
              .then(() => this.context.getJournalDb().retrieveCheckCsn());
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
                  .then(() => this.context.getSystemDb().updateCsn(transaction.csn));
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

  adjustData(csn: number): Promise<any> {
    this.logger.warn(LOG_MESSAGES.ADJUST_DATA, [], [csn]);
    let csnUpdated = false;
    const loopData = { csn };
    return Util.promiseWhile<{ csn: number }>(
      loopData,
      (loopData) => {
        return loopData.csn !== 0;
      },
      (loopData) => {
        return this.context.getJournalDb().findByCsn(loopData.csn)
          .then((journal) => {
            return this.fetchJournal(loopData.csn)
              .then((fetchJournal) => {
                if (!fetchJournal) { return { ...loopData, csn: 0 }; }
                let promise = Promise.resolve();
                if (!csnUpdated) {
                  promise = promise.then(() => this.context.getSystemDb().updateCsn(csn));
                  csnUpdated = true;
                }
                return promise.then(() => {
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
              });
          });
      },
    )
      .catch((err) => {
        this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [103]);
      });
  }
}

class ContextManagementServer extends Proxy {
  private logger: Logger;
  private lastBeforeObj?: { _id?: string, csn?: number };
  private lastCheckPointTime: number = 0;
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
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_EXEC) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        const csn = ProxyHelper.validateNumberRequired(data.csn, "csn");
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_EXEC, [], [csn]);
        return this.exec(csn, data.request, data.atomicId, data.options)
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
        return this.execMany(csn, data.requests, data.atomicId, data.options)
          .catch((reason) => ({ status: "NG", reason }));
      })
        .then((result) => {
          this.logger.info(LOG_MESSAGES.TIME_OF_EXEC, [], [Date.now() - time]);
          return result;
        });
    } else if (url.pathname.endsWith(CORE_NODE.PATH_UPDATE_MANY) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (data) => {
        this.logger.info(LOG_MESSAGES.ON_RECEIVE_UPDATE_MANY, [JSON.stringify(data.query), JSON.stringify(data.operator)], []);
        return this.updateMany(data.query, data.operator, data.atomicId)
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
    } else {
      this.logger.warn(LOG_MESSAGES.SERVER_COMMAND_NOT_FOUND, [method, url.pathname]);
      return ProxyHelper.procError(req, res);
    }
  }

  exec(postulatedCsn: number, request: TransactionRequest, atomicId?: string, options?: ExecOptions): Promise<object> {
    const err = this.checkTransactionRequest(request);
    if (err) {
      return Promise.resolve({
        status: "NG",
        reason: new DadgetError(ERROR.E2002, [err]),
      });
    }

    if (request.type === TransactionType.CHECK) {
      if (this.atomicLockId === atomicId) {
        this.setTransactionTimeout();
        return Promise.resolve({
          status: "OK",
        });
      } else {
        return Promise.resolve({
          status: "NG",
          reason: "transaction mismatch",
        });
      }
    }

    if (this.atomicLockId && this.atomicLockId !== atomicId) {
      return new Promise<object>((resolve, reject) => {
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
          return this.procTransaction(postulatedCsn, request, atomicId);
        }).then(({ transaction, newCsn, updateObject }) => {
          this.checkProtectedCsn();
          if (updateObject) {
            if (!updateObject._id) { updateObject._id = transaction.target; }
            updateObject.csn = newCsn;
          }
          this.lastBeforeObj = updateObject;
          this.notifyAllWaitingList();
          resolve({
            status: "OK",
            csn: newCsn,
            updateObject,
          });
        }, (reason) => {
          this.notifyAllWaitingList();
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
      });
    });
  }

  execMany(postulatedCsn: number, requests: TransactionRequest[], atomicId?: string, options?: ExecOptions): Promise<object> {
    for (const request of requests) {
      const err = this.checkTransactionRequest(request);
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

    if (this.atomicLockId && this.atomicLockId !== atomicId) {
      return new Promise<object>((resolve, reject) => {
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
                return this.procTransaction(0, request, atomicId)
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
            return this.procTransaction(postulatedCsn, { type: TransactionType.BEGIN, target: "" }, atomicId)
              .then(({ newCsn }) => {
                _newCsn = newCsn;
                return Util.promiseEach<TransactionRequest>(
                  requests,
                  (request) => {
                    return this.procTransaction(postulatedCsn, request, atomicId)
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
          this.notifyAllWaitingList();
          resolve({
            status: "OK",
            csn: _newCsn,
          });
        }, (reason) => {
          this.notifyAllWaitingList();
          let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason]);
          if (cause.code === ERROR.E1105.code) { cause = new DadgetError(ERROR.E2004, [cause]); }
          cause.convertInsertsToString();
          this.logger.warn(LOG_MESSAGES.ERROR_CAUSE, [cause.toString()]);
          resolve({
            status: "NG",
            csn: _newCsn,
            reason: cause,
          });
        });
      });
    });
  }

  updateMany(query: object, operator: object, atomicId?: string): Promise<object> {
    if (this.atomicLockId && this.atomicLockId !== atomicId) {
      return new Promise<object>((resolve, reject) => {
        this.queueWaitingList.push(() => {
          this.updateMany(query, operator, atomicId)
            .then((result) => resolve(result)).catch((reason) => reject(reason));
        });
      });
    }

    return new Promise((resolve, reject) => {
      this.context.getLock().acquire(MASTER_LOCK, () => {
        let _newCsn: number;
        let count = 0;
        return this.context.getLock().acquire(TRANSACTION_LOCK, () => {
          return this.context.getSystemDb().getCsn()
            .then((currentCsn) => {
              return Dadget._query(this.context.getNode(), this.context.getDatabase(), query,
                undefined, undefined, undefined, currentCsn, "strict", { _id: 1 })
                .then((result) => {
                  if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
                  if (this.atomicLockId) {
                    return Util.promiseEach<object>(
                      result.resultSet,
                      (row) => {
                        return Dadget._query(this.context.getNode(), this.context.getDatabase(),
                          { _id: (row as any)._id }, undefined, 1, undefined, currentCsn, "strict")
                          .then((result2) => {
                            if (result2.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
                            if (result2.resultSet.length === 0) { throw new Error("Update object not found"); }
                            const obj = result2.resultSet[0];
                            const request = new TransactionRequest();
                            request.type = TransactionType.UPDATE;
                            request.target = (obj as any)._id;
                            request.before = obj;
                            request.operator = operator;
                            count++;
                            return this.procTransaction(0, request, atomicId)
                              .then(({ newCsn }) => {
                                _newCsn = newCsn;
                              });
                          });
                      },
                    );
                  } else {
                    atomicId = Dadget.uuidGen();
                    return this.procTransaction(0, { type: TransactionType.BEGIN, target: "" }, atomicId)
                      .then(({ newCsn }) => {
                        _newCsn = newCsn;
                        return Util.promiseEach<object>(
                          result.resultSet,
                          (row) => {
                            return Dadget._query(this.context.getNode(), this.context.getDatabase(),
                              { _id: (row as any)._id }, undefined, 1, undefined, currentCsn, "strict")
                              .then((result2) => {
                                if (result2.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
                                if (result2.resultSet.length === 0) { throw new Error("Update object not found"); }
                                const obj = result2.resultSet[0];
                                const request = new TransactionRequest();
                                request.type = TransactionType.UPDATE;
                                request.target = (obj as any)._id;
                                request.before = obj;
                                request.operator = operator;
                                count++;
                                return this.procTransaction(0, request, atomicId)
                                  .then(({ newCsn }) => {
                                    _newCsn = newCsn;
                                  });
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
                });
            });
        }).then(() => {
          this.checkProtectedCsn();
          this.lastBeforeObj = undefined;
          this.notifyAllWaitingList();
          resolve({
            status: "OK",
            csn: _newCsn,
            count,
          });
        }, (reason) => {
          this.notifyAllWaitingList();
          let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason]);
          if (cause.code === ERROR.E1105.code) { cause = new DadgetError(ERROR.E2004, [cause]); }
          cause.convertInsertsToString();
          this.logger.warn(LOG_MESSAGES.ERROR_CAUSE, [cause.toString()]);
          resolve({
            status: "NG",
            csn: _newCsn,
            reason: cause,
          });
        });
      });
    });
  }

  private procTransaction(postulatedCsn: number, request: TransactionRequest, atomicId?: string) {
    let transaction: TransactionObject;
    let newCsn: number;
    let updateObject: { _id?: string, csn?: number } | undefined;
    const _request = { ...request, datetime: new Date() };
    if (this.lastBeforeObj && request.before
      && (!request.before._id || this.lastBeforeObj._id === request.before._id)) {
      const objDiff = Util.diff(this.lastBeforeObj, request.before);
      if (objDiff) {
        this.logger.error(LOG_MESSAGES.REQUEST_BEFORE_HAS_MISMATCH, [JSON.stringify(objDiff)]);
        throw new DadgetError(ERROR.E2005, [JSON.stringify(request)]);
      } else {
        this.logger.debug(LOG_MESSAGES.LASTBEFOREOBJ_CHECK_PASSED);
      }
    }
    return this.context.getJournalDb().checkConsistent(postulatedCsn, _request)
      .then(() => this.context.getSystemDb().getCsn())
      .then((currentCsn) => this.context.checkUniqueConstraint(currentCsn, _request))
      .then((_) => {
        updateObject = _;
        return Promise.all([this.context.getSystemDb().getCsn(), this.context.getJournalDb().getLastDigest()])
          .then((values) => {
            newCsn = values[0] + 1;
            if (request.type === TransactionType.FORCE_ROLLBACK) { newCsn = 0; }
            this.logger.info(LOG_MESSAGES.EXEC_NEWCSN, [], [newCsn]);
            const lastDigest = values[1];
            transaction = { ..._request, csn: newCsn, beforeDigest: lastDigest };
            transaction.digest = TransactionObject.calcDigest(transaction);
            transaction.protectedCsn = this.context.getJournalDb().getProtectedCsn();
            return this.procTransactionCtrl(transaction, newCsn, atomicId);
          })
          .then(() => Promise.all([
            this.context.getJournalDb().insert(transaction),
            this.context.getSystemDb().updateCsn(newCsn),
            EJSON.stringify(transaction),
          ]));
      })
      .then(([a, b, pubData]) => {
        if (request.type !== TransactionType.FORCE_ROLLBACK && transaction.digest) {
          this.context.getDigestMap().set(transaction.csn, transaction.digest);
        }
        this.context.getJournalDb().setCheckCsnByTransaction(transaction);
        this.pubDataList.push(pubData);
        if (!this.context.getLock().isBusy(PUBLISH_LOCK)) {
          this.context.getLock().acquire(PUBLISH_LOCK, () => {
            return Util.promiseWhile<string[]>(
              this.pubDataList,
              (data) => {
                return data.length !== 0;
              },
              (data) => {
                const pubData = data.shift();
                if (!pubData) { throw new Error("empty data error"); }
                return this.context.getNode().publish(
                  CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase()), pubData)
                  .then(() => this.pubDataList);
              });
          });
        }
        return { transaction, newCsn, updateObject };
      });
  }

  private setTransactionTimeout() {
    if (this.atomicTimer) { clearTimeout(this.atomicTimer); }
    this.atomicTimer = setTimeout(() => {
      this.atomicTimer = undefined;
      this.logger.warn(LOG_MESSAGES.TRANSACTION_TIMEOUT);
      this.procTransaction(0, { type: TransactionType.ABORT, target: "" }, this.atomicLockId)
        .then(() => this.notifyAllWaitingList());
    }, ATOMIC_OPERATION_MAX_LOCK_TIME);
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
        this.setTransactionTimeout();
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
      this.setTransactionTimeout();
    } else if (atomicId && this.atomicLockId !== atomicId) {
      throw new DadgetError(ERROR.E2010);
    }

    if (transaction.type === TransactionType.BEGIN || transaction.type === TransactionType.BEGIN_IMPORT) {
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
        throw new DadgetError(ERROR.E2009);
      }
      transaction.committedCsn = this.context.committedCsn;
      this.atomicLockId = undefined;
      this.context.committedCsn = undefined;
      this.clearTransactionTimeout();
    }
  }

  private checkTransactionRequest(request: TransactionRequest) {
    let err: string | null = null;
    if (request.type === TransactionType.INSERT || request.type === TransactionType.RESTORE) {
      if (!request.target) { err = "target required in a transaction"; }
      if (request.before) { err = "before not required for INSERT"; }
      if (request.operator) { err = "operator not required for INSERT"; }
      if (!request.new) {
        err = "new required for INSERT";
      } else {
        const keys = Object.keys(request.new);
        if (keys.indexOf("_id") >= 0 || keys.indexOf("csn") >= 0) {
          err = "new object must not contain _id or csn";
        }
      }
    } else if (request.type === TransactionType.UPDATE) {
      if (!request.target) { err = "target required in a transaction"; }
      if (!request.before) { err = "before required for UPDATE"; }
      if (!request.operator) { err = "operator required for UPDATE"; }
      if (request.new) { err = "new not required for UPDATE"; }
    } else if (request.type === TransactionType.DELETE) {
      if (!request.target) { err = "target required in a transaction"; }
      if (!request.before) { err = "before required for DELETE"; }
      if (request.operator) { err = "operator not required for DELETE"; }
      if (request.new) { err = "new not required for DELETE"; }
    } else if (request.type === TransactionType.TRUNCATE) {
      this.logger.warn(LOG_MESSAGES.EXEC_TRUNCATE);
    } else if (request.type === TransactionType.BEGIN) {
      this.logger.warn(LOG_MESSAGES.EXEC_BEGIN);
    } else if (request.type === TransactionType.END) {
      this.logger.warn(LOG_MESSAGES.EXEC_END);
    } else if (request.type === TransactionType.ABORT) {
      this.logger.warn(LOG_MESSAGES.EXEC_ABORT);
    } else if (request.type === TransactionType.BEGIN_IMPORT) {
      this.logger.warn(LOG_MESSAGES.EXEC_BEGIN_IMPORT);
    } else if (request.type === TransactionType.END_IMPORT) {
      this.logger.warn(LOG_MESSAGES.EXEC_END_IMPORT);
    } else if (request.type === TransactionType.ABORT_IMPORT) {
      this.logger.warn(LOG_MESSAGES.EXEC_ABORT_IMPORT);
    } else if (request.type === TransactionType.BEGIN_RESTORE) {
      this.logger.warn(LOG_MESSAGES.EXEC_BEGIN_RESTORE);
    } else if (request.type === TransactionType.END_RESTORE) {
      this.logger.warn(LOG_MESSAGES.EXEC_END_RESTORE);
    } else if (request.type === TransactionType.ABORT_RESTORE) {
      this.logger.warn(LOG_MESSAGES.EXEC_ABORT_RESTORE);
    } else if (request.type === TransactionType.FORCE_ROLLBACK) {
      this.logger.warn(LOG_MESSAGES.EXEC_FORCE_ROLLBACK);
    } else if (request.type === TransactionType.CHECK) {
    } else {
      err = "type not found in a transaction: " + request.type;
    }
    return err;
  }

  private checkProtectedCsn(): void {
    if (Date.now() - this.lastCheckPointTime <= CHECK_POINT_CHECK_PERIOD_MS) { return; }
    this.lastCheckPointTime = Date.now();
    setTimeout(() => {
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
        return this.context.getJournalDb().findByCsn(loopData.csn)
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
      return node.subscribe(CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), this.subscriber)
        .then((key) => { this.subscriberKey = key; });
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
        if (this.mountHandle) { return this.node.unmount(this.mountHandle).catch(); }
      })
      .then(() => {
        if (this.subscriberKey) { return this.node.unsubscribe(this.subscriberKey).catch(); }
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

  checkUniqueConstraint(csn: number, request: TransactionRequest): Promise<object | undefined> {
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
    if (request.type === TransactionType.RESTORE && request.new) { return Promise.resolve(request.new); }
    if (request.type === TransactionType.INSERT && request.new) {
      const newObj = request.new;
      if (serialize(newObj).length >= MAX_OBJECT_SIZE) {
        throw new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]);
      }
      return this._checkUniqueConstraint(csn, newObj)
        .then(() => Promise.resolve(newObj));
    } else if (request.type === TransactionType.UPDATE && request.before) {
      const newObj = TransactionRequest.applyOperator(request);
      if (serialize(newObj).length >= MAX_OBJECT_SIZE) {
        throw new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]);
      }
      return this._checkUniqueConstraint(csn, newObj, request.target)
        .then(() => Promise.resolve(newObj));
    } else if (request.type === TransactionType.DELETE && request.before) {
      return Promise.resolve(request.before);
    } else {
      throw new Error("checkConsistent error");
    }
  }

  private _checkUniqueConstraint(csn: number, obj: { [field: string]: any }, exceptId?: string): Promise<void> {
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
          const val = typeof obj[field] === "undefined" ? null : obj[field];
          condition.push({ [field]: val });
        }
        if (exceptId) {
          condition.push({ _id: { $ne: exceptId } });
        }
        return Dadget._count(this.getNode(), this.database, { $and: condition }, csn, "strict")
          .then((result) => {
            if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
            if (result.resultCount === 0) { return loopData; }
            throw new UniqueError("duplicate data error: " + JSON.stringify(condition));
          });
      },
    ).then(() => { return; });
  }
}

export class ExecOptions {
  /**
   * ユニーク制約エラーの場合にスキップして処理を継続する。（_idは不可）
   */
  continueOnUniqueError?: boolean;

  static continueOnError(options: ExecOptions | undefined, e: any) {
    if (options?.continueOnUniqueError) {
      if (e instanceof UniqueError) return true;
    }
    return false;
  }
}
