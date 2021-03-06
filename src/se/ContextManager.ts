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
import { DadgetError } from "../util/DadgetError";
import * as EJSON from "../util/Ejson";
import { ProxyHelper } from "../util/ProxyHelper";
import { Util } from "../util/Util";
import Dadget from "./Dadget";
import { DatabaseRegistry, IndexDef } from "./DatabaseRegistry";

const KEEP_TIME_AFTER_CONTEXT_MANAGER_MASTER_ACQUIRED_MS = 3000; // 3000ms
const KEEP_TIME_AFTER_SENDING_ROLLBACK_MS = 1000; // 1000ms
const CHECK_POINT_CHECK_PERIOD_MS = 10 * 60 * 1000;  // 10 minutes
const CHECK_POINT_DELETE_PERIOD_MS = 72 * 60 * 60 * 1000; // 72 hours
const MAX_RESPONSE_SIZE_OF_JOURNALS = 10485760;
const ATOMIC_OPERATION_MAX_LOCK_TIME = 10 * 60 * 1000;  // 10 minutes

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

  constructor(protected context: ContextManager) {
    super();
    this.logger.category = "TransJournalSubscriber";
    this.logger.debug("TransactionJournalSubscriber is created");
  }

  onReceive(msg: string) {
    const transaction: TransactionObject = EJSON.parse(msg);
    this.logger.info("received:", transaction.type, transaction.csn);

    if (transaction.protectedCsn) {
      const protectedCsn = transaction.protectedCsn;
      if (this.context.getJournalDb().getProtectedCsn() < protectedCsn) {
        this.logger.info("CHECKPOINT protectedCsn: " + protectedCsn);
        this.context.getJournalDb().setProtectedCsn(protectedCsn);
        setTimeout(() => {
          this.context.getJournalDb().deleteBeforeCsn(protectedCsn)
            .catch((err) => {
              this.logger.error(err.toString());
            });
        });
      }
    }

    this.context.getLock().acquire("transaction", () => {
      if (transaction.type === TransactionType.ABORT_IMPORT) {
        this.logger.warn("ABORT_IMPORT:", transaction.csn);
        return this.context.getJournalDb().deleteAfterCsn(transaction.csn)
          .then(() => this.context.getSystemDb().updateCsn(transaction.csn))
          .catch((err) => {
            this.logger.error(err.toString());
          });
      } else if (transaction.type === TransactionType.ROLLBACK) {
        this.logger.warn("ROLLBACK:", transaction.csn);
        const protectedCsn = this.context.getJournalDb().getProtectedCsn();
        this.context.getJournalDb().setProtectedCsn(Math.min(protectedCsn, transaction.csn));
        return this.context.getJournalDb().findByCsn(transaction.csn)
          .then((tr) => {
            return this.context.getJournalDb().deleteAfterCsn(transaction.csn)
              .then(() => {
                if (tr && tr.digest === transaction.digest) {
                  return this.context.getSystemDb().updateCsn(transaction.csn);
                } else {
                  return this.adjustData(transaction.csn);
                }
              });
          })
          .catch((err) => {
            this.logger.error(err.toString());
          });
      } else {
        // Assume this node is a replication.
        return this.context.getSystemDb().getCsn()
          .then((csn) => {
            if (csn < transaction.csn - 1) {
              this.logger.info("forward csn:", transaction.csn - 1);
              return this.adjustData(transaction.csn - 1);
            }
          })
          .then(() => {
            return Promise.all([this.context.getJournalDb().findByCsn(transaction.csn),
            this.context.getJournalDb().findByCsn(transaction.csn - 1)]);
          })
          .then(([savedTransaction, preTransaction]) => {
            if (!savedTransaction) {
              if (preTransaction && preTransaction.digest === transaction.beforeDigest) {
                this.logger.info("insert transaction:", transaction.csn);
                return this.context.getJournalDb().insert(transaction)
                  .then(() => this.context.getSystemDb().updateCsn(transaction.csn));
              }
            }
          })
          .catch((err) => {
            this.logger.error(err.toString());
          });
      }
    });
  }

  fetchJournal(csn: number): Promise<TransactionObject | null> {
    return Util.fetchJournal(csn, this.context.getDatabase(), this.context.getNode());
  }

  adjustData(csn: number): Promise<any> {
    this.logger.warn("adjustData:" + csn);
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
        this.logger.error(err.toString());
      });
  }
}

class ContextManagementServer extends Proxy {
  private lastBeforeObj?: { _id?: string, csn?: number };
  private lastCheckPointTime: number = 0;
  private atomicLockId?: string = undefined;
  private atomicLockTime: number = 0;
  private atomicLockCsn: number = 0;

  constructor(protected context: ContextManager) {
    super();
    this.logger.category = "ContextManagementServer";
    this.logger.debug("ContextManagementServer is created");
  }

  resetLastBeforeObj() {
    this.lastBeforeObj = undefined;
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
    } else if (url.pathname.endsWith(CORE_NODE.PATH_EXEC) && method === "POST") {
      return ProxyHelper.procPost(req, res, this.logger, (request) => {
        const csn = ProxyHelper.validateNumberRequired(request.csn, "csn");
        this.logger.info(CORE_NODE.PATH_EXEC, "postulatedCsn:", csn);
        return this.exec(csn, request.request);
      });
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

  exec(postulatedCsn: number, request: TransactionRequest): Promise<object> {
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
      this.logger.warn("TRUNCATE");
    } else if (request.type === TransactionType.BEGIN_IMPORT) {
      this.logger.warn("BEGIN_IMPORT");
    } else if (request.type === TransactionType.END_IMPORT) {
      this.logger.warn("END_IMPORT");
    } else if (request.type === TransactionType.ABORT_IMPORT) {
      this.logger.warn("ABORT_IMPORT");
    } else if (request.type === TransactionType.BEGIN_RESTORE) {
      this.logger.warn("BEGIN_RESTORE");
    } else if (request.type === TransactionType.END_RESTORE) {
      this.logger.warn("END_RESTORE");
    } else if (request.type === TransactionType.ABORT_RESTORE) {
      this.logger.warn("ABORT_RESTORE");
    } else {
      err = "type not found in a transaction";
    }
    if (err) {
      return Promise.resolve({
        status: "NG",
        reason: new DadgetError(ERROR.E2002, [err]),
      });
    }

    if (this.atomicLockId) {
      if (this.atomicLockTime + ATOMIC_OPERATION_MAX_LOCK_TIME < Date.now()) {
        // over the time limit
        this.atomicLockId = undefined;
        this.atomicLockCsn = 0;
      } else if (this.atomicLockId !== request.atomicId) {
        return Promise.resolve({
          status: "NG",
          reason: new DadgetError(ERROR.E2007, []),
        });
      } else {
        this.atomicLockTime = Date.now();
      }
    }

    if (request.type === TransactionType.BEGIN_IMPORT || request.type === TransactionType.BEGIN_RESTORE) {
      this.atomicLockId = request.atomicId;
      this.atomicLockTime = Date.now();
      this.atomicLockCsn = 0;
    }
    delete request.atomicId;

    if (request.type === TransactionType.END_IMPORT ||
      request.type === TransactionType.END_RESTORE ||
      request.type === TransactionType.ABORT_RESTORE) {
      this.atomicLockId = undefined;
      this.atomicLockCsn = 0;
    }

    if (request.type === TransactionType.ABORT_IMPORT) {
      const newCsn = this.atomicLockCsn;
      this.atomicLockId = undefined;
      this.atomicLockCsn = 0;
      const transaction = new TransactionObject();
      transaction.csn = newCsn;
      transaction.type = TransactionType.ABORT_IMPORT;
      return this.context.getNode().publish(
        CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase())
        , EJSON.stringify(transaction))
        .then(() => ({
          status: "OK",
          csn: newCsn,
        }));
    }

    let transaction: TransactionObject;
    let newCsn: number;
    let updateObject: { _id?: string, csn?: number };
    return new Promise((resolve, reject) => {
      this.context.getLock().acquire("master", () => {
        return this.context.getLock().acquire("transaction", () => {
          const _request = { ...request, datetime: new Date() };
          if (this.lastBeforeObj && request.before
            && (!request.before._id || this.lastBeforeObj._id === request.before._id)) {
            const objDiff = Util.diff(this.lastBeforeObj, request.before);
            if (objDiff) {
              this.logger.error("a mismatch of request.before", JSON.stringify(objDiff));
              throw new DadgetError(ERROR.E2005, [JSON.stringify(request)]);
            } else {
              this.logger.debug("lastBeforeObj check passed");
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
                  this.logger.info("exec newCsn:", newCsn);
                  const lastDigest = values[1];
                  transaction = { ..._request, csn: newCsn, beforeDigest: lastDigest };
                  transaction.digest = TransactionObject.calcDigest(transaction);
                  transaction.protectedCsn = this.context.getJournalDb().getProtectedCsn();
                  return this.context.getJournalDb().insert(transaction);
                }).then(() => this.context.getSystemDb().updateCsn(newCsn));
            }).then(() => {
              return this.context.getNode().publish(
                CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase())
                , EJSON.stringify(transaction));
            }).then(() => {
              return this.checkProtectedCsn();
            });
        }).then(() => {
          if (!updateObject._id) { updateObject._id = transaction.target; }
          updateObject.csn = newCsn;
          this.lastBeforeObj = updateObject;
          if (request.type === TransactionType.BEGIN_IMPORT) {
            this.atomicLockCsn = newCsn - 1;
          }
          resolve({
            status: "OK",
            csn: newCsn,
            updateObject,
          });
        }, (reason) => {
          let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason]);
          if (cause.code === ERROR.E1105.code) { cause = new DadgetError(ERROR.E2004, [cause]); }
          cause.convertInsertsToString();
          this.logger.warn(cause.toString());
          resolve({
            status: "NG",
            reason: cause,
          });
        });
      });
    });
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
                return protectedCsnJournal.csn;
              } else {
                return this.context.getSystemDb().getCsn();
              }
            });
        })
        .then((protectedCsn) => {
          this.logger.info("CHECKPOINT protectedCsn: " + protectedCsn);
          this.context.getJournalDb().setProtectedCsn(protectedCsn);
          this.context.getJournalDb().deleteBeforeCsn(protectedCsn)
            .catch((err) => {
              this.logger.error(err.toString());
            });
        });
    }, 0);
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

  constructor(option: ContextManagerConfigDef) {
    super(option);
    this.logger.debug(JSON.stringify(option));
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

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug("ContextManager is starting");

    if (!this.option.database) {
      throw new DadgetError(ERROR.E2001, ["Database name is missing."]);
    }
    if (this.option.database.match(/--/)) {
      throw new DadgetError(ERROR.E2001, ["Database name can not contain '--'."]);
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

    promise = promise.then(() => { this.logger.debug("ContextManager is started"); });
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
        this.logger.info("ContextManagementServer is disconnected");
        this.mountHandle = undefined;
        this.server.resetLastBeforeObj();
      },
      onRemount: (mountHandle: string) => {
        this.logger.info("ContextManagementServer is remounted");
        this.server.resetLastBeforeObj();
        this.procAfterContextManagementServerConnect(mountHandle);
      },
    })
      .then((mountHandle) => {
        // マスターを取得した場合のみ実行される
        this.logger.info("ContextManagementServer is connected");
        this.procAfterContextManagementServerConnect(mountHandle);
      });
  }

  private procAfterContextManagementServerConnect(mountHandle: string) {
    this.mountHandle = mountHandle;
    this.getLock().acquire("master", () => {
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

                  const transaction = new TransactionObject();
                  if (tr) {
                    transaction.digest = tr.digest;
                  }
                  transaction.csn = csn;
                  transaction.type = TransactionType.ROLLBACK;
                  // As this is a master, other replications must be rollback.
                  return this.getNode().publish(
                    CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.getDatabase())
                    , EJSON.stringify(transaction));
                });
            })
            .then(() => {
              return new Promise<void>((resolve) => {
                setTimeout(resolve, KEEP_TIME_AFTER_SENDING_ROLLBACK_MS);
              });
            });
        })
        .catch((err) => {
          this.logger.error(err.toString());
          throw err;
        });
    });
  }

  checkUniqueConstraint(csn: number, request: TransactionRequest): Promise<object> {
    if (request.type === TransactionType.TRUNCATE) { return Promise.resolve({}); }
    if (request.type === TransactionType.BEGIN_IMPORT) { return Promise.resolve({}); }
    if (request.type === TransactionType.END_IMPORT) { return Promise.resolve({}); }
    if (request.type === TransactionType.ABORT_IMPORT) { return Promise.resolve({}); }
    if (request.type === TransactionType.BEGIN_RESTORE) { return Promise.resolve({}); }
    if (request.type === TransactionType.END_RESTORE) { return Promise.resolve({}); }
    if (request.type === TransactionType.ABORT_RESTORE) { return Promise.resolve({}); }
    if (request.type === TransactionType.RESTORE && request.new) { return Promise.resolve(request.new); }
    if (request.type === TransactionType.INSERT && request.new) {
      const newObj = request.new;
      if (serialize(newObj).length >= MAX_OBJECT_SIZE) {
        return Promise.reject(new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]));
      }
      return this._checkUniqueConstraint(csn, newObj)
        .then(() => Promise.resolve(newObj));
    } else if (request.type === TransactionType.UPDATE && request.before) {
      const newObj = TransactionRequest.applyOperator(request);
      if (serialize(newObj).length >= MAX_OBJECT_SIZE) {
        return Promise.reject(new DadgetError(ERROR.E2006, [MAX_OBJECT_SIZE]));
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
        const condition: Array<{ [field: string]: any }> = [];
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
            throw new Error("duplicate data error: " + JSON.stringify(condition));
          });
      },
    ).then(() => { return; });
  }
}
