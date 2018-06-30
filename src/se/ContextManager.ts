import { Proxy, ResourceNode, ServiceEngine, Subscriber } from "@chip-in/resource-node";
import * as AsyncLock from "async-lock";
import { diff } from "deep-diff";
import * as http from "http";
import * as URL from "url";
import { CORE_NODE } from "../Config";
import { CsnDb } from "../db/CsnDb";
import { JournalDb } from "../db/JournalDb";
import { PersistentDb } from "../db/PersistentDb";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import * as EJSON from "../util/Ejson";
import { ProxyHelper } from "../util/ProxyHelper";

const KEEP_TIME_AFTER_CONTEXT_MANAGER_MASTER_ACQUIRED = 3000; // 3000ms
const KEEP_TIME_AFTER_SENDING_ROLLBACK = 1000; // 1000ms
const CHECK_POINT_CHECK_PERIOD = 10 * 60 * 1000;
const CHECK_POINT_DELETE_PERIOD = 72 * 60 * 60 * 1000;

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

  constructor(
    protected context: ContextManager,
    protected journalDb: JournalDb,
    protected csnDb: CsnDb) {

    super();
    this.logger.category = "TransJournalSubscriber";
    this.logger.debug("TransactionJournalSubscriber is created");
  }

  onReceive(msg: string) {
    console.log("TransactionJournalSubscriber onReceive:", msg.toString());
    const transaction: TransactionObject = EJSON.parse(msg);
    this.logger.info("received:", transaction.type, transaction.csn);
    this.context.getLock().acquire("transaction", () => {
      if (transaction.type === TransactionType.CHECKPOINT) {
        this.logger.info("CHECKPOINT protectedCsn: " + transaction.protectedCsn);
        // TODO 実装
        return;
      }
      if (transaction.type === TransactionType.ROLLBACK) {
        this.logger.warn("ROLLBACK:", transaction.csn);
        return this.csnDb.update(transaction.csn)
          .then(() => {
            if (!transaction.digest) {
              return this.journalDb.deleteAll();
            }
            return this.journalDb.findByCsn(transaction.csn)
              .then((tr) => {
                if (!tr || tr.digest !== transaction.digest) {
                  // TODO ContextManagerのJournalは不足分を確認してチェックポイントまで取得
                  return this.journalDb.deleteAll();
                } else {
                  return this.journalDb.deleteAfter(transaction.csn);
                }
              });
          });
      }
      // Assume this node is a slave.
      return this.csnDb.getCurrentCsn()
        .then((csn) => {
          if (csn < transaction.csn) {
            this.logger.info("forward csn:", transaction.csn);
            // TODO 不足分取得
            return this.csnDb.update(transaction.csn);
          }
        })
        .then(() => {
          return this.journalDb.findByCsn(transaction.csn);
        })
        .then((savedTransaction) => {
          if (!savedTransaction) {
            this.logger.info("insert transaction:", transaction.csn);
            return this.journalDb.insert(transaction);
          } else {
            if (savedTransaction.digest !== transaction.digest) {
              // ダイジェストが異なる場合は更新して、それ以降でtimeがこのトランザクション以前のジャーナルを削除
              // TODO チェックポイントまで確認
              this.logger.warn("updateAndDeleteAfter:", transaction.csn);
              return this.journalDb.updateAndDeleteAfter(transaction);
            }
          }
        })
        .catch((err) => {
          this.logger.error(err.toString());
        });
    });
  }
}

class ContextManagementServer extends Proxy {
  private lastBeforeObj: { _id?: string, csn?: number };
  private lastCheckPointTime: number = 0;

  constructor(
    protected context: ContextManager,
    protected journalDb: JournalDb,
    protected csnDb: CsnDb) {

    super();
    this.logger.category = "ContextManagementServer";
    this.logger.debug("ContextManagementServer is created");
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
    } else if (url.pathname.endsWith("/exec") && method === "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        this.logger.debug("/exec");
        const request = EJSON.parse(data);
        return this.exec(request.csn, request.request);
      });
    } else if (url.pathname.endsWith("/journal") && method === "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        this.logger.debug("/journal");
        const request = EJSON.parse(data);
        return this.getJournal(request.csn);
      });
    } else {
      this.logger.warn("server command not found!:", method, url.pathname);
      return ProxyHelper.procError(req, res);
    }
  }

  exec(postulatedCsn: number, request: TransactionRequest): Promise<object> {
    this.logger.info("exec csn:", postulatedCsn);
    let transaction: TransactionObject;
    let newCsn: number;
    let updateObject: { _id?: string, csn?: number };

    let err: string | null = null;
    if (!request.target) {
      err = "target required in a transaction";
    }
    if (request.type === TransactionType.INSERT) {
      if (request.before) { err = "before not required on INSERT"; }
      if (request.operator) { err = "operator not required on INSERT"; }
      if (!request.new) { err = "new required on INSERT"; }
    } else if (request.type === TransactionType.UPDATE) {
      if (!request.before) { err = "before required on UPDATE"; }
      if (!request.operator) { err = "operator required on UPDATE"; }
      if (request.new) { err = "new not required on UPDATE"; }
    } else if (request.type === TransactionType.DELETE) {
      if (!request.before) { err = "before required on DELETE"; }
      if (request.operator) { err = "operator not required on DELETE"; }
      if (request.new) { err = "new not required on DELETE"; }
    } else {
      err = "type not found in a transaction";
    }
    if (err) {
      return Promise.resolve({
        status: "NG",
        reason: new DadgetError(ERROR.E2002, [err]),
      });
    }

    return new Promise((resolve, reject) => {
      this.context.getLock().acquire("master", () => {
        return this.context.getLock().acquire("transaction", () => {
          const _request = { ...request, datetime: new Date() };
          if (this.lastBeforeObj && request.before
            && (!request.before._id || this.lastBeforeObj._id === request.before._id)) {
            const objDiff = diff(this.lastBeforeObj, request.before);
            if (objDiff) {
              this.logger.error("a mismatch of request.before", JSON.stringify(objDiff));
              throw new DadgetError(ERROR.E2005, [JSON.stringify(request)]);
            } else {
              this.logger.debug("lastBeforeObj check passed");
            }
          }
          return this.journalDb.checkConsistent(postulatedCsn, _request)
            .then(() => this.csnDb.getCurrentCsn())
            .then((currentCsn) => this.context.checkUniqueConstraint(currentCsn, _request))
            .then((_) => {
              updateObject = _;
              return Promise.all([this.csnDb.increment(), this.journalDb.getLastDigest()])
                .then((values) => {
                  newCsn = values[0];
                  this.logger.info("exec newCsn:", newCsn);
                  const lastDigest = values[1];
                  transaction = Object.assign({
                    csn: newCsn
                    , beforeDigest: lastDigest,
                  }, _request);
                  transaction.digest = TransactionObject.calcDigest(transaction);
                  return this.journalDb.insert(transaction);
                });
            }).then(() => {
              return this.context.getNode().publish(
                CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase())
                , EJSON.stringify(transaction));
            }).then(() => {
              if (Date.now() - this.lastCheckPointTime > CHECK_POINT_CHECK_PERIOD) {
                this.lastCheckPointTime = Date.now();
                setTimeout(() => {
                  const time = new Date();
                  time.setMilliseconds(-CHECK_POINT_DELETE_PERIOD);
                  this.journalDb.getBeforeCheckPointTime(time)
                    .then((journal) => {
                      if (!journal) { return; }
                      return this.journalDb.getOneAfterCsn(journal.csn)
                        .then((protectedCsnJournal) => {
                          if (protectedCsnJournal) {
                            return protectedCsnJournal.csn;
                          } else {
                            return this.csnDb.getCurrentCsn();
                          }
                        })
                        .then((csn) => {
                          const checkPointTransaction = new TransactionObject();
                          checkPointTransaction.type = TransactionType.CHECKPOINT;
                          checkPointTransaction.csn = newCsn;
                          checkPointTransaction.protectedCsn = csn;
                          checkPointTransaction.digest = transaction.digest;
                          checkPointTransaction.beforeDigest = transaction.beforeDigest;
                          return this.context.getNode().publish(
                            CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase())
                            , EJSON.stringify(checkPointTransaction));
                        });
                    });
                }, 0);
              }
            });
        }).then(() => {
          if (!updateObject._id) { updateObject._id = transaction.target; }
          updateObject.csn = newCsn;
          this.lastBeforeObj = updateObject;
          resolve({
            status: "OK",
            csn: newCsn,
            updateObject,
          });
        }, (reason) => {
          let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason]);
          if (cause.code === ERROR.E1105.code) { cause = new DadgetError(ERROR.E2004, [cause]); }
          cause.convertInsertsToString();
          resolve({
            status: "NG",
            reason: cause,
          });
        });
      });
    });
  }

  getJournal(csn: number): Promise<object> {
    return this.journalDb.findByCsn(csn)
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
  private csnDb: CsnDb;
  private subscriber: TransactionJournalSubscriber;
  private server: ContextManagementServer;
  private mountHandle?: string;
  private lock: AsyncLock;
  private subscriberKey: string | null;

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
      return Promise.reject(new DadgetError(ERROR.E2001, ["Database name is missing."]));
    }
    this.database = this.option.database;

    // ストレージを準備
    this.journalDb = new JournalDb(new PersistentDb(this.database));
    this.csnDb = new CsnDb(new PersistentDb(this.database));
    let promise = Promise.all([this.journalDb.start(), this.csnDb.start()]).then((_) => { });

    // スレーブ動作で同期するのためのサブスクライバを登録
    this.subscriber = new TransactionJournalSubscriber(this, this.journalDb, this.csnDb);
    promise = promise.then(() => {
      return node.subscribe(CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), this.subscriber)
        .then((key) => { this.subscriberKey = key; });
    });
    promise = promise.then(() => {
      // コンテキストマネージャのRestサービスを登録
      this.server = new ContextManagementServer(this, this.journalDb, this.csnDb);
      this.connect();
    });

    this.logger.debug("ContextManager is started");
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

  connect(): Promise<void> {
    let promise = Promise.resolve();
    promise = promise.then(() => {
      this.node.mount(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database), "singletonMaster", this.server, {
        onDisconnect: () => {
          this.logger.info("ContextManagementServer is disconnected");
          this.mountHandle = undefined;
        },
        onRemount: (mountHandle: string) => {
          this.logger.info("ContextManagementServer is remounted");
          this.procAfterContextManagementServerConnect(mountHandle);
        },
      })
        .then((mountHandle) => {
          // マスターを取得した場合のみ実行される
          this.logger.info("ContextManagementServer is connected");
          this.procAfterContextManagementServerConnect(mountHandle);
        });
    });
    return promise;
  }

  private procAfterContextManagementServerConnect(mountHandle: string) {
    this.mountHandle = mountHandle;
    this.getLock().acquire("master", () => {
      return new Promise<void>((resolve) => {
        setTimeout(resolve, KEEP_TIME_AFTER_CONTEXT_MANAGER_MASTER_ACQUIRED);
      })
        .then(() => {
          return this.csnDb.getCurrentCsn()
            .then((csn) => {
              return this.journalDb.findByCsn(csn)
                .then((tr) => {
                  const transaction = new TransactionObject();
                  if (tr) {
                    transaction.digest = tr.digest;
                  }
                  transaction.csn = csn;
                  transaction.type = TransactionType.ROLLBACK;
                  // As this is a master, other slaves must be rollback.
                  return this.getNode().publish(
                    CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.getDatabase())
                    , EJSON.stringify(transaction));
                });
            })
            .then(() => {
              return new Promise<void>((resolve) => {
                setTimeout(resolve, KEEP_TIME_AFTER_SENDING_ROLLBACK);
              });
            });
        });
    });
  }

  checkUniqueConstraint(csn: number, request: TransactionRequest): Promise<object> {
    // TODO ユニーク制約についてはクエリーを発行して確認 前提csnはジャーナルの最新を使用しなればならない
    if (request.type === TransactionType.INSERT && request.new) {
      // TODO 追加されたオブジェクトと一意属性が競合していないかを調べる
      return Promise.resolve(request.new);
    } else if (request.type === TransactionType.UPDATE && request.before) {
      const newObj = TransactionRequest.applyOperator(request);
      return Promise.resolve(newObj);
    } else if (request.type === TransactionType.DELETE && request.before) {
      return Promise.resolve(request.before);
    } else {
      throw new Error("checkConsistent error");
    }
  }
}
