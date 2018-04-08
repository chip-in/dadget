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
    context.logger.debug("TransactionJournalSubscriber is created");
  }

  onReceive(msg: string) {
    //    console.log("onReceive:", msg)
    const transaction: TransactionObject = EJSON.parse(msg);
    this.context.getLock().acquire("transaction", () => {
      if (transaction.type === TransactionType.ROLLBACK) {
        // TODO 実装
        return;
      }
      // 自分がスレーブになっていれば保存
      return this.csnDb.getCurrentCsn()
        .then((csn) => {
          if (csn < transaction.csn) {
            return this.csnDb.update(transaction.csn);
          }
          return Promise.resolve();
        })
        .then(() => {
          return this.journalDb.findByCsn(transaction.csn);
        })
        .then((savedTransaction) => {
          if (!savedTransaction) {
            // トランザクションオブジェクトをジャーナルに追加
            return this.journalDb.insert(transaction);
          } else {
            let promise = Promise.resolve();
            if (savedTransaction.digest !== transaction.digest) {
              // ダイジェストが異なる場合は更新して、それ以降でtimeがこのトランザクション以前のジャーナルを削除
              promise = promise.then(() => this.journalDb.updateAndDeleteAfter(transaction));
            }
            return promise;
          }
        })
        .catch((err) => {
          this.context.logger.error(err.toString());
        });
    });
  }
}

class ContextManagementServer extends Proxy {
  private lastBeforeObj: { _id?: string, csn?: number };

  constructor(
    protected context: ContextManager,
    protected journalDb: JournalDb,
    protected csnDb: CsnDb) {

    super();
    context.logger.debug("ContextManagementServer is created");
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) { throw new Error("url is required."); }
    if (!req.method) { throw new Error("method is required."); }
    const url = URL.parse(req.url);
    if (url.pathname == null) { throw new Error("pathname is required."); }
    this.context.logger.debug(url.pathname);
    const method = req.method.toUpperCase();
    this.context.logger.debug(method);
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith("/exec") && method === "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        this.context.logger.debug("/exec");
        const request = EJSON.parse(data);
        return this.exec(request.csn, request.request);
      });
    } else {
      this.context.logger.debug("server command not found!:" + url.pathname);
      return ProxyHelper.procError(req, res);
    }
  }

  exec(csn: number, request: TransactionRequest): Promise<{}> {
    this.context.logger.debug(`exec ${csn}`);
    let transaction: TransactionObject;
    let newCsn: number;
    let updateObject: { _id?: string, csn?: number };

    let err: string | null = null;
    if (!request.target) {
      err = "target required";
    }
    if (request.type === TransactionType.INSERT) {
      if (request.before) { err = "before not required"; }
      if (request.operator) { err = "operator not required"; }
      if (!request.new) { err = "new required"; }
    } else if (request.type === TransactionType.UPDATE) {
      if (!request.before) { err = "before required"; }
      if (!request.operator) { err = "operator required"; }
      if (request.new) { err = "new not required"; }
    } else if (request.type === TransactionType.DELETE) {
      if (!request.before) { err = "before required"; }
      if (request.operator) { err = "operator not required"; }
      if (request.new) { err = "new not required"; }
    } else {
      err = "type not found";
    }
    if (err) {
      return Promise.resolve({
        status: "NG",
        reason: new DadgetError(ERROR.E2002, [err]),
      });
    }

    // コンテキスト通番をインクリメントしてトランザクションオブジェクトを作成
    return new Promise((resolve, reject) => {
      this.context.getLock().acquire("master", () => {
        return this.context.getLock().acquire("transaction", () => {
          const _request = { ...request, datetime: new Date() };
          if (this.lastBeforeObj && request.before
            && (!request.before._id || this.lastBeforeObj._id === request.before._id)) {
            const objDiff = diff(this.lastBeforeObj, request.before);
            if (objDiff) {
              this.context.logger.error("a mismatch of request.before", JSON.stringify(objDiff));
              // throw new DadgetError(ERROR.E2005, [JSON.stringify(request)])
            } else {
              this.context.logger.debug("lastBeforeObj check passed");
            }
          }
          // ジャーナルと照合して矛盾がないかチェック
          return this.journalDb.checkConsistent(csn, _request)
            .then(() => this.context.checkUniqueConstraint(csn, _request))
            .then((_) => {
              updateObject = _;
              return Promise.all([this.csnDb.increment(), this.journalDb.getLastDigest()])
                .then((values) => {
                  newCsn = values[0];
                  const lastDigest = values[1];
                  transaction = Object.assign({
                    csn: newCsn
                    , beforeDigest: lastDigest,
                  }, _request);
                  transaction.digest = TransactionObject.calcDigest(transaction);
                  // トランザクションオブジェクトをジャーナルに追加
                  return this.journalDb.insert(transaction);
                });
            }).then(() => {
              // トランザクションオブジェクトを配信
              return this.context.getNode().publish(
                CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase())
                , EJSON.stringify(transaction));
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
          // トランザクションエラー
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
              const transaction = new TransactionObject();
              transaction.csn = csn;
              transaction.type = TransactionType.ROLLBACK;
              // マスターを取得した場合、他のサブセットを自分と同じcsnまでロールバックさせるメッセージを送信
              return this.getNode().publish(
                CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.getDatabase())
                , EJSON.stringify(transaction));
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
