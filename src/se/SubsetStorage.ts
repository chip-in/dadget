import * as http from "http";
import * as parser from "mongo-parse";
import * as ReadWriteLock from "rwlock";
import * as URL from "url";
import * as EJSON from "../util/Ejson";

import { Proxy, ResourceNode, ServiceEngine, Subscriber } from "@chip-in/resource-node";
import { CORE_NODE } from "../Config";
import { CacheDb } from "../db/CacheDb";
import { CsnDb } from "../db/CsnDb";
import { JournalDb } from "../db/JournalDb";
import { PersistentDb } from "../db/PersistentDb";
import { SubsetDb } from "../db/SubsetDb";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import { LogicalOperator } from "../util/LogicalOperator";
import { ProxyHelper } from "../util/ProxyHelper";
import { CsnMode, default as Dadget, QueryResult } from "./Dadget";
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry";

class UpdateProcessor extends Subscriber {

  private updateQueue: { [csn: number]: TransactionObject } = {};
  private lock: ReadWriteLock;

  constructor(
    protected storage: SubsetStorage,
    protected database: string,
    protected subsetDefinition: SubsetDef) {

    super();
    this.lock = new ReadWriteLock();
  }

  onReceive(msg: string) {
    // TODO チェックポイント
    this.storage.logger.debug("UpdateProcessor onReceive: " + msg.toString());
    const transaction = EJSON.parse(msg) as TransactionObject;
    this.lock.writeLock((release1) => {
      this.storage.logger.debug("get writeLock1");
      this.storage.getLock().writeLock((release2) => {
        this.storage.logger.debug("get writeLock2");
        this.storage.getCsnDb().getCurrentCsn()
          .then((csn) => {
            if (!this.storage.getReady()) {
              this.storage.logger.debug("release writeLock2");
              release2();
              if (csn === transaction.csn) {
                this.storage.setReady();
                this.storage.logger.debug("release writeLock1");
                release1();
              } else {
                this.resetData(transaction.csn)
                  .then(() => {
                    this.storage.logger.debug("release writeLock1");
                    release1();
                  })
                  .catch((e) => {
                    this.storage.logger.debug("release writeLock1");
                    release1();
                  });
              }
            } else if (transaction.type === TransactionType.ROLLBACK) {
              return this.storage.getJournalDb().findByCsnRange(transaction.csn + 1, Number.MAX_VALUE)
                .then((transactions) => {
                  transactions.sort((a, b) => b.csn - a.csn);
                  console.log("ROLLBACK transactions:" + JSON.stringify(transactions));
                  let promise = Promise.resolve();
                  transactions.forEach((trans) => {
                    if (trans.type === TransactionType.INSERT) {
                      promise = promise.then(() => this.storage.getSubsetDb().deleteById(trans.target));
                    } else if (trans.type === TransactionType.UPDATE && trans.before) {
                      promise = promise.then(() => this.storage.getSubsetDb().update(trans.target, trans.before as object));
                    } else if (trans.type === TransactionType.DELETE && trans.before) {
                      promise = promise.then(() => this.storage.getSubsetDb().insert(trans.before as object));
                    }
                  });
                  return promise;
                })
                .then(() => this.storage.getJournalDb().deleteAfter(transaction.csn))
                .then(() => this.storage.getCsnDb().update(transaction.csn))
                .then(() => {
                  this.storage.logger.debug("release writeLock2");
                  release2();
                  this.storage.logger.debug("release writeLock1");
                  release1();
                });
            } else if (csn >= transaction.csn) {
              this.storage.logger.debug("release writeLock2");
              release2();
              this.storage.logger.debug("release writeLock1");
              release1();
            } else {
              this.updateQueue[transaction.csn] = transaction;
              let promise = Promise.resolve();
              while (this.updateQueue[++csn]) {
                const _csn = csn;
                this.storage.logger.debug("subset csn: " + _csn);
                const transaction = this.updateQueue[_csn];
                delete this.updateQueue[_csn];

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
                promise = promise.then(() => this.storage.getCsnDb().update(_csn));
                promise = promise.then(() => {
                  for (const query of this.storage.pullQueryWaitingList(_csn)) {
                    this.storage.logger.debug("do wait query");
                    query();
                  }
                });
              }
              promise.then(() => {
                this.storage.logger.debug("release writeLock2");
                release2();
                this.storage.logger.debug("release writeLock1");
                release1();
              }).catch((e) => {
                this.storage.logger.error("UpdateProcessor Error: ", e.toString());
                this.storage.logger.debug("release writeLock2");
                release2();
                this.storage.logger.debug("release writeLock1");
                release1();
              });
            }
          })
          .catch((e) => {
            this.storage.logger.error("UpdateProcessor Error: ", e.toString());
            this.storage.logger.debug("release writeLock2");
            release2();
            this.storage.logger.debug("release writeLock1");
            release1();
          });
      });
    });
  }

  resetData(csn: number): Promise<void> {
    console.log("resetData:" + csn);
    const query = this.subsetDefinition.query ? this.subsetDefinition.query : {};
    const promise = Promise.resolve();
    return promise.then(() => Dadget._query(this.storage.getNode(), this.database, query, undefined, undefined, undefined, csn, "latest"))
      .then((result) => {
        return new Promise<void>((resolve, reject) => {
          this.storage.getLock().writeLock((release3) => {
            this.storage.logger.debug("get writeLock3");
            Promise.resolve()
              .then(() => this.storage.getJournalDb().deleteAll())
              .then(() => this.storage.getSubsetDb().deleteAll())
              .then(() => this.storage.getSubsetDb().insertMany(result.resultSet))
              .then(() => this.storage.getCsnDb().update(result.csn ? result.csn : csn))
              .then(() => {
                this.storage.logger.debug("release writeLock3");
                release3();
                this.storage.setReady();
                resolve();
              })
              .catch((e) => {
                this.storage.logger.debug("release writeLock3");
                release3();
                reject(e);
              });
          });
        });
      })
      .catch((e) => {
        this.storage.logger.error("UpdateProcessor Error: ", e.toString());
        throw e;
      });
  }
}

/**
 * サブセットストレージコンフィグレーションパラメータ
 */
export class SubsetStorageConfigDef {

  /**
   * データベース名
   */
  database: string;

  /**
   * サブセット名
   */
  subset: string;

  /**
   * true の場合はクエリハンドラを "loadBalancing" モードで登録し、外部にサービスを公開する。 false の場合はクエリハンドラを "localOnly" モードで登録する
   */
  exported: boolean;

  /**
   * サブセットストレージのタイプで persistent か cache のいずれかである
   */
  type: "persistent" | "cache";
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
  private csnDb: CsnDb;
  private mountHandle: string;
  private lock: ReadWriteLock;
  private queryWaitingList: { [csn: number]: Array<() => void> } = {};
  private subscriberKey: string | null;
  private readyFlag: boolean = false;

  constructor(option: SubsetStorageConfigDef) {
    super(option);
    this.logger.debug(JSON.stringify(option));
    this.option = option;
    this.lock = new ReadWriteLock();
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

  getCsnDb(): CsnDb {
    return this.csnDb;
  }

  getLock(): ReadWriteLock {
    return this.lock;
  }

  getReady(): boolean {
    return this.readyFlag;
  }

  setReady(): void {
    console.log("setReady");
    this.readyFlag = true;
  }

  pullQueryWaitingList(csn: number): Array<() => void> {
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
      return Promise.reject(new DadgetError(ERROR.E2401, ["Database name is missing."]));
    }
    this.database = this.option.database;
    if (!this.option.subset) {
      return Promise.reject(new DadgetError(ERROR.E2401, ["Subset name is missing."]));
    }
    this.subsetName = this.option.subset;
    this.logger.debug("subsetName: ", this.subsetName);

    // サブセットの定義を取得する
    const seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length !== 1) {
      return Promise.reject(new DadgetError(ERROR.E2401, ["DatabaseRegistry is missing, or there are multiple ones."]));
    }
    const registry = seList[0] as DatabaseRegistry;
    const metaData = registry.getMetadata();
    this.subsetDefinition = metaData.subsets[this.subsetName];

    // TODO サブセットの定義が変更されていたらリセット

    this.type = this.option.type.toLowerCase();
    if (this.type !== "persistent" && this.type !== "cache") {
      return Promise.reject(new DadgetError(ERROR.E2401, [`SubsetStorage type ${this.type} is not supported.`]));
    }

    // ストレージを準備
    const dbName = this.database + "--" + this.subsetName;
    if (this.type === "cache") {
      this.subsetDb = new SubsetDb(new CacheDb(dbName), this.subsetName, metaData.indexes);
      this.journalDb = new JournalDb(new CacheDb(dbName));
      this.csnDb = new CsnDb(new CacheDb(dbName));
    } else if (this.type === "persistent") {
      this.subsetDb = new SubsetDb(new PersistentDb(dbName), this.subsetName, metaData.indexes);
      this.journalDb = new JournalDb(new PersistentDb(dbName));
      this.csnDb = new CsnDb(new PersistentDb(dbName));
    }

    // Rest サービスを登録する。
    const mountingMode = this.option.exported ? "loadBalancing" : "localOnly";
    this.logger.debug("mountingMode: ", mountingMode);
    const listener = new UpdateProcessor(this, this.database, this.subsetDefinition);
    let promise = this.subsetDb.start();
    promise = promise.then(() => this.journalDb.start());
    promise = promise.then(() => this.csnDb.start());
    promise = promise.then(() =>
      node.mount(CORE_NODE.PATH_SUBSET
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), mountingMode, this),
    ).then((value) => {
      this.mountHandle = value;
    });
    promise = promise.then(() =>
      node.subscribe(CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), listener).then((key) => { this.subscriberKey = key; }),
    );

    this.logger.debug("SubsetStorage is started");
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

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) { throw new Error("url is required."); }
    if (!req.method) { throw new Error("method is required."); }
    const url = URL.parse(req.url);
    if (url.pathname == null) { throw new Error("pathname is required."); }
    this.logger.debug(url.pathname);
    const method = req.method.toUpperCase();
    this.logger.debug(method);
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith("/query") && method === "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        this.logger.debug("/query");
        const request = EJSON.parse(data);
        return this.query(request.csn, request.query, request.sort, request.limit, request.csnMode)
          .then((result) => {
            console.dir(result);
            return { status: "OK", result };
          });
      });
    } else {
      this.logger.debug("server command not found!:" + url.pathname);
      return ProxyHelper.procError(req, res);
    }
  }

  query(csn: number, query: object, sort?: object, limit?: number, csnMode?: CsnMode): Promise<QueryResult> {
    if (!this.readyFlag) {
      return Promise.resolve({ csn, resultSet: [], restQuery: query, csnMode });
    }
    const innerQuery = LogicalOperator.getInsideOfCache(query, this.subsetDefinition.query);
    if (!innerQuery) {
      return Promise.resolve({ csn, resultSet: [], restQuery: query, csnMode });
    }
    const restQuery = LogicalOperator.getOutsideOfCache(query, this.subsetDefinition.query);
    let release: () => void;
    const promise = new Promise<void>((resolve, reject) => {
      this.getLock().readLock((_) => {
        this.logger.debug("get readLock");
        release = () => {
          this.logger.debug("release readLock");
          _();
        };
        resolve();
      });
    });
    // TODO csn がチェックポイント未満はエラー
    return promise
      .then(() => this.getCsnDb().getCurrentCsn())
      .then((currentCsn) => {
        if (csn === 0 || csn === currentCsn || (csn < currentCsn && csnMode === "latest")) {
          return this.getSubsetDb().find(innerQuery, sort, limit)
            .then((result) => {
              release();
              return { csn: currentCsn, resultSet: result, restQuery };
            });
        } else if (csn < currentCsn) {
          this.logger.debug("rollback transactions", String(csn), String(currentCsn));
          // rollback transactions
          return this.getJournalDb().findByCsnRange(csn + 1, currentCsn)
            .then((transactions) => {
              if (transactions.length !== currentCsn - csn) {
                release();
                this.logger.debug("not enough rollback transactions");
                return { csn, resultSet: [], restQuery: query };
              }
              const possibleLimit = typeof limit === "undefined" ? limit : limit + transactions.length;
              return this.getSubsetDb().find(innerQuery, sort, possibleLimit)
                .then((result) => {
                  release();
                  result = SubsetStorage.rollbackAndFind(result, transactions, innerQuery, sort, limit);
                  return { csn, resultSet: result, restQuery };
                });
            });
        } else {
          this.logger.debug("wait for transactions", String(csn), String(currentCsn));
          // wait for transactions
          return new Promise<QueryResult>((resolve, reject) => {
            if (!this.queryWaitingList[csn]) { this.queryWaitingList[csn] = []; }
            this.queryWaitingList[csn].push(() => {
              return this.getSubsetDb().find(innerQuery, sort, limit)
                .then((result) => {
                  resolve({ csn, resultSet: result, restQuery });
                });
            });
            release();
          });
        }
      }).catch((e) => {
        this.logger.debug("SubsetStorage query Error: " + e.toString());
        release();
        return Promise.reject(e);
      });
  }

  static rollbackAndFind(orgList: any[], transactions: TransactionObject[], query: object, sort?: object, limit?: number): any[] {
    transactions.sort((a, b) => b.csn - a.csn);
    console.log("rollbackAndFind transactions:" + JSON.stringify(transactions));

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
    let list = parser.search(dataList, query, sort) as object[];
    if (limit) {
      list = list.slice(0, limit);
    }
    console.log("rollbackAndFind:" + JSON.stringify(list));
    return list;
  }
}
