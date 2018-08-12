import * as http from "http";
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
import { CsnMode, default as Dadget, QueryResult } from "./Dadget";
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry";
import { UpdateManager } from "./UpdateManager";

class UpdateProcessor extends Subscriber {

  private lock: ReadWriteLock;

  constructor(
    protected storage: SubsetStorage,
    protected database: string,
    protected subsetDefinition: SubsetDef) {

    super();
    this.logger.category = "UpdateProcessor";
    this.lock = new ReadWriteLock();
  }

  onReceive(msg: string) {
    console.log("UpdateProcessor received: " + msg.toString());
    const transaction = EJSON.parse(msg) as TransactionObject;
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
              for (let i = csn + 1; i < transaction.csn; i++) {
                // csnが飛んでいた場合はジャーナル取得を行い、そちらから更新
                const _csn = i;
                promise = promise.then(() => this.adjustData(_csn));
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
          if (!journal) { throw new Error("journal not found: " + (transaction.csn - 1)); }
          if (journal.digest !== transaction.beforeDigest) { throw new Error("beforeDigest mismatch:" + transaction.csn); }
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
        console.log("ROLLBACK transactions:" + JSON.stringify(transactions));
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

  fetchJournal(csn: number): Promise<TransactionObject | null> {
    return Util.fetchJournal(csn, this.database, this.storage.getNode());
  }

  private adjustData(csn: number): Promise<void> {
    this.logger.warn("adjustData:" + csn);
    return Promise.resolve()
      .then(() => {
        if (this.storage.getSystemDb().isNew()) {
          return this.resetData(csn);
        } else {
          return this.storage.getJournalDb().getLastJournal()
            .then((journal) => {
              return this.fetchJournal(journal.csn)
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
                            return this.fetchJournal(journal.csn)
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
                    const loopData = {
                      csn: journal.csn,
                      to: csn,
                    };
                    return Util.promiseWhile<{ csn: number, to: number }>(
                      loopData,
                      (loopData) => {
                        return loopData.csn < loopData.to;
                      },
                      (loopData) => {
                        const nextCsn = loopData.csn + 1;
                        return this.fetchJournal(nextCsn)
                          .then((fetchJournal) => {
                            if (!fetchJournal) { throw new Error("journal not found: " + nextCsn); }
                            const subsetTransaction = UpdateManager.convertTransactionForSubset(this.subsetDefinition, fetchJournal);
                            return this.updateSubsetDb(Promise.resolve(), subsetTransaction);
                          })
                          .then(() => ({ ...loopData, csn: nextCsn }));
                      },
                    )
                      .then(() => { this.storage.setReady(); });
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
        const subsetTransaction = UpdateManager.convertTransactionForSubset(this.subsetDefinition, fetchJournal);
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
  private systemDb: SystemDb;
  private mountHandle: string;
  private lock: ReadWriteLock;
  private queryWaitingList: { [csn: number]: Array<() => Promise<any>> } = {};
  private subscriberKey: string | null;
  private readyFlag: boolean = false;
  private updateProcessor: UpdateProcessor;

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

  getSystemDb(): SystemDb {
    return this.systemDb;
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
    const dbName = this.database + "--" + this.subsetName;
    if (this.type === "cache") {
      this.subsetDb = new SubsetDb(new CacheDb(dbName), this.subsetName, metaData.indexes);
      this.journalDb = new JournalDb(new CacheDb(dbName));
      this.systemDb = new SystemDb(new CacheDb(dbName));
    } else if (this.type === "persistent") {
      this.subsetDb = new SubsetDb(new PersistentDb(dbName), this.subsetName, metaData.indexes);
      this.journalDb = new JournalDb(new PersistentDb(dbName));
      this.systemDb = new SystemDb(new PersistentDb(dbName));
    }

    this.updateProcessor = new UpdateProcessor(this, this.database, this.subsetDefinition);
    let promise = this.subsetDb.start();
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
    promise = promise.then(() =>
      node.subscribe(CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), this.updateProcessor).then((key) => { this.subscriberKey = key; }),
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
    const method = req.method.toUpperCase();
    this.logger.debug(method, url.pathname);
    if (method === "OPTIONS") {
      return ProxyHelper.procOption(req, res);
    } else if (url.pathname.endsWith(CORE_NODE.PATH_QUERY) && method === "POST") {
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
      this.logger.warn("server command not found!:", url.pathname);
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

    const protectedCsn = this.getJournalDb().getProtectedCsn();

    return promise
      .then(() => this.getSystemDb().getCsn())
      .then((currentCsn) => {
        if (csn === 0 || csn === currentCsn || (csn < currentCsn && csnMode === "latest")) {
          return this.getSubsetDb().find(innerQuery, sort, limit)
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
              const possibleLimit = limit ? limit + transactions.length : undefined;
              return this.getSubsetDb().find(innerQuery, sort, possibleLimit)
                .then((result) => {
                  release();
                  const resultSet = SubsetStorage.rollbackAndFind(result, transactions, innerQuery, sort, limit);
                  return { csn, resultSet, restQuery };
                });
            });
        } else {
          this.logger.info("wait for transactions", csn, currentCsn);
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

            let promise = Promise.resolve();
            for (let i = currentCsn + 1; i <= csn; i++) {
              const _csn = i;
              promise = promise.then(() => this.updateProcessor.fetchJournal(_csn))
                .then((journal) => { if (journal) { this.updateProcessor.procTransaction(journal); } });
            }
          });
        }
      }).catch((e) => {
        this.logger.warn("SubsetStorage query error: " + e.toString());
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
    let list = Util.mongoSearch(dataList, query, sort) as object[];
    if (limit) {
      list = list.slice(0, limit);
    }
    console.log("rollbackAndFind:" + JSON.stringify(list));
    return list;
  }
}
