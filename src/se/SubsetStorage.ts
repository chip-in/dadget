import * as http from 'http';
import * as URL from 'url'
import * as  ReadWriteLock from "rwlock"
import * as EJSON from '../util/Ejson'

import { ResourceNode, ServiceEngine, Subscriber, Proxy } from '@chip-in/resource-node'
import { SubsetDb } from '../db/SubsetDb'
import { CsnDb } from '../db/CsnDb'
import { JournalDb } from '../db/JournalDb'
import { TransactionObject, TransactionType, TransactionRequest } from '../db/Transaction'
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"
import { QueryResult, CsnMode, default as Dadget } from "./Dadget"
import { ProxyHelper } from "../util/ProxyHelper"
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { CORE_NODE } from "../Config"

class UpdateProcessor extends Subscriber {

  private updateQueue: { [csn: number]: TransactionObject } = {}
  private lock: ReadWriteLock

  constructor(
    protected storage: SubsetStorage
    , protected database: string
    , protected subsetDefinition: SubsetDef) {
    super()
    this.lock = new ReadWriteLock()
  }

  onReceive(msg: string) {
    this.storage.logger.debug("onReceive: " + msg)
    let transaction = EJSON.parse(msg) as TransactionObject;
    this.lock.writeLock(release1 => {
      this.storage.getLock().writeLock(release2 => {
        this.storage.logger.debug("get writeLock")
        this.storage.getCsnDb().getCurrentCsn()
          .then(csn => {
            if (csn == 0 && transaction.csn > 1) {
              // Assume that the csn number of a MQTT retain message is advanced.
              let query = this.subsetDefinition.query ? this.subsetDefinition.query : {}
              Promise.resolve()
                .then(() => {
                  this.storage.logger.debug("release writeLock")
                  release2()
                })
                .then(() => Dadget._query(this.storage.getNode(), this.database, query, undefined, undefined, undefined, transaction.csn, "latest")
                  .catch(e => {
                    if (e.queryResult) return e.queryResult
                    throw e
                  })
                )
                .then(result => {
                  this.storage.getLock().writeLock(release3 => {
                    Promise.resolve()
                      .then(() => this.storage.getSubsetDb().deleteAll())
                      .then(() => this.storage.getSubsetDb().insertAll(result.resultSet))
                      .then(() => this.storage.getCsnDb().update(result.csn ? result.csn : transaction.csn))
                      .then(() => {
                        this.storage.logger.debug("release writeLock")
                        release3()
                        release1()
                      })
                      .catch(e => {
                        this.storage.logger.error("UpdateProcessor Error: ", e.toString())
                        this.storage.logger.debug("release writeLock")
                        release3()
                        release1()
                      })
                  })
                })
                .catch(e => {
                  this.storage.logger.error("UpdateProcessor Error: ", e.toString())
                  this.storage.logger.debug("release writeLock")
                  release2()
                  release1()
                })
            }
            else if (csn >= transaction.csn) {
              this.storage.logger.debug("release writeLock")
              release2()
              release1()
            } else {
              this.updateQueue[transaction.csn] = transaction
              let promise = Promise.resolve()
              while (this.updateQueue[++csn]) {
                let _csn = csn
                this.storage.logger.debug("subset csn: " + _csn)
                let transaction = this.updateQueue[_csn]
                delete this.updateQueue[_csn]

                if (transaction.type == TransactionType.INSERT && transaction.new) {
                  let obj = Object.assign({ _id: transaction.target, csn: transaction.csn }, transaction.new)
                  promise = promise.then(() => this.storage.getSubsetDb().insert(obj))
                }
                else if (transaction.type == TransactionType.UPDATE && transaction.before) {
                  let updateObj = TransactionRequest.applyOperator(transaction)
                  updateObj.csn = transaction.csn
                  promise = promise.then(() => this.storage.getSubsetDb().update(updateObj))
                }
                else if (transaction.type == TransactionType.DELETE && transaction.before) {
                  let before = transaction.before
                  promise = promise.then(() => this.storage.getSubsetDb().delete(before))
                }
                else if (transaction.type == TransactionType.NONE) {
                }
                promise = promise.then(() => this.storage.getJournalDb().insert(transaction))
                promise = promise.then(() => this.storage.getCsnDb().update(_csn))
                promise = promise.then(() => {
                  for (let query of this.storage.pullQueryWaitingList(_csn)) {
                    this.storage.logger.debug("do wait query")
                    query()
                  }
                })
              }
              promise.then(() => {
                this.storage.logger.debug("release writeLock")
                release2()
                release1()
              }).catch(e => {
                this.storage.logger.error("UpdateProcessor Error: ", e.toString())
                this.storage.logger.debug("release writeLock")
                release2()
                release1()
              })
            }
          })
          .catch(e => {
            this.storage.logger.error("UpdateProcessor Error: ", e.toString())
            this.storage.logger.debug("release writeLock")
            release2()
            release1()
          })
      })
    })
  }
}

/**
 * サブセットストレージコンフィグレーションパラメータ
 */
export class SubsetStorageConfigDef {

  /**
   * データベース名
   */
  database: string

  /**
   * サブセット名
   */
  subset: string

  /**
   * true の場合はクエリハンドラを "loadBalancing" モードで登録し、外部にサービスを公開する。 false の場合はクエリハンドラを "localOnly" モードで登録する
   */
  exported: boolean

  /**
   * サブセットストレージのタイプで persistent か cache のいずれかである
   */
  type: "persistent" | "cache"
}

/**
 * サブセットストレージ(SubsetStorage)
 * 
 * サブセットストレージは、クエリハンドラと更新レシーバの機能を提供するサービスエンジンである。
 */
export class SubsetStorage extends ServiceEngine implements Proxy {

  public bootOrder = 50
  private option: SubsetStorageConfigDef
  private node: ResourceNode
  private database: string
  private subsetName: string
  private subsetDefinition: SubsetDef
  private type: string
  private subsetDb: SubsetDb
  private journalDb: JournalDb
  private csnDb: CsnDb
  private mountHandle: string
  private lock: ReadWriteLock
  private queryWaitingList: { [csn: number]: (() => void)[] } = {}
  private subscriberKey: string | null

  constructor(option: SubsetStorageConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
    this.lock = new ReadWriteLock()
  }

  getNode(): ResourceNode {
    return this.node;
  }

  getSubsetDb(): SubsetDb {
    return this.subsetDb
  }

  getJournalDb(): JournalDb {
    return this.journalDb
  }

  getCsnDb(): CsnDb {
    return this.csnDb
  }

  getLock(): ReadWriteLock {
    return this.lock
  }

  pullQueryWaitingList(csn: number): (() => void)[] {
    let list = this.queryWaitingList[csn]
    if (list) {
      delete this.queryWaitingList[csn]
      return list
    }
    return []
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    this.logger.debug("SubsetStorage is starting")

    if (!this.option.database) {
      return Promise.reject(new DadgetError(ERROR.E2401, ["Database name is missing."]));
    }
    this.database = this.option.database
    if (!this.option.subset) {
      return Promise.reject(new DadgetError(ERROR.E2401, ["Subset name is missing."]));
    }
    this.subsetName = this.option.subset
    this.logger.debug("subsetName: ", this.subsetName)

    // サブセットの定義を取得する
    let seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length != 1) {
      return Promise.reject(new DadgetError(ERROR.E2401, ["DatabaseRegistry is missing, or there are multiple ones."]));
    }
    let registry = seList[0] as DatabaseRegistry;
    let metaData = registry.getMetadata()
    this.subsetDefinition = metaData.subsets[this.subsetName];

    this.type = this.option.type.toLowerCase()
    if (this.type != "persistent" && this.type != "cache") {
      return Promise.reject(new DadgetError(ERROR.E2401, [`SubsetStorage type ${this.type} is not supported.`]));
    }

    // ストレージを準備
    let dbName = this.database + '--' + this.subsetName
    this.subsetDb = new SubsetDb(dbName, this.subsetName, metaData.indexes)
    this.journalDb = new JournalDb(dbName)
    this.csnDb = new CsnDb(dbName)

    // Rest サービスを登録する。
    let mountingMode = this.option.exported ? "loadBalancing" : "localOnly"
    this.logger.debug("mountingMode: ", mountingMode)
    let listener = new UpdateProcessor(this, this.database, this.subsetDefinition);
    let promise = this.subsetDb.start()
    promise = promise.then(() => this.journalDb.start())
    promise = promise.then(() => this.csnDb.start())
    promise = promise.then(() =>
      node.subscribe(CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), listener).then(key => { this.subscriberKey = key })
    )
    promise = promise.then(() =>
      node.mount(CORE_NODE.PATH_SUBSET
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), mountingMode, this)
    ).then(value => {
      this.mountHandle = value
    })

    this.logger.debug("SubsetStorage is started")
    return promise
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
      .then(() => {
        if (this.mountHandle) return this.node.unmount(this.mountHandle).catch()
      })
      .then(() => {
        if (this.subscriberKey) return this.node.unsubscribe(this.subscriberKey).catch()
      })
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) throw new Error("url is required.")
    if (!req.method) throw new Error("method is required.")
    let url = URL.parse(req.url)
    if (url.pathname == null) throw new Error("pathname is required.")
    this.logger.debug(url.pathname)
    let method = req.method.toUpperCase()
    this.logger.debug(method)
    if (method == "OPTIONS") {
      return ProxyHelper.procOption(req, res)
    } else if (url.pathname.endsWith("/query") && method == "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        this.logger.debug("/query")
        let request = EJSON.parse(data)
        return this.query(request.csn, request.query, request.sort, request.limit, request.offset, request.csnMode).then(result => ({ status: "OK", result: result }))
      })
    } else {
      this.logger.debug("server command not found!:" + url.pathname)
      return ProxyHelper.procError(req, res)
    }
  }

  query(csn: number, restQuery: object, sort?: object, limit?: number, offset?: number, csnMode?: CsnMode): Promise<QueryResult> {
    // TODO csn が0の場合は、最新のcsnを取得、それ以外の場合はcsnを一致させる
    if (this.type == "cache") {
      // TODO cacheの場合 とりあえず空レスポンス
      this.logger.debug("query: cache")
      return Promise.resolve({ csn: csn, resultSet: [], restQuery: restQuery , csnMode: csnMode})
    } else if (this.type == "persistent") {
      // TODO persistentの場合
      this.logger.debug("query: persistent")
      // TODO csnが0以外の場合はそのcsnに合わせる csnが変更されないようにロックが必要
      let release: () => void
      let promise = new Promise<void>((resolve, reject) => {
        this.getLock().readLock(_ => {
          this.logger.debug("get readLock")
          release = _
          resolve()
        })
      })
      let currentCsn: number
      return promise
        .then(() => this.getCsnDb().getCurrentCsn())
        .then(_ => {
          currentCsn = _
          if (csn > 1 && currentCsn == 0) {
            // Data not acquired yet
            this.logger.debug("Data not acquired yet")
            this.logger.debug("release readLock")
            release()
            return { csn: csn, resultSet: [], restQuery: restQuery }
          }
          else if (csn == 0 || csn == currentCsn || (csn < currentCsn && csnMode === "latest")) {
            return this.getSubsetDb().find(restQuery, sort, limit, offset)
              .then(result => {
                this.logger.debug("release readLock")
                release()
                return { csn: currentCsn, resultSet: result, restQuery: {} }
              })
          }
          else if (csn < currentCsn) {
            this.logger.debug("rollback transactions", String(csn), String(currentCsn))
            // rollback transactions
            // TODO 先にJournalから影響するトランザクションを取得して影響するオブジェクト件数分を多く取得
            let result: object
            return this.getSubsetDb().find(restQuery, sort, limit, offset)
              .then(_ => {
                result = _
                this.logger.debug("release readLock")
                release()
                return this.getJournalDb().findByCsnRange(csn + 1, currentCsn)
              })
              .then(transactions => {
                if (transactions.length < currentCsn - csn) {
                  return { csn: csn, resultSet: [], restQuery: restQuery }
                }
                // TODO 検索条件に合うかどうかで結果に加えるかあるいは除外するかなどの処理

                return { csn: csn, resultSet: result, restQuery: {} }
              })
          }
          else {
            this.logger.debug("wait for transactions", String(csn), String(currentCsn))
            // wait for transactions
            return new Promise<QueryResult>((resolve, reject) => {
              if (!this.queryWaitingList[csn]) this.queryWaitingList[csn] = []
              this.queryWaitingList[csn].push(() => {
                return this.getSubsetDb().find(restQuery, sort, limit, offset)
                  .then(result => {
                    resolve({ csn: csn, resultSet: result, restQuery: {} })
                  })
              })
              this.logger.debug("release readLock")
              release()
            })
          }
        }).catch(e => {
          this.logger.debug("SubsetStorage query Error: " + e.toString())
          this.logger.debug("release readLock")
          release()
          return Promise.reject(e)
        })
    }
    throw new Error(`SubsetStorage type ${this.type} is not supported.`)
  }
}