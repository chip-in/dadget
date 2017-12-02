import * as http from 'http';
import * as URL from 'url'
import * as  ReadWriteLock from "rwlock"
import * as EJSON from '../util/Ejson'

import { ResourceNode, ServiceEngine, Subscriber, Proxy } from '@chip-in/resource-node'
import { SubsetDb } from '../db/SubsetDb'
import { CsnDb } from '../db/CsnDb'
import { JournalDb } from '../db/JournalDb'
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"
import { TransactionObject, TransactionType, TransactionRequest } from '../db/Transaction'
import { QuestResult } from "./Dadget"
import { ProxyHelper } from "../util/ProxyHelper"
import { CORE_NODE } from "../Config"

class UpdateProcessor extends Subscriber {

  private updateQueue: { [csn: number]: TransactionObject } = {}

  constructor(
    protected storage: SubsetStorage
    , protected database: string
    , protected subsetDefinition: SubsetDef) {
    super()
  }

  onReceive(msg: string) {
    this.storage.logger.debug("onReceive: " + msg)
    let transaction = EJSON.parse(msg) as TransactionObject;
    this.storage.getLock().writeLock(release => {
      this.storage.logger.debug("get writeLock")
      return this.storage.getCsnDb().getCurrentCsn()
        .then(csn => {
          if (csn >= transaction.csn) {
            this.storage.logger.debug("release writeLock")
            release()
          } else {
            this.updateQueue[transaction.csn] = transaction
            let promise = Promise.resolve()
            while (this.updateQueue[++csn]) {
              let _csn = csn
              this.storage.logger.debug("subset csn: " + _csn)
              transaction = this.updateQueue[_csn]
              delete this.updateQueue[_csn]
              if (transaction.type == TransactionType.INSERT && transaction.new) {
                let obj = Object.assign({ _id: transaction.target, csn: transaction.csn }, transaction.new)
                promise = promise.then(() => this.storage.getSubsetDb().insert(obj))
              } else if (transaction.type == TransactionType.UPDATE && transaction.before) {
                let updateObj = TransactionRequest.applyOperator(transaction)
                promise = promise.then(() => this.storage.getSubsetDb().update(updateObj))
              } else if (transaction.type == TransactionType.DELETE && transaction.before) {
                let before = transaction.before
                promise = promise.then(() => this.storage.getSubsetDb().delete(before))
              } else if (transaction.type == TransactionType.NONE) {
              }
              promise = promise.then(() => this.storage.getJournalDb().insert(transaction))
              promise = promise.then(() => this.storage.getCsnDb().update(_csn))
              promise = promise.then(() => {
                for(let query of this.storage.pullQueryWaitingList(_csn)){
                  query()
                }
              })
            }
            promise.then(() => {
              this.storage.logger.debug("release writeLock")
              release()
            }).catch(e => {
              this.storage.logger.debug("release writeLock: Error", e)
              release()
            })
          }
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

  private option: SubsetStorageConfigDef
  private node: ResourceNode
  private database: string
  private subsetName: string
  private subsetDefinition: SubsetDef
  private subsetDb: SubsetDb
  private journalDb: JournalDb
  private csnDb: CsnDb
  private mountHandle: string
  private lock: ReadWriteLock
  private queryWaitingList: { [csn: number]: (() => void)[] } = {}

  constructor(option: SubsetStorageConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
    this.lock = new ReadWriteLock()
  }

  getPriority(): number {
    return this.subsetDefinition.priority
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
    if(list){
      delete this.queryWaitingList[csn]
      return list
    }
    return []
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    this.logger.debug("SubsetStorage is started")

    if (!this.option.database) {
      return Promise.reject(new Error("Database name is missing."));
    }
    this.database = this.option.database
    this.subsetName = this.option.subset

    // サブセットの定義を取得する
    let seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length != 1) {
      return Promise.reject(new Error("DatabaseRegistry is missing, or there are multiple ones."));
    }
    let registry = seList[0] as DatabaseRegistry;
    let metaData = registry.getMetadata()
    this.subsetDefinition = metaData.subsets[this.subsetName];

    // ストレージを準備
    this.subsetDb = new SubsetDb(this.database, this.subsetName, metaData.indexes)
    this.journalDb = new JournalDb(this.database + '--' + this.subsetName)
    this.csnDb = new CsnDb(this.database + '--' + this.subsetName)

    // Rest サービスを登録する。
    let mountingMode = this.option.exported ? "loadBalancing" : "localOnly"
    let listener = new UpdateProcessor(this, this.database, this.subsetDefinition);
    let promise = this.subsetDb.start()
    promise = promise.then(() => this.journalDb.start())
    promise = promise.then(() => this.csnDb.start())
    promise = promise.then(() =>
      node.subscribe(CORE_NODE.PATH_SUBSET_TRANSACTION
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), listener)
    )
    promise = promise.then(() =>
      node.mount(CORE_NODE.PATH_SUBSET
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName), mountingMode, this)
    ).then(value => {
      this.mountHandle = value
    })

    return promise
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
      .then(() => {
        if (this.mountHandle) return this.node.unmount(this.mountHandle)
      });
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url || !req.method) throw new Error()
    let url = URL.parse(req.url)
    if (url.pathname == null) throw new Error()
    this.logger.debug(url.pathname)
    let method = req.method.toUpperCase()
    this.logger.debug(method)
    if (method == "OPTIONS") {
      return ProxyHelper.procOption(req, res)
    } else if (url.pathname.endsWith("/query") && method == "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        this.logger.debug("/query")
        let request = EJSON.parse(data)
        return this.query(request.csn, request.query, request.sort, request.limit, request.offset)
      })
    } else {
      this.logger.debug("server command not found!:" + url.pathname)
      return ProxyHelper.procError(req, res)
    }
  }

  query(csn: number, restQuery: object, sort?: object, limit?: number, offset?: number): Promise<QuestResult> {
    // TODO csn が0の場合は、最新のcsnを取得、それ以外の場合はcsnを一致させる
    let type = this.option.type.toLowerCase()
    if (type == "cache") {
      // TODO cacheの場合 とりあえず空レスポンス
      this.logger.debug("query: cache")
      return Promise.resolve({ csn: csn, resultSet: [], restQuery: restQuery })
    } else if (type == "persistent") {
      // TODO persistentの場合
      this.logger.debug("query: persistent")
      // TODO csnが0以外の場合はそのcsnに合わせる csnが変更されないようにロックが必要
      let release: () => void
      let promise = new Promise<void>((resolve, reject) => {
        this.getLock().readLock(_ => {
          release = _
          resolve()
        })
      })
      // TODO 戻す場合と待つ場合
      let currentCsn: number
      return promise
        .then(() => this.getCsnDb().getCurrentCsn())
        .then(_ => {
          currentCsn = _
          if (csn == 0 || csn == currentCsn) {
            return this.getSubsetDb().find(restQuery, sort, limit, offset)
              .then(result => {
                release()
                return { csn: currentCsn, resultSet: result, restQuery: {} }
              })
          } else if (csn < currentCsn) {
            // rollback transactions
            // TODO limitはcsnの差分だけ多めに確保して最後に調整
            let result: object
            return this.getSubsetDb().find(restQuery, sort, limit, offset)
              .then(_ => {
                result = _
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
          } else {
            // wait for transactions
            return new Promise<QuestResult>((resolve, reject) => {
              if (!this.queryWaitingList[csn]) this.queryWaitingList[csn] = []
              this.queryWaitingList[csn].push(() => {
                return this.getSubsetDb().find(restQuery, sort, limit, offset)
                  .then(result => {
                    resolve({ csn: currentCsn, resultSet: result, restQuery: {} })
                  })
              })
              release()
            })
          }
        })
    }
    throw new Error("SubsetStorage has no type")
  }
}