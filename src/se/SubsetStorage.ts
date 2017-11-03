import { ResourceNode, ServiceEngine, Subscriber, Proxy } from '@chip-in/resource-node'
import { SubsetDb } from '../db/SubsetDb'
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"
import { TransactionObject, TransactionType } from '../db/Transaction'
import { QuestResult } from "./Dadget"
import { ProxyHelper } from "../util/ProxyHelper"
import { CORE_NODE } from "../Config"
import * as http from 'http';
import * as URL from 'url'

class UpdateProcessor extends Subscriber {

  protected storage: SubsetStorage
  protected database: string
  protected subsetDefinition: SubsetDef

  constructor(storage: SubsetStorage, database: string, subsetDefinition: SubsetDef) {
    super()
    this.storage = storage
    this.database = database
    this.subsetDefinition = subsetDefinition
  }

  onReceive(msg: string) {
    this.storage.getNode().logger.debug("onReceive: " + msg)

    let transaction = JSON.parse(msg) as TransactionObject;
    if (transaction.type == TransactionType.INSERT && transaction.new) {
      let obj = Object.assign({ _id: transaction.target, csn: transaction.csn }, transaction.new)
      this.storage.getSubsetDb().insert(obj)
    }

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
  private mountHandle: string

  constructor(option: SubsetStorageConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
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

  start(node: ResourceNode): Promise<void> {
    this.node = node
    node.logger.debug("SubsetStorage is started")

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
    this.subsetDefinition = registry.getMetadata().subsets[this.subsetName];

    // ストレージを準備
    this.subsetDb = new SubsetDb(this.database, this.subsetName)

    // Rest サービスを登録する。
    let mountingMode = this.option.exported ? "loadBalancing" : "localOnly"
    let listener = new UpdateProcessor(this, this.database, this.subsetDefinition);
    let promise = this.subsetDb.start()
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
    this.node.logger.debug(url.pathname)
    let method = req.method.toUpperCase()
    this.node.logger.debug(method)
    if (method == "OPTIONS") {
      return ProxyHelper.procOption(req, res)
    } else if (url.pathname.endsWith("/query") && method == "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        console.log("/query")
        let request = JSON.parse(data)
        return this.query(request.csn, request.query)
      })
    } else {
      this.node.logger.debug("server command not found!:" + url.pathname)
      return ProxyHelper.procError(req, res)
    }
  }

  query(csn: number, restQuery: object): Promise<QuestResult> {
    // TODO csn が0の場合は、最新のcsnを取得、それ以外の場合はcsnを一致させる
    let type = this.option.type.toLowerCase()
    if (type == "cache") {
      // TODO cacheの場合 とりあえず空レスポンス
      console.log("query: cache")
      return Promise.resolve({ csn: csn, resultSet: [], restQuery: restQuery })
    } else if (type == "persistent") {
      // TODO persistentの場合
      console.log("query: persistent")
      return this.getSubsetDb().find(restQuery).then(result => ({ csn: csn, resultSet: result, restQuery: {} }))
    }
    throw new Error("SubsetStorage has no type")
  }
}