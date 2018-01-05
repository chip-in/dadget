import * as http from 'http';
import * as URL from 'url'
import * as  AsyncLock from "async-lock"
import * as EJSON from '../util/Ejson'

import { ResourceNode, ServiceEngine, Subscriber, Proxy } from '@chip-in/resource-node'
import { TransactionRequest, TransactionObject, TransactionType } from '../db/Transaction'
import { CsnDb } from '../db/CsnDb'
import { JournalDb } from '../db/JournalDb'
import { ProxyHelper } from "../util/ProxyHelper"
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { CORE_NODE } from "../Config"

/**
 * コンテキストマネージャコンフィグレーションパラメータ
 */
export class ContextManagerConfigDef {

  /**
   * データベース名
   */
  database: string
}

class TransactionJournalSubscriber extends Subscriber {

  constructor(
    protected context: ContextManager
    , protected journalDB: JournalDb
    , protected csnDB: CsnDb) {

    super()
    context.logger.debug("TransactionJournalSubscriber is created")
  }

  onReceive(msg: string) {
    //    console.log("onReceive:", msg)
    let transaction: TransactionObject = EJSON.parse(msg)
    this.context.getLock().acquire("transaction", () => {
      // 自分がスレーブになっていれば保存
      return this.csnDB.getCurrentCsn()
        .then(csn => {
          if (csn < transaction.csn) {
            return this.csnDB.update(transaction.csn)
          }
          return Promise.resolve()
        })
        .then(() => {
          return this.journalDB.findByCsn(transaction.csn)
        })
        .then(savedTransaction => {
          if (!savedTransaction) {
            // トランザクションオブジェクトをジャーナルに追加
            return this.journalDB.insert(transaction)
          } else {
            let promise = Promise.resolve()
            if (savedTransaction.digest != transaction.digest) {
              // ダイジェストが異なる場合は更新して、それ以降でtimeがこのトランザクション以前のジャーナルを削除
              promise = promise.then(() => this.journalDB.updateAndDeleteAfter(transaction))
            }
            // マスター権を喪失している場合は再接続
            if (this.context.getMountHandle()) {
              promise = promise.then(() => this.context.connect())
            }
            return promise
          }
        })
        .catch(err => {
          this.context.logger.error(err.toString())
        })
    })
  }
}

class ContextManagementServer extends Proxy {

  constructor(
    protected context: ContextManager
    , protected journalDB: JournalDb
    , protected csnDB: CsnDb) {

    super()
    context.logger.debug("ContextManagementServer is created")
  }

  onReceive(req: http.IncomingMessage, res: http.ServerResponse): Promise<http.ServerResponse> {
    if (!req.url) throw new Error("url is required.")
    if (!req.method) throw new Error("method is required.")
    let url = URL.parse(req.url)
    if (url.pathname == null) throw new Error("pathname is required.")
    this.context.logger.debug(url.pathname)
    let method = req.method.toUpperCase()
    this.context.logger.debug(method)
    if (method == "OPTIONS") {
      return ProxyHelper.procOption(req, res)
    } else if (url.pathname.endsWith("/exec") && method == "POST") {
      return ProxyHelper.procPost(req, res, (data) => {
        this.context.logger.debug("/exec")
        let request = EJSON.parse(data)
        return this.exec(request.csn, request.request)
      })
    } else {
      this.context.logger.debug("server command not found!:" + url.pathname)
      return ProxyHelper.procError(req, res)
    }
  }

  exec(csn: number, request: TransactionRequest): Promise<{}> {
    this.context.logger.debug(`exec ${csn}`)
    let transaction: TransactionObject
    let newCsn: number
    let updateObject: { _id?: string, csn?: number }

    let err: string | null = null
    if (!request.target) {
      err = 'target required'
    }
    if (request.type == TransactionType.INSERT) {
      if (request.before) err = 'before not required'
      if (request.operator) err = 'operator not required'
      if (!request.new) err = 'new required'
    } else if (request.type == TransactionType.UPDATE) {
      if (!request.before) err = 'before required'
      if (!request.operator) err = 'operator required'
      if (request.new) err = 'new not required'
    } else if (request.type == TransactionType.DELETE) {
      if (!request.before) err = 'before required'
      if (request.operator) err = 'operator not required'
      if (request.new) err = 'new not required'
    } else {
      err = 'type not found'
    }
    if (err) {
      return Promise.resolve({
        status: "NG",
        reason: new DadgetError(ERROR.E2002, [err])
      })
    }

    // TODO マスターを取得したばかりの時は時間待ち


    // コンテキスト通番をインクリメントしてトランザクションオブジェクトを作成
    return new Promise((resolve, reject) => {
      this.context.getLock().acquire("transaction", () => {
        let _request = { ...request, datetime: new Date() }
        // ジャーナルと照合して矛盾がないかチェック
        return this.journalDB.checkConsistent(csn, _request)
          .then(() => this.context.checkUniqueConstraint(csn, _request))
          .then(_ => {
            updateObject = _
            return Promise.all([this.csnDB.increment(), this.journalDB.getLastDigest()])
              .then(values => {
                newCsn = values[0]
                let lastDigest = values[1]
                transaction = Object.assign({
                  csn: newCsn
                  , beforeDigest: lastDigest
                }, _request)
                transaction.digest = TransactionObject.calcDigest(transaction);
                // トランザクションオブジェクトをジャーナルに追加
                return this.journalDB.insert(transaction)
              })
          }).then(() => {
            // トランザクションオブジェクトを配信
            return this.context.getNode().publish(
              CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.context.getDatabase())
              , EJSON.stringify(transaction))
          })
      }).then(() => {
        if (!updateObject._id) updateObject._id = transaction.target
        updateObject.csn = newCsn
        resolve({
          status: "OK",
          updateObject: updateObject
        })
      }, reason => {
        // トランザクションエラー
        let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2003, [reason])
        if (cause.code == ERROR.E1105.code) cause = new DadgetError(ERROR.E2004, [cause])
        cause.convertInsertsToString()
        resolve({
          status: "NG",
          reason: cause
        })
      })
    })
  }
}

/**
 * コンテキストマネージャ(ContextManager)
 * 
 * コンテキストマネージャは、逆接続プロキシの Rest API で exec メソッドを提供する。
 */
export class ContextManager extends ServiceEngine {

  public bootOrder = 20
  private option: ContextManagerConfigDef
  private node: ResourceNode
  private database: string
  private journalDb: JournalDb
  private csnDb: CsnDb
  private subscriber: TransactionJournalSubscriber
  private server: ContextManagementServer
  private mountHandle?: string
  private lock: AsyncLock
  private subscriberKey: string | null

  constructor(option: ContextManagerConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
    this.lock = new AsyncLock()
  }

  getNode(): ResourceNode {
    return this.node;
  }

  getDatabase(): string {
    return this.database;
  }

  getLock(): AsyncLock {
    return this.lock
  }

  getMountHandle(): string | undefined {
    return this.mountHandle
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    this.logger.debug("ContextManager is starting")

    if (!this.option.database) {
      return Promise.reject(new DadgetError(ERROR.E2001, ["Database name is missing."]));
    }
    this.database = this.option.database

    // ストレージを準備
    this.journalDb = new JournalDb(this.database)
    this.csnDb = new CsnDb(this.database)
    let promise = Promise.all([this.journalDb.start(), this.csnDb.start()]).then(_ => { })

    // スレーブ動作で同期するのためのサブスクライバを登録
    this.subscriber = new TransactionJournalSubscriber(this, this.journalDb, this.csnDb)
    promise = promise.then(() => {
      return node.subscribe(CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), this.subscriber).then(key => { this.subscriberKey = key })
    })
    promise = promise.then(() => {
      // コンテキストマネージャのRestサービスを登録
      this.server = new ContextManagementServer(this, this.journalDb, this.csnDb)
      this.connect()
    })

    this.logger.debug("ContextManager is started")
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

  connect(): Promise<void> {
    let promise = Promise.resolve()
    const mountHandle = this.mountHandle
    this.mountHandle = undefined

    if (mountHandle) {
      promise = promise.then(() => { return this.node.unmount(mountHandle).catch() })
    }
    promise = promise.then(() => {
      this.node.mount(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database), "singletonMaster", this.server)
        .then(mountHandle => {
          // マスターを取得した場合のみ実行される
          // TODO 時間待ちのための時刻保存
          // TODO マスターを取得した場合、他のサブセットを自分と同じcsnまでロールバックさせるメッセージを送信
          this.mountHandle = mountHandle
        })
    })
    promise = promise.then(() => new Promise<void>(resolve => {
      setTimeout(resolve, 1000)
    }))
    return promise
  }

  checkUniqueConstraint(csn: number, request: TransactionRequest): Promise<object> {
    // TODO ユニーク制約についてはクエリーを発行して確認 前提csnはジャーナルの最新を使用しなればならない
    if (request.type == TransactionType.INSERT && request.new) {
      // TODO 追加されたオブジェクトと一意属性が競合していないかを調べる
      return Promise.resolve(request.new)
    } else if (request.type == TransactionType.UPDATE && request.before) {
      let newObj = TransactionRequest.applyOperator(request)
      return Promise.resolve(newObj)
    } else if (request.type == TransactionType.DELETE && request.before) {
      return Promise.resolve(request.before)
    } else {
      throw new Error('checkConsistent error');
    }
  }
}