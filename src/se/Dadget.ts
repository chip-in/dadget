import { ResourceNode, ServiceEngine, Subscriber } from '@chip-in/resource-node'
import { TransactionRequest, TransactionObject, TransactionType } from '../db/Transaction'
import { ContextManager } from "./ContextManager"
import { DatabaseRegistry } from "./DatabaseRegistry"
import { QueryHandler } from "./QueryHandler"
import { SubsetStorage } from "./SubsetStorage"
import { UpdateManager } from "./UpdateManager"
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { CORE_NODE } from "../Config"
import { v1 as uuidv1 } from 'uuid'
import * as EJSON from '../util/Ejson'

/**
 * Dadgetコンフィグレーションパラメータ
 */
export class DadgetConfigDef {

  /**
   * データベース名
   */
  database: string
}

export type CsnMode = "strict" | "latest"

/**
 * 結果オブジェクト
 */
export class QueryResult {

  /**
   * トランザクションをコミットしたコンテキスト通番
   */
  csn: number

  /**
   * クエリに合致したオブジェクトの配列
   */
  resultSet: object[]

  /**
   * 問い合わせに対するオブジェクトを全て列挙できなかった場合に、残った集合に対するクエリ（サブセットのクエリハンドラの場合のみで、クエリルータの返却時は undefined）
   */
  restQuery: object

  queryHandlers?: QueryHandler[]

  csnMode?: CsnMode
}

const PREQUERY_CSN = -1

/**
 * API(Dadget)
 *
 * 更新APIとクエリルータ機能を提供するAPIである。execメソッド（更新API）とqueryメソッド（クエリルータ）を提供する。
 */
export default class Dadget extends ServiceEngine {

  public bootOrder = 60
  private option: DadgetConfigDef
  private node: ResourceNode
  private database: string
  private currentCsn: number = PREQUERY_CSN
  private notifyCsn: number = 0
  private updateListeners: { [id: string]: { listener: (csn: number) => void, csn: number, minInterval: number, notifyTime: number } } = {}
  private updateListenerKey: string | null
  private latestCsn: number

  constructor(option: DadgetConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
  }

  /**
   * デフォルトサービスクラスを登録
   * @param node 
   */
  static registerServiceClasses(node: ResourceNode) {
    node.registerServiceClasses({
      DatabaseRegistry,
      ContextManager,
      Dadget,
      UpdateManager,
      QueryHandler,
      SubsetStorage
    });
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    this.logger.debug("Dadget is starting")
    if (!this.option.database) {
      return Promise.reject(new DadgetError(ERROR.E2101, ["Database name is missing."]));
    }
    this.database = this.option.database
    this.logger.debug("Dadget is started")
    return Promise.resolve();
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  /**
   * クエリハンドラの優先度順にソート
   * @param seList
   */
  static sortQueryHandlers(seList: QueryHandler[]): QueryHandler[] {
    seList.sort((a, b) => b.getPriority() - a.getPriority())
    return seList;
  }

  /**
   * サブセットのクエリの結果をマージ
   * @param resultSet1
   * @param resultSet2
   */
  static margeResultSet(resultSet1: object[], resultSet2: object[]): object[] {
    // TODO ソート？
    return [...resultSet1, ...resultSet2];
  }

  public static _query(node: ResourceNode, database: string, query: object, sort?: object, limit?: number, offset?: number, csn?: number, csnMode?: CsnMode): Promise<QueryResult> {
    let queryHandlers = node.searchServiceEngine("QueryHandler", { database: database }) as QueryHandler[]
    queryHandlers = Dadget.sortQueryHandlers(queryHandlers)
    if (!csn) csn = 0
    let resultSet: object[] = []
    return Promise.resolve({ csn: csn, resultSet: resultSet, restQuery: query, queryHandlers: queryHandlers, csnMode: csnMode })
      .then(function queryFallback(request): Promise<QueryResult> {
        if (!Object.keys(request.restQuery).length) return Promise.resolve(request)
        if (request.queryHandlers.length == 0) {
          let error = new Error("The queryHandlers has been empty before completing queries.") as any
          error.queryResult = request
          throw error
        }
        let qh = request.queryHandlers.shift()
        if (qh == null) throw new Error("The queryHandlers has been empty before completing queries.")
        return qh.query(request.csn, request.restQuery, sort, limit, offset, csnMode)
          .then((result) => queryFallback({
            csn: result.csn,
            resultSet: Dadget.margeResultSet(request.resultSet, result.resultSet),
            restQuery: result.restQuery,
            queryHandlers: request.queryHandlers,
            csnMode: result.csnMode
          }));
      })
  }

  /**
   * query メソッドはクエリルータを呼び出して、問い合わせを行い、結果オブジェクトを返す。
   *
   * @param query mongoDBと同じクエリーオブジェクト
   * @param sort  mongoDBと同じソートオブジェクト
   * @param limit 最大取得件数
   * @param offset 開始位置
   * @param csn 問い合わせの前提CSN
   * @param csnMode 問い合わせの前提CSNの意味付け
   * @returns 取得した結果オブジェクトを返すPromiseオブジェクト
   */
  query(query: object, sort?: object, limit?: number, offset?: number, csn?: number, csnMode?: CsnMode): Promise<QueryResult> {
    if (this.latestCsn && !csn) {
      csn = this.latestCsn
      csnMode = "latest"
    }
    return Dadget._query(this.node, this.database, query, sort, limit, offset, csn, csnMode)
      .then(result => {
        // TODO クエリ完了後の処理
        // 通知処理
        // csn が0の場合は代入
        this.currentCsn = result.csn
        for (let id in this.updateListeners) {
          let listener = this.updateListeners[id]
          if (listener.csn == PREQUERY_CSN) {
            listener.csn = result.csn
            listener.notifyTime = Date.now()
          }
        }
        setTimeout(() => {
          this.notifyAll()
        })

        // TODO クエリが空にならなかった場合（＝ wholeContents サブセットのサブセットストレージが同期処理中で準備が整っていない場合）5秒ごとに4回くらい再試行した後、エラーとなる
        return result
      })
      .catch(reason => {
        let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2102, [reason.toString()])
        return Promise.reject(cause)
      })
  }

  /**
   * count メソッドはクエリルータを呼び出して、問い合わせを行い、件数を返す。
   *
   * @param query mongoDBと同じクエリーオブジェクト
   * @param csn 問い合わせの前提CSN
   * @param csnMode 問い合わせの前提CSNの意味付け
   * @returns 取得した件数を返すPromiseオブジェクト
   */
  count(query: object, csn?: number, csnMode?: CsnMode): Promise<number> {
    // TODO 実装の効率化
    return this.query(query, undefined, undefined, undefined, csn, csnMode).then(result => result.resultSet.length)
  }

  /**
   * targetに設定するUUIDを生成
   */
  static uuidGen(): string {
    return uuidv1()
  }

  /**
   * execメソッドはコンテキストマネージャの Rest API を呼び出してトランザクション要求を実行する。
   *
   * @param csn トランザクションの前提となるコンテキスト通番(トランザクションの type が "insert" のときは 0 を指定でき、その場合は不整合チェックを行わない)
   * @param request トランザクションの内容を持つオブジェクト
   * @return 更新されたオブジェクト
   */
  exec(csn: number, request: TransactionRequest): Promise<object> {
    request.type = request.type.toLowerCase() as TransactionType
    if (request.type != TransactionType.INSERT && request.type != TransactionType.UPDATE && request.type != TransactionType.DELETE) {
      throw new Error("The TransactionType is not supported.")
    }
    let sendData = {
      csn: csn,
      request: request
    }
    return this.node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database) + "/exec", {
      method: 'POST',
      body: EJSON.stringify(sendData),
      headers: {
        "Content-Type": "application/json"
      }
    })
      .then(fetchResult => {
        if(typeof fetchResult.ok !== "undefined" && !fetchResult.ok) throw Error(fetchResult.statusText)
        return fetchResult.json()
      })
      .then(_ => {
        let result = EJSON.deserialize(_)
        this.logger.debug("exec:", JSON.stringify(result))
        if (result.status == "OK") {
          this.latestCsn = result.csn
          return result.updateObject
        } else if (result.reason) {
          let reason = result.reason as DadgetError
          throw new DadgetError({ code: reason.code, message: reason.message }, reason.inserts, reason.ns)
        } else {
          throw JSON.stringify(result)
        }
      })
      .catch(reason => {
        let cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2103, [reason.toString()])
        return Promise.reject(cause)
      })
  }

  private notifyAll() {
    for (let id in this.updateListeners) {
      let listener = this.updateListeners[id]
      if (listener.csn != PREQUERY_CSN && this.notifyCsn > listener.csn) {
        let now = Date.now()
        if (listener.minInterval == 0 || now - listener.notifyTime >= listener.minInterval) {
          listener.notifyTime = now
          listener.csn = this.notifyCsn
          listener.listener(this.notifyCsn)
        } else {
          setTimeout(() => {
            this.notifyAll()
          }, now - listener.notifyTime);
        }
      }
    }
  }

  /**
   * データベースの更新通知のリスナを登録する
   * @param listener 更新があった場合、csn を引数にしてこの関数を呼び出す
   * @param minInterval 通知の間隔の最小値をミリ秒で指定
   * @return 更新通知取り消しに指定するID
   */
  addUpdateListener(listener: (csn: number) => void, minInterval?: number): string {
    let parent = this
    if (Object.keys(this.updateListeners).length == 0) {
      class NotifyListener extends Subscriber {

        constructor() {
          super()
          this.logger.debug("NotifyListener is created")
        }

        onReceive(transctionJSON: string) {
          let transaction = EJSON.parse(transctionJSON) as TransactionObject
          parent.notifyCsn = transaction.csn
          setTimeout(() => {
            parent.notifyAll()
          })
        }
      }

      if (!this.updateListenerKey) {
        this.node.subscribe(CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), new NotifyListener())
          .then(key => { this.updateListenerKey = key })
      }
    }

    let id = uuidv1()
    this.updateListeners[id] = {
      listener: listener
      , csn: this.currentCsn
      , minInterval: minInterval || 0
      , notifyTime: 0
    }
    return id
  }

  /**
   * データベースの更新通知のリスナを解除する
   * @param id 登録時のID
   */
  removeUpdateListener(id: string) {
    delete this.updateListeners[id]
    if (Object.keys(this.updateListeners).length == 0) {
      if (this.updateListenerKey) {
        this.node.unsubscribe(this.updateListenerKey)
        this.updateListenerKey = null
      }
    }
  }

  /**
   * データベースの更新通知のリスナを全解除
   */
  resetUpdateListener() {
    this.updateListeners = {}
    if (this.updateListenerKey) {
      this.node.unsubscribe(this.updateListenerKey)
      this.updateListenerKey = null
    }
  }

  addUpdateListenerForSubset(subset: string, listener: (csn: number) => void, minInterval?: number) {

  }

}
