import { ResourceNode, ServiceEngine } from '@chip-in/resource-node'
import { TransactionRequest, TransactionObject, TransactionType } from '../db/Transaction'
import { QueryHandler } from "./QueryHandler"
import { CORE_NODE } from "../Config"
import { v1 as uuidv1 } from 'uuid'
import * as EJSON from 'mongodb-extended-json'

/**
 * Dadgetコンフィグレーションパラメータ
 */
export class DadgetConfigDef {

  /**
   * データベース名
   */
  database: string
}

/**
 * 結果オブジェクト
 */
export class QuestResult {

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
}

/**
 * API(Dadget)
 *
 * 更新APIとクリルータ機能を提供するAPIである。execメソッド（更新API）とqueryメソッド（クエリルータ）を提供する。
 */
export class Dadget extends ServiceEngine {

  private option: DadgetConfigDef
  private node: ResourceNode
  private database: string

  constructor(option: DadgetConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    this.database = this.option.database
    this.logger.debug("Dadget is started")
    return Promise.resolve();
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  /**
   * query メソッドはクエリルータを呼び出して、問い合わせを行い、結果オブジェクトを返す。
   *
   * @param query mongoDBと同じクエリーオブジェクト
   * @param sort  mongoDBと同じソートオブジェクト
   * @param limit 最大取得件数
   * @param offset 開始位置
   * @returns 取得した結果オブジェクトを返すPromiseオブジェクト
   */
  query(query: object, sort?: object, limit?: number, offset?: number): Promise<QuestResult> {
    let node = this.node
    let queryHandlers = this.node.searchServiceEngine("QueryHandler", { database: this.database }) as QueryHandler[]
    queryHandlers = sortQueryHandlers(queryHandlers)
    let csn = 0
    let resultSet: object[] = []
    return Promise.resolve({ csn: csn, resultSet: resultSet, restQuery: query, queryHandlers: queryHandlers })
      .then(function queryFallback(request): Promise<QuestResult> {
        if (!Object.keys(request.restQuery).length) return Promise.resolve(request)
        let qh = request.queryHandlers.shift()
        if (qh == null) {
          throw new Error("The queryHandlers has been empty before completing queries.")
        }
        return qh.query(request.csn, request.restQuery)
          .then((result) => queryFallback({
            csn: result.csn,
            resultSet: margeResultSet(request.resultSet, result.resultSet),
            restQuery: result.restQuery,
            queryHandlers: request.queryHandlers
          }));
      }).then(result => {
        //クエリ完了後の処理
        // TODO クエリが空にならなかった場合（＝ wholeContents サブセットのサブセットストレージが同期処理中で準備が整っていない場合）5秒ごとに4回くらい再試行した後、エラーとなる
        return result
      })

    /**
     * クエリハンドラの優先度順にソート
     * @param seList
     */
    function sortQueryHandlers(seList: QueryHandler[]): QueryHandler[] {
//      this.logger.debug("before sort:")
//      for (let se of seList) this.logger.debug(se.getPriority().toString())
      seList.sort((a, b) => b.getPriority() - a.getPriority())
//      this.logger.debug("after sort:")
//      for (let se of seList) this.logger.debug(se.getPriority().toString())
      return seList;
    }

    /**
     * サブセットのクエリの結果をマージ
     * @param resultSet1
     * @param resultSet2
     */
    function margeResultSet(resultSet1: object[], resultSet2: object[]): object[] {
      // TODO ソート？
      return [...resultSet1, ...resultSet2];
    }
  }
  /**
   * count メソッドはクエリルータを呼び出して、問い合わせを行い、件数を返す。
   *
   * @param query mongoDBと同じクエリーオブジェクト
   * @returns 取得した件数を返すPromiseオブジェクト
   */
  count(query: object): Promise<number> {
    // TODO 実装
    return Promise.resolve(1)
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
    if(request.type != TransactionType.INSERT && request.type != TransactionType.UPDATE && request.type != TransactionType.DELETE){
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
    .then(fetchResult => fetchResult.json())
    .then(_ => {
      let result = EJSON.deserialize(_)
      this.logger.debug("exec:", JSON.stringify(result))
      if (result.status == "OK") {
        return result.updateObject
      } else {
        throw new Error(result.reason)
      }
    })
  }
}
