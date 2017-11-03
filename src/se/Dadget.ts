import { ResourceNode, ServiceEngine } from '@chip-in/resource-node'
import { TransactionRequest, TransactionObject } from '../db/Transaction'
import { QueryHandler } from "./QueryHandler"
import { CORE_NODE } from "../Config"
import { v1 as uuidv1 } from 'uuid'

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
    node.logger.debug("Dadget is started")
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
    let queryHandlers = this.node.searchServiceEngine("QueryHandler", { database: this.database }) as QueryHandler[];
    queryHandlers = sortQueryHandlers(queryHandlers);
    let csn = 0
    let resultSet: object[] = []
    return Promise.resolve({ csn: csn, resultSet: resultSet, restQuery: query, queryHandlers: queryHandlers })
      .then(function queryFallback(request): Promise<QuestResult> {
        if (!Object.keys(request.restQuery).length) return Promise.resolve(request); // クエリが空集合なので、ここまでの結果を返す
        let qh = request.queryHandlers.shift(); // 先頭のクエリハンドラを取得
        if (qh == null) {
          // まだクエリーが空になってないのにクエリハンドラが残ってない
          throw new Error("The queryHandlers has been empty before completing queries.");
        }
        return qh.query(request.csn, request.restQuery)
          .then((result) => queryFallback({ // 次のクエリハンドラにフォールバック
            csn: result.csn, // 前提となるコンテキスト通番をリレー
            resultSet: margeResultSet(request.resultSet, result.resultSet),
            restQuery: result.restQuery, // 残ったクエリ
            queryHandlers: request.queryHandlers // 残ったクエリハンドラ
          }));
      }).then(result => {
        //クエリ完了後の処理
        return result
      })

    /**
     * クエリハンドラの優先度順にソート
     * @param seList
     */
    function sortQueryHandlers(seList: QueryHandler[]): QueryHandler[] {
      node.logger.debug("before sort:")
      for (let se of seList) node.logger.debug(se.getPriority().toString())
      seList.sort((a, b) => b.getPriority() - a.getPriority())
      node.logger.debug("after sort:")
      for (let se of seList) node.logger.debug(se.getPriority().toString())
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
    let sendData = {
      csn: csn,
      request: request
    }
    return this.node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database) + "/exec", {
      method: 'POST',
      body: JSON.stringify(sendData),
      headers : {
        "Content-Type": "application/json"
      }
    })
    .then(fetchResult => {
      console.dir(fetchResult)
      return fetchResult.json()
    })
    .then(result => {
      console.log("exec:", JSON.stringify(result))
      if (result.status == "OK") {
        return result.updateObject
      } else {
        throw new Error(result.reason)
      }
    })
  }
}
