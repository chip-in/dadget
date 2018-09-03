import * as parser from "mongo-parse";
import { v1 as uuidv1 } from "uuid";

import { ResourceNode, ServiceEngine, Subscriber } from "@chip-in/resource-node";
import { CORE_NODE } from "../Config";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import * as EJSON from "../util/Ejson";
import { Util } from "../util/Util";
import { ContextManager } from "./ContextManager";
import { DatabaseRegistry } from "./DatabaseRegistry";
import { QueryHandler } from "./QueryHandler";
import { SubsetStorage } from "./SubsetStorage";
import { UpdateManager } from "./UpdateManager";

const QUERY_ERROR_RETRY_COUNT = 4;
const QUERY_ERROR_WAIT_TIME = 5000;

/**
 * Dadgetコンフィグレーションパラメータ
 */
export class DadgetConfigDef {

  /**
   * データベース名
   */
  database: string;
}

export type CsnMode = "strict" | "latest";

/**
 * 結果オブジェクト
 */
export class QueryResult {

  /**
   * トランザクションをコミットしたコンテキスト通番
   */
  csn: number;

  /**
   * クエリに合致したオブジェクトの配列
   */
  resultSet: object[];

  /**
   * 問い合わせに対するオブジェクトを全て列挙できなかった場合に、残った集合に対するクエリ（サブセットのクエリハンドラの場合のみで、APIからの返却時は undefined）
   */
  restQuery: object | undefined;

  queryHandlers?: QueryHandler[];

  csnMode?: CsnMode;
}

/**
 * 計数結果オブジェクト
 */
export class CountResult {

  /**
   * トランザクションをコミットしたコンテキスト通番
   */
  csn: number;

  /**
   * クエリに合致したオブジェクトの数
   */
  resultCount: number;

  /**
   * 問い合わせに対するオブジェクトを全て列挙できなかった場合に、残った集合に対するクエリ（サブセットのクエリハンドラの場合のみで、APIからの返却時は undefined）
   */
  restQuery: object | undefined;

  queryHandlers?: QueryHandler[];

  csnMode?: CsnMode;
}

const PREQUERY_CSN = -1;

/**
 * API(Dadget)
 *
 * 更新APIとクエリルータ機能を提供するAPIである。execメソッド（更新API）とqueryメソッド（クエリルータ）を提供する。
 */
export default class Dadget extends ServiceEngine {

  public bootOrder = 60;
  private option: DadgetConfigDef;
  private node: ResourceNode;
  private database: string;
  private currentCsn: number = PREQUERY_CSN;
  private notifyCsn: number = 0;
  private updateListeners: { [id: string]: { listener: (csn: number) => void, csn: number, minInterval: number, notifyTime: number } } = {};
  private updateListenerKey: string | null;
  private latestCsn: number;

  constructor(option: DadgetConfigDef) {
    super(option);
    this.logger.debug(JSON.stringify(option));
    this.option = option;
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
      SubsetStorage,
    });
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug("Dadget is starting");
    if (!this.option.database) {
      throw new DadgetError(ERROR.E2101, ["Database name is missing."]);
    }
    if (this.option.database.match(/--/)) {
      throw new DadgetError(ERROR.E2101, ["Database name can not contain '--'."]);
    }
    this.database = this.option.database;
    this.logger.debug("Dadget is started");
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
    seList.sort((a, b) => b.getPriority() - a.getPriority());
    return seList;
  }

  public static _query(
    node: ResourceNode,
    database: string,
    query: object,
    sort?: object,
    limit?: number,
    offset?: number,
    csn?: number,
    csnMode?: CsnMode): Promise<QueryResult> {

    let queryHandlers = node.searchServiceEngine("QueryHandler", { database }) as QueryHandler[];
    if (queryHandlers.length === 0) { throw new Error("QueryHandlers required"); }
    queryHandlers = Dadget.sortQueryHandlers(queryHandlers);
    if (!csn) { csn = 0; }
    const _offset = offset ? offset : 0;
    const maxLimit = limit ? limit + _offset : undefined;
    const resultSet: object[] = [];
    return Promise.resolve({ csn, resultSet, restQuery: query, queryHandlers, csnMode } as QueryResult)
      .then(function queryFallback(request): Promise<QueryResult> {
        if (!request.restQuery) { return Promise.resolve(request); }
        if (!request.queryHandlers || request.queryHandlers.length === 0) {
          return Promise.resolve(request);
        }
        const qh = request.queryHandlers.shift();
        if (qh == null) { throw new Error("never happen"); }
        return qh.query(request.csn, request.restQuery, sort, maxLimit, csnMode)
          .then((result) => queryFallback({
            csn: result.csn,
            resultSet: [...request.resultSet, ...result.resultSet],
            restQuery: result.restQuery,
            queryHandlers: request.queryHandlers,
            csnMode: result.csnMode,
          }));
      })
      .then((result) => {
        const itemMap: { [id: string]: any } = {};
        // TODO 理論上hasDupulicateが存在しなければcountは効率化できる
        let hasDupulicate = false;
        for (const item of result.resultSet as Array<{ _id: string }>) {
          if (itemMap[item._id]) {
            console.log("hasDupulicate:" + item._id);
            hasDupulicate = true;
          }
          itemMap[item._id] = item;
        }
        let list: object[];
        if (hasDupulicate) {
          list = [];
          for (const id of Object.keys(itemMap)) {
            list.push(itemMap[id]);
          }
        } else {
          list = result.resultSet;
        }
        if (sort) {
          list = Util.mongoSearch(list, {}, sort) as object[];
          if (_offset) {
            if (limit) {
              list = list.slice(_offset, _offset + limit);
            } else {
              list = list.slice(_offset);
            }
          } else {
            if (limit) {
              list = list.slice(0, limit);
            }
          }
        }
        return { ...result, resultSet: list };
      });
  }

  public static _count(
    node: ResourceNode,
    database: string,
    query: object,
    csn?: number,
    csnMode?: CsnMode): Promise<CountResult> {

    let queryHandlers = node.searchServiceEngine("QueryHandler", { database }) as QueryHandler[];
    if (queryHandlers.length === 0) { throw new Error("QueryHandlers required"); }
    queryHandlers = Dadget.sortQueryHandlers(queryHandlers);
    if (!csn) { csn = 0; }
    const resultCount = 0;
    return Promise.resolve({ csn, resultCount, restQuery: query, queryHandlers, csnMode } as CountResult)
      .then(function queryFallback(request): Promise<CountResult> {
        if (!request.restQuery) { return Promise.resolve(request); }
        if (!request.queryHandlers || request.queryHandlers.length === 0) {
          return Promise.resolve(request);
        }
        const qh = request.queryHandlers.shift();
        if (qh == null) { throw new Error("never happen"); }
        return qh.count(request.csn, request.restQuery, csnMode)
          .then((result) => queryFallback({
            csn: result.csn,
            resultCount: request.resultCount + result.resultCount,
            restQuery: result.restQuery,
            queryHandlers: request.queryHandlers,
            csnMode: result.csnMode,
          }));
      });
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
      csn = this.latestCsn;
      csnMode = "latest";
    }
    let count = QUERY_ERROR_RETRY_COUNT;
    const retryAction = (_: any) => {
      count--;
      return new Promise<QueryResult>((resolve) => {
        setTimeout(() => {
          Dadget._query(this.node, this.database, query, sort, limit, offset, csn, csnMode)
            .then((result) => {
              resolve(result);
            });
        }, QUERY_ERROR_WAIT_TIME);
      });
    };
    return Dadget._query(this.node, this.database, query, sort, limit, offset, csn, csnMode)
      .then((result) => Util.promiseWhile<QueryResult>(result, (result) => !!(result.restQuery && count > 0), retryAction))
      .then((result) => {
        if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
        this.currentCsn = result.csn;
        for (const id of Object.keys(this.updateListeners)) {
          const listener = this.updateListeners[id];
          if (listener.csn === PREQUERY_CSN) {
            listener.csn = result.csn;
            listener.notifyTime = Date.now();
          }
        }
        setTimeout(() => {
          this.notifyAll();
        });
        return result;
      })
      .catch((reason) => {
        console.dir(reason);
        const cause = reason instanceof DadgetError ? reason :
          (reason.code ? DadgetError.from(reason) : new DadgetError(ERROR.E2102, [reason.toString()]));
        return Promise.reject(cause);
      });
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
    if (this.latestCsn && !csn) {
      csn = this.latestCsn;
      csnMode = "latest";
    }
    let count = QUERY_ERROR_RETRY_COUNT;
    const retryAction = (_: any) => {
      count--;
      return new Promise<CountResult>((resolve) => {
        setTimeout(() => {
          Dadget._count(this.node, this.database, query, csn, csnMode)
            .then((result) => {
              resolve(result);
            });
        }, QUERY_ERROR_WAIT_TIME);
      });
    };
    return Dadget._count(this.node, this.database, query, csn, csnMode)
      .then((result) => Util.promiseWhile<CountResult>(result, (result) => !!(result.restQuery && count > 0), retryAction))
      .then((result) => {
        if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing count queries."); }
        this.currentCsn = result.csn;
        for (const id of Object.keys(this.updateListeners)) {
          const listener = this.updateListeners[id];
          if (listener.csn === PREQUERY_CSN) {
            listener.csn = result.csn;
            listener.notifyTime = Date.now();
          }
        }
        setTimeout(() => {
          this.notifyAll();
        });
        return result.resultCount;
      })
      .catch((reason) => {
        console.dir(reason);
        const cause = reason instanceof DadgetError ? reason :
          (reason.code ? DadgetError.from(reason) : new DadgetError(ERROR.E2102, [reason.toString()]));
        return Promise.reject(cause);
      });
  }

  /**
   * targetに設定するUUIDを生成
   */
  static uuidGen(): string {
    return uuidv1();
  }

  /**
   * execメソッドはコンテキストマネージャの Rest API を呼び出してトランザクション要求を実行する。
   *
   * @param csn トランザクションの前提となるコンテキスト通番(トランザクションの type が "insert" のときは 0 を指定でき、その場合は不整合チェックを行わない)
   * @param request トランザクションの内容を持つオブジェクト
   * @return 更新されたオブジェクト
   */
  exec(csn: number, request: TransactionRequest): Promise<object> {
    request.type = request.type.toLowerCase() as TransactionType;
    if (request.type !== TransactionType.INSERT && request.type !== TransactionType.UPDATE && request.type !== TransactionType.DELETE) {
      throw new Error("The TransactionType is not supported.");
    }
    const sendData = { csn, request };
    return this.node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database) + CORE_NODE.PATH_EXEC, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: EJSON.stringify(sendData),
    })
      .then((fetchResult) => {
        if (typeof fetchResult.ok !== "undefined" && !fetchResult.ok) { throw Error(fetchResult.statusText); }
        return fetchResult.json();
      })
      .then((_) => {
        const result = EJSON.deserialize(_);
        console.log("Dadget exec result: " + JSON.stringify(result));
        if (result.status === "OK") {
          this.latestCsn = result.csn;
          return result.updateObject;
        } else if (result.reason) {
          const reason = result.reason as DadgetError;
          throw new DadgetError({ code: reason.code, message: reason.message }, reason.inserts, reason.ns);
        } else {
          throw JSON.stringify(result);
        }
      })
      .catch((reason) => {
        const cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2103, [reason.toString()]);
        return Promise.reject(cause);
      });
  }

  private notifyAll() {
    for (const id of Object.keys(this.updateListeners)) {
      const listener = this.updateListeners[id];
      if (listener.csn !== PREQUERY_CSN && this.notifyCsn > listener.csn) {
        const now = Date.now();
        if (listener.minInterval === 0 || now - listener.notifyTime >= listener.minInterval) {
          listener.notifyTime = now;
          listener.csn = this.notifyCsn;
          listener.listener(this.notifyCsn);
        } else {
          setTimeout(() => {
            this.notifyAll();
          }, now - listener.notifyTime);
        }
      }
    }
  }

  private notifyRollback(notifyCsn: number) {
    for (const id of Object.keys(this.updateListeners)) {
      const listener = this.updateListeners[id];
      if (listener.csn !== PREQUERY_CSN && listener.csn > notifyCsn) {
        listener.csn = notifyCsn;
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
    const parent = this;
    if (Object.keys(this.updateListeners).length === 0) {
      class NotifyListener extends Subscriber {

        constructor() {
          super();
          this.logger.category = "NotifyListener";
          this.logger.debug("NotifyListener is created");
        }

        onReceive(transctionJSON: string) {
          const transaction = EJSON.parse(transctionJSON) as TransactionObject;
          this.logger.info("received:", transaction.type, transaction.csn);
          if (transaction.type === TransactionType.ROLLBACK) {
            parent.notifyCsn = transaction.csn;
            parent.notifyRollback(transaction.csn);
          } else if (transaction.csn > parent.notifyCsn) {
            parent.notifyCsn = transaction.csn;
            setTimeout(() => {
              parent.notifyAll();
            });
          }
        }
      }

      if (!this.updateListenerKey) {
        this.node.subscribe(CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), new NotifyListener())
          .then((key) => { this.updateListenerKey = key; });
      }
    }

    const id = uuidv1();
    this.updateListeners[id] = {
      listener,
      csn: this.currentCsn,
      minInterval: minInterval || 0,
      notifyTime: 0,
    };
    return id;
  }

  /**
   * データベースの更新通知のリスナを解除する
   * @param id 登録時のID
   */
  removeUpdateListener(id: string) {
    delete this.updateListeners[id];
    if (Object.keys(this.updateListeners).length === 0) {
      if (this.updateListenerKey) {
        this.node.unsubscribe(this.updateListenerKey);
        this.updateListenerKey = null;
      }
    }
  }

  /**
   * データベースの更新通知のリスナを全解除
   */
  resetUpdateListener() {
    this.updateListeners = {};
    if (this.updateListenerKey) {
      this.node.unsubscribe(this.updateListenerKey);
      this.updateListenerKey = null;
    }
  }

  addUpdateListenerForSubset(subset: string, listener: (csn: number) => void, minInterval?: number) {
    // TODO 実装

  }

}
