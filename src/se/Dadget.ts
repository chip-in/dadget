import * as parser from "mongo-parse";
import { v1 as uuidv1 } from "uuid";

import { Logger as ChipInLogger } from "@chip-in/logger";
import { ResourceNode, ServiceEngine, Subscriber } from "@chip-in/resource-node";
import { CORE_NODE, setAccessControlAllowOrigin, SPLIT_IN_INDEXED_DB, SPLIT_IN_SUBSET_DB } from "../Config";
import { PersistentDb } from "../db/container/PersistentDb";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { LOG_MESSAGES } from "../LogMessages";
import { DadgetError } from "../util/DadgetError";
import * as EJSON from "../util/Ejson";
import { Logger } from "../util/Logger";
import { Util } from "../util/Util";
import { ATOMIC_OPERATION_MAX_LOCK_TIME, ContextManager, ExecOptions, TransactionUpdateDetail } from "./ContextManager";
import { DatabaseRegistry } from "./DatabaseRegistry";
import { QueryHandler } from "./QueryHandler";
import { SubsetStorage } from "./SubsetStorage";
import { UpdateManager } from "./UpdateManager";
import { UniqueCache } from "./UniqueCache";

const QUERY_ERROR_RETRY_COUNT = 4;
const QUERY_ERROR_WAIT_TIME = 5000;
export const CLIENT_VERSION = 1;

/**
 * Dadgetコンフィグレーションパラメータ
 */
export class DadgetConfigDef {

  /**
   * データベース名
   */
  database: string;

  /**
   * 未使用サブセット自動削除フラグ
   */
  autoDeleteSubset?: boolean;

  /**
   * ジャーナルのnewデータの文字列化による高速化フラグ
   */
  useJournalStringification?: boolean;
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

  public static enableDeleteSubset = true;

  public bootOrder = 60;
  private logger: Logger;
  private option: DadgetConfigDef;
  private node: ResourceNode;
  private database: string;
  private currentCsn: number = PREQUERY_CSN;
  private notifyCsn: number = 0;
  private updateListeners: { [id: string]: { listener: (csn: number) => void, csn: number, minInterval: number, notifyTime: number } } = {};
  private updateListenerKey: string | null;
  private latestCsn: number;
  private hasSubset = false;
  private lockNotify = false;

  constructor(option: DadgetConfigDef) {
    super(option);
    this.logger = Logger.getLogger("Dadget", option.database);
    this.logger.debug(LOG_MESSAGES.CREATED, ["Dadget"]);
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
      UniqueCache,
    });
  }

  getDatabase() {
    return this.option.database;
  }

  _clone() {
    const dadget = Object.assign(new Dadget(this.option), this);
    dadget.updateListeners = {};
    dadget.updateListenerKey = null;
    return dadget;
  }

  _afterCommit(dadget: Dadget) {
    if (this.latestCsn < dadget.latestCsn) this.latestCsn = dadget.latestCsn;
  }

  /**
   * Dadgetの取得
   */
  static getDb(node: ResourceNode, database: string): Dadget {
    const seList = node.searchServiceEngine("Dadget", { database });
    if (seList.length !== 1) {
      throw new Error("Dadget is missing:" + database);
    }
    return seList[0] as Dadget;
  }

  /**
   * トランザクション実行
   */
  static async execTransaction(node: ResourceNode, databases: string[], callback: (...seList: Dadget[]) => Promise<void>) {
    const seList = databases.map((database) => new DadgetTr(Dadget.getDb(node, database)));
    const seMap: { [name: string]: DadgetTr } = seList.reduce((map: any, se: DadgetTr) => { map[se.getDatabase()] = se; return map; }, {});
    const sorted = [...databases].sort();
    let checkInterval;
    try {
      for (const db of sorted) {
        await seMap[db]._begin();
      }
      // Time-out prevention
      checkInterval = setInterval(() => seList.map((se) => se._check()), ATOMIC_OPERATION_MAX_LOCK_TIME / 2);
      await callback.apply(null, seList);
      seList.map((se) => se._fix());
      clearInterval(checkInterval);
      checkInterval = undefined;
      await Promise.all(seList.map((se) => se._check()));
      await Promise.all(seList.map((se) => se._commit()));
    } catch (err) {
      if (checkInterval) { clearInterval(checkInterval); }
      await Promise.all(seList.map((se) => se._rollback()));
      throw err;
    }
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug(LOG_MESSAGES.STARTING, ["Dadget"]);
    if (!this.option.database) {
      throw new DadgetError(ERROR.E2101, ["Database name is missing."]);
    }
    if (this.option.database.match(/--/)) {
      throw new DadgetError(ERROR.E2101, ["Database name can not contain '--'."]);
    }
    const database = this.database = this.option.database;

    const subsetStorages = node.searchServiceEngine("SubsetStorage", { database }) as SubsetStorage[];
    // Delete unused persistent databases
    if (Dadget.enableDeleteSubset && this.option.autoDeleteSubset) {
      PersistentDb.getAllStorage()
        .then((storageList) => {
          const subsetNames = subsetStorages
            .filter((subset) => subset.getType() === "persistent")
            .map((subset) => subset.getDbName());
          for (const storageName of storageList) {
            if (!storageName.startsWith(database + SPLIT_IN_SUBSET_DB)) { continue; }
            const [dbName] = storageName.split(SPLIT_IN_INDEXED_DB);
            if (subsetNames.indexOf(dbName) < 0) {
              this.logger.warn(LOG_MESSAGES.DELETE_STORAGE, [storageName]);
              PersistentDb.deleteStorage(storageName);
            }
          }
        })
        .catch((reason) => {
          this.logger.warn(LOG_MESSAGES.FAILED_SWEEP_STORAGE, [reason.toString()]);
        });
    }

    if (subsetStorages.length > 0) {
      this.hasSubset = true;
      subsetStorages[0].notifyListener = this;
    }

    this.logger.debug(LOG_MESSAGES.STARTED, ["Dadget"]);
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
    csnMode?: CsnMode,
    projection?: object): Promise<QueryResult> {

    let queryHandlers = node.searchServiceEngine("QueryHandler", { database }) as QueryHandler[];
    if (queryHandlers.length === 0) { throw new Error("QueryHandlers required"); }
    queryHandlers = Dadget.sortQueryHandlers(queryHandlers);
    if (!csn) { csn = 0; }
    const resultSet: object[] = [];
    return Promise.resolve({ csn, resultSet, restQuery: query, queryHandlers, csnMode } as QueryResult)
      .then(function queryFallback(request): Promise<QueryResult> {
        if (!request.restQuery) { return Promise.resolve(request); }
        if (!request.queryHandlers || request.queryHandlers.length === 0) {
          return Promise.resolve(request);
        }
        const qh = request.queryHandlers.shift();
        if (qh == null) { throw new Error("never happen"); }
        return qh.query(request.csn, request.restQuery, sort, limit, csnMode, projection, offset)
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
        let hasDupulicate = false;
        for (const item of result.resultSet as { _id: string }[]) {
          if (itemMap[item._id]) {
            console.warn("hasDupulicate:" + item._id);
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
          if (limit && limit > 0) {
            list = list.slice(0, limit);
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
   * @param projection mongoDBと同じprojectionオブジェクト
   * @returns 取得した結果オブジェクトを返すPromiseオブジェクト
   */
  query(query: object, sort?: object, limit?: number, offset?: number, csn?: number, csnMode?: CsnMode, projection?: object): Promise<QueryResult> {
    if (this.latestCsn && !csn) {
      csn = this.latestCsn;
      csnMode = "latest";
    }
    let count = QUERY_ERROR_RETRY_COUNT;
    const retryAction = (_: any) => {
      count--;
      return new Promise<QueryResult>((resolve) => {
        setTimeout(() => {
          Dadget._query(this.node, this.database, query, sort, limit, offset, csn, csnMode, projection)
            .then((result) => {
              resolve(result);
            });
        }, QUERY_ERROR_WAIT_TIME);
      });
    };
    return Dadget._query(this.node, this.database, query, sort, limit, offset, csn, csnMode, projection)
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
        const cause = reason instanceof DadgetError ? reason :
          (reason.code && reason.message ? DadgetError.from(reason) : new DadgetError(ERROR.E2102, [reason.toString()]));
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
        const cause = reason instanceof DadgetError ? reason :
          (reason.code && reason.message ? DadgetError.from(reason) : new DadgetError(ERROR.E2102, [reason.toString()]));
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
   * @param csn トランザクションの前提となるコンテキスト通番(トランザクションの type が "insert" で重複のない UUID の場合は 0 を指定できる。それ以外の場合は更新するオブジェクトを取得、または重複IDが存在ないことを確認した query メソッドを実行した時の結果オブジェクトのcsn。)
   * @param request トランザクションの内容を持つオブジェクト
   * @param options 更新オプション(任意)
   * @return 更新されたオブジェクト(更新オプションが指定される場合、NULLの可能性がある)
   */
  exec(csn: number, request: TransactionRequest, options?: ExecOptions): Promise<object | null> {
    request.type = request.type.toLowerCase() as TransactionType;
    if (request.type !== TransactionType.INSERT &&
      request.type !== TransactionType.UPDATE &&
      request.type !== TransactionType.UPSERT &&
      request.type !== TransactionType.REPLACE &&
      request.type !== TransactionType.DELETE) {
      return Promise.reject(new DadgetError(ERROR.E2104));
    }
    return this._exec(csn, request, undefined, options);
  }

  _exec(csn: number, request: TransactionRequest, atomicId: string | undefined, options?: ExecOptions): Promise<object | null> {
    if (this.option.useJournalStringification) {
      if (request.new) request.new = EJSON.stringify(request.new);
      if (request.before) request.before = EJSON.stringify(request.before);
    }
    const sendData = { csn, request, atomicId, options, version: CLIENT_VERSION };
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
        if (result.status === "OK") {
          this.latestCsn = result.csn;
          return result.updateObject;
        } else if (result.reason) {
          throw DadgetError.from(result.reason);
        } else {
          throw new Error(JSON.stringify(result));
        }
      })
      .catch((reason) => {
        const cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2103, [reason.toString()]);
        return Promise.reject(cause);
      });
  }

  /**
   * execManyメソッドはコンテキストマネージャの Rest API を呼び出して複数のトランザクション要求を実行する。
   *
   * @param csn exec メソッドのcsnと同じ
   * @param request トランザクションの内容を持つオブジェクトの配列
   * @param options 更新オプション(任意)
   */
  execMany(csn: number, requests: TransactionRequest[], options?: ExecOptions): Promise<void> {
    for (const request of requests) {
      request.type = request.type.toLowerCase() as TransactionType;
      if (request.type !== TransactionType.INSERT &&
        request.type !== TransactionType.UPDATE &&
        request.type !== TransactionType.UPSERT &&
        request.type !== TransactionType.REPLACE &&
        request.type !== TransactionType.DELETE) {
        return Promise.reject(new DadgetError(ERROR.E2104));
      }
    }
    return this._execMany(csn, requests, undefined, options);
  }

  _execMany(csn: number, requests: TransactionRequest[], atomicId: string | undefined, options?: ExecOptions): Promise<void> {
    if (this.option.useJournalStringification) {
      for (const request of requests) {
        if (request.new) request.new = EJSON.stringify(request.new);
        if (request.before) request.before = EJSON.stringify(request.before);
      }
    }
    const sendData = { csn, requests, atomicId, options, version: CLIENT_VERSION };
    return this.node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database) + CORE_NODE.PATH_EXEC_MANY, {
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
        if (result.status === "OK") {
          this.latestCsn = result.csn;
        } else if (result.reason) {
          if (result.csn) this.latestCsn = result.csn;
          throw DadgetError.from(result.reason);
        } else {
          throw new Error(JSON.stringify(result));
        }
      })
      .catch((reason) => {
        const cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2105, [reason.toString()]);
        return Promise.reject(cause);
      });
  }

  /**
   * updateMany メソッドはクエリーで取得した結果に対し更新オペレーターを適用して更新する。
   *
   * @param query mongoDBと同じクエリーオブジェクト
   * @param operator 更新内容を記述するオペレータ。意味はmongoDB に準ずる。
   * @return 変更された行数
   */
  updateMany(query: object, operator: object): Promise<number> {
    return this._updateMany(query, operator, undefined);
  }

  _updateMany(query: object, operator: object, atomicId: string | undefined): Promise<number> {
    const sendData = { query, operator, atomicId, version: CLIENT_VERSION };
    return this.node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database) + CORE_NODE.PATH_UPDATE_MANY, {
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
        if (result.status === "OK") {
          this.latestCsn = result.csn;
          return result.count;
        } else if (result.reason) {
          if (result.csn) this.latestCsn = result.csn;
          throw DadgetError.from(result.reason);
        } else {
          throw new Error(JSON.stringify(result));
        }
      })
      .catch((reason) => {
        const cause = reason instanceof DadgetError ? reason : new DadgetError(ERROR.E2106, [reason.toString()]);
        return Promise.reject(cause);
      });
  }

  /**
   * clearメソッドは全データの削除を実行する。
   */
  clear(): Promise<void> {
    return this._clear();
  }

  _clear(force?: boolean): Promise<void> {
    const request = new TransactionRequest();
    request.type = force ? TransactionType.FORCE_ROLLBACK : TransactionType.TRUNCATE;
    request.target = "";
    return this._exec(0, request, undefined).then(() => { });
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
        listener.listener(notifyCsn);
      }
    }
  }

  procNotify(transaction: TransactionObject) {
    if (transaction.type === TransactionType.BEGIN ||
      transaction.type === TransactionType.BEGIN_IMPORT ||
      transaction.type === TransactionType.BEGIN_RESTORE) {
      this.lockNotify = true;
      return;
    }
    if (transaction.type === TransactionType.ABORT ||
      transaction.type === TransactionType.ABORT_IMPORT ||
      transaction.type === TransactionType.ABORT_RESTORE) {
      this.lockNotify = false;
      return;
    }
    if (transaction.type === TransactionType.END ||
      transaction.type === TransactionType.END_IMPORT ||
      transaction.type === TransactionType.END_RESTORE) {
      this.lockNotify = false;
    }
    if (this.lockNotify || transaction.committedCsn !== undefined) { return; }
    if (transaction.type === TransactionType.FORCE_ROLLBACK) {
      this.notifyCsn = transaction.csn;
      this.latestCsn = transaction.csn;
      this.notifyRollback(transaction.csn);
    } else if (transaction.csn > this.notifyCsn) {
      this.notifyCsn = transaction.csn;
      setTimeout(() => {
        this.notifyAll();
      });
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
    if (Object.keys(this.updateListeners).length === 0 && !this.hasSubset) {
      class NotifyListener extends Subscriber {
        private logger: Logger;

        constructor() {
          super();
          this.logger = Logger.getLogger("NotifyListener", parent.option.database);
          this.logger.debug(LOG_MESSAGES.CREATED, ["NotifyListener"]);
        }

        onReceive(transctionJSON: string) {
          const transaction = EJSON.parse(transctionJSON) as TransactionObject;
          this.logger.info(LOG_MESSAGES.RECEIVED_TYPE_CSN, [transaction.type], [transaction.csn]);
          parent.procNotify(transaction);
        }
      }

      if (!this.updateListenerKey) {
        this.node.subscribe(CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), new NotifyListener())
          .then((key) => { this.updateListenerKey = key; })
          .catch((err) => {
            this.logger.error(LOG_MESSAGES.ERROR_MSG, [err.toString()], [300]);
          });
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
        this.node.unsubscribe(this.updateListenerKey).catch(e => console.warn(e));
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
      this.node.unsubscribe(this.updateListenerKey).catch(e => console.warn(e));
      this.updateListenerKey = null;
    }
  }

  /**
   * 指定されたcsnでの更新内容を取得
   */
  fetchUpdateDetails(csn: number): Promise<TransactionUpdateDetail[]> {
    const sendData = { csn };
    return this.node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database) + CORE_NODE.PATH_GET_UPDATE_DATA, {
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
        if (result.status === "OK") {
          return result.list;
        } else {
          throw new Error("The specified journal data does not exist.");
        }
      })
      .catch((reason) => {
        const cause = new DadgetError(ERROR.E2108, [reason.toString()]);
        return Promise.reject(cause);
      });
  }

  /**
   * 最新CSNの取得
   */
  fetchLatestCsn(): Promise<number> {
    const sendData = {};
    return this.node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, this.database) + CORE_NODE.PATH_GET_LATEST_CSN, {
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
        if (result.status === "OK") {
          return result.csn;
        } else {
          throw new Error("The csn data does not exist.");
        }
      })
      .catch((reason) => {
        const cause = new DadgetError(ERROR.E2109, [reason.toString()]);
        return Promise.reject(cause);
      });
  }

  /**
   * Access-Control-Allow-Origin を設定する
   */
  static setServerAccessControlAllowOrigin(origin: string) {
    setAccessControlAllowOrigin(origin);
  }

  static getLogger() {
    return ChipInLogger;
  }
}

class DadgetTr {
  private orgDadget: Dadget;
  private dadget: Dadget;
  private atomicId?: string;
  private fixFlag = false;

  constructor(dadget: Dadget) {
    this.orgDadget = dadget;
    this.dadget = dadget._clone();
  }

  getDatabase() {
    return this.dadget.getDatabase();
  }

  _fix() {
    this.fixFlag = true;
  }

  _check(): Promise<void> {
    if (this.atomicId === undefined) { return Promise.reject(new DadgetError(ERROR.E2107)); }
    return this.dadget._exec(0, { type: TransactionType.CHECK, target: "" }, this.atomicId).then(() => { });
  }

  _begin(): Promise<void> {
    if (this.atomicId) { return Promise.reject("transaction is running"); }
    this.atomicId = Dadget.uuidGen();
    return this.dadget._exec(0, { type: TransactionType.BEGIN, target: "" }, this.atomicId).then(() => { });
  }

  _commit(): Promise<void> {
    if (this.atomicId === undefined) { return Promise.reject(new DadgetError(ERROR.E2107)); }
    return this.dadget._exec(0, { type: TransactionType.END, target: "" }, this.atomicId).then(() => {
      this.orgDadget._afterCommit(this.dadget);
    });
  }

  _rollback(): Promise<void> {
    if (this.atomicId === undefined) { return Promise.resolve(); }
    return this.dadget._exec(0, { type: TransactionType.ABORT, target: "" }, this.atomicId).then(() => { });
  }

  exec(csn: number, request: TransactionRequest, options?: ExecOptions): Promise<object | null> {
    if (this.fixFlag) { return Promise.reject(new DadgetError(ERROR.E2107)); }
    request.type = request.type.toLowerCase() as TransactionType;
    if (request.type !== TransactionType.INSERT &&
      request.type !== TransactionType.UPDATE &&
      request.type !== TransactionType.UPSERT &&
      request.type !== TransactionType.REPLACE &&
      request.type !== TransactionType.DELETE) {
      return Promise.reject(new DadgetError(ERROR.E2104));
    }
    return this.dadget._exec(csn, request, this.atomicId, options);
  }

  execMany(csn: number, requests: TransactionRequest[], options?: ExecOptions): Promise<void> {
    if (this.fixFlag) { return Promise.reject(new DadgetError(ERROR.E2107)); }
    for (const request of requests) {
      request.type = request.type.toLowerCase() as TransactionType;
      if (request.type !== TransactionType.INSERT &&
        request.type !== TransactionType.UPDATE &&
        request.type !== TransactionType.UPSERT &&
        request.type !== TransactionType.REPLACE &&
        request.type !== TransactionType.DELETE) {
        return Promise.reject(new DadgetError(ERROR.E2104));
      }
    }
    return this.dadget._execMany(csn, requests, this.atomicId, options);
  }

  updateMany(query: object, operator: object): Promise<number> {
    if (this.fixFlag) { return Promise.reject(new DadgetError(ERROR.E2107)); }
    return this.dadget._updateMany(query, operator, this.atomicId);
  }

  query(query: object, sort?: object, limit?: number, offset?: number, csn?: number, csnMode?: CsnMode, projection?: object): Promise<QueryResult> {
    return this.dadget.query(query, sort, limit, offset, csn, csnMode, projection);
  }

  count(query: object, csn?: number, csnMode?: CsnMode): Promise<number> {
    return this.dadget.count(query, csn, csnMode);
  }
}
