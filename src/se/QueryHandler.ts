import * as URL from "url";
import * as EJSON from "../util/Ejson";

import { ResourceNode, ServiceEngine } from "@chip-in/resource-node";
import { CORE_NODE, EXPORT_LIMIT_NUM, MAX_EXPORT_NUM, SPLIT_IN_SUBSET_DB } from "../Config";
import { ERROR } from "../Errors";
import { LOG_MESSAGES } from "../LogMessages";
import { DadgetError } from "../util/DadgetError";
import { Logger } from "../util/Logger";
import Dadget, { CountResult, CsnMode, QueryResult, CLIENT_VERSION } from "./Dadget";
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry";
import { Util } from "../util/Util";

/**
 * クエリハンドラコンフィグレーションパラメータ
 */
export class QueryHandlerConfigDef {

  /**
   * データベース名
   */
  database: string;

  /**
   * サブセット名
   */
  subset: string;
}

/**
 * クエリハンドラ(QueryHandler)
 */
export class QueryHandler extends ServiceEngine {

  public bootOrder = 30;
  private logger: Logger;
  private option: QueryHandlerConfigDef;
  private node: ResourceNode;
  private database: string;
  private subsetName: string;
  private subsetDefinition: SubsetDef;

  constructor(option: QueryHandlerConfigDef) {
    super(option);
    this.option = option;
    this.database = option.database;
    this.subsetName = option.subset;
    this.logger = Logger.getLogger("QueryHandler", this.getDbName());
    this.logger.debug(LOG_MESSAGES.CREATED, ["QueryHandler"]);
  }

  getPriority(): number {
    return this.subsetDefinition.priority;
  }

  getNode(): ResourceNode {
    return this.node;
  }

  getDbName(): string {
    return this.database + SPLIT_IN_SUBSET_DB + this.subsetName;
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug(LOG_MESSAGES.STARTING, ["QueryHandler"]);

    if (!this.database) {
      throw new DadgetError(ERROR.E2301, ["Database name is missing."]);
    }
    if (this.database.match(/--/)) {
      throw new DadgetError(ERROR.E2301, ["Database name can not contain '--'."]);
    }

    if (!this.subsetName) {
      throw new DadgetError(ERROR.E2301, ["Subset name is missing."]);
    }
    if (this.subsetName.match(/--/)) {
      throw new DadgetError(ERROR.E2301, ["Subset name can not contain '--'."]);
    }

    // サブセットの定義を取得する
    const seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length !== 1) {
      throw new DadgetError(ERROR.E2301, ["DatabaseRegistry is missing, or there are multiple ones."]);
    }
    const registry = seList[0] as DatabaseRegistry;
    this.subsetDefinition = registry.getMetadata().subsets[this.subsetName];

    this.logger.debug(LOG_MESSAGES.STARTED, ["QueryHandler"]);
    return Promise.resolve();
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  query(csn: number, query: object, sort?: object, limit?: number, csnMode?: CsnMode, projection?: object, offset?: number): Promise<QueryResult> {
    this.logger.info(LOG_MESSAGES.QUERY_CSN, [csnMode || ""], [csn]);
    this.logger.info(LOG_MESSAGES.QUERY, [JSON.stringify(query)]);
    const request = {
      csn,
      query: EJSON.stringify(query),
      sort: sort ? EJSON.stringify(sort) : undefined,
      limit,
      csnMode,
      projection: projection ? EJSON.stringify(projection) : undefined,
      offset,
      version: CLIENT_VERSION,
    };
    return this.node.fetch(CORE_NODE.PATH_SUBSET
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName) + CORE_NODE.PATH_QUERY, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: EJSON.stringify(request),
    })
      .then((result) => {
        if (typeof result.ok !== "undefined" && !result.ok) { throw new Error("fetch error:" + result.statusText); }
        return result.json();
      })
      .then(EJSON.asyncDeserialize)
      .then((data) => {
        this.logger.info(LOG_MESSAGES.DEBUG_LOG, [`query result: ${data.status}, ${data.result?.csn}, ${data.result?.restQuery}`]);
        if (data.status === "NG") { throw data.reason.code && data.reason.message ? DadgetError.from(data.reason) : new Error(JSON.stringify(data.reason)); }
        if (data.status === "HUGE") { return this._handle_huge_response(data.result, projection); }
        if (data.status === "OK") { return data.result; }
        throw new Error("fetch error:" + JSON.stringify(data));
      })
      .catch((error) => {
        this.logger.warn(LOG_MESSAGES.QUERY_ERROR, [error.toString(), EJSON.stringify(request)]);
        return { csn, resultSet: [], restQuery: query, csnMode, error };
      });
  }
  _handle_huge_response(result: QueryResult, projection?: object): Promise<QueryResult> {
    const csn = result.csn;
    let result2 = { ...result, resultSet: [] } as QueryResult;
    return Util.promiseWhile<{ ids: object[] }>(
      { ids: [...result.resultSet] },
      (whileData) => {
        return whileData.ids.length !== 0;
      },
      (whileData) => {
        const idMap = new Map();
        const ids = [];
        for (let i = 0; i < MAX_EXPORT_NUM; i++) {
          const row = whileData.ids.shift();
          if (row) {
            const id = (row as any)._id;
            idMap.set(id, id);
            ids.push(id);
          }
        }
        return Dadget._query(this.getNode(), this.database, { _id: { $in: ids } }, undefined, EXPORT_LIMIT_NUM, undefined, csn, "strict", projection)
          .then((rowData) => {
            if (rowData.resultSet.length === 0) { return whileData; }
            for (const data of rowData.resultSet) {
              result2.resultSet.push(data);
              idMap.delete((data as any)._id);
            }
            for (const id of idMap.keys()) {
              whileData.ids.push({ _id: id });
            }
            return whileData;
          });
      })
      .then(() => {
        return result2;
      });
  }

  count(csn: number, query: object, csnMode?: CsnMode): Promise<CountResult> {
    this.logger.info(LOG_MESSAGES.COUNT_CSN, [csnMode || ""], [csn]);
    this.logger.info(LOG_MESSAGES.COUNT, [JSON.stringify(query)]);
    const request = { csn, query: EJSON.stringify(query), csnMode, version: CLIENT_VERSION };
    return this.node.fetch(CORE_NODE.PATH_SUBSET
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName) + CORE_NODE.PATH_COUNT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: EJSON.stringify(request),
    })
      .then((result) => {
        if (typeof result.ok !== "undefined" && !result.ok) { throw new Error("fetch error:" + result.statusText); }
        return result.json();
      })
      .then(EJSON.asyncDeserialize)
      .then((data) => {
        this.logger.info(LOG_MESSAGES.DEBUG_LOG, [`count result: ${data.status}, ${data.result?.csn}, ${data.result?.restQuery}`]);
        if (data.status === "NG") { throw data.reason.code && data.reason.message ? DadgetError.from(data.reason) : new Error(JSON.stringify(data.reason)); }
        if (data.status === "OK") { return data.result; }
        throw new Error("fetch error:" + JSON.stringify(data));
      })
      .catch((error) => {
        this.logger.warn(LOG_MESSAGES.COUNT_ERROR, [error.toString(), EJSON.stringify(request)]);
        return { csn, resultCount: 0, restQuery: query, csnMode, error };
      });
  }

  wait(csn: number): Promise<void> {
    const request = { csn };
    return this.node.fetch(CORE_NODE.PATH_SUBSET
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName) + CORE_NODE.PATH_WAIT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: EJSON.stringify(request),
    })
      .then((result) => {
        if (typeof result.ok !== "undefined" && !result.ok) { throw Error("fetch error:" + result.statusText); }
        this.logger.info(LOG_MESSAGES.DEBUG_LOG, ["subset waited"]);
        return result.json();
      })
      .then(EJSON.asyncDeserialize)
      .then((data) => {
        if (data.status === "NG") { throw Error(JSON.stringify(data.reason)); }
        if (data.status === "OK") return;
        throw new Error("fetch error:" + JSON.stringify(data));
      })
      .catch((reason) => {
        this.logger.warn(LOG_MESSAGES.ERROR_MSG, [reason.toString()], [400]);
        return;
      });
  }
}
