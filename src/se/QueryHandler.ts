import * as URL from "url";
import * as EJSON from "../util/Ejson";

import { ResourceNode, ServiceEngine } from "@chip-in/resource-node";
import { CORE_NODE, SPLIT_IN_SUBSET_DB } from "../Config";
import { ERROR } from "../Errors";
import { LOG_MESSAGES } from "../LogMessages";
import { DadgetError } from "../util/DadgetError";
import { Logger } from "../util/Logger";
import { CountResult, CsnMode, QueryResult } from "./Dadget";
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry";

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
    this.logger.debug(LOG_MESSAGES.QUERY, [JSON.stringify(query)]);
    const request = {
      csn,
      query: EJSON.stringify(query),
      sort: sort ? EJSON.stringify(sort) : undefined,
      limit,
      csnMode,
      projection: projection ? EJSON.stringify(projection) : undefined,
      offset,
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
        if (typeof result.ok !== "undefined" && !result.ok) { throw Error("fetch error:" + result.statusText); }
        return result.json();
      })
      .then((_) => {
        const data = EJSON.deserialize(_);
        if (data.status === "NG") { throw Error(JSON.stringify(data.reason)); }
        if (data.status === "OK") { return data.result; }
        throw new Error("fetch error:" + JSON.stringify(data));
      })
      .catch((reason) => {
        this.logger.warn(LOG_MESSAGES.QUERY_ERROR, [reason.toString(), EJSON.stringify(request)]);
        return { csn, resultSet: [], restQuery: query, csnMode };
      });
  }

  count(csn: number, query: object, csnMode?: CsnMode): Promise<CountResult> {
    this.logger.info(LOG_MESSAGES.COUNT_CSN, [csnMode || ""], [csn]);
    this.logger.debug(LOG_MESSAGES.COUNT, [JSON.stringify(query)]);
    const request = { csn, query: EJSON.stringify(query), csnMode };
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
        if (typeof result.ok !== "undefined" && !result.ok) { throw Error("fetch error:" + result.statusText); }
        return result.json();
      })
      .then((_) => {
        const data = EJSON.deserialize(_);
        if (data.status === "NG") { throw Error(JSON.stringify(data.reason)); }
        if (data.status === "OK") { return data.result; }
        throw new Error("fetch error:" + JSON.stringify(data));
      })
      .catch((reason) => {
        this.logger.warn(LOG_MESSAGES.COUNT_ERROR, [reason.toString(), EJSON.stringify(request)]);
        return { csn, resultCount: 0, restQuery: query, csnMode };
      });
  }
}
