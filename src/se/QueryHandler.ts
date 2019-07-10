import * as URL from "url";
import * as EJSON from "../util/Ejson";

import { ResourceNode, ServiceEngine } from "@chip-in/resource-node";
import { CORE_NODE } from "../Config";
import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
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
  private option: QueryHandlerConfigDef;
  private node: ResourceNode;
  private database: string;
  private subsetName: string;
  private subsetDefinition: SubsetDef;

  constructor(option: QueryHandlerConfigDef) {
    super(option);
    this.logger.debug(JSON.stringify(option));
    this.option = option;
  }

  getPriority(): number {
    return this.subsetDefinition.priority;
  }

  getNode(): ResourceNode {
    return this.node;
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug("QueryHandler is starting");

    if (!this.option.database) {
      throw new DadgetError(ERROR.E2301, ["Database name is missing."]);
    }
    if (this.option.database.match(/--/)) {
      throw new DadgetError(ERROR.E2301, ["Database name can not contain '--'."]);
    }
    this.database = this.option.database;

    if (!this.option.subset) {
      throw new DadgetError(ERROR.E2301, ["Subset name is missing."]);
    }
    if (this.option.subset.match(/--/)) {
      throw new DadgetError(ERROR.E2301, ["Subset name can not contain '--'."]);
    }
    this.subsetName = this.option.subset;

    // サブセットの定義を取得する
    const seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length !== 1) {
      throw new DadgetError(ERROR.E2301, ["DatabaseRegistry is missing, or there are multiple ones."]);
    }
    const registry = seList[0] as DatabaseRegistry;
    this.subsetDefinition = registry.getMetadata().subsets[this.subsetName];

    this.logger.debug("QueryHandler is started");
    return Promise.resolve();
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  query(csn: number, query: object, sort?: object, limit?: number, csnMode?: CsnMode, projection?: object): Promise<QueryResult> {
    this.logger.debug("query:" + JSON.stringify(query));
    const request = {
      csn,
      query: EJSON.stringify(query),
      sort: sort ? EJSON.stringify(sort) : undefined,
      limit,
      csnMode,
      projection: projection ? EJSON.stringify(projection) : undefined,
    };
    const reqUrl = URL.format({
      pathname: CORE_NODE.PATH_SUBSET
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName) + CORE_NODE.PATH_QUERY,
      query: request,
    });
    return this.node.fetch(reqUrl)
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
        this.logger.warn("query error:" + reason.toString() + ", url:" + reqUrl);
        return { csn, resultSet: [], restQuery: query, csnMode };
      });
  }

  count(csn: number, query: object, csnMode?: CsnMode): Promise<CountResult> {
    const request = { csn, query: EJSON.stringify(query), csnMode };
    const reqUrl = URL.format({
      pathname: CORE_NODE.PATH_SUBSET
        .replace(/:database\b/g, this.database)
        .replace(/:subset\b/g, this.subsetName) + CORE_NODE.PATH_COUNT,
      query: request,
    });
    return this.node.fetch(reqUrl)
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
        this.logger.warn("count error:" + reason.toString() + ", url:" + reqUrl);
        return { csn, resultCount: 0, restQuery: query, csnMode };
      });
  }
}
