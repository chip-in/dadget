import * as EJSON from "../util/Ejson"

import { ResourceNode, ServiceEngine } from "@chip-in/resource-node"
import { CORE_NODE } from "../Config"
import { ERROR } from "../Errors"
import { DadgetError } from "../util/DadgetError"
import { CsnMode, QueryResult } from "./Dadget"
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"

/**
 * クエリハンドラコンフィグレーションパラメータ
 */
export class QueryHandlerConfigDef {

  /**
   * データベース名
   */
  database: string

  /**
   * サブセット名
   */
  subset: string
}

/**
 * クエリハンドラ(QueryHandler)
 */
export class QueryHandler extends ServiceEngine {

  public bootOrder = 30
  private option: QueryHandlerConfigDef
  private node: ResourceNode
  private database: string
  private subsetName: string
  private subsetDefinition: SubsetDef

  constructor(option: QueryHandlerConfigDef) {
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

  start(node: ResourceNode): Promise<void> {
    this.node = node
    this.logger.debug("QueryHandler is starting")

    if (!this.option.database) {
      return Promise.reject(new DadgetError(ERROR.E2301, ["Database name is missing."]));
    }
    this.database = this.option.database
    if (!this.option.subset) {
      return Promise.reject(new DadgetError(ERROR.E2301, ["Subset name is missing."]));
    }
    this.subsetName = this.option.subset

    // サブセットの定義を取得する
    const seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database })
    if (seList.length !== 1) {
      return Promise.reject(new DadgetError(ERROR.E2301, ["DatabaseRegistry is missing, or there are multiple ones."]));
    }
    const registry = seList[0] as DatabaseRegistry
    this.subsetDefinition = registry.getMetadata().subsets[this.subsetName]

    this.logger.debug("QueryHandler is started")
    return Promise.resolve()
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
  }

  query(csn: number, restQuery: object, sort?: object, limit?: number, offset?: number, csnMode?: CsnMode): Promise<QueryResult> {
    const request = { csn, query: restQuery, sort, limit, offset, csnMode }
    return this.node.fetch(CORE_NODE.PATH_SUBSET
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName) + "/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: EJSON.stringify(request),
      })
      .then((result) => {
        if (typeof result.ok !== "undefined" && !result.ok) { throw Error("fetch error:" + result.statusText) }
        return result.json()
      })
      .then((_) => {
        const data = EJSON.deserialize(_)
        if (data.status === "NG") { throw data.reason }
        if (data.status === "OK") { return data.result }
        throw new Error("fetch error:" + JSON.stringify(data))
      })
  }
}
