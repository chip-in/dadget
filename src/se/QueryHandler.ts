import * as EJSON from '../util/Ejson'

import { ResourceNode, ServiceEngine } from '@chip-in/resource-node'
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"
import { QuestResult } from "./Dadget"
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { CORE_NODE } from "../Config"

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
    let seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database })
    if (seList.length != 1) {
      return Promise.reject(new DadgetError(ERROR.E2301, ["DatabaseRegistry is missing, or there are multiple ones."]));
    }
    let registry = seList[0] as DatabaseRegistry
    this.subsetDefinition = registry.getMetadata().subsets[this.subsetName]

    this.logger.debug("QueryHandler is started")
    return Promise.resolve()
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
  }

  query(csn: number, restQuery: object, sort?: object, limit?: number, offset?: number): Promise<QuestResult> {
    // TODO csn が0の場合は、最新のcsnを取得、それ以外の場合はcsnを一致させる
    let request = {
      csn: csn
      , query: restQuery
      , sort: sort
      , limit: limit
      , offset: offset
    }
    return this.node.fetch(CORE_NODE.PATH_SUBSET
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName) + "/query", {
        method: 'POST',
        body: EJSON.stringify(request),
        headers: {
          "Content-Type": "application/json"
        }
      })
      .then(result => result.json())
      .then(_ => {
        let data = EJSON.deserialize(_)
        if(data.status == "NG") throw data.reason
        if(data.status == "OK") return data.result
        throw new Error("fetch error:" + JSON.stringify(data))
      })
  }
}