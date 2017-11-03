import { ResourceNode, ServiceEngine } from '@chip-in/resource-node'
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"
import { QuestResult } from "./Dadget"
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
    node.logger.debug("QueryHandler is started")

    if (!this.option.database) {
      return Promise.reject(new Error("Database name is missing."))
    }
    this.database = this.option.database
    this.subsetName = this.option.subset

    // サブセットの定義を取得する
    let seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database })
    if (seList.length != 1) {
      return Promise.reject(new Error("DatabaseRegistry is missing, or there are multiple ones."))
    }
    let registry = seList[0] as DatabaseRegistry
    this.subsetDefinition = registry.getMetadata().subsets[this.subsetName]

    return Promise.resolve()
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve()
  }

  query(csn: number, restQuery: object): Promise<QuestResult> {
    // TODO csn が0の場合は、最新のcsnを取得、それ以外の場合はcsnを一致させる
    let request = {
      csn: csn,
      query: restQuery
    }
    return this.node.fetch(CORE_NODE.PATH_SUBSET
      .replace(/:database\b/g, this.database)
      .replace(/:subset\b/g, this.subsetName) + "/query", {
        method: 'POST',
        body: JSON.stringify(request),
        headers : {
          "Content-Type": "application/json"
        }
      })
      .then(result => result.json())
      .then(result => {
        return { csn: result.csn, resultSet: result.resultSet, restQuery: result.restQuery }
      })
  }
}