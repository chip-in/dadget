import * as EJSON from '../util/Ejson'

import { ResourceNode, ServiceEngine, Subscriber } from '@chip-in/resource-node'
import { TransactionObject } from '../db/Transaction'
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { CORE_NODE } from "../Config"

/**
 * 更新マネージャコンフィグレーションパラメータ
 */
export class UpdateManagerConfigDef {

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
 * 更新マネージャ(UpdateManager)
 * 
 * 更新マネージャは、コンテキストマネージャが発信する更新情報（トランザクションオブジェクト）を受信して更新トランザクションをサブセットへのトランザクションに変換し更新レシーバに転送する。
 */
export class UpdateManager extends ServiceEngine {

  private option: UpdateManagerConfigDef
  private node: ResourceNode
  private database: string
  private subset: string
  private subsetDefinition: SubsetDef
  private updateListenerKey: string | null

  constructor(option: UpdateManagerConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    this.logger.debug("UpdateManager is starting")

    if (!this.option.database) {
      return Promise.reject(new DadgetError(ERROR.E2501, ["Database name is missing."]));
    }
    this.database = this.option.database
    if (!this.option.subset) {
      return Promise.reject(new DadgetError(ERROR.E2501, ["Subset name is missing."]));
    }
    let subset = this.subset = this.option.subset

    // (参照コードの再現のため内部クラスにしたケース)
    class UpdateListener extends Subscriber {

      protected database: string
      protected subsetDefinition: SubsetDef

      constructor(database: string, subsetDefinition: SubsetDef) {
        super()
        this.database = database
        this.subsetDefinition = subsetDefinition
        this.logger.debug("UpdateListener is created")
      }

      convertTransactionForSubset(transaction: TransactionObject): TransactionObject {
        // TODO サブセット用のトランザクション内容に変換
        return transaction
      }

      onReceive(transctionJSON: string) {
        let transaction = EJSON.parse(transctionJSON) as TransactionObject
        let subsetTransaction = this.convertTransactionForSubset(transaction);
        node.publish(CORE_NODE.PATH_SUBSET_TRANSACTION
          .replace(/:database\b/g, this.database)
          .replace(/:subset\b/g, subset), EJSON.stringify(subsetTransaction));
      }
    }

    // サブセットの定義を取得する
    let seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length != 1) {
      return Promise.reject(new DadgetError(ERROR.E2501, ["DatabaseRegistry is missing, or there are multiple ones."]));
    }
    let registry = seList[0] as DatabaseRegistry;
    this.subsetDefinition = registry.getMetadata().subsets[this.subset];

    // トランザクションを購読する
    let promise = Promise.resolve()
    let listener = new UpdateListener(this.database, this.subsetDefinition)
    promise = promise.then(() => node.subscribe(
      CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), listener)).then(key => { this.updateListenerKey = key })
    this.logger.debug("UpdateManager is started")
    return promise
  }

  stop(node: ResourceNode): Promise<void> {
    let promise = Promise.resolve()
    if (this.updateListenerKey) promise = this.node.unsubscribe(this.updateListenerKey)
    return promise
  }
}
