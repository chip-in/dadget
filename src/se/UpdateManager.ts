import { ResourceNode, ServiceEngine, Subscriber } from '@chip-in/resource-node'
import { TransactionObject } from '../db/Transaction'
import { DatabaseRegistry, SubsetDef } from "./DatabaseRegistry"
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

  constructor(option: UpdateManagerConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    node.logger.debug("UpdateManager is started")

    if (!this.option.database) {
      return Promise.reject(new Error("Database name is missing."));
    }
    this.database = this.option.database
    let subset = this.subset = this.option.subset

    // (参照コードの再現のため内部クラスにしたケース)
    class UpdateListener extends Subscriber {

      protected database: string
      protected subsetDefinition: SubsetDef

      constructor(database: string, subsetDefinition: SubsetDef) {
        super()
        this.database = database
        this.subsetDefinition = subsetDefinition
        node.logger.debug("UpdateListener is created")
      }

      convertTransactionForSubset(transaction: TransactionObject): TransactionObject {
        // TODO サブセット用のトランザクション内容に変換
        return transaction
      }

      onReceive(transctionJSON: string) {
        let transaction = JSON.parse(transctionJSON);
        let subsetTransaction = this.convertTransactionForSubset(transaction);
        node.publish(CORE_NODE.PATH_SUBSET_TRANSACTION
          .replace(/:database\b/g, this.database)
          .replace(/:subset\b/g, subset), JSON.stringify(subsetTransaction));
      }
    }

    // サブセットの定義を取得する
    let seList = node.searchServiceEngine("DatabaseRegistry", { database: this.database });
    if (seList.length != 1) {
      return Promise.reject(new Error("DatabaseRegistry is missing, or there are multiple ones."));
    }
    let registry = seList[0] as DatabaseRegistry;
    this.subsetDefinition = registry.getMetadata().subsets[this.subset];

    // トランザクションを購読する
    let promise = Promise.resolve()
    let listener = new UpdateListener(this.database, this.subsetDefinition)
    promise = promise.then(() => node.subscribe(
      CORE_NODE.PATH_TRANSACTION.replace(/:database\b/g, this.database), listener))
    return promise
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }
}
