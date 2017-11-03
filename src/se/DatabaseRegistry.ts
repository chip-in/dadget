import { ResourceNode, ServiceEngine } from '@chip-in/resource-node'

/**
 * サブセット定義
 */
export class SubsetDef {

  /**
   * 優先度
   */
  priority: number

  /**
   * サブセットのクエリ
   */
  query?: object
}

/**
 * インデックス定義
 */
export class IndexDef {

  /**
   * インデックスの属性名をキーとしたハッシュオブジェクトで値はインデックスが昇順(1)か降順(-1)かを示す。その仕様はmongo db の createIndex の第一引数に準じる
   */
  index: object

  /**
   * インデックスの属性を指定する。その仕様はmongo db の createIndex の第二引数に準じる
   */
  property?: object
}

/**
 * データベースメタデータ
 */
export class DatabaseMetadata {

  /**
   * インデックス設定
   */
  indexes: IndexDef[]

  /**
   * サブセット定義
   */
  subsets: { [subsetName: string]: SubsetDef }
}

/**
 * データベースレジストリコンフィグレーションパラメータ
 */
export class DatabaseRegistryConfigDef {

  /**
   * データベース名
   */
  database: string

  /**
   * メタデータ
   */
  metadata: DatabaseMetadata
}

/**
 * データベースレジストリ(DatabaseRegistry)
 */
export class DatabaseRegistry extends ServiceEngine {

  private option: DatabaseRegistryConfigDef
  private node: ResourceNode

  constructor(option: DatabaseRegistryConfigDef) {
    super(option)
    this.logger.debug(JSON.stringify(option))
    this.option = option
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node
    node.logger.debug("DatabaseRegistry is started")
    return Promise.resolve();
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  getMetadata(): DatabaseMetadata {
    return this.option.metadata
  }
}