import { ResourceNode, ServiceEngine } from "@chip-in/resource-node";
import { ERROR } from "../Errors";
import { LOG_MESSAGES } from "../LogMessages";
import { DadgetError } from "../util/DadgetError";
import { Logger } from "../util/Logger";

/**
 * サブセット定義
 */
export class SubsetDef {

  /**
   * 優先度
   */
  priority: number;

  /**
   * サブセットのクエリ
   */
  query?: object;
}

/**
 * インデックス定義
 */
export class IndexDef {

  /**
   * インデックスの属性名をキーとしたハッシュオブジェクトで値はインデックスが昇順(1)か降順(-1)かを示す。その仕様はmongo db の createIndex の第一引数に準じる
   */
  index: { [field: string]: number };

  /**
   * インデックスの属性を指定する。その仕様はmongo db の createIndex の第二引数に準じる
   */
  property?: { unique: boolean };
}

/**
 * データベースメタデータ
 */
export class DatabaseMetadata {

  /**
   * インデックス設定
   */
  indexes?: IndexDef[];

  /**
   * サブセット定義
   */
  subsets: { [subsetName: string]: SubsetDef };
}

/**
 * データベースレジストリコンフィグレーションパラメータ
 */
export class DatabaseRegistryConfigDef {

  /**
   * データベース名
   */
  database: string;

  /**
   * メタデータ
   */
  metadata: DatabaseMetadata;
}

/**
 * データベースレジストリ(DatabaseRegistry)
 */
export class DatabaseRegistry extends ServiceEngine {

  public bootOrder = 10;
  private logger: Logger;
  private option: DatabaseRegistryConfigDef;
  private node: ResourceNode;

  constructor(option: DatabaseRegistryConfigDef) {
    super(option);
    this.logger = Logger.getLogger("DatabaseRegistry", option.database);
    this.logger.debug(LOG_MESSAGES.CREATED, ["DatabaseRegistry"]);
    this.option = option;
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug(LOG_MESSAGES.STARTING, ["DatabaseRegistry"]);
    if (!this.option.database) {
      throw new DadgetError(ERROR.E2201, ["Database name is missing."]);
    }
    if (this.option.database.match(/--/)) {
      throw new DadgetError(ERROR.E2201, ["Database name can not contain '--'."]);
    }
    if (!this.option.metadata) {
      throw new DadgetError(ERROR.E2201, ["metadata is missing."]);
    }
    this.logger.debug(LOG_MESSAGES.STARTED, ["DatabaseRegistry"]);
    return Promise.resolve();
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  getMetadata(): DatabaseMetadata {
    return this.option.metadata;
  }
}
