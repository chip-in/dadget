import { ResourceNode, ServiceEngine } from "@chip-in/resource-node";

/**
 * 更新マネージャコンフィグレーションパラメータ(廃止)
 */
export class UpdateManagerConfigDef {

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
 * 更新マネージャ(廃止)
 */
export class UpdateManager extends ServiceEngine {

  public bootOrder = 40;

  constructor(option: UpdateManagerConfigDef) {
    super(option);
  }

  start(_node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  stop(_node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }
}
