import * as ReadWriteLock from "rwlock";

import { ResourceNode, ServiceEngine } from "@chip-in/resource-node";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { LOG_MESSAGES } from "../LogMessages";
import { DadgetError } from "../util/DadgetError";
import { Logger } from "../util/Logger";
import { default as Dadget } from "./Dadget";
import { ContextManager } from "./ContextManager";

/**
 * ユニーク制約キャッシュSEコンフィグレーションパラメータ
 */
export class UniqueCacheConfigDef {

  /**
   * データベース名
   */
  readonly database: string;

  /**
   * ユニーク制約対象
   */
  readonly field: string;
}

/**
 * ユニーク制約キャッシュ(UniqueCache)
 *
 * ユニーク制約キャッシュは、ユニーク制約をインメモリで処理するためのサービスエンジンである。
 */
export class UniqueCache extends ServiceEngine {

  public bootOrder = 15;
  private currentCsn: number;
  private logger: Logger;
  private option: UniqueCacheConfigDef;
  private node: ResourceNode;
  private contextManager: ContextManager;
  private database: string;
  private field: string;
  private lock: ReadWriteLock;
  private updateLock: ReadWriteLock;
  private readyFlag: boolean = false;
  private errorFlag: boolean = false;
  private index = new Set();

  constructor(option: UniqueCacheConfigDef) {
    super(option);
    this.option = option;
    this.database = option.database;
    this.field = option.field;
    this.logger = Logger.getLogger("UniqueCache", this.database);
    this.logger.debug(LOG_MESSAGES.CREATED, ["UniqueCache"]);
    this.lock = new ReadWriteLock();
    this.updateLock = new ReadWriteLock();
  }

  getOption(): UniqueCacheConfigDef {
    return this.option;
  }

  getNode(): ResourceNode {
    return this.node;
  }

  getLock(): ReadWriteLock {
    return this.lock;
  }

  getReady(): boolean {
    return this.readyFlag;
  }

  pause(): void {
    this.readyFlag = false;
  }

  setReady(): void {
    this.readyFlag = true;
  }

  getCsn(): Promise<number> {
    return Promise.resolve(this.currentCsn);
  }

  updateCsn(csn: number): Promise<void> {
    this.currentCsn = csn;
    return Promise.resolve();
  }

  getContextManager(): ContextManager {
    if (this.contextManager) {
      return this.contextManager;
    }
    const seList = this.getNode().searchServiceEngine("ContextManager", { database: this.database });
    if (seList.length !== 1) {
      throw new DadgetError(ERROR.E2601, ["ContextManager is missing, or there are multiple ones."]);
    }
    this.contextManager = seList[0] as ContextManager;
    return this.contextManager;
  }

  start(node: ResourceNode): Promise<void> {
    this.node = node;
    this.logger.debug(LOG_MESSAGES.STARTING, ["UniqueCache"]);

    if (!this.database) {
      throw new DadgetError(ERROR.E2601, ["Database name is missing."]);
    }
    if (!this.field) {
      throw new DadgetError(ERROR.E2601, ["Field name is missing."]);
    }
    if (typeof this.field !== "string") {
      throw new DadgetError(ERROR.E2601, ["Field type error"]);
    }

    this.logger.debug(LOG_MESSAGES.STARTED, ["UniqueCache"]);

    return Promise.resolve();
  }

  stop(node: ResourceNode): Promise<void> {
    return Promise.resolve();
  }

  static _convertKey(fields: string | string[], obj: any) {
    if (obj === undefined || obj === null) return null;
    if (typeof fields === "string") {
      fields = fields.split(',');
    }
    const v = [];
    for (let field of fields) {
      const val = obj[field];
      if (val === undefined || val === null) return null;
      v.push(val);
    }
    return JSON.stringify(v);
  }

  private _insert(obj: any): Promise<void> {
    const val = UniqueCache._convertKey(this.field, obj);
    if (val !== undefined && val !== null) {
      if (this.index.has(val)) {
        this.errorFlag = true;
      } else {
        this.index.add(val);
      }
    }
    return Promise.resolve();
  }

  private _insertMany(list: object[]): Promise<void> {
    for (const obj of list) {
      this._insert(obj);
    }
    return Promise.resolve();
  }

  private _update(before: any, obj: any): Promise<void> {
    if (UniqueCache._convertKey(this.field, before) !== UniqueCache._convertKey(this.field, obj)) {
      this._delete(before);
      this._insert(obj);
    }
    return Promise.resolve();
  }

  private _delete(obj: any): Promise<void> {
    const val = UniqueCache._convertKey(this.field, obj);
    if (val !== undefined && val !== null) {
      this.index.delete(val);
    }
    return Promise.resolve();
  }

  private _deleteAll(): Promise<void> {
    this.index.clear();
    this.errorFlag = false;
    return Promise.resolve();
  }

  private _has(val: any): Promise<boolean> {
    if (this.errorFlag) return Promise.resolve(true);
    return Promise.resolve(this.index.has(val));
  }

  /**
   * トランザクションの処理を行う
   */
  procTransaction(transaction: TransactionObject) {
    console.log("UniqueCache waiting writeLock1");
    this.updateLock.writeLock((_release1) => {
      console.log("UniqueCache got writeLock1");
      const release1 = () => {
        console.log("UniqueCache released writeLock1");
        _release1();
      };
      console.log("UniqueCache waiting writeLock2");
      this.getLock().writeLock((_release2) => {
        console.log("UniqueCache got writeLock2");
        const release2 = () => {
          console.log("UniqueCache released writeLock2");
          _release2();
        };
        this.getCsn()
          .then((csn) => {
            if (!this.getReady()) {
              return this.adjustData(transaction.csn)
                .then(() => { release2(); })
                .then(() => { release1(); });
            } else if (csn > transaction.csn && transaction.type === TransactionType.FORCE_ROLLBACK) {
              return this.resetData(transaction.csn, true)
                .then(() => { release2(); })
                .then(() => { release1(); });
            } else if (csn >= transaction.csn) {
              release2();
              release1();
              return;
            } else {
              let promise = Promise.resolve();
              if (csn < transaction.csn - 1) {
                promise = promise.then(() => this.adjustData(transaction.csn - 1));
              }
              promise = this.updateSubsetDb(promise, transaction);
              return promise.then(() => {
                release2();
                release1();
              });
            }
          })
          .catch((e) => {
            this.logger.error(LOG_MESSAGES.ERROR_MSG, [e.toString()], [202]);
            release2();
            release1();
          });
      });
    });
  }

  private updateSubsetDb(promise: Promise<void>, transaction: TransactionObject): Promise<void> {
    if (transaction.csn > 1) {
      promise = promise.then(() => this.getContextManager().getJournalDb().findByCsn(transaction.csn - 1))
        .then((journal) => {
          if (!journal) {
            throw new Error("journal not found, csn:" + (transaction.csn - 1)
              + ", database:" + this.database);
          }
          if (journal.digest !== transaction.beforeDigest) {
            throw new Error("beforeDigest mismatch, csn:" + transaction.csn
              + ", database:" + this.database
              + ", journal digest:" + journal.digest
              + ", transaction beforeDigest:" + transaction.beforeDigest);
          }
        });
    }

    const type = transaction.type;
    if ((type === TransactionType.INSERT || type === TransactionType.RESTORE) && transaction.new) {
      const newObj = TransactionRequest.getNew(transaction);
      const obj = { ...newObj, _id: transaction.target, csn: transaction.csn };
      promise = promise.then(() => this._insert(obj));
    } else if (type === TransactionType.UPDATE && transaction.before) {
      const before = TransactionRequest.getBefore(transaction);
      const updateObj = TransactionRequest.applyOperator(transaction, before);
      updateObj.csn = transaction.csn;
      promise = promise.then(() => this._update(before, updateObj));
    } else if (type === TransactionType.UPSERT || type === TransactionType.REPLACE) {
      if (transaction.before) {
        const before = TransactionRequest.getBefore(transaction);
        const updateObj = TransactionRequest.applyOperator(transaction, before);
        updateObj.csn = transaction.csn;
        promise = promise.then(() => this._update(before, updateObj));
      } else {
        const updateObj = TransactionRequest.applyOperator(transaction);
        updateObj.csn = transaction.csn;
        promise = promise.then(() => this._insert(updateObj));
      }
    } else if (type === TransactionType.DELETE && transaction.before) {
      const before = TransactionRequest.getBefore(transaction);
      promise = promise.then(() => this._delete(before));
    } else if (type === TransactionType.TRUNCATE) {
      promise = promise.then(() => this._deleteAll());
    } else if (type === TransactionType.BEGIN || type === TransactionType.BEGIN_IMPORT) {
      promise = promise.then(() => { });
    } else if (type === TransactionType.END || type === TransactionType.END_IMPORT) {
      promise = promise.then(() => { });
    } else if (type === TransactionType.ABORT || type === TransactionType.ABORT_IMPORT) {
      if (transaction.committedCsn === undefined) { throw new Error("committedCsn required"); }
      const committedCsn = transaction.committedCsn;
      promise = promise.then(() => {
        return this.rollbackSubsetDb(committedCsn, false)
          .catch((e) => {
            this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [203]);
            return this.resetData(committedCsn, false);
          });
      });
    } else if (type === TransactionType.FORCE_ROLLBACK) {
      const committedCsn = transaction.csn;
      promise = promise.then(() => {
        if (committedCsn === 0) { return this.resetData0() }
        return this.rollbackSubsetDb(committedCsn, true)
          .catch((e) => {
            this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [204]);
            return this.resetData(committedCsn, true);
          });
      });
    } else if (type === TransactionType.BEGIN_RESTORE) {
    } else if (type === TransactionType.END_RESTORE || type === TransactionType.ABORT_RESTORE) {
    } else if (type !== TransactionType.NONE) {
      promise = promise.then(() => { throw new Error("Unsupported type: " + type); });
    }
    promise = promise.catch((e) => this.logger.warn(LOG_MESSAGES.UPDATE_SUBSET_ERROR, [e.toString()]));
    promise = promise.then(() => this.updateCsn(transaction.csn));
    return promise;
  }

  private rollbackSubsetDb(csn: number, withJournal: boolean): Promise<void> {
    this.logger.warn(LOG_MESSAGES.ROLLBACK_TRANSACTIONS, [], [csn]);
    // Csn of the range is not csn + 1 for keeping last journal
    const firstJournalCsn = withJournal ? csn : csn + 1;
    return this.getContextManager().getJournalDb().findByCsnRange(firstJournalCsn, this.currentCsn)
      .then((transactions) => {
        transactions.sort((a, b) => b.csn - a.csn);
        if (transactions.length === 0 || transactions[transactions.length - 1].csn !== firstJournalCsn) {
          throw new Error("Lack of transactions");
        }
        let promise = Promise.resolve();
        let committedCsn: number | undefined;
        transactions.forEach((transaction) => {
          if (transaction.csn === csn) { return; }
          if (committedCsn !== undefined && committedCsn < transaction.csn) { return; }
          const type = transaction.type;
          if (type === TransactionType.INSERT || type === TransactionType.RESTORE) {
            const newObj = TransactionRequest.getNew(transaction);
            promise = promise.then(() => this._delete(newObj));
          } else if (type === TransactionType.UPDATE && transaction.before) {
            const before = TransactionRequest.getBefore(transaction);
            const updateObj = TransactionRequest.applyOperator(transaction, before);
            promise = promise.then(() => this._update(updateObj, before));
          } else if (type === TransactionType.UPSERT || type === TransactionType.REPLACE) {
            if (transaction.before) {
              const before = TransactionRequest.getBefore(transaction);
              const updateObj = TransactionRequest.applyOperator(transaction, before);
              promise = promise.then(() => this._update(updateObj, before));
            } else {
              const updateObj = TransactionRequest.applyOperator(transaction);
              promise = promise.then(() => this._delete(updateObj));
            }
          } else if (type === TransactionType.DELETE && transaction.before) {
            const before = TransactionRequest.getBefore(transaction);
            promise = promise.then(() => this._insert(before));
          } else if (type === TransactionType.TRUNCATE) {
            throw new Error("Cannot roll back TRUNCATE");
          } else if (type === TransactionType.BEGIN) {
          } else if (type === TransactionType.END) {
          } else if (type === TransactionType.ABORT || type === TransactionType.ABORT_IMPORT) {
            if (transaction.committedCsn === undefined) { throw new Error("committedCsn required"); }
            committedCsn = transaction.committedCsn;
            if (transaction.committedCsn < csn) {
              const _committedCsn = transaction.committedCsn;
              promise = promise.then(() => this.rollforwardSubsetDb(_committedCsn, csn));
            }
          } else if (type === TransactionType.BEGIN_IMPORT) {
          } else if (type === TransactionType.END_IMPORT) {
          } else if (type === TransactionType.BEGIN_RESTORE) {
          } else if (type === TransactionType.END_RESTORE) {
          } else if (type === TransactionType.ABORT_RESTORE) {
          } else if (type !== TransactionType.NONE && type !== TransactionType.FORCE_ROLLBACK) {
            throw new Error("Unsupported type: " + type);
          }
        });
        if (withJournal) {
          promise = promise.then(() => this.updateCsn(csn));
        }
        return promise;
      });
  }

  private rollforwardSubsetDb(fromCsn: number, toCsn: number): Promise<void> {
    this.logger.warn(LOG_MESSAGES.ROLLFORWARD_TRANSACTIONS, [], [fromCsn, toCsn]);
    return this.getContextManager().getJournalDb().findByCsnRange(fromCsn + 1, toCsn)
      .then((transactions) => {
        transactions.sort((a, b) => a.csn - b.csn);
        if (transactions.length !== toCsn - fromCsn) {
          throw new Error("Lack of rollforward transactions");
        }
        let promise = Promise.resolve();
        transactions.forEach((transaction) => {
          promise = this.updateSubsetDb(promise, transaction);
        });
        return promise;
      });
  }

  private adjustData(csn: number): Promise<void> {
    this.logger.info(LOG_MESSAGES.ADJUST_DATA, [], [csn]);
    return this.resetData(csn, true);
  }

  resetData(csn: number, withJournal: boolean): Promise<void> {
    if (csn === 0) { return this.resetData0(); }
    this.logger.info(LOG_MESSAGES.RESET_DATA, [], [csn]);
    this.pause();
    const query = {};
    const projection = { _id: 1 } as any;
    for (let field of this.field.split(',')) {
      projection[field] = 1;
    }
    return Dadget._query(this.getNode(), this.database, query, undefined, undefined, undefined, csn, "strict", projection)
      .then((result) => {
        if (result.restQuery) { throw new Error("The queryHandlers has been empty before completing queries."); }
        return this._deleteAll().then(() => result);
      })
      .then((result) => {
        return this._insertMany(result.resultSet);
      })
      .then(() => { if (withJournal) { this.updateCsn(csn); } })
      .then(() => { this.setReady(); })
      .catch((e) => {
        this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [206]);
      });
  }

  private resetData0(): Promise<void> {
    this.logger.warn(LOG_MESSAGES.RESET_DATA0);
    this.pause();
    return Promise.resolve()
      .then(() => this._deleteAll())
      .then(() => this.updateCsn(0))
      .then(() => this.setReady())
      .catch((e) => {
        this.logger.error(LOG_MESSAGES.ERROR_MSG, [e.toString()], [207]);
      });
  }

  has(val: any, csn: number): Promise<boolean> {
    if (!this.readyFlag) return Promise.resolve(true);
    if (val === undefined || val === null) return Promise.resolve(false);

    let release: () => void;
    const promise = new Promise<void>((resolve, reject) => {
      this.getLock().readLock((unlock) => {
        this.logger.debug(LOG_MESSAGES.GET_READLOCK);
        release = () => {
          this.logger.debug(LOG_MESSAGES.RELEASE_READLOCK);
          unlock();
        };
        resolve();
      });
    });

    return promise
      .then(() => this.getCsn())
      .then((currentCsn) => {
        if (csn === currentCsn) {
          return this._has(val)
            .then((result) => {
              release();
              return result;
            });
        } else {
          release();
          return true;
        }
      }).catch((e) => {
        this.logger.warn(LOG_MESSAGES.ERROR_MSG, [e.toString()], [208]);
        release();
        return Promise.reject(e);
      });
  }
}
