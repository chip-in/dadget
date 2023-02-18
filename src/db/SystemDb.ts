import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import { IDb } from "./container/IDb";

const SYSTEM_COLLECTION = "system";
const CSN_ID = "csn";
const QUERY_HASH_ID = "query_hash";

export class SystemDb {
  private isNewDb = false;
  private isFirstCsnAccess = true;
  private queryHash: string;
  private csn?: number;
  private resetTimer?: any;

  constructor(private db: IDb) {
    db.setCollection(SYSTEM_COLLECTION);
    console.log("SystemDb is created");
  }

  private resetCache() {
    if (this.resetTimer) { clearTimeout(this.resetTimer); }
    this.resetTimer = setTimeout(() => {
      // for debug
      this.csn = undefined;
      this.resetTimer = undefined;
    }, 5000);
  }

  isNew(): boolean {
    return this.isNewDb;
  }

  start(): Promise<void> {
    return this.db.start()
      .then(() => {
        return this.db.findOne({ _id: CSN_ID });
      })
      .then((result) => {
        if (result) { return; }
        this.isNewDb = true;
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1001, [err.toString()])));
  }

  async startTransaction() {
    return await this.db.startTransaction();
  }

  async commitTransaction(session: any) {
    return await this.db.commitTransaction(session);
  }

  async abortTransaction(session: any) {
    return await this.db.abortTransaction(session);
  }

  checkQueryHash(hash: string): Promise<boolean> {
    this.queryHash = hash;
    return this.db.findOne({ _id: QUERY_HASH_ID })
      .then((result: { hash: string } | null) => {
        if (!result) {
          return this.db.insertOne({ _id: QUERY_HASH_ID, hash }).then(() => false);
        } else {
          if (result.hash === hash) { return false; }
          console.log("Query hash has been changed.");
          this.isNewDb = true;
          return true;
        }
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1005, [err.toString()])));
  }

  updateQueryHash(session?: any): Promise<void> {
    return this.db.updateOneById(QUERY_HASH_ID, { $set: { hash: this.queryHash } }, session).then(() => { })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1006, [err.toString()])));
  }

  private prepareCsn(session?: any): Promise<void> {
    if (!this.isFirstCsnAccess) { return Promise.resolve(); }
    this.isFirstCsnAccess = false;
    return this.db.findOne({ _id: CSN_ID }, session)
      .then((result) => {
        if (result) {
          if (this.isNewDb) {
            return this.db.updateOneById(CSN_ID, { $set: { seq: 0 } }, session);
          }
          return;
        }
        return this.db.insertOne({ _id: CSN_ID, seq: 0 }, session).then(() => { });
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1007, [err.toString()])));
  }

  /**
   * get current CSN
   */
  getCsn(): Promise<number> {
    if (this.isNewDb) { return Promise.resolve(0); }
    if (this.csn !== undefined) { return Promise.resolve(this.csn); }
    return this.prepareCsn()
      .then(() => this.db.findOne({ _id: CSN_ID }))
      .then((result: { seq: number } | null) => {
        if (!result) { throw new Error("csn not found"); }
        this.csn = result.seq;
        this.resetCache();
        return result.seq;
      })
      .catch((reason) => Promise.reject(new DadgetError(ERROR.E1003, [reason.toString()])));
  }

  updateCsn(seq: number, session?: any): Promise<void> {
    return this.prepareCsn(session)
      .then(() => this.db.updateOneById(CSN_ID, { $set: { seq } }, session))
      .then(() => {
        this.isNewDb = false;
        this.csn = seq;
        this.resetCache();
      })
      .catch((reason) => Promise.reject(new DadgetError(ERROR.E1004, [reason.toString()])));
  }
}
