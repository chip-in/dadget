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

  constructor(private db: IDb) {
    db.setCollection(SYSTEM_COLLECTION);
    console.log("SystemDb is created");
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

  updateQueryHash(): Promise<void> {
    return this.db.updateOneById(QUERY_HASH_ID, { $set: { hash: this.queryHash } }).then(() => { })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1006, [err.toString()])));
  }

  private prepareCsn(): Promise<void> {
    if (!this.isFirstCsnAccess) { return Promise.resolve(); }
    this.isFirstCsnAccess = false;
    return this.db.findOne({ _id: CSN_ID })
      .then((result) => {
        if (result) {
          if (this.isNewDb) {
            return this.db.updateOneById(CSN_ID, { $set: { seq: 0 } });
          }
          return;
        }
        return this.db.insertOne({ _id: CSN_ID, seq: 0 }).then(() => { });
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1007, [err.toString()])));
  }

  /**
   * increment csn
   */
  incrementCsn(): Promise<number> {
    return this.prepareCsn()
      .then(() => this.db.increment(CSN_ID, "seq"))
      .then((result) => {
        this.isNewDb = false;
        console.log("increment csn value:", result);
        return result;
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1002, [err.toString()])));
  }

  /**
   * get current CSN
   */
  getCsn(): Promise<number> {
    if (this.isNewDb) { return Promise.resolve(0); }
    return this.prepareCsn()
      .then(() => this.db.findOne({ _id: CSN_ID }))
      .then((result: { seq: number } | null) => {
        if (!result) { throw new Error("csn not found"); }
        console.log("current csn value:", result.seq);
        return result.seq;
      })
      .catch((reason) => Promise.reject(new DadgetError(ERROR.E1003, [reason.toString()])));
  }

  updateCsn(seq: number): Promise<void> {
    return this.prepareCsn()
      .then(() => this.db.updateOneById(CSN_ID, { $set: { seq } }))
      .then(() => {
        this.isNewDb = false;
        console.log("update csn value:", seq);
      })
      .catch((reason) => Promise.reject(new DadgetError(ERROR.E1004, [reason.toString()])));
  }
}
