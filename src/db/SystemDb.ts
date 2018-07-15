import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import { IDb } from "./container/IDb";

const CSN_ID = "csn";
const SYSTEM_COLLECTION = "system";

export class SystemDb {
  private _isNew = false;

  constructor(private db: IDb) {
    db.setCollection(SYSTEM_COLLECTION);
    console.log("SystemDb is created");
  }

  isNew(): boolean {
    return this._isNew;
  }

  start(): Promise<void> {
    return this.db.start()
      .then(() => {
        return this.db.findOne({ _id: CSN_ID });
      })
      .then((result) => {
        if (result) { return; }
        this._isNew = true;
        return this.db.insertOne({ _id: CSN_ID, seq: 0 }).then(() => { });
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1001, [err.toString()])));
  }

  /**
   * increment csn
   */
  incrementCsn(): Promise<number> {
    return this.db.increment(CSN_ID, "seq")
      .then((result) => {
        this._isNew = false;
        console.log("increment csn value:", result);
        return result;
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1002, [err.toString()])));
  }

  /**
   * get current CSN
   */
  getCsn(): Promise<number> {
    return this.db.findOne({ _id: CSN_ID })
      .then((result) => {
        if (!result) { throw new Error("csn not found"); }
        const val = result as any;
        console.log("current csn value:", val.seq);
        return val.seq;
      })
      .catch((reason) => Promise.reject(new DadgetError(ERROR.E1003, [reason.toString()])));
  }

  updateCsn(seq: number): Promise<void> {
    return this.db.updateOneById(CSN_ID, { $set: { seq } })
      .then(() => {
        this._isNew = false;
        console.log("update csn value:", seq);
      })
      .catch((reason) => Promise.reject(new DadgetError(ERROR.E1004, [reason.toString()])));
  }
}
