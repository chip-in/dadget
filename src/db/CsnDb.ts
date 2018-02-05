import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { IDb } from "./IDb"

const CSN_ID = "csn"
const SYSTEM_COLLECTION = "__system__"

export class CsnDb {

  constructor(private db: IDb) {
    db.setCollection(SYSTEM_COLLECTION)
    console.log("CsnDB is created")
  }

  start(): Promise<void> {
    return this.db.start()
      .then(() => {
        return this.db.findOne({ _id: CSN_ID })
      })
      .then(result => {
        if (result) return;
        return this.db.insertOne({ _id: CSN_ID, seq: 0 }).then(() => { })
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1001, [err.toString()])))
  }

  /**
   * increment csn
   */
  increment(): Promise<number> {
    return this.db.increment(CSN_ID)
      .then(result => {
        console.log("increment value:", result);
        return result
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1002, [err.toString()])))
  }

  /**
   * Obtain current CSN
   */
  getCurrentCsn(): Promise<number> {
    return this.db.findOne({ _id: CSN_ID })
      .then(result => {
        if (!result) throw "csn not found"
        const val = result as any
        console.log("current value:", val.seq);
        return val.seq
      })
      .catch(reason => Promise.reject(new DadgetError(ERROR.E1003, [reason.toString()])))
  }

  update(seq: number): Promise<void> {
    return this.db.updateOneById(CSN_ID, { $set: { seq: seq } })
      .then(() => {
        console.log("update value:", seq);
      })
      .catch(reason => Promise.reject(new DadgetError(ERROR.E1004, [reason.toString()])))
  }
}