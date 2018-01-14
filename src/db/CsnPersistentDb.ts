import { MongoClient, Db } from 'mongodb'
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { MONGO_DB, Mongo } from "../Config"
import { CsnDb } from "./CsnDb"

export class CsnPersistentDb implements CsnDb {
  protected dbUrl: string

  constructor(database: string) {
    this.dbUrl = Mongo.getUrl() + database
    console.log("CsnDB is created")
  }

  start(): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        return db.collection(MONGO_DB.SYSTEM_COLLECTION).findOne({ _id: MONGO_DB.CSN_ID })
      })
      .then(result => {
        if (result) return;
        return _db.collection(MONGO_DB.SYSTEM_COLLECTION).insertOne({ _id: MONGO_DB.CSN_ID, seq: 0 }).then(() => { })
      })
      .then(result => {
        _db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1001, [err.toString()])))
  }

  /**
   * increment csn
   */
  increment(): Promise<number> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        return db.collection(MONGO_DB.SYSTEM_COLLECTION).findOneAndUpdate({ _id: MONGO_DB.CSN_ID }, { $inc: { seq: 1 } }, { returnOriginal: false })
      })
      .then(result => {
        _db.close()
        if (result.ok) {
          console.log("increment value:", result.value.seq);
          return result.value.seq
        } else {
          return Promise.reject(new DadgetError(ERROR.E1002, [result.lastErrorObject.toString()]))
        }
      })
  }

  /**
   * Obtain current CSN
   */
  getCurrentCsn(): Promise<number> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        return db.collection(MONGO_DB.SYSTEM_COLLECTION).findOne({ _id: MONGO_DB.CSN_ID })
      })
      .then(result => {
        _db.close()
        if (!result) throw "csn not found"
        console.log("current value:", result.seq);
        return result.seq
      })
      .catch(reason => Promise.reject(new DadgetError(ERROR.E1003, [reason.toString()])))
  }

  update(seq: number): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        return db.collection(MONGO_DB.SYSTEM_COLLECTION).updateOne({ _id: MONGO_DB.CSN_ID }, { $set: { seq: seq } })
      })
      .then(result => {
        _db.close()
        if (!result.result.ok || result.result.nModified != 1) throw "failed to update csn: " + JSON.stringify(result)
        console.log("update value:", seq);
        return Promise.resolve()
      })
      .catch(reason => Promise.reject(new DadgetError(ERROR.E1004, [reason.toString()])))
  }
}