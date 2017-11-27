import { MongoClient, Db } from 'mongodb'
import { MONGO_DB } from "../Config"

export class CsnDb {
  protected dbUrl: string

  constructor(database: string) {
    this.dbUrl = MONGO_DB.URL + database
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
        if(result) return;
        return _db.collection(MONGO_DB.SYSTEM_COLLECTION).insertOne({ _id: MONGO_DB.CSN_ID, seq: 0 }).then(()=>{})
      })
      .then(result => {
        _db.close()
      })
      .catch((err) => {
        console.log(err.stack);
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "CsnOnMongoDB failed to start cause=%1",
          inserts: [err.stack.toString()]
        })
      })
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
          return Promise.reject({
            ns: "dadget.chip-in.net",
            code: 223,
            message: "CsnOnMongoDB failed to increment cause=%1",
            inserts: [result.lastErrorObject.toString()]
          })
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
        if (result) {
          console.log("current value:", result.seq);
          return result.seq
        } else {
          return Promise.reject({
            ns: "dadget.chip-in.net",
            code: 223,
            message: "CsnOnMongoDB failed to get current cause=none",
            inserts: []
          })
        }
      })
      .catch(reason => {
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "CsnOnMongoDB failed to get current cause=%1",
          inserts: [reason.toString()]
        })
      })
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
        if (result.result.ok) {
          console.log("update value:", seq);
          return Promise.resolve()
        } else {
          return Promise.reject({
            ns: "dadget.chip-in.net",
            code: 223,
            message: "CsnOnMongoDB failed to update",
            inserts: [result.toString()]
          })
        }
      })
      .catch(reason => {
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "CsnOnMongoDB failed to update cause=%1",
          inserts: [reason.toString()]
        })
      })
  }
}