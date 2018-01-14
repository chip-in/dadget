import { MongoClient, Db } from 'mongodb'
import * as EJSON from '../util/Ejson'

import { TransactionRequest, TransactionObject, TransactionType } from '../db/Transaction'
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { MONGO_DB, Mongo } from "../Config"
import { JournalDb } from "./JournalDb"

export class JournalPersistentDb implements JournalDb {
  protected dbUrl: string

  constructor(database: string) {
    this.dbUrl = Mongo.getUrl() + database
    console.log("JournalDb is created")
  }

  start(): Promise<void> {
    let db: Db
    return MongoClient.connect(this.dbUrl)
      .then(_ => {
        db = _
        return db.collection(MONGO_DB.JOURNAL_COLLECTION).createIndexes([
          {
            name: "csn_index",
            key: { csn: -1 },
            unique: 1
          },
          {
            name: "target_index",
            key: { target: 1, csn: -1 }
          }
        ])
      })
      .then(result => {
        db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1101, [err.toString()])))
  }

  checkConsistent(csn: number, request: TransactionRequest): Promise<void> {
    let db: Db
    return MongoClient.connect(this.dbUrl)
      .then(_ => {
        db = _
        return db.collection(MONGO_DB.JOURNAL_COLLECTION).find({ target: request.target }).sort({ csn: -1 }).limit(1).next()
      })
      .then(result => {
        db.close()
        console.log("checkConsistent", JSON.stringify(result));
        if (request.type == TransactionType.INSERT && request.new) {
          if (!result || result.type == TransactionType.DELETE) return
          throw new DadgetError(ERROR.E1102);
        } else if (request.before) {
          if (!result) throw new DadgetError(ERROR.E1103);
          if (result.type == TransactionType.DELETE) throw new DadgetError(ERROR.E1104);
          if (result.csn > csn) throw new DadgetError(ERROR.E1105, [result.csn, csn]);
          return
        } else {
          throw new DadgetError(ERROR.E1106);
        }
      })
  }

  getLastDigest(): Promise<string> {
    let db: Db
    return MongoClient.connect(this.dbUrl)
      .then(_ => {
        db = _
        return db.collection(MONGO_DB.JOURNAL_COLLECTION).find().sort({ csn: -1 }).limit(1).next()
      })
      .then(result => {
        console.log("getLastDigest", JSON.stringify(result));
        db.close()
        if (result) {
          console.log("getLastDigest:", result._id, result.digest);
          return result.digest
        } else {
          return ""
        }
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1107, [err.toString()])))
  }

  insert(transaction: TransactionObject): Promise<void> {
    let db: Db
    return MongoClient.connect(this.dbUrl)
      .then(_ => {
        db = _
        console.log("insert:", JSON.stringify(transaction));
        // mongodbの制限によりoperatorを文字列化
        let saveVal: any = { ...transaction }
        if (transaction.operator) {
          saveVal["operator"] = EJSON.stringify(transaction.operator)
        }
        return db.collection(MONGO_DB.JOURNAL_COLLECTION).insertOne(saveVal)
      })
      .then(result => {
        db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1108, [err.toString()])))
  }

  findByCsn(csn: number): Promise<TransactionObject | null> {
    let db: Db
    console.log("findByCsn:", csn);
    return MongoClient.connect(this.dbUrl)
      .then(_ => {
        db = _
        return db.collection(MONGO_DB.JOURNAL_COLLECTION).findOne({ csn: csn })
      })
      .then(transaction => {
        db.close()
        if (transaction) {
          console.log(JSON.stringify(transaction))
          console.log("findByCsn digest:", transaction.digest)
          return transaction
        } else {
          console.log("findByCsn: transaction none");
          return null
        }
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1109, [err.toString()])))
  }

  findByCsnRange(from: number, to: number): Promise<TransactionObject[]> {
    let db: Db
    console.log("findByCsnRange:", from, to);
    return MongoClient.connect(this.dbUrl)
      .then(_ => {
        db = _
        return db.collection(MONGO_DB.JOURNAL_COLLECTION).find({ $and: [{ csn: { $gte: from } }, { csn: { $lte: to } }] }).sort({ csn: -1 }).toArray()
      })
      .then(transactions => {
        db.close()
        return transactions
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1110, [err.toString()])))
  }

  updateAndDeleteAfter(transaction: TransactionObject): Promise<void> {
    let db: Db
    return MongoClient.connect(this.dbUrl)
      .then(_ => {
        db = _
        // TODO 廃棄トランザクションをログへ出力し、削除
        console.log("update:", JSON.stringify(transaction));
        return db.collection(MONGO_DB.JOURNAL_COLLECTION).updateOne({ csn: transaction.csn }, transaction)
      })
      .then(result => {
        db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1111, [err.toString()])))
  }
}