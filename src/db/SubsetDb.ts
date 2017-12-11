import * as hash from "object-hash"
import { MongoClient, Db } from 'mongodb'

import { IndexDef } from '../se/DatabaseRegistry';
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { MONGO_DB } from "../Config"

export class SubsetDb {
  protected dbUrl: string

  constructor(database: string, protected subsetName: string, protected indexDefList: IndexDef[]) {
    this.dbUrl = MONGO_DB.URL + database
    console.log("SubsetDb is created:", subsetName)
  }

  start(): Promise<void> {
    let _db: Db
    let indexMap: { [key: string]: IndexDef } = {}
    let indexNameList: { [key: string]: any } = {}
    if (this.indexDefList) {
      for (let indexDef of this.indexDefList) {
        let name = hash.MD5(indexDef)
        indexMap[name] = indexDef
      }
    }
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        return _db.collection(MONGO_DB.SUBSET_COLLECTION).indexes()
      })
      .then(indexes => {
        // インデックスの削除
        let indexPromisies: Promise<any>[] = []
        for (let index of indexes) {
          if (index.name !== '_id_' && !indexMap[index.name]) {
            indexPromisies.push(_db.collection(MONGO_DB.SUBSET_COLLECTION).dropIndex(index.name))
          }
          indexNameList[index.name] = true
        }
        return Promise.all(indexPromisies)
      })
      .then(() => {
        // インデックスの追加
        let indexPromisies: Promise<any>[] = []
        for (let indexName in indexMap) {
          if (!indexNameList[indexName]) {
            let fields = indexMap[indexName].index
            let options: { [key: string]: any } = indexMap[indexName].property ? { ...indexMap[indexName].property } : {}
            delete options['unique']
            options.name = indexName
            indexPromisies.push(_db.collection(MONGO_DB.SUBSET_COLLECTION).createIndex(fields, options))
          }
        }
        return Promise.all(indexPromisies)
      })
      .then(() => {
        _db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1201, [err.toString()])))
  }

  insert(obj: object): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("insert:", JSON.stringify(obj));
        return _db.collection(MONGO_DB.SUBSET_COLLECTION).insertOne(obj)
      })
      .then(result => {
        _db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1202, [err.toString()])))
  }

  update(obj: { [key: string]: any }): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("update:", JSON.stringify(obj));
        return _db.collection(MONGO_DB.SUBSET_COLLECTION).replaceOne({ _id: obj["_id"] }, obj)
      })
      .then(result => {
        _db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1203, [err.toString()])))
  }

  delete(obj: { [key: string]: any }): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("delete:", JSON.stringify(obj));
        return _db.collection(MONGO_DB.SUBSET_COLLECTION).deleteOne({ _id: obj["_id"] })
      })
      .then(result => {
        _db.close()
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1204, [err.toString()])))
  }

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        let cursor = db.collection(MONGO_DB.SUBSET_COLLECTION).find(query)
        if (sort) cursor = cursor.sort(sort)
        if (offset) cursor = cursor.skip(offset)
        if (limit) cursor = cursor.limit(limit)
        return cursor.toArray()
      })
      .then(result => {
        _db.close()
        console.log("find:", JSON.stringify(result));
        return result
      })
      .catch(err => Promise.reject(new DadgetError(ERROR.E1205, [err.toString()])))
  }
}