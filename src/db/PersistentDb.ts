import { Db, MongoClient } from "mongodb"
import { Mongo } from "../Config"
import { IDb } from "./IDb"

export class PersistentDb implements IDb {
  private static dbMap: { [database: string]: Db } = {}
  private collection: string

  constructor(protected database: string) {
    console.log("PersistentDb is created")
  }

  setCollection(collection: string) {
    this.collection = collection
  }

  start(): Promise<void> {
    if (!PersistentDb.dbMap[this.database]) {
      return MongoClient.connect(Mongo.getUrl() + this.database)
        .then((_) => {
          PersistentDb.dbMap[this.database] = _
        })
    } else {
      return Promise.resolve()
    }
  }

  findOne(query: object): Promise<object | null> {
    return PersistentDb.dbMap[this.database].collection(this.collection).findOne(query)
  }

  findOneBySort(query: object, sort: object): Promise<any> {
    return PersistentDb.dbMap[this.database].collection(this.collection).find(query).sort(sort).limit(1).next()
  }

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any> {
    let cursor = PersistentDb.dbMap[this.database].collection(this.collection).find(query)
    if (sort) { cursor = cursor.sort(sort) }
    if (offset) { cursor = cursor.skip(offset) }
    if (limit) { cursor = cursor.limit(limit) }
    return cursor.toArray()
  }

  insertOne(doc: object): Promise<void> {
    return PersistentDb.dbMap[this.database].collection(this.collection).insertOne(doc).then(() => { })
  }

  insertMany(docs: object[]): Promise<void> {
    return PersistentDb.dbMap[this.database].collection(this.collection).insertMany(docs).then(() => { })
  }

  increment(id: string, field: string): Promise<number> {
    return PersistentDb.dbMap[this.database].collection(this.collection)
      .findOneAndUpdate({ _id: id }, { $inc: { [field]: 1 } }, { returnOriginal: false })
      .then((result) => {
        if (result.ok) {
          return result.value[field]
        } else {
          return Promise.reject(result.lastErrorObject.toString())
        }
      })
  }

  updateOneById(id: string, update: object): Promise<void> {
    return PersistentDb.dbMap[this.database].collection(this.collection).updateOne({ _id: id }, update)
      .then((result) => {
        if (!result.result.ok || result.result.nModified !== 1) { throw new Error("failed to update: " + JSON.stringify(result)) }
      })
  }

  updateOne(filter: object, update: object): Promise<void> {
    return PersistentDb.dbMap[this.database].collection(this.collection).updateOne(filter, update)
      .then((result) => {
        if (!result.result.ok || result.result.nModified !== 1) { throw new Error("failed to update: " + JSON.stringify(result)) }
      })
  }

  replaceOneById(id: string, doc: object): Promise<void> {
    return PersistentDb.dbMap[this.database].collection(this.collection).replaceOne({ _id: id }, doc)
      .then((result) => {
        if (!result.result.ok || result.result.nModified !== 1) { throw new Error("failed to replace: " + JSON.stringify(result)) }
      })
  }

  deleteOneById(id: string): Promise<void> {
    return PersistentDb.dbMap[this.database].collection(this.collection).deleteOne({ _id: id })
      .then((result) => {
        if (!result.result.ok) { throw new Error("failed to delete: " + JSON.stringify(result)) }
      })
  }

  deleteAll(): Promise<void> {
    return PersistentDb.dbMap[this.database].collection(this.collection).deleteMany({})
      .then((result) => {
        if (!result.result.ok) { throw new Error("failed to delete: " + JSON.stringify(result)) }
      })
  }

  createIndexes(indexMap: { [name: string]: { index: object, property?: object } }): Promise<void> {
    const db = PersistentDb.dbMap[this.database]
    const indexNameList: { [name: string]: any } = {}
    return db.createCollection(this.collection)
      .then((_) => {
        return db.collection(this.collection).indexes()
      })
      .then((indexes) => {
        // インデックスの削除
        const indexPromisies: Array<Promise<any>> = []
        for (const index of indexes) {
          if (index.name !== "_id_" && !indexMap[index.name]) {
            indexPromisies.push(db.collection(this.collection).dropIndex(index.name))
          }
          indexNameList[index.name] = true
        }
        return Promise.all(indexPromisies)
      })
      .then(() => {
        // インデックスの追加
        const indexPromisies: Array<Promise<any>> = []
        for (const indexName in indexMap) {
          if (!indexNameList[indexName]) {
            const fields = indexMap[indexName].index
            const options: { [key: string]: any } = indexMap[indexName].property ? { ...indexMap[indexName].property } : {}
            options.name = indexName
            indexPromisies.push(db.collection(this.collection).createIndex(fields, options))
          }
        }
        return Promise.all(indexPromisies).then(() => { })
      })
  }
}
