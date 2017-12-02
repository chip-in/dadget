import * as hash from "object-hash"
import { MongoClient, Db } from 'mongodb'
import { MONGO_DB } from "../Config"
import { IndexDef } from '../se/DatabaseRegistry';

export class SubsetDb {
  protected dbUrl: string
  
  constructor(database: string, protected subsetName: string, protected indexDefList: IndexDef[]) {
    this.dbUrl = MONGO_DB.URL + database
    console.log("SubsetDb is created:", subsetName)
  }

  start(): Promise<void> {
    let _db: Db
    let indexMap : {[key: string]: IndexDef} = {}
    let indexNameList: {[key: string]: any} = {}
    if(this.indexDefList){
      for(let indexDef of this.indexDefList){
        let name = hash.MD5(indexDef)
        indexMap[name] = indexDef
      }
    }
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        return _db.collection(this.subsetName).indexes()
      })
      .then(indexes => {
        // インデックスの削除
        let indexPromisies: Promise<any>[] = []
        for(let index of indexes){
          if(index.name !== '_id_' && !indexMap[index.name]){
            indexPromisies.push(_db.collection(this.subsetName).dropIndex(index.name))
          }
          indexNameList[index.name] = true
        }
        return Promise.all(indexPromisies)
      })
      .then(() => {
        // インデックスの追加
        let indexPromisies: Promise<any>[] = []
        for(let indexName in indexMap){
          if(!indexNameList[indexName]){
            let fields = indexMap[indexName].index
            let options: {[key: string]: any} = indexMap[indexName].property ? {...indexMap[indexName].property} : {}
            delete options['unique']
            options.name = indexName
            indexPromisies.push(_db.collection(this.subsetName).createIndex(fields, options))
          }
        }
        return Promise.all(indexPromisies)
      })
      .then(() => {
        _db.close()
      })
      .catch((err) => {
        console.log(err.stack);
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "SubsetDb failed to start cause=%1",
          inserts: [err.stack.toString()]
        })
      })
  }

  insert(obj: object): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("insert:", JSON.stringify(obj));
        return _db.collection(this.subsetName).insertOne(obj)
      })
      .then(result => {
        _db.close()
      })
      .catch(err => {
        console.log(err.stack);
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "SubsetDb failed to insert cause=%1",
          inserts: [err.stack.toString()]
        })
      })
  }

  update(obj: {[key:string]: any}): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("update:", JSON.stringify(obj));
        return _db.collection(this.subsetName).replaceOne({_id: obj["_id"]}, obj)
      })
      .then(result => {
        _db.close()
      })
      .catch(err => {
        console.log(err.stack);
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "SubsetDb failed to update cause=%1",
          inserts: [err.stack.toString()]
        })
      })
  }

  delete(obj: {[key:string]: any}): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("delete:", JSON.stringify(obj));
        return _db.collection(this.subsetName).deleteOne({_id: obj["_id"]})
      })
      .then(result => {
        _db.close()
      })
      .catch(err => {
        console.log(err.stack);
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "SubsetDb failed to delete cause=%1",
          inserts: [err.stack.toString()]
        })
      })
  }

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        let cursor = db.collection(this.subsetName).find(query)
        if(sort) cursor = cursor.sort(sort)
        if(offset) cursor = cursor.skip(offset)
        if(limit) cursor = cursor.limit(limit)
        return cursor.toArray()
      })
      .then(result => {
        _db.close()
        console.log("find:", JSON.stringify(result));
        return result
      })
      .catch(reason => {
        return Promise.reject({
          ns: "dadget.chip-in.net",
          code: 223,
          message: "SubsetDb failed to find cause=%1",
          inserts: [reason.toString()]
        })
      })
  }
}