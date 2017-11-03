import { MongoClient, Db } from 'mongodb'
import { MONGO_DB } from "../Config"

export class SubsetDb {
  protected dbUrl: string
  
  constructor(database: string, protected subsetName: string) {
    this.dbUrl = MONGO_DB.URL + database
    console.log("SubsetDb is created:", subsetName)
  }

  start(): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        // TODO hashでnameを付けて、インデックスの追加削除
        return db.collection(this.subsetName).createIndexes([
          {
            name: "distributionIdindex",
            key: { distributionId: 1 },
            unique: true
          }
        ])
      })
      .then(result => {
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

  update(obj: {_id: number}): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("update:", JSON.stringify(obj));
        return _db.collection(this.subsetName).updateOne({_id: obj._id}, obj)
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

  delete(obj: {_id: number}): Promise<void> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        console.log("delete:", JSON.stringify(obj));
        return _db.collection(this.subsetName).deleteOne({_id: obj._id})
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

  find(query: object): Promise<any> {
    let _db: Db
    return MongoClient.connect(this.dbUrl)
      .then(db => {
        _db = db
        return db.collection(this.subsetName).find(query).toArray()
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