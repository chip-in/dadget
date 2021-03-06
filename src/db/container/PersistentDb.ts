import { Db, MongoClient } from "mongodb";
import { Mongo } from "../../Config";
import { IDb } from "./IDb";

export class PersistentDb implements IDb {
  private static dbMap: { [database: string]: Db } = {};
  private db: Db;
  private collection: string;
  private indexMap: { [name: string]: { index: object, property?: object } };

  static convertQuery(query: any): object {
    for (const key of Object.keys(query)) {
      if (key === "$not" && query.$not.$regex) {
        query.$not = new RegExp(query.$not.$regex, query.$not.$options);
      } else if (query[key] instanceof Object) {
        query[key] = PersistentDb.convertQuery(query[key]);
      }
    }
    return query;
  }

  public static getAllStorage(): Promise<string[]> {
    return MongoClient.connect(Mongo.getUrl())
      .then((db) => db.admin().listDatabases())
      .then((list) => list.databases.map((_: { name: string; }) => _.name));
  }

  public static deleteStorage(name: string) {
    return MongoClient.connect(Mongo.getUrl() + name)
      .then((db) => db.dropDatabase());
  }

  constructor(protected database: string) {
    console.log("PersistentDb is created");
  }

  setCollection(collection: string) {
    this.collection = collection;
  }

  setIndexes(indexMap: { [name: string]: { index: object, property?: object } }): void {
    this.indexMap = indexMap;
  }

  start(): Promise<void> {
    if (!PersistentDb.dbMap[this.database]) {
      return MongoClient.connect(Mongo.getUrl() + this.database)
        .then((_) => this.renameOldName(_))
        .then((_) => {
          this.db = _;
          PersistentDb.dbMap[this.database] = _;
          return this.createIndexes();
        });
    } else {
      this.db = PersistentDb.dbMap[this.database];
      return this.renameOldName(this.db)
        .then(() => this.createIndexes());
    }
  }

  renameOldName(db: Db): Promise<Db> {
    const oldName = "__" + this.collection + "__";
    return db.collection(oldName).count({})
      .then((count) => {
        if (count > 0) {
          return db.renameCollection(oldName, this.collection)
            .then(() => db);
        }
        return db;
      });
  }

  findOne(query: object): Promise<object | null> {
    return this.db.collection(this.collection).findOne(PersistentDb.convertQuery(query));
  }

  findByRange(field: string, from: any, to: any, dir: number): Promise<any[]> {
    return this.find({ $and: [{ [field]: { $gte: from } }, { [field]: { $lte: to } }] }, { [field]: dir });
  }

  findOneBySort(query: object, sort: object): Promise<any> {
    return this.db.collection(this.collection).find(PersistentDb.convertQuery(query)).sort(sort).limit(1).next();
  }

  find(query: object, sort?: object, limit?: number, offset?: number, projection?: object): Promise<any[]> {
    let cursor = this.db.collection(this.collection).find(PersistentDb.convertQuery(query), projection);
    if (sort) { cursor = cursor.sort(sort); }
    if (offset) { cursor = cursor.skip(offset); }
    if (limit) { cursor = cursor.limit(limit); }
    return cursor.toArray();
  }

  count(query: object): Promise<number> {
    return this.db.collection(this.collection).count(PersistentDb.convertQuery(query));
  }

  insertOne(doc: object): Promise<void> {
    return this.db.collection(this.collection).insertOne(doc).then(() => { });
  }

  insertMany(docs: object[]): Promise<void> {
    return this.db.collection(this.collection).insertMany(docs).then(() => { });
  }

  increment(id: string, field: string): Promise<number> {
    return this.db.collection(this.collection)
      .findOneAndUpdate({ _id: id }, { $inc: { [field]: 1 } }, { returnOriginal: false })
      .then((result) => {
        if (result.ok) {
          return result.value[field];
        } else {
          return Promise.reject(result.lastErrorObject.toString());
        }
      });
  }

  updateOneById(id: string, update: object): Promise<void> {
    return this.db.collection(this.collection).updateOne({ _id: id }, update)
      .then((result) => {
        if (!result.result.ok || result.result.n !== 1) { throw new Error("failed to update: " + JSON.stringify(result)); }
      });
  }

  updateOne(filter: object, update: object): Promise<void> {
    return this.db.collection(this.collection).updateOne(filter, update)
      .then((result) => {
        if (!result.result.ok || result.result.n !== 1) { throw new Error("failed to update: " + JSON.stringify(result)); }
      });
  }

  replaceOneById(id: string, doc: object): Promise<void> {
    return this.db.collection(this.collection).replaceOne({ _id: id }, doc)
      .then((result) => {
        if (!result.result.ok || result.result.n !== 1) { throw new Error("failed to replace: " + JSON.stringify(result)); }
      });
  }

  deleteOneById(id: string): Promise<void> {
    return this.db.collection(this.collection).deleteOne({ _id: id })
      .then((result) => {
        if (!result.result.ok) { throw new Error("failed to delete: " + JSON.stringify(result)); }
      });
  }

  deleteAll(): Promise<void> {
    const newName = this.collection + "-" + new Date().toISOString();
    return this.db.collection(this.collection).count({})
      .then((count) => {
        if (count > 0) {
          return this.db.renameCollection(this.collection, newName)
            .then(() => { });
        }
      });
  }

  private createIndexes(): Promise<void> {
    if (!this.indexMap) { return Promise.resolve(); }
    const indexMap = this.indexMap;
    const indexNameList: { [name: string]: any } = {};
    return this.db.createCollection(this.collection)
      .then((_) => {
        return this.db.collection(this.collection).indexes();
      })
      .then((indexes) => {
        // インデックスの削除
        const indexPromisies: Array<Promise<any>> = [];
        for (const index of indexes) {
          if (index.name !== "_id_" && !indexMap[index.name]) {
            indexPromisies.push(this.db.collection(this.collection).dropIndex(index.name));
          }
          indexNameList[index.name] = true;
        }
        return Promise.all(indexPromisies);
      })
      .then(() => {
        // インデックスの追加
        const indexPromisies: Array<Promise<any>> = [];
        for (const indexName in indexMap) {
          if (!indexNameList[indexName]) {
            const fields = indexMap[indexName].index;
            const options: { [key: string]: any } = indexMap[indexName].property ? { ...indexMap[indexName].property } : {};
            options.name = indexName;
            indexPromisies.push(this.db.collection(this.collection).createIndex(fields, options));
          }
        }
        return Promise.all(indexPromisies).then(() => { });
      });
  }
}
