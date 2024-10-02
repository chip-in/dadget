import { ClientSession, Db, MongoClient } from "mongodb";
import { Mongo, SPLIT_IN_ONE_DB } from "../../Config";
import { IDb } from "./IDb";
import { Logger } from "../../util/Logger";
import { LOG_MESSAGES } from "../../LogMessages";
import * as AsyncLock from "async-lock";

export class PersistentDb implements IDb {
  private static connection: MongoClient;
  private static lock = new AsyncLock();
  private db: Db;
  private database: string;
  private subsetName: string;
  private collection: string;
  private indexMap: { [name: string]: { index: object, property?: object } };
  private logger: Logger;

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

  private static async getConnection() {
    return PersistentDb.lock.acquire("getConnection", async () => {
      if (!PersistentDb.connection) {
        PersistentDb.connection = await MongoClient.connect(Mongo.getUrl(), Mongo.getOption())
        if (Mongo.useTransaction()) {
          const logger = Logger.getLoggerWoDB("PersistentDb");
          logger.info(LOG_MESSAGES.USE_TRANSACTION);
        }
      }
      return PersistentDb.connection;
    });
  }

  private static errorExit(error: any, num: number, throwErrorMode?: boolean): any {
    if (throwErrorMode) { throw error; }
    const logger = Logger.getLoggerWoDB("PersistentDb");
    logger.error(LOG_MESSAGES.ERROR_MSG, [error.toString()], [num]);
    process.exit(1);
  }

  public static getAllStorage(): Promise<string[]> {
    if (Mongo.isOneDb()) {
      return PersistentDb.getConnection()
        .then((client) => client.db(Mongo.getDbName("")).listCollections())
        .then((result) => result.toArray())
        .then((list) => {
          const set = new Set();
          for (const row of list) {
            set.add(row.name.split(SPLIT_IN_ONE_DB)[0]);
          }
          return Array.from(set);
        })
        .catch((error) => PersistentDb.errorExit(error, 1));
    } else {
      return PersistentDb.getConnection()
        .then((client) => client.db().admin().listDatabases())
        .then((list) => list.databases.map((_: { name: string; }) => _.name))
        .catch((error) => PersistentDb.errorExit(error, 1));
    }
  }

  public static deleteStorage(name: string) {
    if (Mongo.isOneDb()) {
      return (async () => {
        try {
          const client = await PersistentDb.getConnection();
          const result = await client.db(Mongo.getDbName("")).listCollections();
          const list = await result.toArray();
          for (const row of list) {
            if (row.name.split(SPLIT_IN_ONE_DB)[0] === name) {
              await client.db(Mongo.getDbName("")).dropCollection(row.name);
            }
          }
        } catch (error) {
          PersistentDb.errorExit(error, 2);
        }
      })();
    } else {
      return PersistentDb.getConnection()
        .then((client) => client.db(name).dropDatabase())
        .catch((error) => PersistentDb.errorExit(error, 2));
    }
  }

  constructor(database: string) {
    this.subsetName = database;
    this.database = Mongo.getDbName(database)
    this.logger = Logger.getLoggerWoDB("PersistentDb");
    console.log("PersistentDb is created");
  }

  async startTransaction() {
    if (!Mongo.useTransaction()) {
      return undefined;
    }
    try {
      const time = Date.now();
      let client = await PersistentDb.getConnection();
      const session = client.startSession();
      session.startTransaction();
      if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["startTransaction"], [Date.now() - time]);
      return session;
    } catch (error) {
      PersistentDb.errorExit(error, 100);
    }
  }

  async commitTransaction(session?: ClientSession) {
    if (session) {
      try {
        const time = Date.now();
        await session.commitTransaction();
        await session.endSession();
        if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["commitTransaction"], [Date.now() - time]);
      } catch (error) {
        PersistentDb.errorExit(error, 101);
      }
    }
  }

  async abortTransaction(session?: ClientSession) {
    if (session) {
      try {
        const time = Date.now();
        await session.abortTransaction();
        await session.endSession();
        if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["abortTransaction"], [Date.now() - time]);
      } catch (error) {
        PersistentDb.errorExit(error, 102);
      }
    }
  }

  setCollection(collection: string) {
    this.collection = Mongo.getCollectionName(this.subsetName, collection);
  }

  setIndexes(indexMap: { [name: string]: { index: object, property?: object } }): void {
    this.indexMap = indexMap;
  }

  start(): Promise<void> {
    return PersistentDb.getConnection()
      .then((client) => {
        this.db = client.db(this.database);
        return this.createIndexes();
      })
      .catch((error) => PersistentDb.errorExit(error, 3));
  }

  findOne(query: object, session?: ClientSession): Promise<object | null> {
    const time = Date.now();
    return this.db.collection(this.collection).findOne(PersistentDb.convertQuery(query), { session })
      .then((result) => {
        if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["findOne"], [Date.now() - time]);
        return result;
      })
      .catch((error) => PersistentDb.errorExit(error, 4));
  }

  findByRange(field: string, from: any, to: any, dir: number, projection?: object, session?: ClientSession): Promise<any[]> {
    const time = Date.now();
    return this.find({ $and: [{ [field]: { $gte: from } }, { [field]: { $lte: to } }] }, { [field]: dir }, undefined, undefined, projection, session)
      .then((result) => {
        if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["findByRange"], [Date.now() - time]);
        return result;
      })
      .catch((error) => PersistentDb.errorExit(error, 5));
  }

  findOneBySort(query: object, sort: object): Promise<any> {
    const time = Date.now();
    return this.db.collection(this.collection).find(PersistentDb.convertQuery(query)).sort(sort as any).limit(1).next()
      .then((result) => {
        if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["findOneBySort"], [Date.now() - time]);
        return result;
      })
      .catch((error) => PersistentDb.errorExit(error, 6));
  }

  async find(query: object, sort?: object, limit?: number, offset?: number, projection?: object, session?: ClientSession, throwErrorMode?: boolean): Promise<any[]> {
    const time = Date.now();
    let cursor = this.db.collection(this.collection).find(PersistentDb.convertQuery(query), { allowDiskUse: true, projection, session })
    if (sort) { cursor = cursor.sort(sort as any); }
    if (offset) { cursor = cursor.skip(offset); }
    if (limit) { cursor = cursor.limit(limit); }
    let list = [];
    try {
      while (await cursor.hasNext()) {
        const obj = await cursor.next();
        if (!obj) {
          continue;
        }
        list.push(obj);
      }
    } catch (error) {
      PersistentDb.errorExit(error, 7, throwErrorMode);
    }
    if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["find"], [Date.now() - time]);
    return list;
  }

  count(query: object, throwErrorMode?: boolean): Promise<number> {
    const time = Date.now();
    return this.db.collection(this.collection).countDocuments(PersistentDb.convertQuery(query))
      .then((result) => {
        if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["count"], [Date.now() - time]);
        return result;
      })
      .catch((error) => PersistentDb.errorExit(error, 8, throwErrorMode));
  }

  insertOne(doc: object, session?: ClientSession, throwErrorMode?: boolean): Promise<void> {
    const time = Date.now();
    return this.db.collection(this.collection).insertOne(doc, { session }).then(() => { })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["insertOne"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 9, throwErrorMode));
  }

  insertMany(docs: object[], session?: ClientSession): Promise<void> {
    const time = Date.now();
    return this.db.collection(this.collection).insertMany(docs, { session }).then(() => { })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["insertMany"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 10));
  }

  increment(id: string, field: string): Promise<number> {
    const time = Date.now();
    return this.db.collection(this.collection)
      .findOneAndUpdate({ _id: id }, { $inc: { [field]: 1 } }, { returnDocument: "after" })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["increment"], [Date.now() - time]) })
      .then((result: any) => {
        if (result.ok) {
          return result.value[field];
        } else {
          return Promise.reject(result.lastErrorObject.toString());
        }
      })
      .catch((error) => PersistentDb.errorExit(error, 11));
  }

  updateOneById(id: string, update: object, session?: ClientSession): Promise<void> {
    const time = Date.now();
    return this.db.collection(this.collection).updateOne({ _id: id }, update, { session })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["updateOneById"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 12));
  }

  updateOne(filter: object, update: object, session?: ClientSession): Promise<void> {
    const time = Date.now();
    return this.db.collection(this.collection).updateOne(filter, update, { session })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["updateOne"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 13));
  }

  replaceOneById(id: string, doc: object, session?: ClientSession, throwErrorMode?: boolean): Promise<void> {
    const time = Date.now();
    (doc as any)._id = id;
    return this.db.collection(this.collection).replaceOne({ _id: id }, doc, { upsert: true, session })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["replaceOneById"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 14, throwErrorMode));
  }

  deleteOneById(id: string, session?: ClientSession, throwErrorMode?: boolean): Promise<void> {
    const time = Date.now();
    return this.db.collection(this.collection).deleteOne({ _id: id }, { session })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["deleteOneById"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 15, throwErrorMode));
  }

  deleteByRange(field: string, from: any, to: any, session?: ClientSession): Promise<void> {
    const time = Date.now();
    const query = { $and: [{ [field]: { $gte: from } }, { [field]: { $lte: to } }] };
    return this.db.collection(this.collection).deleteMany(PersistentDb.convertQuery(query), { session })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["deleteByRange"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 16));
  }

  deleteAll(session?: ClientSession, throwErrorMode?: boolean): Promise<void> {
    const time = Date.now();
    return this.db.collection(this.collection).deleteMany({}, { session })
      .then(() => { if (Date.now() - time > 100) this.logger.info(LOG_MESSAGES.MONGODB_LOG, ["deleteAll"], [Date.now() - time]) })
      .catch((error) => PersistentDb.errorExit(error, 17, throwErrorMode));
  }

  private createIndexes(): Promise<void> {
    if (!this.indexMap) { return Promise.resolve(); }
    const indexMap = this.indexMap;
    const indexNameList: { [name: string]: any } = {};
    return this.db.collections()
      .then((collections) => {
        for (const collection of collections) {
          if (collection.collectionName === this.collection) {
            return collection;
          }
        }
        return this.db.createCollection(this.collection);
      })
      .then((collection) => collection.indexes())
      .then((indexes) => {
        // インデックスの削除
        const indexPromises: Promise<any>[] = [];
        for (const index of indexes) {
          if (index.name !== "_id_" && !indexMap[index.name]) {
            indexPromises.push(this.db.collection(this.collection).dropIndex(index.name));
          }
          indexNameList[index.name] = true;
        }
        return Promise.all(indexPromises);
      })
      .then(() => {
        // インデックスの追加
        const indexPromises: Promise<any>[] = [];
        for (const indexName in indexMap) {
          if (!indexNameList[indexName]) {
            const fields = indexMap[indexName].index as any;
            const options: { [key: string]: any } = indexMap[indexName].property ? { ...indexMap[indexName].property } : {};
            options.name = indexName;
            indexPromises.push(this.db.collection(this.collection).createIndex(fields, options));
          }
        }
        return Promise.all(indexPromises).then(() => { });
      })
      .catch((error) => PersistentDb.errorExit(error, 18));
  }
}
