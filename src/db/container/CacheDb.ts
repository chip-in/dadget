import { v1 as uuidv1 } from "uuid";
import { Util } from "../../util/Util";
import { TransactionRequest } from "../Transaction";
import { IDb } from "./IDb";

export class CacheDb implements IDb {
  private static dataMap: { [database: string]: { [collection: string]: { [_id: string]: object } } } = {};
  private data: { [_id: string]: object };

  constructor(protected database: string) {
    console.log("CacheDb is created");
  }

  async startTransaction() {
  }

  async commitTransaction(session?: any) {
  }

  async abortTransaction(session?: any) {
  }

  setCollection(collection: string) {
    if (!CacheDb.dataMap[this.database]) {
      CacheDb.dataMap[this.database] = {};
    }
    if (CacheDb.dataMap[this.database][collection]) {
      this.data = CacheDb.dataMap[this.database][collection];
    } else {
      this.data = {};
      CacheDb.dataMap[this.database][collection] = this.data;
    }
  }

  setIndexes(indexMap: { [name: string]: { index: object, property?: object } }): void {
  }

  start(): Promise<void> {
    return Promise.resolve();
  }

  findOne(query: object, session?: any): Promise<object | null> {
    const dataList = [];
    for (const _id of Object.keys(this.data)) {
      dataList.push(this.data[_id]);
    }
    const list = Util.mongoSearch(dataList, query);
    if (list && list instanceof Array && list.length > 0) {
      return Promise.resolve(list[0]);
    }
    return Promise.resolve(null);
  }

  findByRange(field: string, from: any, to: any, dir: number, projection?: object, session?: any): Promise<any[]> {
    return this.find({ $and: [{ [field]: { $gte: from } }, { [field]: { $lte: to } }] }, { [field]: dir }, undefined, undefined, projection);
  }

  findOneBySort(query: object, sort: object): Promise<any> {
    const dataList = [];
    for (const _id of Object.keys(this.data)) {
      dataList.push(this.data[_id]);
    }
    const list = Util.mongoSearch(dataList, query, sort);
    if (list && list instanceof Array && list.length > 0) {
      return Promise.resolve(list[0]);
    }
    return Promise.resolve(null);
  }

  find(query: object, sort?: object, limit?: number, offset?: number, projection?: object, session?: any): Promise<any[]> {
    const dataList = [];
    for (const _id of Object.keys(this.data)) {
      dataList.push(this.data[_id]);
    }
    let list = (Util.mongoSearch(dataList, query, sort) as object[]).map((val) => Util.project(val, projection));
    if (offset) {
      if (limit) {
        list = list.slice(offset, offset + limit);
      } else {
        list = list.slice(offset);
      }
    } else {
      if (limit) {
        list = list.slice(0, limit);
      }
    }
    return Promise.resolve(list);
  }

  count(query: object): Promise<number> {
    const dataList = [];
    for (const _id of Object.keys(this.data)) {
      dataList.push(this.data[_id]);
    }
    const list = Util.mongoSearch(dataList, query) as object[];
    const count = list.length;
    return Promise.resolve(count);
  }

  insertOne(doc: { _id: string }, session?: any, throwErrorMode?: boolean): Promise<void> {
    if (!(doc as any)._id) { (doc as any)._id = uuidv1(); }
    this.data[doc._id] = doc;
    return Promise.resolve();
  }

  insertMany(docs: Array<{ _id: string }>, session?: any): Promise<void> {
    for (const doc of docs) {
      if (!(doc as any)._id) { (doc as any)._id = uuidv1(); }
      this.data[doc._id] = doc;
    }
    return Promise.resolve();
  }

  increment(id: string, field: string): Promise<number> {
    const row = this.data[id] as any;
    row[field]++;
    this.data[id] = row;
    return Promise.resolve(row[field]);
  }

  updateOneById(id: string, update: object, session?: any): Promise<void> {
    const row = this.data[id] as any;
    this.data[id] = TransactionRequest.applyMongodbUpdate(row, update as any);
    return Promise.resolve();
  }

  updateOne(filter: object, update: object, session?: any): Promise<void> {
    return this.findOne(filter).then((row) => {
      if (row) {
        const obj = row as any;
        this.data[obj._id] = TransactionRequest.applyMongodbUpdate(obj, update as any);
      }
    });
  }

  replaceOneById(id: string, doc: object, session?: any, throwErrorMode?: boolean): Promise<void> {
    (doc as any)._id = id;
    this.data[id] = doc;
    return Promise.resolve();
  }

  deleteOneById(id: string, session?: any, throwErrorMode?: boolean): Promise<void> {
    delete this.data[id];
    return Promise.resolve();
  }

  deleteByRange(field: string, from: any, to: any, session?: any): Promise<void> {
    for (const id of Object.keys(this.data)) {
      const val = (this.data[id] as any)[field];
      if (from <= val && val <= to) {
        delete this.data[id];
      }
    }
    return Promise.resolve();
  }

  deleteAll(session?: any, throwErrorMode?: boolean): Promise<void> {
    for (const id of Object.keys(this.data)) {
      delete this.data[id];
    }
    return Promise.resolve();
  }
}
