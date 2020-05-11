import * as hash from "object-hash";

import { ERROR } from "../Errors";
import { IndexDef } from "../se/DatabaseRegistry";
import { DadgetError } from "../util/DadgetError";
import { IDb } from "./container/IDb";

const SUBSET_COLLECTION = "subset_data";

export class SubsetDb {

  constructor(private db: IDb, protected subsetName: string, protected indexDefList: IndexDef[]) {
    db.setCollection(SUBSET_COLLECTION);
    console.log("SubsetDb is created:", subsetName);
  }

  start(): Promise<void> {
    const indexMap: { [key: string]: IndexDef } = {};
    const indexNameList: { [key: string]: any } = {};
    if (this.indexDefList) {
      for (const indexDef of this.indexDefList) {
        const name = hash.MD5(indexDef);
        if (indexDef.property) { delete indexDef.property.unique; }
        indexMap[name] = indexDef;
      }
    }
    this.db.setIndexes(indexMap);
    return this.db.start()
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1201, [err.toString()])));
  }

  insert(obj: object): Promise<void> {
    return this.db.insertOne(obj)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1202, [err.toString()])));
  }

  insertMany(obj: object[]): Promise<void> {
    if (obj.length === 0) { return Promise.resolve(); }
    return this.db.insertMany(obj)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1206, [err.toString()])));
  }

  update(id: string, obj: object): Promise<void> {
    return this.db.replaceOneById(id, obj)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1203, [err.toString()])));
  }

  deleteById(id: string): Promise<void> {
    return this.db.deleteOneById(id)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1204, [err.toString()])));
  }

  deleteAll(): Promise<void> {
    return this.db.deleteAll()
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1207, [err.toString()])));
  }

  find(query: object, sort?: object, limit?: number, projection?: object, offset?: number): Promise<any[]> {
    console.log("find:", JSON.stringify(query));
    return this.db.find(query, sort, limit, offset, projection)
      .then((result) => {
        return result;
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1205, [err.toString()])));
  }

  count(query: object): Promise<number> {
    console.log("count:", JSON.stringify(query));
    return this.db.count(query)
      .then((count) => {
        return count;
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1208, [err.toString()])));
  }
}
