import * as hash from "object-hash"

import { ERROR } from "../Errors"
import { IndexDef } from "../se/DatabaseRegistry";
import { DadgetError } from "../util/DadgetError"
import { IDb } from "./IDb"

const SUBSET_COLLECTION = "subset_data"

export class SubsetDb {

  constructor(private db: IDb, protected subsetName: string, protected indexDefList: IndexDef[]) {
    db.setCollection(SUBSET_COLLECTION)
    console.log("SubsetDb is created:", subsetName)
  }

  start(): Promise<void> {
    const indexMap: { [key: string]: IndexDef } = {}
    const indexNameList: { [key: string]: any } = {}
    if (this.indexDefList) {
      for (const indexDef of this.indexDefList) {
        const name = hash.MD5(indexDef)
        if (indexDef.property) { delete indexDef.property.unique }
        indexMap[name] = indexDef
      }
    }
    return this.db.start()
      .then(() => {
        return this.db.createIndexes(indexMap)
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1201, [err.toString()])))
  }

  insert(obj: object): Promise<void> {
    console.log("insert:", JSON.stringify(obj));
    return this.db.insertOne(obj)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1202, [err.toString()])))
  }

  insertMany(obj: object[]): Promise<void> {
    if (obj.length === 0) { return Promise.resolve() }
    console.log("insertAll:");
    return this.db.insertMany(obj)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1206, [err.toString()])))
  }

  update(obj: { [key: string]: any }): Promise<void> {
    console.log("update:", JSON.stringify(obj));
    return this.db.replaceOneById(obj._id, obj)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1203, [err.toString()])))
  }

  delete(obj: { [key: string]: any }): Promise<void> {
    console.log("delete:", JSON.stringify(obj));
    return this.db.deleteOneById(obj._id)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1204, [err.toString()])))
  }

  deleteAll(): Promise<void> {
    console.log("deleteAll:");
    return this.db.deleteAll()
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1207, [err.toString()])))
  }

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any> {
    return this.db.find(query, sort, limit, offset)
      .then((result) => {
        console.log("find:", JSON.stringify(result));
        return result
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1205, [err.toString()])))
  }
}
