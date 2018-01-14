import { IndexDef } from '../se/DatabaseRegistry';
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { SubsetDb } from "./SubsetDb"

export class SubsetCacheDbOnMemory implements SubsetDb {

  constructor(database: string, protected subsetName: string, protected indexDefList: IndexDef[]) {
    console.log("SubsetCacheDbOnMemory is created")
  }

  start(): Promise<void> {
    return Promise.resolve()
  }

  insert(obj: object): Promise<void> {
    return Promise.resolve()
  }

  insertAll(obj: object[]): Promise<void> {
    return Promise.resolve()
  }

  update(obj: { [key: string]: any }): Promise<void> {
    return Promise.resolve()
  }

  delete(obj: { [key: string]: any }): Promise<void> {
    return Promise.resolve()
  }

  deleteAll(): Promise<void> {
    return Promise.resolve()
  }

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any> {
    console.log("ddd")
    return Promise.resolve([{
      id: "dddddd",
      text: "result.title",
      completed: false
    }])
  }
}