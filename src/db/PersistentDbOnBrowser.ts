import { IDb } from "./IDb"

export class PersistentDb implements IDb {
  private collection: string

  constructor(protected database: string) {
    console.log("PersistentDbOnBrowser is created")
  }

  setCollection(collection: string) {
    this.collection = collection
  }

  start(): Promise<void> {
    return Promise.resolve()
  }

  findOne(query: object): Promise<object | null> {
    return Promise.resolve(null)
  }

  findOneBySort(query: object, sort: object): Promise<any> {
    return Promise.resolve(null)
  }

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any> {
    return Promise.resolve(null)
  }

  insertOne(doc: object): Promise<void> {
    return Promise.resolve()
  }

  insertMany(docs: object[]): Promise<void> {
    return Promise.resolve()
  }

  increment(id: string, field: string): Promise<number> {
    return Promise.resolve(0)
  }

  updateOneById(id: string, update: object): Promise<void> {
    return Promise.resolve()
  }

  updateOne(filter: object, update: object): Promise<void> {
    return Promise.resolve()
  }

  replaceOneById(id: string, doc: object): Promise<void> {
    return Promise.resolve()
  }

  deleteOneById(id: string): Promise<void> {
    return Promise.resolve()
  }

  deleteAll(): Promise<void> {
    return Promise.resolve()
  }

  createIndexes(indexMap: { [name: string]: { index: object, property?: object } }): Promise<void> {
    return Promise.resolve()
  }
}
