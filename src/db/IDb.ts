
export interface IDb {

  setCollection(collection: string): void

  start(): Promise<void>

  findOne(query: object): Promise<object | null>

  findOneBySort(query: object, sort: object): Promise<any>

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any>

  insertOne(doc: object): Promise<void>

  insertMany(docs: object[]): Promise<void>

  increment(id: string): Promise<number>

  updateOneById(id: string, update: object): Promise<void>

  updateOne(filter: object, update: object): Promise<void>

  replaceOne(id: string, doc: object): Promise<void>

  deleteOne(id: string): Promise<void>

  deleteAll(): Promise<void>

  createIndexes(indexMap: { [name: string]: { index: object, property?: object } }): Promise<void>
}