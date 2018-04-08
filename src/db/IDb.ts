
export interface IDb {

  setCollection(collection: string): void;

  setIndexes(indexMap: { [name: string]: { index: object, property?: object } }): void;

  start(): Promise<void>;

  findOne(query: object): Promise<object | null>;

  findByRange(field: string, from: any, to: any, dir: number): Promise<any[]>;

  findOneBySort(query: object, sort: object): Promise<any>;

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any[]>;

  insertOne(doc: object): Promise<void>;

  insertMany(docs: object[]): Promise<void>;

  increment(id: string, field: string): Promise<number>;

  updateOneById(id: string, update: object): Promise<void>;

  updateOne(filter: object, update: object): Promise<void>;

  replaceOneById(id: string, doc: object): Promise<void>;

  deleteOneById(id: string): Promise<void>;

  deleteAll(): Promise<void>;
}
