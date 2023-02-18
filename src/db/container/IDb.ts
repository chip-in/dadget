
export interface IDb {

  startTransaction(): Promise<any>;

  commitTransaction(session?: any): Promise<void>;

  abortTransaction(session?: any): Promise<void>;

  setCollection(collection: string): void;

  setIndexes(indexMap: { [name: string]: { index: object, property?: object } }): void;

  start(): Promise<void>;

  findOne(query: object, session?: any): Promise<object | null>;

  findByRange(field: string, from: any, to: any, dir: number, projection?: object, session?: any): Promise<any[]>;

  findOneBySort(query: object, sort: object): Promise<any>;

  find(query: object, sort?: object, limit?: number, offset?: number, projection?: object, session?: any): Promise<any[]>;

  count(query: object): Promise<number>;

  insertOne(doc: object, session?: any): Promise<void>;

  insertMany(docs: object[], session?: any): Promise<void>;

  increment(id: string, field: string): Promise<number>;

  updateOneById(id: string, update: object, session?: any): Promise<void>;

  updateOne(filter: object, update: object, session?: any): Promise<void>;

  replaceOneById(id: string, doc: object, session?: any): Promise<void>;

  deleteOneById(id: string, session?: any): Promise<void>;

  deleteByRange(field: string, from: any, to: any, session?: any): Promise<void>;

  deleteAll(session?: any): Promise<void>;
}
