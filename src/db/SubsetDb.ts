
export interface SubsetDb {

  start(): Promise<void>

  insert(obj: object): Promise<void>

  insertAll(obj: object[]): Promise<void>

  update(obj: { [key: string]: any }): Promise<void>

  delete(obj: { [key: string]: any }): Promise<void>

  deleteAll(): Promise<void>

  find(query: object, sort?: object, limit?: number, offset?: number): Promise<any>
}