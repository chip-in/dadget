
export class SubsetDb {

  constructor() {
    console.log("SubsetDbOnBrowser is created")
  }

  start(): Promise<void> {
    return Promise.resolve()
  }

  insert(obj: object): Promise<void> {
    return Promise.resolve()
  }

  update(obj: {_id: number}): Promise<void> {
    return Promise.resolve()
  }

  delete(obj: {_id: number}): Promise<void> {
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