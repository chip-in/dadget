import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { MONGO_DB } from "../Config"

export class CsnDb {
  protected dbUrl: string

  constructor(database: string) {
    this.dbUrl = MONGO_DB.URL + database
    console.log("CsnDBOnBrowser is created")
  }

  start(): Promise<void> {
    return Promise.resolve()
  }

  /**
   * increment csn
   */
  increment(): Promise<number> {
    return Promise.resolve(0)
  }

  /**
   * Obtain current CSN
   */
  getCurrentCsn(): Promise<number> {
    return Promise.resolve(0)
  }

  update(seq: number): Promise<void> {
    return Promise.resolve()
  }
}