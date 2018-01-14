import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { CsnDb } from "./CsnDb"

export class CsnCacheDbOnMemory implements CsnDb {

  constructor(database: string) {
    console.log("CsnCacheDbOnMemory is created")
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