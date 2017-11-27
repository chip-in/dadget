import { TransactionRequest, TransactionObject, TransactionType } from '../db/Transaction'
import { MONGO_DB } from "../Config"

export class JournalDb {
  protected dbUrl: string

  constructor(database: string) {
    this.dbUrl = MONGO_DB.URL + database
    console.log("JournalDbOnBrowser is created")
  }

  start(): Promise<void> {
    return Promise.resolve()
  }

  checkConsistent(csn: number, request: TransactionRequest): Promise<void> {
    return Promise.resolve()
  }

  getLastDigest(): Promise<string> {
    return Promise.resolve("")
  }

  insert(transaction: TransactionObject): Promise<void> {
    return Promise.resolve()
  }

  findByCsn(csn: number): Promise<TransactionObject | null> {
    return Promise.resolve(null)
  }

  findByCsnRange(from: number, to: number): Promise<TransactionObject[]> {
    return Promise.resolve([])
  }

  updateAndDeleteAfter(transaction: TransactionObject): Promise<void> {
    return Promise.resolve()
  }
}