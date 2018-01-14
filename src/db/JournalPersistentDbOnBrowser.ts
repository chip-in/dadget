import { TransactionRequest, TransactionObject, TransactionType } from '../db/Transaction'
import { DadgetError } from "../util/DadgetError"
import { ERROR } from "../Errors"
import { JournalDb } from "./JournalDb"

export class JournalPersistentDb implements JournalDb {

  constructor(database: string) {
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