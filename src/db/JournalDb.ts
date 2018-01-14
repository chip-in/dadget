import { TransactionRequest, TransactionObject, TransactionType } from '../db/Transaction'

export interface JournalDb {

  start(): Promise<void>

  checkConsistent(csn: number, request: TransactionRequest): Promise<void>

  getLastDigest(): Promise<string>

  insert(transaction: TransactionObject): Promise<void>

  findByCsn(csn: number): Promise<TransactionObject | null>

  findByCsnRange(from: number, to: number): Promise<TransactionObject[]>

  updateAndDeleteAfter(transaction: TransactionObject): Promise<void>
}