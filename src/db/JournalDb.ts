import * as EJSON from "../util/Ejson";

import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import { IDb } from "./container/IDb";

const JOURNAL_COLLECTION = "journal";

export class JournalDb {
  private protectedCsn: number = 0;

  constructor(private db: IDb) {
    db.setCollection(JOURNAL_COLLECTION);
    console.log("JournalDb is created");
  }

  getProtectedCsn() {
    return this.protectedCsn;
  }

  setProtectedCsn(protectedCsn: number) {
    this.protectedCsn = protectedCsn;
  }

  start(): Promise<void> {
    this.db.setIndexes({
      csn_index: {
        index: { csn: -1 },
        property: { unique: true },
      },
      target_index: {
        index: { target: -1, csn: -1 },
      },
      datetime_index: {
        index: { datetime: -1, csn: -1 },
      },
    });
    return this.db.start()
      .then(() => this.db.findOneBySort({}, { csn: 1 }))
      .then((_) => {
        if (_) { this.protectedCsn = _.csn; }
        console.log("protectedCsn: ", this.protectedCsn);
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1101, [err.toString()])));
  }

  checkConsistent(postulatedCsn: number, request: TransactionRequest): Promise<void> {
    if (postulatedCsn && postulatedCsn < this.protectedCsn) {
      throw new DadgetError(ERROR.E1113, [postulatedCsn, this.protectedCsn]);
    }
    if (request.type === TransactionType.TRUNCATE) { return Promise.resolve(); }
    if (request.type === TransactionType.BEGIN_IMPORT) { return Promise.resolve(); }
    if (request.type === TransactionType.END_IMPORT) { return Promise.resolve(); }
    if (request.type === TransactionType.ABORT_IMPORT) { return Promise.resolve(); }
    if (request.type === TransactionType.BEGIN_RESTORE) { return Promise.resolve(); }
    if (request.type === TransactionType.END_RESTORE) { return Promise.resolve(); }
    if (request.type === TransactionType.RESTORE) { return Promise.resolve(); }
    return this.db.findOneBySort({ target: request.target }, { csn: -1 })
      .then((result) => {
        if (request.type === TransactionType.INSERT && request.new) {
          if (!result || result.type === TransactionType.DELETE) { return; }
          throw new DadgetError(ERROR.E1102, [request.target]);
        } else if (request.before) {
          if (!result) {
            if (request.before.csn < this.protectedCsn) { return; }
            throw new DadgetError(ERROR.E1103);
          }
          if (result.type === TransactionType.DELETE) { throw new DadgetError(ERROR.E1104); }
          if (result.csn > request.before.csn) { throw new DadgetError(ERROR.E1105, [result.csn, request.before.csn]); }
          return;
        } else {
          throw new DadgetError(ERROR.E1106);
        }
      });
  }

  getLastDigest(): Promise<string> {
    return this.db.findOneBySort({}, { csn: -1 })
      .then((result) => {
        if (result) {
          return result.digest;
        } else {
          return "";
        }
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1107, [err.toString()])));
  }

  getLastJournal(): Promise<TransactionObject> {
    return this.db.findOneBySort({}, { csn: -1 })
      .then((result) => {
        if (result) {
          return result;
        } else {
          throw new DadgetError(ERROR.E1116, ["last journal not found"]);
        }
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1116, [err.toString()])));
  }

  static serializeTrans(transaction: TransactionObject): object {
    const saveTrans: any = { ...transaction };
    if (transaction.operator) {
      saveTrans.operator = EJSON.stringify(transaction.operator);
    }
    delete saveTrans.protectedCsn;
    return saveTrans;
  }

  static deserializeTrans(obj: any): TransactionObject {
    const transaction = { ...obj } as TransactionObject;
    if (transaction.operator) {
      transaction.operator = EJSON.parse(obj.operator);
    }
    return transaction;
  }

  insert(transaction: TransactionObject): Promise<void> {
    return this.db.insertOne(JournalDb.serializeTrans(transaction))
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1108, [err.toString()])));
  }

  findByCsn(csn: number): Promise<TransactionObject | null> {
    console.log("findByCsn:", csn);
    return this.db.findOne({ csn })
      .then((result) => {
        if (result) {
          const transaction = JournalDb.deserializeTrans(result);
          console.log("findByCsn digest:", transaction.digest);
          return transaction;
        } else {
          console.log("findByCsn: transaction none");
          return null;
        }
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1109, [err.toString()])));
  }

  findByCsnRange(from: number, to: number): Promise<TransactionObject[]> {
    console.log("findByCsnRange:", from, to);
    return this.db.findByRange("csn", from, to, -1)
      .then((list) => list.map(JournalDb.deserializeTrans))
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1110, [err.toString()])));
  }

  deleteAfterCsn(csn: number): Promise<void> {
    console.log("deleteAfterCsn:", csn);
    return this.findByCsnRange(csn + 1, Number.MAX_VALUE)
      .then((transactions) => {
        let promise = Promise.resolve();
        for (const transaction of transactions) {
          promise = promise.then(() => this.db.deleteOneById((transaction as any)._id));
        }
        return promise;
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1112, [err.toString()])));
  }

  deleteBeforeCsn(csn: number): Promise<void> {
    console.log("deleteBeforeCsn:", csn);
    if (!csn || csn <= 1) { return Promise.resolve(); }
    return this.findByCsnRange(1, csn - 1)
      .then((transactions) => {
        let promise = Promise.resolve();
        for (const transaction of transactions) {
          promise = promise.then(() => this.db.deleteOneById((transaction as any)._id));
        }
        return promise;
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1112, [err.toString()])));
  }

  deleteAll(): Promise<void> {
    console.log("deleteAll");
    return this.db.deleteAll()
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1112, [err.toString()])));
  }

  replace(oldTransaction: TransactionObject, newTransaction: TransactionObject): Promise<void> {
    console.log("replace:", (oldTransaction as any)._id);
    return this.db.replaceOneById((oldTransaction as any)._id, JournalDb.serializeTrans(newTransaction))
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1117, [err.toString()])));
  }

  getBeforeCheckPointTime(time: Date): Promise<TransactionObject | null> {
    console.log("getBeforeCheckPointTime:", time);
    return this.db.findOneBySort({ datetime: { $lt: time } }, { csn: -1 })
      .then((result) => {
        if (!result) { return null; }
        return JournalDb.deserializeTrans(result);
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1114, [err.toString()])));
  }

  getOneAfterCsn(csn: number): Promise<TransactionObject | null> {
    console.log("getOneAfterCsn:", csn);
    return this.db.findOneBySort({ csn: { $gt: csn } }, { csn: 1 })
      .then((result) => {
        if (!result) { return null; }
        return JournalDb.deserializeTrans(result);
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1115, [err.toString()])));
  }
}
