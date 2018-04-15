import * as EJSON from "../util/Ejson";

import { Logger } from "@chip-in/resource-node";
import { TransactionObject, TransactionRequest, TransactionType } from "../db/Transaction";
import { ERROR } from "../Errors";
import { DadgetError } from "../util/DadgetError";
import { IDb } from "./IDb";

const JOURNAL_COLLECTION = "__journal__";

export class JournalDb {

  constructor(private db: IDb) {
    db.setCollection(JOURNAL_COLLECTION);
    console.log("JournalDb is created");
  }

  start(): Promise<void> {
    this.db.setIndexes({
      csn_index: {
        index: { csn: -1 },
        property: { unique: true },
      },
      target_index: {
        index: { target: 1, csn: -1 },
      },
    });
    return this.db.start()
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1101, [err.toString()])));
  }

  checkConsistent(csn: number, request: TransactionRequest): Promise<void> {
    return this.db.findOneBySort({ target: request.target }, { csn: -1 })
      .then((result) => {
        console.log("checkConsistent", JSON.stringify(result));
        if (request.type === TransactionType.INSERT && request.new) {
          if (!result || result.type === TransactionType.DELETE) { return; }
          throw new DadgetError(ERROR.E1102);
        } else if (request.before) {
          if (!result) { throw new DadgetError(ERROR.E1103); }
          if (result.type === TransactionType.DELETE) { throw new DadgetError(ERROR.E1104); }
          if (result.csn > csn) { throw new DadgetError(ERROR.E1105, [result.csn, csn]); }
          return;
        } else {
          throw new DadgetError(ERROR.E1106);
        }
      });
  }

  getLastDigest(): Promise<string> {
    return this.db.findOneBySort({}, { csn: -1 })
      .then((result) => {
        console.log("getLastDigest", JSON.stringify(result));
        if (result) {
          console.log("getLastDigest:", result._id, result.digest);
          return result.digest;
        } else {
          return "";
        }
      })
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1107, [err.toString()])));
  }

  insert(transaction: TransactionObject): Promise<void> {
    console.log("insert:", JSON.stringify(transaction));
    // mongodbの制限によりoperatorを文字列化
    const saveVal: any = { ...transaction };
    if (transaction.operator) {
      saveVal.operator = EJSON.stringify(transaction.operator);
    }
    return this.db.insertOne(saveVal)
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1108, [err.toString()])));
  }

  findByCsn(csn: number): Promise<TransactionObject | null> {
    console.log("findByCsn:", csn);
    return this.db.findOne({ csn })
      .then((result) => {
        if (result) {
          const transaction = result as TransactionObject;
          console.log(JSON.stringify(transaction));
          console.log("findByCsn digest:", transaction.digest);
          return transaction as TransactionObject;
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
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1110, [err.toString()])));
  }

  deleteAfter(csn: number): Promise<void> {
    console.log("deleteAfter:", csn);
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

  deleteAll(): Promise<void> {
    console.log("deleteAll");
    return this.db.deleteAll()
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1112, [err.toString()])));
  }

  updateAndDeleteAfter(transaction: TransactionObject): Promise<void> {
    console.log("update:", JSON.stringify(transaction));
    return this.deleteAfter(transaction.csn)
      .then(() => this.db.updateOne({ csn: transaction.csn }, transaction))
      .catch((err) => Promise.reject(new DadgetError(ERROR.E1111, [err.toString()])));
  }
}
