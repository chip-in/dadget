import { ResourceNode } from "@chip-in/resource-node";
import * as dDiff from "deep-diff";
import * as parser from "mongo-parse";
import * as URL from "url";
import { CORE_NODE } from "../Config";
import { TransactionObject } from "../db/Transaction";
import * as EJSON from "../util/Ejson";
import { DadgetError } from "./DadgetError";

export class Util {

  static promiseWhile<T>(
    data: T,
    condition: (data: T) => boolean,
    action: (data: T) => Promise<T>,
  ): Promise<T> {
    const whilst = (data: T): Promise<T> => {
      return condition(data) ? action(data).then(whilst) : Promise.resolve(data);
    };
    try {
      return whilst(data);
    } catch (error) {
      return Promise.reject(error);
    }
  }

  static promiseEach<T>(
    list: T[],
    action: (data: T) => Promise<any>,
  ): Promise<any> {
    return Util.promiseWhile<T[]>(
      [...list],
      (list) => list.length > 0,
      (list) => {
        const row = list.shift();
        return row ? action(row).then(() => list) : Promise.resolve([]);
      },
    );
  }

  static mongoSearch(documents: any[], query: object, sort?: any, validate?: boolean): any {
    const parsedQuery = parser.parse(query);
    return documents.filter((doc) => {
      return parsedQuery.matches(doc, validate);
    }).sort((a, b) => {
      if (!sort) { return 0; }
      // tslint:disable-next-line:forin
      for (const k in sort) {
        let result = sortCompare(a, b, k);
        if (result !== 0) {
          if (sort[k] < 0) {
            result = -result;
          }
          return result;
        }
      }
      return 0;
    });

    function sortCompare(a: any, b: any, sortProperty: any) {
      const aVal = parser.DotNotationPointers(a, sortProperty)[0].val;
      const bVal = parser.DotNotationPointers(b, sortProperty)[0].val;
      if (aVal === null || typeof aVal === "undefined") {
        if (bVal === null || typeof bVal === "undefined") {
          return 0;
        } else {
          return -1;
        }
      }
      if (bVal === null || typeof bVal === "undefined") {
        return 1;
      }
      if (aVal > bVal) {
        return 1;
      } else if (aVal < bVal) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  static fetchJournal(csn: number, database: string, node: ResourceNode, subset?: string): Promise<TransactionObject | null> {
    const pathname = (subset ?
      CORE_NODE.PATH_SUBSET_UPDATOR.replace(/:database\b/g, database).replace(/:subset\b/g, subset) :
      CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, database)) + CORE_NODE.PATH_GET_TRANSACTION;
    return node.fetch(pathname, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: EJSON.stringify({ csn }),
    })
      .then((fetchResult) => {
        if (typeof fetchResult.ok !== "undefined" && !fetchResult.ok) { throw Error(fetchResult.statusText); }
        return fetchResult.json();
      })
      .then((_) => {
        const result = EJSON.deserialize(_);
        console.log("fetchJournal: ", csn);
        if (result.status === "OK") {
          return result.journal as TransactionObject;
        } else if (result.status === "NG") {
          return null;
        } else if (result.reason) {
          const reason = result.reason as DadgetError;
          throw new DadgetError({ code: reason.code, message: reason.message }, reason.inserts, reason.ns);
        } else {
          throw new Error(JSON.stringify(result));
        }
      });
  }

  static fetchJournals(
    fromCsn: number,
    toCsn: number,
    database: string,
    node: ResourceNode,
    callback: (obj: TransactionObject) => Promise<void>,
    subset?: string,
  ): Promise<void> {
    const pathname = (subset ?
      CORE_NODE.PATH_SUBSET_UPDATOR.replace(/:database\b/g, database).replace(/:subset\b/g, subset) :
      CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, database)) + CORE_NODE.PATH_GET_TRANSACTIONS;
    const mainLoop = {
      csn: fromCsn,
    };
    return Util.promiseWhile<{ csn: number }>(
      mainLoop,
      (mainLoop) => mainLoop.csn <= toCsn,
      (mainLoop) => {
        return node.fetch(pathname, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: EJSON.stringify({ fromCsn: mainLoop.csn, toCsn }),
        })
          .then((fetchResult) => {
            if (typeof fetchResult.ok !== "undefined" && !fetchResult.ok) { throw Error(fetchResult.statusText); }
            return fetchResult.json();
          })
          .then((result) => {
            console.log("fetchJournals: ", fromCsn, toCsn);
            if (result.status === "OK") {
              const loopData = {
                csn: mainLoop.csn,
                journals: result.journals,
              };
              return Util.promiseWhile<{ csn: number, journals: string[] }>(
                loopData,
                (loopData) => loopData.journals.length > 0,
                (loopData) => {
                  const journalStr = loopData.journals.shift();
                  if (!journalStr) { throw new Error("empty journal string"); }
                  const journal = EJSON.deserialize(JSON.parse(journalStr)) as TransactionObject;
                  return callback(journal)
                    .then(() => ({
                      csn: journal.csn,
                      journals: loopData.journals,
                    }));
                },
              ).then((loopData) => loopData.csn + 1);
            } else if (result.reason) {
              const reason = result.reason as DadgetError;
              throw new DadgetError({ code: reason.code, message: reason.message }, reason.inserts, reason.ns);
            } else {
              throw new Error(JSON.stringify(result));
            }
          })
          .then((csn) => ({ csn }));
      })
      .then(() => { });
  }

  static diff(lhs: object, rhs: object): object[] {
    if (dDiff.diff) {
      return dDiff.diff(lhs, rhs);
    } else {
      // for browsers
      return (dDiff as any).default.diff(lhs, rhs);
    }
  }

  static project(data: any, projection?: any): object {
    if (!projection || Object.keys(projection).length === 0) { return data; }
    let mode = projection._id;
    for (const key in projection) {
      if (key !== "_id") { mode = projection[key]; }
    }
    let newData;
    if (mode === 0) {
      newData = { ...data };
      for (const key in data) {
        if (projection[key] === 0) { delete newData[key]; }
      }
    } else {
      newData = {} as any;
      for (const key in data) {
        if (key === "_id") {
          if (projection[key] !== 0) { newData[key] = data[key]; }
        } else if (projection[key]) { newData[key] = data[key]; }
      }
    }
    return newData;
  }
}
