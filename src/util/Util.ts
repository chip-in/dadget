import { ResourceNode } from "@chip-in/resource-node";
import * as dDiff from "deep-diff";
import * as parser from "mongo-parse";
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
    return whilst(data);
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

  static fetchJournal(csn: number, database: string, node: ResourceNode): Promise<TransactionObject | null> {
    return node.fetch(CORE_NODE.PATH_CONTEXT.replace(/:database\b/g, database) + CORE_NODE.PATH_GET_TRANSACTION, {
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
        console.log("fetchJournal: ", JSON.stringify(result));
        if (result.status === "OK") {
          return result.journal as TransactionObject;
        } else if (result.status === "NG") {
          return null;
        } else if (result.reason) {
          const reason = result.reason as DadgetError;
          throw new DadgetError({ code: reason.code, message: reason.message }, reason.inserts, reason.ns);
        } else {
          throw JSON.stringify(result);
        }
      });
  }

  static diff(lhs: object, rhs: object): object[] {
    if (dDiff.diff) {
      return dDiff.diff(lhs, rhs);
    } else {
      // for browsers
      return (dDiff as any).default.diff(lhs, rhs);
    }
  }
}
