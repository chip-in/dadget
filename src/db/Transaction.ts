import * as parser from "mongo-parse";
import * as hash from "object-hash";
import { Util } from "../util/Util";
import * as EJSON from "../util/Ejson";
import { v1 as uuidv1 } from "uuid";

export const enum TransactionType {
  INSERT = "insert",
  UPDATE = "update",
  UPSERT = "upsert",
  REPLACE = "replace",
  DELETE = "delete",
  NONE = "none",
  CHECK = "check",
  FORCE_ROLLBACK = "rollback",
  TRUNCATE = "truncate",
  BEGIN = "begin",
  END = "end",
  ABORT = "abort",
  BEGIN_IMPORT = "begin_import",
  END_IMPORT = "end_import",
  ABORT_IMPORT = "abort_import",
  BEGIN_RESTORE = "begin_restore",
  END_RESTORE = "end_restore",
  ABORT_RESTORE = "abort_restore",
  RESTORE = "restore",
}

/**
 * トランザクション要求
 */
export class TransactionRequest {

  /**
   * "insert", "update", "delete" のいずれか
   */
  type: TransactionType;

  /**
   * RFC4122 準拠のUUIDであり、オブジェクトの_id属性の値である。
   */
  target: string;

  /**
   * insertのときのみ
   *
   * 追加するオブジェクト。中身の形式は自由であるが、chip-in で予約されている _id, csn の2つの属性を含んでいてはいけない
   */
  new?: object | string;

  /**
   * update,delete のとき
   *
   * 更新/削除するオブジェクトの直前の値（前提となるコンテキスト通番での値）で、_id, csn の2つの属性を含んでいなければならない
   */
  before?: { [key: string]: any } | string;

  /**
   * updateのときのみ
   *
   * 更新内容を記述するオペレータ。意味はmongoDB に準ずる。
   */
  operator?: { [op: string]: any };

  static getNew(self: TransactionRequest): object {
    return typeof self.new === "string" ? EJSON.parse(self.new) : self.new;
  }

  static getBefore(self: TransactionRequest): { [key: string]: any } {
    if (!self.before) { throw new Error("transaction.before is missing."); }
    return typeof self.before === "string" ? EJSON.parse(self.before) : self.before;
  }

  static getRawBefore(self: TransactionRequest): { [key: string]: any } {
    if (!self.before) { throw new Error("transaction.before is missing."); }
    return typeof self.before === "string" ? JSON.parse(self.before) : self.before;
  }

  /**
   * 更新operator適用
   * @param transaction
   */
  static applyOperator(transaction: TransactionRequest, beforeObj?: object, newObj?: object): { [key: string]: any } {
    if (!transaction.before) {
      return TransactionRequest.getNew(transaction);
    } else if (transaction.operator) {
      return TransactionRequest._applyOperator(transaction, beforeObj);
    } else {
      if (!newObj) newObj = TransactionRequest.getNew(transaction);
      if (!beforeObj) beforeObj = TransactionRequest.getBefore(transaction);
      return (transaction.type === TransactionType.UPSERT ? Object.assign(beforeObj, newObj) : newObj);
    }
  }

  static _applyOperator(transaction: TransactionRequest, beforeObj?: object): { [key: string]: any } {
    if (!transaction.operator) { throw new Error("transaction.operator is missing."); }
    const before = beforeObj ? beforeObj : TransactionRequest.getBefore(transaction);
    const transactionObject = transaction as TransactionObject;
    const updateObj = TransactionRequest.applyMongodbUpdate(before, transaction.operator, transactionObject.datetime);
    if (transactionObject.csn) { updateObj.csn = transactionObject.csn; }
    return updateObj;
  }

  static applyMongodbUpdate(obj: object, operator: { [op: string]: object }, currentDate?: Date): { [key: string]: any } {
    for (const op of Object.keys(operator)) {
      const list = operator[op] as { [key: string]: any };
      switch (op) {
        case "$currentDate":
          // Timestamp is not supported.
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => currentDate);
          }
          break;
        case "$inc":
          for (const key of Object.keys(list)) {
            if (!Number.isInteger(list[key])) { throw new Error("$inc must be Integer"); }
            obj = updateField(obj, key.split("."), (org) => {
              const val = typeof org === "undefined" ? 0 : org;
              if (!Number.isInteger(val)) { throw new Error("A value of the field is not Integer:" + key + ", " + JSON.stringify(val)); }
              return val + list[key];
            });
          }
          break;
        case "$min":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => Math.min(org, list[key]));
          }
          break;
        case "$max":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => Math.max(org, list[key]));
          }
          break;
        case "$mul":
          for (const key of Object.keys(list)) {
            if (!Number.isInteger(list[key])) { throw new Error("$inc must be Integer"); }
            obj = updateField(obj, key.split("."), (org) => {
              const val = typeof org === "undefined" ? 0 : org;
              if (!Number.isInteger(val)) { throw new Error("A value of the field is not Integer:" + key + ", " + JSON.stringify(val)); }
              return val * list[key];
            });
          }
          break;
        case "$rename":
          for (const key of Object.keys(list)) {
            const newField = list[key];
            let val: any;
            obj = updateField(obj, key.split("."), (org) => {
              val = org;
              return undefined;
            });
            obj = updateField(obj, newField.split("."), (org) => val);
          }
          break;
        case "$set":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => list[key]);
          }
          break;
        case "$setOnInsert":
          throw new Error("$setOnInsert is not supported");
        case "$unset":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => undefined);
          }
          break;
        case "$":
          throw new Error("Update Operator $ is not supported");
        case "$addToSet":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => {
              let newVal: any[] = typeof org === "undefined" ? [] : org;
              if (!Array.isArray(newVal)) { throw new Error("A value of the field is not Array:" + key + ", " + JSON.stringify(newVal)); }
              newVal = [...newVal];
              const values: any[] = list[key].$each ? list[key].$each : [list[key]];
              if (!Array.isArray(values)) { throw new Error("$each value must be Array:" + key + ", " + JSON.stringify(values)); }
              for (const value of values) {
                if (newVal.indexOf(value) < 0) { newVal.push(value); }
              }
              return newVal;
            });
          }
          break;
        case "$pop":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => {
              if (typeof org === "undefined") { return org; }
              if (!Array.isArray(org)) { throw new Error("A value of the field is not Array:" + key + ", " + JSON.stringify(org)); }
              if (!Number.isInteger(list[key])) { throw new Error("$pop value must be Integer"); }
              if (org.length === 0) { return org; }
              return list[key] < 0 ? org.slice(1) : org.slice(0, -1);
            });
          }
          break;
        case "$pull":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => {
              if (typeof org === "undefined") { return org; }
              if (!Array.isArray(org)) { throw new Error("A value of the field is not Array:" + key + ", " + JSON.stringify(org)); }
              if (org.length === 0) { return org; }
              const newVal: any[] = [];
              if (typeof list[key] !== "object" || list[key] instanceof Date) {
                for (const value of org) {
                  if (!isEqual(value, list[key])) { newVal.push(value); }
                }
              } else {
                if (Object.keys(list[key])[0].startsWith("$")) {
                  const query = parser.parse({ _: list[key] });
                  for (const value of org) {
                    if (!query.matches({ _: value }, false)) { newVal.push(value); }
                  }
                } else {
                  const query = parser.parse(list[key]);
                  for (const value of org) {
                    if (!query.matches(value, false)) { newVal.push(value); }
                  }
                }
              }
              return newVal;
            });
          }
          break;
        case "$pushAll":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => {
              let newVal: any[] = typeof org === "undefined" ? [] : org;
              if (!Array.isArray(list[key])) { throw new Error("$pushAll must be Array"); }
              if (!Array.isArray(newVal)) { throw new Error("A value of the field is not Array:" + key + ", " + JSON.stringify(newVal)); }
              newVal = [...newVal, ...list[key]];
              return newVal;
            });
          }
          break;
        case "$push":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => {
              let newVal: any[] = typeof org === "undefined" ? [] : [...org];
              if (typeof list[key] === "object" && list[key].$each) {
                if (!Array.isArray(list[key].$each)) { throw new Error("$each must be Array"); }
                if (typeof list[key].$position !== "undefined") {
                  const pos = list[key].$position;
                  if (!Number.isInteger(pos)) { throw new Error("$position must be Integer"); }
                  newVal = [...newVal.slice(0, pos), ...list[key].$each, ...newVal.slice(pos)];
                } else {
                  newVal = [...newVal, ...list[key].$each];
                }
                if (typeof list[key].$sort !== "undefined") {
                  newVal = Util.mongoSearch(newVal, {}, list[key].$sort, false);
                }
                if (typeof list[key].$slice !== "undefined") {
                  if (!Number.isInteger(list[key].$slice)) { throw new Error("$slice must be Integer"); }
                  newVal = newVal.slice(0, list[key].$slice);
                }
              } else {
                newVal.push(list[key]);
              }
              return newVal;
            });
          }
          break;
        case "$pullAll":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => {
              if (typeof org === "undefined") { return org; }
              if (!Array.isArray(org)) { throw new Error("A value of the field is not Array:" + key + ", " + JSON.stringify(org)); }
              if (org.length === 0) { return org; }
              const newVal: any[] = [];
              for (const value of org) {
                if (!inArray(value, list[key])) { newVal.push(value); }
              }
              return newVal;
            });
          }
          break;
        case "$bit":
          for (const key of Object.keys(list)) {
            obj = updateField(obj, key.split("."), (org) => {
              if (!Number.isInteger(org)) { throw new Error("A value of the field is not Integer:" + key + ", " + JSON.stringify(org)); }
              const val = list[key];
              if (val.and) {
                if (!Number.isInteger(val.and)) { throw new Error("The bit AND operator value is not Integer:" + val.and); }
                return org & val.and;
              } else if (val.or) {
                if (!Number.isInteger(val.or)) { throw new Error("The bit OR operator value is not Integer:" + val.or); }
                return org | val.or;
              } else if (val.xor) {
                if (!Number.isInteger(val.xor)) { throw new Error("The bit XOR operator value is not Integer:" + val.xor); }
                return org ^ val.xor;
              }
            });
          }
          break;
        default:
          throw new Error("Not supported operator: " + op);
      }
    }
    return obj;
  }
}

function isEqual(a: any, b: any) {
  if (a instanceof Date) {
    return b instanceof Date && a.getTime() === b.getTime();
  } else {
    return a === b;
  }
}

function inArray(a: any, obj: any[]) {
  if (!Array.isArray(obj)) { throw new Error("The value is not Array:" + JSON.stringify(obj)); }
  for (const val of obj) {
    if (isEqual(a, val)) { return true; }
  }
  return false;
}

function updateField(obj: { [key: string]: any }, keys: string[], func: (org: any) => any): { [key: string]: any } {
  const key = keys.shift();
  if (key == null) { throw new Error(); }
  if (keys.length === 0) {
    if (Array.isArray(obj)) {
      const pos = Number(key);
      if (!Number.isInteger(pos)) { throw new Error("An value of position is not Integer:" + pos); }
      const newVal = func(obj[pos]);
      if (typeof newVal === "undefined") {
        return [...obj.slice(0, pos), ...obj.slice(pos + 1)];
      }
      return [...obj.slice(0, pos), newVal, ...obj.slice(pos + 1)];
    } else {
      const newVal = func(obj[key]);
      if (typeof newVal === "undefined") {
        const newObj = { ...obj };
        delete newObj[key];
        return newObj;
      }
      return { ...obj, [key]: newVal };
    }
  } else {
    if (Array.isArray(obj)) {
      const pos = Number(key);
      if (!Number.isInteger(pos)) { throw new Error("An value of position is not Integer:" + pos); }
      return [...obj.slice(0, pos), updateField(obj[pos], keys, func), ...obj.slice(pos + 1)];
    } else {
      return { ...obj, [key]: updateField(obj[key], keys, func) };
    }
  }
}

/**
 * トランザクションオブジェクト
 */
export class TransactionObject extends TransactionRequest {

  /**
   * トランザクションハッシュ生成
   * @param transaction
   */
  static calcDigest(transaction: TransactionObject) {
    return uuidv1();
  }

  /**
   * トランザクションをコミットしたコンテキスト通番
   */
  csn: number;

  /**
   * 	トランザクションをコミットした時刻
   */
  datetime: Date;

  /**
   * 1個前のトランザクションオブジェクトのdigestの値
   */
  beforeDigest: string;

  /**
   * ジャーナルチェックのためのUUID
   */
  digest?: string;

  /**
   * 保護CSN
   */
  protectedCsn?: number;

  /**
   * コミット済みCSN
   */
  committedCsn?: number;
}
