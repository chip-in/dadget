const { Logger } = require("@chip-in/logger");

Logger.setLogLevel("trace");
const loggerApi = Logger.getLogger("Ejson.dadget.chip-in.net");

export function stringify(obj: object): string {
  loggerApi.trace(1, "begin stringify");
  const val = JSON.stringify(serialize(obj));
  loggerApi.trace(2, "end stringify");
  return val;
}
export function parse(str: string): any {
  loggerApi.trace(3, "begin parse");
  const val = deserialize(JSON.parse(str));
  loggerApi.trace(4, "end parse");
  return val;
}

/**
 * Deserialize is not reverse of stringify. Deserialize converts from a plain object to a object for Mongo db.
 * @param val object
 */
export function deserialize(val: any): any {
  if (Array.isArray(val)) { return deconvertArray(val); }
  if (val === null) { return null; }
  if (typeof val === "object") {
    if (val.$date) { return new Date(val.$date); }
    if (val.$undefined) { return null; }
    return deconvertObject(val);
  }
  return val;

}

function deconvertArray(val: any[]): any[] {
  const out: any[] = [];
  for (const row of val) {
    out.push(deserialize(row));
  }
  return out;
}

function deconvertObject(obj: { [key: string]: any }): { [key: string]: any } {
  const out: { [key: string]: any } = {};
  for (const key of Object.keys(obj)) {
    out[key] = deserialize(obj[key]);
  }
  return out;
}

function serialize(val: any): any {
  if (val instanceof Date) {
    return { $date: val.toISOString() };
  }
  if (Array.isArray(val)) { return convertArray(val); }
  if (val === null) { return null; }
  if (typeof val === "undefined") { return null; }
  if (typeof val === "object") { return convertObject(val); }
  return val;
}

function convertArray(val: any[]): any[] {
  const out: any[] = [];
  for (const row of val) {
    out.push(serialize(row));
  }
  return out;
}

function convertObject(obj: { [key: string]: any }): { [key: string]: any } {
  const out: { [key: string]: any } = {};
  for (const key of Object.keys(obj)) {
    out[key] = serialize(obj[key]);
  }
  return out;
}
