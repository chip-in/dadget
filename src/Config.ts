import { MongoClientOptions } from "mongodb";
import * as fs from "fs";
import * as path from "path";

export const SPLIT_IN_SUBSET_DB = "--";
export const SPLIT_IN_ONE_DB = "==";
export const SPLIT_IN_INDEXED_DB = "__";
export const MAX_EXPORT_NUM = 1000;
export const EXPORT_LIMIT_NUM = -1;
export const MAX_STRING_LENGTH = 500 * 1024 * 1024;

export class Mongo {
  private static option: MongoClientOptions;
  private static url: [string, string | null];
  private static _useTransaction = false;

  private static _getUrl() {
    if (Mongo.url) return Mongo.url;
    const baseUrl = (process.env.MONGODB_URL ? process.env.MONGODB_URL : "mongodb://localhost:27017/") as string;
    const re = /^mongodb:\/\/(?<id_pw>[^:\s]+:[^@\s]*@)?(?<hosts>[^@\/\s]+)(\/(?<db>[^?\s]*)(?<query>\?[^\s\n]+)?)?$/;
    const url = baseUrl.match(re);
    if (!url || !url.groups) throw "Invalid mongodb URL:" + baseUrl;
    const newUrl = "mongodb://" + (url.groups.id_pw || "") + url.groups.hosts + "/" + (url.groups.query || "");
    if (!url.groups.db) {
      Mongo.url = [newUrl, null];
    } else {
      Mongo.url = [newUrl, url.groups.db];
    }
    return Mongo.url;
  }

  static getUrl() {
    return Mongo._getUrl()[0];
  }

  static isOneDb() {
    return !!Mongo._getUrl()[1];
  }

  static getDbName(database: string) {
    return Mongo._getUrl()[1] || database;
  }

  static getCollectionName(database: string, collection: string) {
    if (Mongo._getUrl()[1]) {
      return database + SPLIT_IN_ONE_DB + collection
    } else {
      return collection
    }
  }

  static useTransaction() {
    return Mongo._useTransaction;
  }

  static getOption(): MongoClientOptions {
    if (Mongo.option) return Mongo.option;
    const option: any = {};
    const env = process.env;
    const intList = [
      "poolSize",
      "keepAlive",
      "connectTimeoutMS",
      "socketTimeoutMS",
      "reconnectTries",
      "reconnectInterval",
      "haInterval",
      "secondaryAcceptableLatencyMS",
      "acceptableLatencyMS",
      "w",
      "wtimeout",
      "bufferMaxEntries",
      "maxStalenessSeconds",
    ];
    const boolList = [
      "ssl",
      "sslValidate",
      "tls",
      "tlsInsecure",
      "autoReconnect",
      "noDelay",
      "ha",
      "connectWithNoPrimary",
      "j",
      "forceServerObjectId",
      "serializeFunctions",
      "ignoreUndefined",
      "raw",
      "promoteLongs",
      "promoteBuffers",
      "promoteValues",
      "domainsEnabled",
    ];
    const strList = [
      "sslPass",
      "tlsCAFile",
      "tlsCertificateKeyFile",
      "tlsCertificateKeyFilePassword",
      "replicaSet",
      "authSource",
      "w",
      "appname",
      "loggerLevel",
    ];
    const fileList = [
      "sslCA",
      "sslCRL",
      "sslCert",
      "sslKey",
    ];
    block: for (const key of Object.keys(env)) {
      if (key !== "MONGODB_URL" && key.startsWith("MONGODB_")) {
        const val: any = env[key];
        const lKey = key.substring("MONGODB_".length).replace('_', '').toLowerCase();
        for (const name of intList) {
          if (lKey === name.toLowerCase()) {
            const v = parseInt(val, 10);
            if (!isNaN(v)) {
              option[name] = v;
              continue block;
            }
          }
        }
        for (const name of boolList) {
          if (lKey === name.toLowerCase()) {
            const v = parseInt(val, 10);
            option[name] = val.toLowerCase() === "true";
            continue block;
          }
        }
        for (const name of strList) {
          if (lKey === name.toLowerCase()) {
            option[name] = val;
            continue block;
          }
        }
        for (const name of fileList) {
          if (lKey === name.toLowerCase()) {
            if (fs.existsSync(val)) {
              option[name] = val;
            } else {
              const filePath = path.join(process.cwd(), "." + name);
              fs.writeFileSync(filePath, val, { mode: 0o660 });
              option[name] = filePath;
            }
            continue block;
          }
        }
      }
    }
    if ("MONGODB_USE_TRANSACTION" in env && env["MONGODB_USE_TRANSACTION"]) {
      Mongo._useTransaction = true;
    }
    Mongo.option = option;
    return option;
  }
}

let accessControlAllowOrigin: string;

export function setAccessControlAllowOrigin(origin: string) {
  accessControlAllowOrigin = origin;
}

export function getAccessControlAllowOrigin(origin: string) {
  if (!origin) { return null; }
  const allowOrigin = accessControlAllowOrigin || process.env.ACCESS_CONTROL_ALLOW_ORIGIN;
  if (!allowOrigin) { return null; }
  const allowOrigins = allowOrigin.split(",");
  origin = origin.toString();
  for (const allowOrigin of allowOrigins) {
    if (allowOrigin.toLowerCase().replace(/\/$/, "") === origin.toLowerCase().replace(/\/$/, "")) { return origin; }
  }
  return null;
}

export const CORE_NODE = {
  PATH_A: "/a/:app_name/",
  PATH_C: "/c/:node_id",
  PATH_D: "/d/:database_name/",
  PATH_R: "/r",
  PATH_M: "/m",
  PATH_N: "/n/:node_type",
  PATH_TRANSACTION: "/m/d/:database/transaction",
  PATH_CONTEXT: "/d/:database/context",
  PATH_SUBSET_TRANSACTION: "/m/d/:database/subset/:subset/transaction",
  PATH_SUBSET: "/d/:database/subset/:subset",
  PATH_SUBSET_UPDATOR: "/d/:database/updator/:subset",
  PATH_EXEC: "/exec",
  PATH_EXEC_MANY: "/execMany",
  PATH_UPDATE_MANY: "/updateMany",
  PATH_GET_TRANSACTION: "/getTransactionJournal/_get",
  PATH_GET_TRANSACTION_OLD: "/getTransactionJournal",
  PATH_GET_TRANSACTIONS: "/getTransactionJournals/_get",
  PATH_GET_TRANSACTIONS_OLD: "/getTransactionJournals",
  PATH_GET_UPDATE_DATA: "/getUpdateData/_get",
  PATH_GET_LATEST_CSN: "/getLatestCsn/_get",
  PATH_QUERY: "/query/_get",
  PATH_QUERY_OLD: "/query",
  PATH_COUNT: "/count/_get",
  PATH_COUNT_OLD: "/count",
  PATH_WAIT: "/query/_wait",
};

export const MAX_OBJECT_SIZE = 8 * 1024 * 1024 - 2048;
