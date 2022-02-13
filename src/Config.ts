import { MongoClientOptions } from "mongodb";

export const SPLIT_IN_SUBSET_DB = "--";
export const SPLIT_IN_ONE_DB = "==";
export const SPLIT_IN_INDEXED_DB = "__";

export class Mongo {
  static option: MongoClientOptions;
  static url: [string, string | null];

  static _getUrl() {
    if (Mongo.url) return Mongo.url;
    const baseUrl = (process.env.MONGODB_URL ? process.env.MONGODB_URL : "mongodb://localhost:27017/") as string;
    const url = new URL(baseUrl);
    if (url.pathname.length <= 1) {
      Mongo.url = [baseUrl, null];
    } else {
      const dbName = url.pathname.substring(1);
      Mongo.url = [baseUrl.substring(0, baseUrl.length - dbName.length), dbName];

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

  static getOption(): MongoClientOptions {
    if (Mongo.option) return Mongo.option;
    const option: any = { useUnifiedTopology: true };
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
      "sslCert",
      "sslKey",
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
        if (lKey === "sslca") {
          option.sslCA = [val];
          continue block;
        }
      }
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
  PATH_GET_TRANSACTION: "/_get/getTransactionJournal",
  PATH_GET_TRANSACTION_OLD: "/getTransactionJournal",
  PATH_GET_TRANSACTIONS: "/_get/getTransactionJournals",
  PATH_GET_TRANSACTIONS_OLD: "/getTransactionJournals",
  PATH_QUERY: "/_get/query",
  PATH_QUERY_OLD: "/query",
  PATH_COUNT: "/_get/count",
  PATH_COUNT_OLD: "/count",
};

export const MAX_OBJECT_SIZE = 8 * 1024 * 1024;
