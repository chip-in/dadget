import { MongoClientOptions } from "mongodb";

export class Mongo {
  static option: MongoClientOptions;

  static getUrl() {
    return (process.env.MONGODB_URL ? process.env.MONGODB_URL : "mongodb://localhost:27017/") as string;
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
  PATH_GET_TRANSACTION: "/getTransactionJournal",
  PATH_GET_TRANSACTIONS: "/getTransactionJournals",
  PATH_QUERY: "/query",
  PATH_COUNT: "/count",
};

export const MAX_OBJECT_SIZE = 8 * 1024 * 1024;
