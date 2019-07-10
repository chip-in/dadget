
export class Mongo {
  static getUrl() {
    let url = (process.env.MONGODB_URL ? process.env.MONGODB_URL : "mongodb://localhost:27017/") as string;
    if (url.slice(-1) !== "/") { url += "/"; }
    return url;
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
  PATH_EXEC: "/exec",
  PATH_GET_TRANSACTION: "/getTransactionJournal",
  PATH_GET_TRANSACTIONS: "/getTransactionJournals",
  PATH_QUERY: "/query",
  PATH_COUNT: "/count",
};
