
export class Mongo {
  static getUrl() {
    let url = (process.env.MONGODB_URL ? process.env.MONGODB_URL : "mongodb://localhost:27017/") as string;
    if (url.slice(-1) !== "/") { url += "/"; }
    return url;
  }
}

const ACCESS_CONTROL_ALLOW_ORIGIN = process.env.ACCESS_CONTROL_ALLOW_ORIGIN;

export function getAccessControlAllowOrigin(origin: string) {
  if (!origin) { return null; }
  if (!ACCESS_CONTROL_ALLOW_ORIGIN) { return null; }
  const allowOrigins = ACCESS_CONTROL_ALLOW_ORIGIN.split(",");
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
  PATH_QUERY: "/query",
};
