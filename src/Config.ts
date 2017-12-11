export const MONGO_DB = {
  URL: 'mongodb://localhost:27017/'
  , SYSTEM_COLLECTION: "__system__"
  , JOURNAL_COLLECTION: "__journal__"
  , SUBSET_COLLECTION: "subset_data"
  , CSN_ID: "csn"
}

export const CORE_NODE = {
  PATH_A: "/a/:app_name/"
  , PATH_R: "/r"
  , PATH_D: "/d/:database_name/"
  , PATH_M: "/m"
  , PATH_N: "/n/:node_type"
  , PATH_C: "/c/:node_id"
  , PATH_TRANSACTION: "/m/d/:database/transaction"
  , PATH_CONTEXT: "/d/:database/context"
  , PATH_SUBSET_TRANSACTION: "/m/d/:database/subset/:subset/transaction"
  , PATH_SUBSET: "/d/:database/subset/:subset"
}
