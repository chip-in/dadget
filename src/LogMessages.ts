import { Logger } from "./util/Logger";

export const LOG_MESSAGES = {
  CREATED: { code: 1, msg: "%1 is created" },
  MSG_RECEIVED: { code: 2, msg: "msg received, type: %1, csn: %d1" },
  CHECKPOINT_RECEIVED: { code: 3, msg: "CHECKPOINT protectedCsn: %d1" },
  ERROR_MSG: { code: 4, msg: "%1 (%d1)" },
  ABORT_IMPORT: { code: 5, msg: "ABORT_IMPORT csn: %d1" },
  ROLLBACK: { code: 6, msg: "ROLLBACK csn: %d1" },
  ADJUST_DATA: { code: 7, msg: "adjustData csn: %d1" },
  ON_RECEIVE: { code: 8, msg: "onReceive %1 path: %2" },
  ON_RECEIVE_EXEC: { code: 9, msg: "/exec postulatedCsn %d1" },
  ON_RECEIVE_GET_TRANSACTION: { code: 10, msg: "/getTransactionJournal csn %d1" },
  ON_RECEIVE_GET_TRANSACTIONS: { code: 11, msg: "/getTransactionJournals, from: %d1, to: %d2" },
  SERVER_COMMAND_NOT_FOUND: { code: 12, msg: "server command not found!, method: %1, pathname: %2" },
  EXEC_TRUNCATE: { code: 13, msg: "TRUNCATE" },
  EXEC_BEGIN_IMPORT: { code: 14, msg: "BEGIN_IMPORT" },
  EXEC_END_IMPORT: { code: 15, msg: "END_IMPORT" },
  EXEC_ABORT_IMPORT: { code: 16, msg: "ABORT_IMPORT" },
  EXEC_BEGIN_RESTORE: { code: 17, msg: "BEGIN_RESTORE" },
  EXEC_END_RESTORE: { code: 18, msg: "END_RESTORE" },
  EXEC_ABORT_RESTORE: { code: 19, msg: "ABORT_RESTORE" },
  REQUEST_BEFORE_HAS_MISMATCH: { code: 20, msg: "request.before has mismatch: %1" },
  LASTBEFOREOBJ_CHECK_PASSED: { code: 21, msg: "lastBeforeObj check passed" },
  EXEC_NEWCSN: { code: 22, msg: "exec newCsn: %d1" },
  ERROR_CAUSE: { code: 23, msg: "error: %1" },
  CHECKPOINT_PROTECTEDCSN: { code: 24, msg: "CHECKPOINT protectedCsn: %d1" },
  STARTING: { code: 26, msg: "%1 is starting" },
  STARTED: { code: 27, msg: "%1 is started" },
  DISCONNECTED: { code: 28, msg: "%1 is disconnected" },
  REMOUNTED: { code: 29, msg: "%1 is remounted" },
  CONNECTED: { code: 30, msg: "%1 is connected" },
  DELETE_STORAGE: { code: 31, msg: "Delete storage: %1" },
  FAILED_SWEEP_STORAGE: { code: 33, msg: "Failed sweep storages: %1" },
  QUERY: { code: 34, msg: "query: %1" },
  QUERY_ERROR: { code: 35, msg: "query error: %1, query: %2" },
  COUNT_ERROR: { code: 36, msg: "count error: %1, query: %2" },
  RECEIVED_TYPE_CSN: { code: 37, msg: "received, type: %1, csn: %d1" },
  PROCTRANSACTION: { code: 38, msg: "procTransaction, type: %1, csn: %d1" },
  QUEUED_QUERY: { code: 39, msg: "queued query csn: %d1" },
  UPDATE_SUBSET_DB: { code: 40, msg: "update subset db csn: %d1" },
  ROLLBACK_TRANSACTIONS: { code: 41, msg: "ROLLBACK transactions, csn: %d1" },
  RESET_DATA: { code: 42, msg: "resetData csn: %d1" },
  RESET_DATA0: { code: 43, msg: "resetData0" },
  MOUNTING_MODE: { code: 44, msg: "mountingMode: %1" },
  SUBSET_QUERY_WARN: { code: 45, msg: "Subset Storage requires resetting because the query has been changed." },
  QUERY_CSN: { code: 46, msg: "query, csn: %d1, mode: %1" },
  COUNT_CSN: { code: 47, msg: "count, csn: %d1, mode: %1" },
  COUNT: { code: 48, msg: "count: %1" },
  GET_READLOCK: { code: 49, msg: "get readLock" },
  RELEASE_READLOCK: { code: 50, msg: "release readLock" },
  ROLLBACK_TRANSACTIONS2: { code: 51, msg: "rollback transactions, csn: %d1, currentCsn: %d2" },
  INSUFFICIENT_ROLLBACK_TRANSACTIONS: { code: 52, msg: "insufficient rollback transactions" },
  WAIT_FOR_TRANSACTIONS: { code: 53, msg: "wait for transactions, csn: %d1, currentCsn: %d2" },
  PROCGET_ERROR: { code: 54, msg: "procGet error: %1" },
  PROCPOST_ERROR: { code: 55, msg: "procPost error: %1" },
  ON_RECEIVE_EXEC_MANY: { code: 56, msg: "/execMany postulatedCsn %d1" },
  EXEC_BEGIN: { code: 57, msg: "BEGIN_TRANSACTION" },
  EXEC_END: { code: 58, msg: "END_TRANSACTION" },
  EXEC_ABORT: { code: 59, msg: "ABORT_TRANSACTION" },
  ON_RECEIVE_UPDATE_MANY: { code: 60, msg: "/updateMany, query: %1, operator: %2" },
  ROLLFORWARD_TRANSACTIONS: { code: 61, msg: "ROLLFORWARD transactions, from: %d1, to: %d2" },
  TRANSACTION_TIMEOUT: { code: 62, msg: "Transaction timeout" },
  TIME_OF_EXEC: { code: 63, msg: "time of exec: %d1 ms" },
  UPDATE_SUBSET_ERROR: { code: 64, msg: "update subset db error: %1" }, // メッセージ変更禁止
  EXEC_FORCE_ROLLBACK: { code: 65, msg: "FORCE_ROLLBACK" },
  ON_RECEIVE_GET_UPDATE_DATA: { code: 66, msg: "/getUpdateData csn %d1" },
  USE_TRANSACTION: { code: 67, msg: "use mongodb transaction" },
  ON_RECEIVE_GET_LATEST_CSN: { code: 68, msg: "/getLatestCsn" },
  PROCEED_TRANSACTION: { code: 69, msg: "proceedTransaction, csn: %d1" },
  QUEUE_WAITING: { code: 70, msg: "queue waiting" },
  MONGODB_URL: { code: 71, msg: "MONGODB_URL: %1" },
  MONGODB_OPTION: { code: 72, msg: "MONGODB_OPTION: %1" },
  MONGODB_LOG: { code: 73, msg: "MONGODB: %1, %d1" },
};

Logger.checkLogType(LOG_MESSAGES);
