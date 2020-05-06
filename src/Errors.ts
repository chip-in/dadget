export const ERROR = {

  /**
   * SystemDb failed to start.
   */
  E1001: { code: 1001, message: "SystemDb failed to start. cause=%1" },

  /**
   * SystemDb failed to increment the value of csn.
   */
  E1002: { code: 1002, message: "SystemDb failed to increment the value of csn. cause=%1" },

  /**
   * SystemDb failed to get the current value of csn.
   */
  E1003: { code: 1003, message: "SystemDb failed to get the current value of csn. cause=%1" },

  /**
   * SystemDb failed to update the value of csn.
   */
  E1004: { code: 1004, message: "SystemDb failed to update the value of csn. cause=%1" },

  /**
   * SystemDb failed to check the hash value of the subset query.
   */
  E1005: { code: 1005, message: "SystemDb failed to check the hash value of the subset query. cause=%1" },

  /**
   * SystemDb failed to update the hash value of the subset query.
   */
  E1006: { code: 1006, message: "SystemDb failed to update the hash value of the subset query. cause=%1" },

  /**
   * SystemDb failed to prepare the value of csn.
   */
  E1007: { code: 1007, message: "SystemDb failed to prepare the value of csn. cause=%1" },

  /**
   * JournalDB failed to start.
   */
  E1101: { code: 1101, message: "JournalDB failed to start. cause=%1" },

  /**
   * Consistent Error: already exists
   */
  E1102: { code: 1102, message: "Consistent Error: already exists. target=%1" },

  /**
   * Consistent Error: Not found
   */
  E1103: { code: 1103, message: "Consistent Error: Not found" },

  /**
   * Consistent Error: The object has already been deleted
   */
  E1104: { code: 1104, message: "Consistent Error: The object has already been deleted" },

  /**
   * Consistent Error: The csn is old. Update the data.
   */
  E1105: { code: 1105, message: "Consistent Error: The csn is old. Update the data. object.csn=%1, before.csn=%2" },

  /**
   * Consistent Error: "before" data is required
   */
  E1106: { code: 1106, message: 'Consistent Error: "before" data is required' },

  /**
   * JournalDB failed to get a last digest.
   */
  E1107: { code: 1107, message: "JournalDB failed to get a last digest. cause=%1" },

  /**
   * JournalDB failed to insert a transaction.
   */
  E1108: { code: 1108, message: "JournalDB failed to insert a transaction. cause=%1" },

  /**
   * JournalDB failed to do findOne by csn.
   */
  E1109: { code: 1109, message: "JournalDB failed to do findOne by csn. cause=%1" },

  /**
   * JournalDB failed to do find by csn.
   */
  E1110: { code: 1110, message: "JournalDB failed to do find by csn. cause=%1" },

  /**
   * JournalDB failed to delete journals.
   */
  E1112: { code: 1112, message: "JournalDB failed to delete journals. cause=%1" },

  /**
   * Consistent Error: The csn is old. Update the data.
   */
  E1113: { code: 1113, message: "Consistent Error: The csn is old. Update the data. postulateCsn=%1, protectedCsn=%2" },

  /**
   * JournalDB failed to get a check point journal.
   */
  E1114: { code: 1114, message: "JournalDB failed to get a check point journal. cause=%1" },

  /**
   * JournalDB failed to get a check point journal.
   */
  E1115: { code: 1115, message: "JournalDB failed to get a check point journal. cause=%1" },

  /**
   * JournalDB failed to get a last journal.
   */
  E1116: { code: 1116, message: "JournalDB failed to get a last journal. cause=%1" },

  /**
   * JournalDB failed to replace a journal.
   */
  E1117: { code: 1117, message: "JournalDB failed to replace a journal. cause=%1" },

  /**
   * SubsetDb failed to start.
   */
  E1201: { code: 1201, message: "SubsetDb failed to start. cause=%1" },

  /**
   * SubsetDb failed to insert subset data.
   */
  E1202: { code: 1202, message: "SubsetDb failed to insert subset data. cause=%1" },

  /**
   * SubsetDb failed to update subset data.
   */
  E1203: { code: 1203, message: "SubsetDb failed to update subset data. cause=%1" },

  /**
   * SubsetDb failed to delete subset data.
   */
  E1204: { code: 1204, message: "SubsetDb failed to delete subset data. cause=%1" },

  /**
   * SubsetDb failed to find subset data.
   */
  E1205: { code: 1205, message: "SubsetDb failed to find subset data. cause=%1" },

  /**
   * SubsetDb failed to insertMany subset data.
   */
  E1206: { code: 1206, message: "SubsetDb failed to insertMany subset data. cause=%1" },

  /**
   * SubsetDb failed to deleteAll subset data.
   */
  E1207: { code: 1207, message: "SubsetDb failed to deleteAll subset data. cause=%1" },

  /**
   * SubsetDb failed to count subset data.
   */
  E1208: { code: 1208, message: "SubsetDb failed to count subset data. cause=%1" },

  /**
   * ContextManager Config Error
   */
  E2001: { code: 2001, message: "ContextManager Config Error: %1" },

  /**
   * Transaction Data Format Error
   */
  E2002: { code: 2002, message: "Transaction Data Format Error: %1" },

  /**
   * ContextManagementServer failed to execute a transaction.
   */
  E2003: { code: 2003, message: "ContextManagementServer failed to execute a transaction. cause=%1" },

  /**
   * ContextManagementServer failed to execute a transaction. Csn update is required.
   */
  E2004: { code: 2004, message: "ContextManagementServer failed to execute a transaction. Csn update is required. cause=%1" },

  /**
   * A bad "before" object caused failing to execute a transaction.
   */
  E2005: { code: 2005, message: 'A bad "before" object caused failing to execute a transaction. for request = %1' },

  /**
   * Too large new object Error
   */
  E2006: { code: 2006, message: "Too large new object Error: An object must be less than %1 bytes" },

  /**
   * Dadget Config Error
   */
  E2101: { code: 2101, message: "Dadget Config Error: %1" },

  /**
   * Dadget Undefined Query Error
   */
  E2102: { code: 2102, message: "Dadget Undefined Query Error: %1" },

  /**
   * Dadget Undefined Exec Error
   */
  E2103: { code: 2103, message: "Dadget Undefined Exec Error: %1" },

  /**
   * Dadget unsupported TransactionType
   */
  E2104: { code: 2104, message: "The TransactionType is not supported." },

  /**
   * DatabaseRegistry Config Error
   */
  E2201: { code: 2201, message: "DatabaseRegistry Config Error: %1" },

  /**
   * QueryHandler Config Error
   */
  E2301: { code: 2301, message: "QueryHandler Config Error: %1" },

  /**
   * SubsetStorage Config Error
   */
  E2401: { code: 2401, message: "SubsetStorage Config Error: %1" },

  /**
   * No supported query for a csn less than protected csn Error
   */
  E2402: { code: 2402, message: "No supported query for a csn less than protected csn: csn=%1, protectedCsn=%2" },

  /**
   * UpdateManager Config Error
   */
  E2501: { code: 2501, message: "UpdateManager Config Error: %1" },

  /**
   * Undefined Error
   */
  E3001: { code: 3001, message: "Undefined Error: %1" },
};
