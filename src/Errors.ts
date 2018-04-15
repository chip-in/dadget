export const ERROR = {

  /**
   * CsnDB failed to start.
   */
  E1001: { code: 1001, message: "CsnDB failed to start. cause=%1" },

  /**
   * CsnDB failed to increment.
   */
  E1002: { code: 1002, message: "CsnDB failed to increment. cause=%1" },

  /**
   * CsnDB failed to get current value.
   */
  E1003: { code: 1003, message: "CsnDB failed to get current value. cause=%1" },

  /**
   * CsnDB failed to update csn.
   */
  E1004: { code: 1004, message: "CsnDB failed to update csn. cause=%1" },

  /**
   * JournalDB failed to start.
   */
  E1101: { code: 1101, message: "JournalDB failed to start. cause=%1" },

  /**
   * checkConsistent error: already exists
   */
  E1102: { code: 1102, message: "checkConsistent error: already exists" },

  /**
   * checkConsistent error: Not found
   */
  E1103: { code: 1103, message: "checkConsistent error: Not found" },

  /**
   * checkConsistent error: The object has already been deleted
   */
  E1104: { code: 1104, message: "checkConsistent error: The object has already been deleted" },

  /**
   * checkConsistent error: The csn is old. Update the data.
   */
  E1105: { code: 1105, message: "checkConsistent error: The csn is old. Update the data." },

  /**
   * checkConsistent error: "before" data is required
   */
  E1106: { code: 1106, message: 'checkConsistent error: "before" data is required' },

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
   * JournalDB failed to rebase journals.
   */
  E1111: { code: 1111, message: "JournalDB failed to rebase journals. cause=%1" },

  /**
   * JournalDB failed to delete journals.
   */
  E1112: { code: 1112, message: "JournalDB failed to delete journals. cause=%1" },

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
   * UpdateManager Config Error
   */
  E2501: { code: 2501, message: "UpdateManager Config Error: %1" },

  /**
   * Undefined Error
   */
  E3001: { code: 3001, message: "Undefined Error: %1" },
};
