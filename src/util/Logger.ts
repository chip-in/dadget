import { Log, Logger as ChipInLogger } from "@chip-in/logger";

export class Logger {
  private logger: Log;

  constructor(className: string, database?: string) {
    if (database) {
      this.logger = ChipInLogger.getLogger(database + "." + className + ".Dadget.chip-in.net");
    } else {
      this.logger = ChipInLogger.getLogger(className + ".Dadget.chip-in.net");
    }
  }

  static getLogger(className: string, database: string): Logger {
    return new Logger(className, database);
  }

  static getLoggerWoDB(className: string): Logger {
    return new Logger(className);
  }

  static checkLogType(types: { [key: string]: { code: number, msg: string } }) {
    const chkSet = new Set<number>();
    for (const key in types) {
      if (Object.prototype.hasOwnProperty.call(types, key)) {
        const element = types[key];
        if (chkSet.has(element.code)) {
          throw new Error("Types has a duplicate code.");
        }
        chkSet.add(element.code);
      }
    }
  }

  critical(type: { code: number, msg: string }, inserts?: string[], numInserts?: number[], timeInserts?: string[]) {
    this.logger.critical(type.code, type.msg, inserts, numInserts, timeInserts);
  }
  error(type: { code: number, msg: string }, inserts?: string[], numInserts?: number[], timeInserts?: string[]) {
    this.logger.error(type.code, type.msg, inserts, numInserts, timeInserts);
  }
  warn(type: { code: number, msg: string }, inserts?: string[], numInserts?: number[], timeInserts?: string[]) {
    this.logger.warn(type.code, type.msg, inserts, numInserts, timeInserts);
  }
  log(type: { code: number, msg: string }, inserts?: string[], numInserts?: number[], timeInserts?: string[]) {
    this.logger.log(type.code, type.msg, inserts, numInserts, timeInserts);
  }
  info(type: { code: number, msg: string }, inserts?: string[], numInserts?: number[], timeInserts?: string[]) {
    this.logger.info(type.code, type.msg, inserts, numInserts, timeInserts);
  }
  debug(type: { code: number, msg: string }, inserts?: string[], numInserts?: number[], timeInserts?: string[]) {
    this.logger.debug(type.code, type.msg, inserts, numInserts, timeInserts);
  }
  trace(type: { code: number, msg: string }, inserts?: string[], numInserts?: number[], timeInserts?: string[]) {
    this.logger.trace(type.code, type.msg, inserts, numInserts, timeInserts);
  }
}
