declare module "@chip-in/logger" {
  import { ResourceNode } from "@chip-in/resource-node";

  export class Logger {
    static getLogger(fqdn: string): Log;
    static setLogLevel(logLevel: number | string): void;
    static setMaxStringLength(maxStringLength: number): void;
    static attachUploader(resourceNode: ResourceNode): void;
  }
  export class Log {
    critical(code: number, message: string, inserts?: Array<string>, numInserts?: Array<number>, timeInserts?: Array<string>, language?: string): void;
    error(code: number, message: string, inserts?: Array<string>, numInserts?: Array<number>, timeInserts?: Array<string>, language?: string): void;
    warn(code: number, message: string, inserts?: Array<string>, numInserts?: Array<number>, timeInserts?: Array<string>, language?: string): void;
    log(code: number, message: string, inserts?: Array<string>, numInserts?: Array<number>, timeInserts?: Array<string>, language?: string): void;
    info(code: number, message: string, inserts?: Array<string>, numInserts?: Array<number>, timeInserts?: Array<string>, language?: string): void;
    debug(code: number, message: string, inserts?: Array<string>, numInserts?: Array<number>, timeInserts?: Array<string>, language?: string): void;
    trace(code: number, message: string, inserts?: Array<string>, numInserts?: Array<number>, timeInserts?: Array<string>, language?: string): void;
  }
}