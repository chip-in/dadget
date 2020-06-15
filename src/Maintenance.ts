import * as byline from "byline";
import * as fs from "fs";
import { Db, MongoClient } from "mongodb";
import { promisify } from "util";
import { Mongo } from "./Config";
import { TransactionType } from "./db/Transaction";
import Dadget from "./se/Dadget";
import { Util } from "./util/Util";

export class Maintenance {
  static reset(target: string): void {
    console.info("reset DB:", target);
    let db: Db;
    const dbUrl = Mongo.getUrl() + target;
    MongoClient.connect(dbUrl)
      .then((_) => {
        db = _;
        return db.admin().listDatabases();
      }).then((dbs) => {
        let promise = Promise.resolve();
        for (const curDb of dbs.databases) {
          if (curDb.name.startsWith(target)) {
            console.info(curDb.name);
            const targetDb = db.db(curDb.name);
            promise = promise.then(() => targetDb.dropDatabase());
          }
        }
        promise.then(() => db.close());
      });
  }

  static export(dadget: Dadget, fileName: string): Promise<void> {
    return promisify(fs.open)(fileName, "w")
      .then((fd) => {
        return dadget.query({}, undefined, undefined, undefined, undefined, undefined, { _id: 1 })
          .then((result) => {
            const csn = result.csn;
            return Util.promiseWhile<{ resultSet: object[] }>(
              result,
              (result) => {
                return result.resultSet.length !== 0;
              },
              (result) => {
                const row = result.resultSet.shift();
                return dadget.query({ _id: (row as any)._id }, undefined, undefined, undefined, csn, "strict")
                  .then((rowData) => {
                    if (rowData.resultSet.length === 0) { return result; }
                    const data = rowData.resultSet[0];
                    return promisify(fs.write)(fd, JSON.stringify(data) + "\n").then(() => result);
                  });
              });
          })
          .then(() => promisify(fs.close)(fd));
      });
  }

  static import(dadget: Dadget, fileName: string): Promise<void> {
    const stream = byline(fs.createReadStream(fileName));
    return dadget.exec(0, { type: TransactionType.TRUNCATE, target: "" })
      .then(() => {
        return Util.promiseWhile<{ line: string }>(
          { line: stream.read() as string },
          (row) => {
            return null !== row.line;
          },
          (row) => {
            const data = JSON.parse(row.line);
            const target = data._id;
            delete data._id;
            delete data.csn;
            return dadget.exec(0, { type: TransactionType.IMPORT, target, new: data })
              .then(() => ({ line: stream.read() as string }));
          },
        );
      })
      .then(() => dadget.exec(0, { type: TransactionType.FINISH_IMPORT, target: "" }))
      .then(() => { return; });
  }
}
