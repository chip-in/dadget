import * as byline from "byline";
import * as fs from "fs";
import { Db, MongoClient } from "mongodb";
import { promisify } from "util";
import { Mongo } from "./Config";
import { TransactionType } from "./db/Transaction";
import Dadget from "./se/Dadget";
import { Util } from "./util/Util";

const MAX_EXPORT_NUM = 100;

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
            return Util.promiseWhile<{ ids: object[] }>(
              { ids: [...result.resultSet] },
              (whileData) => {
                return whileData.ids.length !== 0;
              },
              (whileData) => {
                const idMap = new Map();
                const ids = [];
                for (let i = 0; i < MAX_EXPORT_NUM; i++) {
                  const row = whileData.ids.shift();
                  if (row) {
                    const id = (row as any)._id;
                    idMap.set(id, id);
                    ids.push(id);
                  }
                }
                return dadget.query({ _id: { $in: ids } }, undefined, -1, undefined, csn, "strict")
                  .then((rowData) => {
                    if (rowData.resultSet.length === 0) { return whileData; }
                    let out = "";
                    for (const data of rowData.resultSet) {
                      out += JSON.stringify(data) + "\n";
                      idMap.delete((data as any)._id);
                    }
                    for (const id of idMap.keys()) {
                      whileData.ids.push({ _id: id });
                    }
                    return promisify(fs.write)(fd, out).then(() => whileData);
                  });
              });
          })
          .then(() => promisify(fs.close)(fd));
      });
  }

  static import(dadget: Dadget, fileName: string, idName: string): Promise<void> {
    const stream = byline(fs.createReadStream(fileName));
    const atomicId = Dadget.uuidGen();
    return dadget._exec(0, { type: TransactionType.BEGIN_IMPORT, target: "", atomicId })
      .then(() => {
        return Util.promiseWhile<{ line: string }>(
          { line: stream.read() as string },
          (row) => {
            return null !== row.line;
          },
          (row) => {
            const data = JSON.parse(row.line);
            const target = data[idName];
            delete data._id;
            delete data.csn;
            return dadget._exec(0, { type: TransactionType.INSERT, target, new: data, atomicId })
              .then(() => ({ line: stream.read() as string }));
          },
        );
      })
      .catch((reason) => {
        return dadget._exec(0, { type: TransactionType.ABORT_IMPORT, target: "", atomicId })
          .then(() => { throw reason; });
      })
      .then(() => dadget._exec(0, { type: TransactionType.END_IMPORT, target: "", atomicId }))
      .then(() => { return; });
  }

  static restore(dadget: Dadget, fileName: string): Promise<void> {
    const stream = byline(fs.createReadStream(fileName));
    const atomicId = Dadget.uuidGen();
    return dadget._exec(0, { type: TransactionType.BEGIN_RESTORE, target: "", atomicId })
      .then(() => dadget._exec(0, { type: TransactionType.TRUNCATE, target: "", atomicId }))
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
            return dadget._exec(0, { type: TransactionType.RESTORE, target, new: data, atomicId })
              .then(() => ({ line: stream.read() as string }));
          },
        );
      })
      .catch((reason) => {
        return dadget._exec(0, { type: TransactionType.ABORT_RESTORE, target: "", atomicId })
          .then(() => { throw reason; });
      })
      .then(() => dadget._exec(0, { type: TransactionType.END_RESTORE, target: "", atomicId }))
      .then(() => { return; });
  }
}
