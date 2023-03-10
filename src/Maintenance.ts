import * as readline from "readline";
import * as fs from "fs";
import { MongoClient } from "mongodb";
import { promisify } from "util";
import * as split2 from "split2";
import * as through2 from "through2";
import { Mongo, SPLIT_IN_SUBSET_DB } from "./Config";
import { TransactionRequest, TransactionType } from "./db/Transaction";
import Dadget from "./se/Dadget";
import { Util } from "./util/Util";
import * as EJSON from "./util/Ejson";

const MAX_EXPORT_NUM = 100;
const MAX_UPLOAD_BYTES = 100 * 1024;

export class Maintenance {
  static reset(target: string): void {
    console.info("reset DB:", target);
    let client: MongoClient;
    MongoClient.connect(Mongo.getUrl(), Mongo.getOption())
      .then((_) => {
        client = _;
        return client.db().admin().listDatabases();
      }).then((dbs) => {
        let promise = Promise.resolve(true);
        for (const curDb of dbs.databases) {
          if (curDb.name === target || curDb.name.startsWith(target + SPLIT_IN_SUBSET_DB)) {
            console.info(curDb.name);
            const targetDb = client.db(curDb.name);
            promise = promise.then(() => targetDb.dropDatabase());
          }
        }
        promise.then(() => client.close());
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
                      out += EJSON.stringify(data) + "\n";
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

  private static async uploadStream(fileName: string, dadget: Dadget, type: TransactionType, idName: string, atomicId: string) {
    let list: TransactionRequest[] = [];
    let listSize = 0;
    let promise1 = Promise.resolve();
    let promise2 = Promise.resolve();
    let error: any = null;
    return new Promise<void>((resolve, reject) => {
      const func = async (list: TransactionRequest[]) => {
        if (error) {
          await promise2;
          throw error;
        }
        promise1 = dadget._execMany(0, list, atomicId).catch((e) => {
          if (!error) error = e;
        });
        await promise2;
        promise2 = promise1;
        if (error) {
          await promise2;
          throw error;
        }
      }
      fs.createReadStream(fileName, { encoding: 'utf8' })
        .pipe(split2())
        .pipe(
          through2((chunk, enc, callback) => {
            const line = chunk.toString();
            const data = EJSON.parse(line);
            if (!data.hasOwnProperty(idName)) {
              reject("data has no " + idName + " property.");
              return;
            }
            const target = data[idName];
            delete data._id;
            delete data.csn;
            listSize += line.length;
            list.push({ type, target, new: data });
            if (listSize < MAX_UPLOAD_BYTES) {
              callback(); //next step, no process
            } else {
              //call the method that creates a promise, and at the end
              //just empty the buffer, and process the next chunk
              func(list).then(() => {
                list = [];
                listSize = 0;
                callback();
              }).catch((e) => {
                reject(e);
              });
            }
          }))
        .on('error', error => {
          reject(error);
        })
        .on('finish', () => {
          if (error) {
            reject(error);
            return;
          }
          //any remaining data still needs to be sent
          //resolve the outer promise only when the final batch has completed processing
          if (list.length > 0) {
            func(list).then(() => promise2).then(() => {
              if (error) {
                reject(error);
              } else {
                resolve();
              }
            }).catch((e) => {
              reject(e);
            });
          } else {
            promise2.then(() => {
              if (error) {
                reject(error);
              } else {
                resolve();
              }
            }).catch((e) => {
              reject(e);
            });
          }
        });
    });
  }

  static import(dadget: Dadget, fileName: string, idName: string): Promise<void> {
    const atomicId = Dadget.uuidGen();
    return dadget._exec(0, { type: TransactionType.BEGIN_IMPORT, target: "" }, atomicId)
      .then(() => Maintenance.uploadStream(fileName, dadget, TransactionType.INSERT, idName, atomicId))
      .catch((reason) => {
        return dadget._exec(0, { type: TransactionType.ABORT_IMPORT, target: "" }, atomicId)
          .then(() => { throw reason; });
      })
      .then(() => dadget._exec(0, { type: TransactionType.END_IMPORT, target: "" }, atomicId))
      .then(() => { return; });
  }

  static restore(dadget: Dadget, fileName: string): Promise<void> {
    const atomicId = Dadget.uuidGen();
    return dadget._exec(0, { type: TransactionType.BEGIN_RESTORE, target: "" }, atomicId)
      .then(() => dadget._exec(0, { type: TransactionType.TRUNCATE, target: "" }, atomicId))
      .then(() => Maintenance.uploadStream(fileName, dadget, TransactionType.RESTORE, "_id", atomicId))
      .catch((reason) => {
        return dadget._exec(0, { type: TransactionType.ABORT_RESTORE, target: "" }, atomicId)
          .then(() => { throw reason; });
      })
      .then(() => dadget._exec(0, { type: TransactionType.END_RESTORE, target: "" }, atomicId))
      .then(() => { return; });
  }

  static clear(dadget: Dadget, force: boolean): Promise<void> {
    return dadget._clear(force)
      .then(() => { return; });
  }
}
