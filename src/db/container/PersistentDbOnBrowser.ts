import * as parser from "mongo-parse";
import * as hash from "object-hash";
import { v1 as uuidv1 } from "uuid";
import { Util } from "../../util/Util";
import { TransactionRequest } from "../Transaction";
import { IDb } from "./IDb";

const OBJECT_STORE_NAME = "data";
const DADGET_SCHEMA = "__dadget_schema_ver";
const SCHEMA_VER = 1;
const INDEX_VER = "index_ver";
const INDEX_VER_NAME = "name";
const userAgent = window.navigator.userAgent;
const isIE = userAgent.indexOf("MSIE") !== -1 || userAgent.indexOf("Trident/") !== -1 || userAgent.indexOf("Edge/") !== -1;

export class PersistentDb implements IDb {
  private db: IDBDatabase;
  private collection: string;
  private indexMap: { [name: string]: { index: object, property?: object } };
  private indexRevMap: { [fields: string]: string } = {};
  private isStarted = false;

  public static getAllStorage(): Promise<string[]> {
    const dbList: string[] = [];
    return new Promise<void>((resolve, reject) => {
      const dadgetSchema = PersistentDb.openDadgetSchema(reject);
      dadgetSchema.onsuccess = (event) => {
        const schemaDb = (event.target as IDBRequest).result as IDBDatabase;
        if (!schemaDb.objectStoreNames.contains(INDEX_VER)) {
          return resolve();
        }
        const transaction = schemaDb.transaction([INDEX_VER], "readonly");
        const indexVerStore = transaction.objectStore(INDEX_VER);
        const request = indexVerStore.openCursor();
        request.onsuccess = (event) => {
          if (event.target === null) { return resolve(); }
          const cursor = (event.target as IDBRequest).result as IDBCursorWithValue;
          if (cursor) {
            dbList.push(cursor.value[INDEX_VER_NAME].toString());
            cursor.continue();
          } else {
            resolve();
          }
        };
      };
    })
      .then(() => dbList);
  }

  public static deleteStorage(name: string) {
    const request = indexedDB.deleteDatabase(name);
    request.onsuccess = () => {
      console.log("db delete success", name);
      const dadgetSchema = PersistentDb.openDadgetSchema();
      dadgetSchema.onsuccess = (event) => {
        const schemaDb = (event.target as IDBRequest).result as IDBDatabase;
        const transaction = schemaDb.transaction([INDEX_VER], "readwrite");
        transaction.objectStore(INDEX_VER).delete(name);
      };
    };
  }

  constructor(protected database: string) {
    console.log("PersistentDbOnBrowser is created");
  }

  setCollection(collection: string) {
    this.collection = collection;
  }

  setIndexes(indexMap: { [name: string]: { index: object, property?: object } }): void {
    if (this.isStarted) { throw new Error("setIndexes must be earlier than start"); }
    this.indexMap = indexMap;
  }

  start(): Promise<void> {
    this.isStarted = true;
    const storageName = this.database + "__" + this.collection;
    const indexHash = this.indexMap ? hash(this.indexMap) : "";
    let dbVer = 1;
    return new Promise((resolve, reject) => {
      const dadgetSchema = PersistentDb.openDadgetSchema(reject);
      dadgetSchema.onsuccess = (event) => {
        const schemaDb = (event.target as IDBRequest).result as IDBDatabase;
        const transaction = schemaDb.transaction([INDEX_VER], "readwrite");
        const indexVerStore = transaction.objectStore(INDEX_VER);
        const request = indexVerStore.get(storageName);
        request.onerror = (event) => {
          reject("index_ver request error: " + request.error);
        };
        request.onsuccess = (event) => {
          if (request.result) {
            dbVer = request.result.ver;
            if (request.result.hash !== indexHash) { dbVer++; }
          }
          console.log("dbVer: " + dbVer);
          indexVerStore.put({ name: storageName, ver: dbVer, hash: indexHash });
        };
        transaction.onerror = (event) => {
          reject("index_ver transaction error: " + transaction.error);
        };
        transaction.oncomplete = (event) => {
          const request = indexedDB.open(storageName, dbVer);
          request.onupgradeneeded = (event) => {
            const db = (event.target as IDBRequest).result as IDBDatabase;
            const upgradeTransaction = (event.target as IDBRequest).transaction;
            if (upgradeTransaction == null) { return reject("upgradeTransaction is null."); }
            console.log("create: " + storageName);
            let hasObjectStore = false;
            // tslint:disable-next-line:prefer-for-of
            for (let i = 0; i < db.objectStoreNames.length; i++) {
              if (db.objectStoreNames[i] === OBJECT_STORE_NAME) { hasObjectStore = true; }
            }
            const objectStore = hasObjectStore ?
              upgradeTransaction.objectStore(OBJECT_STORE_NAME) : db.createObjectStore(OBJECT_STORE_NAME, { keyPath: "_id" });
            this.createIndexes(objectStore);
          };
          request.onerror = (event) => {
            reject(storageName + " open error:" + request.error);
          };
          request.onsuccess = (event) => {
            console.log("open: " + storageName);
            this.db = (event.target as IDBRequest).result;
            // console.log(this.logAll());

            const transaction = this.db.transaction(OBJECT_STORE_NAME);
            transaction.onerror = (event) => {
              reject(storageName + " transaction error: " + transaction.error);
            };
            const objectStore = transaction.objectStore(OBJECT_STORE_NAME);
            // tslint:disable-next-line:prefer-for-of
            for (let i = 0; i < objectStore.indexNames.length; i++) {
              const index = objectStore.index(objectStore.indexNames[i]);
              const fieldsName = index.keyPath instanceof Array ? index.keyPath.join(",") : index.keyPath;
              console.log("index " + index.name + ": " + fieldsName);
              this.indexRevMap[fieldsName] = index.name;
            }
            resolve();
          };
        };
      };
    });
  }

  private static openDadgetSchema(reject?: (reason?: any) => void) {
    const request = indexedDB.open(DADGET_SCHEMA, SCHEMA_VER);
    request.onupgradeneeded = (event) => {
      const schemaDb = (event.target as IDBRequest).result as IDBDatabase;
      const objectStore = schemaDb.createObjectStore(INDEX_VER, { keyPath: INDEX_VER_NAME });
    };
    request.onerror = (event) => {
      if (reject) {
        reject("indexedDB open error: " + request.error);
      } else {
        console.error("indexedDB open error: " + request.error);
      }
    };
    return request;
  }

  private createIndexes(objectStore: IDBObjectStore): void {
    if (!this.indexMap) { return; }
    const indexMap = this.indexMap;
    const indexNames: string[] = [];
    // tslint:disable-next-line:prefer-for-of
    for (let i = 0; i < objectStore.indexNames.length; i++) {
      indexNames.push(objectStore.indexNames[i]);
    }
    for (const indexName of indexNames) {
      objectStore.deleteIndex(indexName);
    }
    for (const indexName of Object.keys(indexMap)) {
      const indexDef = indexMap[indexName];
      const fields = Object.keys(indexDef.index);
      const keyPath = fields.length === 1 ? fields[0] : fields;
      const fieldsName = fields.join(",");
      console.log("create index:" + fieldsName);
      if (!this.indexRevMap[fieldsName]) {
        this.indexRevMap[fieldsName] = indexName;
        const unique = (indexDef.property && (indexDef.property as any).unique) ? (indexDef.property as any).unique : false;
        objectStore.createIndex(indexName, keyPath, { unique });
      }
    }
  }

  private logAll(): string {
    const transaction = this.db.transaction(OBJECT_STORE_NAME);
    transaction.objectStore(OBJECT_STORE_NAME).openCursor().onsuccess = (event) => {
      const cursor = (event.target as IDBRequest).result;
      if (cursor) {
        console.dir(cursor.value);
        cursor.continue();
      }
    };
    transaction.onerror = (event) => {
      console.error("logAll error");
    };
    return "logAll: " + this.database + "__" + this.collection;
  }

  findOne(query: object): Promise<object | null> {
    const fields = Object.keys(query);
    if (fields.length !== 1) { return Promise.reject("not supported query for findOne: " + JSON.stringify(query)); }

    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME);
      const key = fields[0];
      const val = (query as any)[key];
      const objectStore = transaction.objectStore(OBJECT_STORE_NAME);
      let request: IDBRequest;
      if (key === "_id") {
        request = objectStore.get(val);
      } else {
        const index = this.indexRevMap[key];
        if (!index) { return reject("Index is not defined for key: " + JSON.stringify(query)); }
        request = objectStore.index(index).get(val);
      }
      request.onsuccess = (event) => {
        const obj = (event.target as IDBRequest).result;
        if (obj) {
          resolve(obj);
        } else {
          resolve(null);
        }
      };
      request.onerror = (event) => {
        reject("findOne request error: " + request.error);
      };
      transaction.onerror = (event) => {
        reject("findOne transaction error: " + transaction.error);
      };
    });
  }

  findByRange(field: string, from: any, to: any, dir: number, projection?: object): Promise<any[]> {
    const indexName = this.indexRevMap[field];
    if (!indexName) { return Promise.reject("Index is not defined for key: " + field); }

    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME);
      const dbIndex = transaction.objectStore(OBJECT_STORE_NAME).index(indexName);
      const lower = from <= to ? from : to;
      const upper = from > to ? from : to;
      const dataList: any[] = [];
      const request = dbIndex.openCursor(IDBKeyRange.bound(lower, upper), dir > 0 ? "next" : "prev");
      request.onsuccess = (event) => {
        const cursor = (event.target as IDBRequest).result;
        if (cursor) {
          dataList.push(Util.project(cursor.value, projection));
          cursor.continue();
        }
      };
      request.onerror = (event) => {
        reject("findByRange request error: " + request.error);
      };
      transaction.oncomplete = (event) => {
        resolve(dataList);
      };
      transaction.onerror = (event) => {
        reject("findByRange transaction error: " + transaction.error);
      };
    });
  }

  findOneBySort(query: object, sort: object): Promise<any> {
    const queryFields = Object.keys(query);
    if (queryFields.length > 1) { return Promise.reject("not supported sort for findOneBySort: " + JSON.stringify(query)); }
    const sortFields = Object.keys(sort);
    if (sortFields.length !== 1) { return Promise.reject("not supported sort for findOneBySort: " + JSON.stringify(sort)); }
    const rawFields = [...queryFields, ...sortFields];
    const fields = (rawFields.length === 2 && rawFields[0] === rawFields[1]) ? [rawFields[0]] : rawFields;
    const fieldsName = fields.join(",");
    const indexName = this.indexRevMap[fieldsName];
    if (!indexName) { return Promise.reject("Index is not defined for key: " + fieldsName); }
    const dir = (sort as any)[sortFields[0]] > 0 ? "next" : "prev";

    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME);
      const dbIndex = transaction.objectStore(OBJECT_STORE_NAME).index(indexName);
      if (isIE) {
        this.find(query, sort, 1)
          .then((objList) => {
            if (objList.length > 0) {
              resolve(objList[0]);
            } else {
              resolve(null);
            }
          });
      } else {
        let request: IDBRequest;
        if (queryFields.length === 0) {
          request = dbIndex.openCursor(undefined, dir);
        } else {
          const key = queryFields[0];
          const val = (query as any)[key];
          if (typeof val === "object" && !(val instanceof Date)) {
            if (val.hasOwnProperty("$lt") && dir === "prev" && fields.length === 2) {
              request = dbIndex.openCursor(IDBKeyRange.bound([], [val.$lt], false, true), dir);
            } else if (val.hasOwnProperty("$gt") && dir === "next" && fields.length === 1) {
              request = dbIndex.openCursor(IDBKeyRange.bound(val.$gt, [], true, false), dir);
            } else {
              return Promise.reject("findOneBySort is not supported for the query: " + JSON.stringify(query));
            }
          } else {
            request = dbIndex.openCursor(IDBKeyRange.bound([val], [val, []]), dir);
          }
        }
        request.onsuccess = (event) => {
          const cursor = (event.target as IDBRequest).result;
          if (cursor) {
            resolve(cursor.value);
          } else {
            resolve(null);
          }
        };
        request.onerror = (event) => {
          reject("findOneBySort request error: " + request.error);
        };
      }
      transaction.onerror = (event) => {
        reject("findOneBySort transaction error: " + transaction.error);
      };
    });
  }

  find(query: object, sort?: object, limit?: number, offset?: number, projection?: object): Promise<any[]> {
    const sortFields = sort ? Object.keys(sort) : [];
    const parserQuery = parser.parse(query);

    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME);
      const objectStore = transaction.objectStore(OBJECT_STORE_NAME);
      let request: IDBRequest;
      const fieldsName = sortFields.join(",");
      const indexName = isIE ? false : this.indexRevMap[fieldsName];
      let listNum = 0;
      const dataList: any[] = [];
      if (!indexName) {
        request = objectStore.openCursor();
      } else {
        const dir = (sort as any)[sortFields[0]] > 0 ? "next" : "prev";
        request = objectStore.index(indexName).openCursor(undefined, dir);
        if (limit) { listNum = limit + (offset ? offset : 0); }
      }
      request.onsuccess = (event) => {
        const cursor = (event.target as IDBRequest).result;
        if (cursor) {
          if (parserQuery.matches(cursor.value, false)) { dataList.push(Util.project(cursor.value, projection)); }
          if (!listNum || dataList.length < listNum) { cursor.continue(); }
        }
      };
      request.onerror = (event) => {
        reject("find request error: " + request.error);
      };
      transaction.oncomplete = (event) => {
        let list = indexName ? dataList : Util.mongoSearch(dataList, {}, sort) as object[];
        if (offset) {
          if (limit) {
            list = list.slice(offset, offset + limit);
          } else {
            list = list.slice(offset);
          }
        } else {
          if (limit) {
            list = list.slice(0, limit);
          }
        }
        resolve(list);
      };
      transaction.onerror = (event) => {
        reject("find transaction error: " + transaction.error);
      };
    });
  }

  count(query: object): Promise<number> {
    return this.find(query).then((list) => list.length);
  }

  insertOne(doc: object): Promise<void> {
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      if (!(doc as any)._id) { (doc as any)._id = uuidv1(); }
      transaction.objectStore(OBJECT_STORE_NAME).add(doc);
      transaction.oncomplete = (event) => {
        resolve();
      };
      transaction.onerror = (event) => {
        reject("insertOne transaction error: " + transaction.error);
      };
    });
  }

  insertMany(docs: object[]): Promise<void> {
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      const objectStore = transaction.objectStore(OBJECT_STORE_NAME);
      for (const doc of docs) {
        if (!(doc as any)._id) { (doc as any)._id = uuidv1(); }
        objectStore.add(doc);
      }
      transaction.oncomplete = (event) => {
        resolve();
      };
      transaction.onerror = (event) => {
        reject("insertMany transaction error: " + transaction.error);
      };
    });
  }

  increment(id: string, field: string): Promise<number> {
    let val: number;
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      const request = transaction.objectStore(OBJECT_STORE_NAME).get(id);
      request.onsuccess = (event) => {
        const obj = (event.target as IDBRequest).result;
        if (!obj) {
          return reject("not found a value for increment: " + id);
        }
        obj[field]++;
        val = obj[field];
        transaction.objectStore(OBJECT_STORE_NAME).put(obj);
      };
      request.onerror = (event) => {
        reject("increment request error: " + request.error);
      };
      transaction.oncomplete = (event) => {
        resolve(val);
      };
      transaction.onerror = (event) => {
        reject("increment transaction error: " + transaction.error);
      };
    });
  }

  updateOneById(id: string, update: object): Promise<void> {
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      const request = transaction.objectStore(OBJECT_STORE_NAME).get(id);
      request.onsuccess = (event) => {
        const obj = (event.target as IDBRequest).result;
        if (!obj) {
          return reject("not found a value for updateOneById: " + id);
        }
        const newObj = TransactionRequest.applyMongodbUpdate(obj, update as any);
        transaction.objectStore(OBJECT_STORE_NAME).put(newObj);
      };
      request.onerror = (event) => {
        reject("updateOneById request error: " + request.error);
      };
      transaction.oncomplete = (event) => {
        resolve();
      };
      transaction.onerror = (event) => {
        reject("updateOneById transaction error: " + transaction.error);
      };
    });
  }

  updateOne(query: object, update: object): Promise<void> {
    const fields = Object.keys(query);
    if (fields.length !== 1) { return Promise.reject("not supported query for updateOne: " + JSON.stringify(query)); }

    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      const key = fields[0];
      const val = (query as any)[key];
      const objectStore = transaction.objectStore(OBJECT_STORE_NAME);
      let request: IDBRequest;
      if (key === "_id") {
        request = objectStore.get(val);
      } else {
        const index = this.indexRevMap[key];
        if (!index) { return reject("Index is not defined for key: " + JSON.stringify(query)); }
        request = objectStore.index(index).get(val);
      }
      request.onsuccess = (event) => {
        const obj = (event.target as IDBRequest).result;
        if (obj) {
          const newObj = TransactionRequest.applyMongodbUpdate(obj, update as any);
          transaction.objectStore(OBJECT_STORE_NAME).put(newObj);
        }
      };
      request.onerror = (event) => {
        reject("updateOne request error: " + request.error);
      };
      transaction.oncomplete = (event) => {
        resolve();
      };
      transaction.onerror = (event) => {
        reject("updateOne transaction error: " + transaction.error);
      };
    });
  }

  replaceOneById(id: string, doc: object): Promise<void> {
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      (doc as any)._id = id;
      transaction.objectStore(OBJECT_STORE_NAME).put(doc);
      transaction.oncomplete = (event) => {
        resolve();
      };
      transaction.onerror = (event) => {
        reject("replaceOneById transaction error: " + transaction.error);
      };
    });
  }

  deleteOneById(id: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      transaction.objectStore(OBJECT_STORE_NAME).delete(id);
      transaction.oncomplete = (event) => {
        resolve();
      };
      transaction.onerror = (event) => {
        reject("deleteOneById transaction error: " + transaction.error);
      };
    });
  }

  deleteByRange(field: string, from: any, to: any): Promise<void> {
    return this.findByRange(field, from, to, -1, { _id: 1 })
      .then((transactions) => {
        let promise = Promise.resolve();
        for (const transaction of transactions) {
          promise = promise.then(() => this.deleteOneById((transaction as any)._id));
        }
        return promise;
      });
  }

  deleteAll(): Promise<void> {
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(OBJECT_STORE_NAME, "readwrite");
      transaction.objectStore(OBJECT_STORE_NAME).clear();
      transaction.oncomplete = (event) => {
        resolve();
      };
      transaction.onerror = (event) => {
        reject("deleteAll transaction error: " + transaction.error);
      };
    });
  }
}
