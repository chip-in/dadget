import { Db, MongoClient } from "mongodb";
import { Mongo } from "./Config";

const mode = process.argv[2];
if (!mode) { process.exit(); }

if (mode === "reset") {
  const target = process.argv[3];
  console.log("reset db:", target);
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
          console.log(curDb.name);
          const targetDb = db.db(curDb.name);
          promise = promise.then(() => targetDb.dropDatabase());
        }
      }
      promise.then(() => db.close());
    });
}
