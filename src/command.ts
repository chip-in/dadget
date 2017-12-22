import { MongoClient, Db } from 'mongodb'
import { MONGO_DB, Mongo } from "./Config"

let mode = process.argv[2];
if (!mode) process.exit();

if (mode == "reset") {
  let target = process.argv[3];
  console.log("reset db:", target)
  let db: Db
  let dbUrl = Mongo.getUrl() + target
  MongoClient.connect(dbUrl)
    .then(_ => {
      db = _
      return db.admin().listDatabases()
    }).then((dbs) => {
      dbs = dbs.databases;
      let promise = Promise.resolve()
      for (var i = 0; i < dbs.length; i++) {
        if (dbs[i].name.startsWith(target)){
          console.log(dbs[i].name)
          let targetDb = db.db(dbs[i].name)
          promise = promise.then(() => targetDb.dropDatabase())
        }
      }
      promise.then(() => db.close())
    })
}

