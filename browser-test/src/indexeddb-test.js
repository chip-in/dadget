var chai = require('chai')
var { PersistentDb } = require('../../lib/db/PersistentDbOnBrowser')

describe('PersistentDb', () => {
  it('increment', () => {
    const db = new PersistentDb("test");
    db.setCollection("Collection");
    return db.start()
      .then(() => {
        return db.deleteAll()
      })
      .then(() => {
        return db.insertOne({ _id: "test", seq: 1 })
      })
      .then(() => {
        return db.increment("test", "seq")
      })
      .then(() => {
        return db.findOne({ _id: "test" })
      })
      .then((val) => {
        chai.assert.equal(val.seq, 2)
      })
  });

  it('many', () => {
    const db = new PersistentDb("test_many");
    db.setCollection("Collection");
    return Promise.resolve()
      .then(() => {
        return new Promise((resolve, reject) => {
          var req = indexedDB.deleteDatabase("test_many__Collection")
          req.onsuccess = function () {
            console.log("Deleted database successfully");
            resolve();
          };
          req.onerror = function () {
            reject("Couldn't delete database");
          };
          req.onblocked = function () {
            reject("Couldn't delete database due to the operation being blocked");
          };
        })
      })
      .then(() => {
        db.setIndexes({
          a_num_index: {
            index: { a: 1, num: 1 }
          },
          num_index: {
            index: { num: 1 }
          }
        })
      })
      .then(() => {
        console.log("a")
        return db.start()
      })
      .then(() => {
        console.log("c")
        return db.deleteAll()
      })
      .then(() => {
        console.log("d")
        return db.insertMany([{ _id: "test1", num: 1, a: 1 }, { _id: "test2", num: 2, a: 1 }])
      })
      .then(() => {
        console.log("e")
        return db.find({ a: 1 })
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test1", num: 1, a: 1 }, { _id: "test2", num: 2, a: 1 }])
      })
      .then(() => {
        console.log("f")
        return db.find({ a: 1 }, { num: -1 })
      })
      .then((val) => {
        console.log("f2")
        console.dir(val)
        chai.assert.deepEqual(val, [{ _id: "test2", num: 2, a: 1 }, { _id: "test1", num: 1, a: 1 }])
      })
      .then(() => {
        console.log("g")
        return db.find({ a: 1 }, { num: -1 }, 1)
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test2", num: 2, a: 1 }])
      })
      .then(() => {
        console.log("h")
        return db.find({ a: 1 }, { num: -1 }, 1, 1)
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test1", num: 1, a: 1 }])
      })
      .then(() => {
        console.log("i")
        return db.findOneBySort({ a: 1 }, { num: -1 })
      })
      .then((val) => {
        chai.assert.deepEqual(val, { _id: "test2", num: 2, a: 1 })
      })
      .then(() => {
        console.log("j")
        return db.updateOneById("test1", { "$set": { num: 3 } })
      })
      .then(() => {
        console.log("k")
        return db.find({ a: 1 })
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }, { _id: "test2", num: 2, a: 1 }])
      })
      .then(() => {
        console.log("k2")
        return db.findOne({ num: 2 })
      })
      .then((val) => {
        chai.assert.deepEqual(val, { _id: "test2", num: 2, a: 1 })
      })
      .then(() => {
        console.log("l")
        return db.updateOne({ num: 2 }, { "$set": { num: 4 } })
      })
      .then(() => {
        console.log("l2")
        return db.updateOne({ num: 99 }, { "$set": { num: 9 } })
      })
      .then(() => {
        console.log("m")
        return db.find({ a: 1 })
      })
      .then((val) => {
        console.dir(val)
        chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }, { _id: "test2", num: 4, a: 1 }])
      })
      .then(() => {
        console.log("n")
        return db.replaceOneById("test2", { _id: "test2", num: 5, a: 6 })
      })
      .then(() => {
        console.log("o")
        return db.find({})
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }, { _id: "test2", num: 5, a: 6 }])
      })
      .then(() => {
        console.log("findByRange")
        return db.findByRange("num", 3, 5, 1)
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }, { _id: "test2", num: 5, a: 6 }])
      })
      .then(() => {
        console.log("findByRange1")
        return db.findByRange("num", 3, 5, -1)
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test2", num: 5, a: 6 }, { _id: "test1", num: 3, a: 1 }])
      })
      .then(() => {
        console.log("findByRange2")
        return db.findByRange("num", 3, 4, -1)
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }])
      })
      .then(() => {
        console.log("p")
        return db.deleteOneById("test2")
      })
      .then(() => {
        console.log("q")
        return db.find({})
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }])
      })
      .then(() => {
        console.log("r")
        return db.insertOne({ _id: "test", seq: 1 })
      })
      .then(() => {
        console.log("s")
        return db.find({})
      })
      .then((val) => {
        chai.assert.deepEqual(val, [{ _id: "test", seq: 1 }, { _id: "test1", num: 3, a: 1 }])
      })
      .then(() => {
        console.log("t")
        return db.deleteAll()
      })
      .then(() => {
        console.log("u")
        return db.find({})
      })
      .then((val) => {
        chai.assert.deepEqual(val, [])
      })
  });

});