var chai = require('chai')
var {CacheDb} = require('../lib/db/CacheDb')

describe('CacheDb', () => {
  it('increment', () => {
    const db = new CacheDb("test");
    db.setCollection("Collection");
    return db.start()
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
    const db = new CacheDb("test_many");
    db.setCollection("Collection");
    const db2 = new CacheDb("test_many");
    db2.setCollection("Collection");
    return db.start()
    .then(() => {
      return db.insertMany([{ _id: "test1", num: 1, a: 1 }, { _id: "test2", num: 2, a: 1 }])
    })
    .then(() => {
      return db2.find({a: 1})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test1", num: 1, a: 1 }, { _id: "test2", num: 2, a: 1 }])
    })
    .then(() => {
      return db2.find({a: 1},{num: -1})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test2", num: 2, a: 1 }, { _id: "test1", num: 1, a: 1 }])
    })
    .then(() => {
      return db2.find({a: 1},{num: -1},1)
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test2", num: 2, a: 1 }])
    })
    .then(() => {
      return db2.find({a: 1},{num: -1},1,1)
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test1", num: 1, a: 1 }])
    })
    .then(() => {
      return db2.findOneBySort({a: 1},{num: -1})
    })
    .then((val) => {
      chai.assert.deepEqual(val, { _id: "test2", num: 2, a: 1 })
    })
    .then(() => {
      return db.updateOneById("test1", {"$set":{num:3}})
    })
    .then(() => {
      return db2.find({a: 1})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }, { _id: "test2", num: 2, a: 1 }])
    })
    .then(() => {
      return db.updateOne({num:2}, {"$set":{num:4}})
    })
    .then(() => {
      return db2.find({a: 1})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }, { _id: "test2", num: 4, a: 1 }])
    })
    .then(() => {
      return db.replaceOneById("test2", { _id: "test2", num: 5, a: 6 })
    })
    .then(() => {
      return db2.find({})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }, { _id: "test2", num: 5, a: 6 }])
    })
    .then(() => {
      return db.deleteOneById("test2")
    })
    .then(() => {
      return db2.find({})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 }])
    })
    .then(() => {
      return db.insertOne({ _id: "test", seq: 1 })
    })
    .then(() => {
      return db2.find({})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [{ _id: "test1", num: 3, a: 1 },{ _id: "test", seq: 1 }])
    })
    .then(() => {
      return db.deleteAll()
    })
    .then(() => {
      return db2.find({})
    })
    .then((val) => {
      chai.assert.deepEqual(val, [])
    })
  });

});