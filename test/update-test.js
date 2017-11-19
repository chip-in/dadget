var chai = require('chai')
var { TransactionRequest } = require('../lib//db/Transaction')

describe('update', function () {
  describe('#$inc', function () {
    it('{ i: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$inc": { i: 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.deepEqual(res, { i: 3 })
    });

    it('{ i.d: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: { d: 1 }, t: 1 },
        operator: { "$inc": { "i.d": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: { d: 1 }, t: 1 })
      chai.assert.deepEqual(res, { i: { d: 3 }, t: 1 })
    });

    it('{ i.2: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [1,2,3,4], t: 1 },
        operator: { "$inc": { "i.2": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1,2,3,4], t: 1 })
      chai.assert.deepEqual(res, { i: [1,2,5,4], t: 1 })
    });

    it('{ i.2.a: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [1,2,{},4], t: 1 },
        operator: { "$inc": { "i.2.a": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1,2,{},4], t: 1 })
      chai.assert.deepEqual(res, { i: [1,2,{a :2},4], t: 1 })
    });
  });

  describe('#$set', function () {
    it('{ i: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$set": { i: 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.deepEqual(res, { i: 2 })
    });

    it('{ i.d: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: { d: 1 }, t: 1 },
        operator: { "$set": { "i.d": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: { d: 1 }, t: 1 })
      chai.assert.deepEqual(res, { i: { d: 2 }, t: 1 })
    });

    it('{ i.2: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [1,2,3,4], t: 1 },
        operator: { "$set": { "i.2": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1,2,3,4], t: 1 })
      chai.assert.deepEqual(res, { i: [1,2,2,4], t: 1 })
    });

    it('{ i.2.a: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [1,2,{},4], t: 1 },
        operator: { "$set": { "i.2.a": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1,2,{},4], t: 1 })
      chai.assert.deepEqual(res, { i: [1,2,{a :2},4], t: 1 })
    });
  });
});