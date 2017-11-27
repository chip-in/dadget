var chai = require('chai')
var { TransactionRequest } = require('../lib//db/Transaction')

describe('update', function () {
  describe('$currentDate', function () {
    it('{ date: true  }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$currentDate": { date: true } },
        datetime: new Date()
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.isTrue(res.date instanceof Date)
    });
  });

  describe('$inc', function () {
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
        before: { i: [1, 2, 3, 4], t: 1 },
        operator: { "$inc": { "i.2": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1, 2, 3, 4], t: 1 })
      chai.assert.deepEqual(res, { i: [1, 2, 5, 4], t: 1 })
    });

    it('{ i.2.a: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [1, 2, {}, 4], t: 1 },
        operator: { "$inc": { "i.2.a": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1, 2, {}, 4], t: 1 })
      chai.assert.deepEqual(res, { i: [1, 2, { a: 2 }, 4], t: 1 })
    });
  });

  describe('$min', function () {
    it('{ i: 3 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$min": { i: 3 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.deepEqual(res, { i: 1 })
    });
    it('{ i: 0 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$min": { i: 0 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.deepEqual(res, { i: 0 })
    });
  });

  describe('$max', function () {
    it('{ i: 3 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$max": { i: 3 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.deepEqual(res, { i: 3 })
    });
    it('{ i: 0 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$max": { i: 0 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.deepEqual(res, { i: 1 })
    });
  });

  describe('$mul', function () {
    it('{ i: 3 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 2 },
        operator: { "$mul": { i: 3 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 2 })
      chai.assert.deepEqual(res, { i: 6 })
    });
    it('{ i: 1 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: {},
        operator: { "$mul": { i: 1 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, {})
      chai.assert.deepEqual(res, { i: 0 })
    });
  });

  describe('$rename', function () {
    it('{ i: "j" }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 2 },
        operator: { "$rename": { i: "j" } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 2 })
      chai.assert.deepEqual(res, { j: 2 })
    });
  });

  describe('$set', function () {
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
        before: { i: [1, 2, 3, 4], t: 1 },
        operator: { "$set": { "i.2": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1, 2, 3, 4], t: 1 })
      chai.assert.deepEqual(res, { i: [1, 2, 2, 4], t: 1 })
    });

    it('{ i.2.a: 2 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [1, 2, {}, 4], t: 1 },
        operator: { "$set": { "i.2.a": 2 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [1, 2, {}, 4], t: 1 })
      chai.assert.deepEqual(res, { i: [1, 2, { a: 2 }, 4], t: 1 })
    });
  });

  describe('$unset', function () {
    it('{ i: "" }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 2, j: 3 },
        operator: { "$unset": { i: "" } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 2, j: 3 })
      chai.assert.deepEqual(res, { j: 3 })
    });
  });

  describe('$addToSet', function () {
    it('{ i: 7 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [2, 4, 6] },
        operator: { "$addToSet": { i: 7 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [2, 4, 6] })
      chai.assert.deepEqual(res, { i: [2, 4, 6, 7] })
    });
    it('{ i: 6 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [2, 4, 6] },
        operator: { "$addToSet": { i: 6 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [2, 4, 6] })
      chai.assert.deepEqual(res, { i: [2, 4, 6] })
    });
    it('{ $addToSet: { tags: { $each: [ "camera", "electronics", "accessories" ] } } }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { tags: ["electronics", "supplies"] },
        operator: { $addToSet: { tags: { $each: ["camera", "electronics", "accessories"] } } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { tags: ["electronics", "supplies"] })
      chai.assert.deepEqual(res, { tags: ["electronics", "supplies", "camera", "accessories"] })
    });
  });

  describe('$pop', function () {
    it('{ i: -1 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [8, 9, 10] },
        operator: { "$pop": { i: -1 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [8, 9, 10] })
      chai.assert.deepEqual(res, { i: [9, 10] })
    });
    it('{ i: 1 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [9, 10] },
        operator: { "$pop": { i: 1 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [9, 10] })
      chai.assert.deepEqual(res, { i: [9] })
    });
  });

  describe('$pull', function () {
    it('{ $pull: { fruits: { $in: ["apples", "oranges"] }, vegetables: "carrots" } }', function () {
      let req = {
        type: "update",
        target: 1,
        before: {
          fruits: ["apples", "pears", "oranges", "grapes", "bananas"],
          vegetables: ["carrots", "celery", "squash", "carrots"]
        },
        operator: { $pull: { fruits: { $in: ["apples", "oranges"] }, vegetables: "carrots" } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, {
        fruits: ["apples", "pears", "oranges", "grapes", "bananas"],
        vegetables: ["carrots", "celery", "squash", "carrots"]
      })
      chai.assert.deepEqual(res, {
        "fruits": ["pears", "grapes", "bananas"],
        "vegetables": ["celery", "squash"]
      })
    });
    it('{ $pull: { votes: { $gte: 6 } } }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { votes: [3, 5, 6, 7, 7, 8] },
        operator: { $pull: { votes: { $gte: 6 } } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { votes: [3, 5, 6, 7, 7, 8] })
      chai.assert.deepEqual(res, { votes: [3, 5] })
    });
    it('{ $pull: { results: { score: 8 , item: "B" } } }', function () {
      let req = {
        type: "update",
        target: 1,
        before: {
          results: [
            { item: "A", score: 5 },
            { item: "B", score: 8, comment: "Strongly agree" }
          ]
        },
        operator: { $pull: { results: { score: 8, item: "B" } } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, {
        results: [
          { item: "A", score: 5 },
          { item: "B", score: 8, comment: "Strongly agree" }
        ]
      })
      chai.assert.deepEqual(res, { "results": [{ "item": "A", "score": 5 }] })
    });
    it('{ $pull: { results: { answers: { $elemMatch: { q: 2, a: { $gte: 8 } } } } }}', function () {
      let req = {
        type: "update",
        target: 1,
        before: {
          results: [
            { item: "A", score: 5, answers: [{ q: 1, a: 4 }, { q: 2, a: 6 }] },
            { item: "B", score: 8, answers: [{ q: 1, a: 8 }, { q: 2, a: 9 }] }
          ]
        },
        operator: { $pull: { results: { answers: { $elemMatch: { q: 2, a: { $gte: 8 } } } } } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, {
        results: [
          { item: "A", score: 5, answers: [{ q: 1, a: 4 }, { q: 2, a: 6 }] },
          { item: "B", score: 8, answers: [{ q: 1, a: 8 }, { q: 2, a: 9 }] }
        ]
      })
      chai.assert.deepEqual(res, {
        "results": [
          { "item": "A", "score": 5, "answers": [{ "q": 1, "a": 4 }, { "q": 2, "a": 6 }] }
        ]
      })
    });
    it('{ $pull: { d: date3 } }', function () {
      let date1 = new Date()
      let date2 = new Date(Date.parse("2017-1-1"))
      let date3 = new Date(Date.parse("2017-1-1"))
      let req = {
        type: "update",
        target: 1,
        before: { d: [date1, date2] },
        operator: { $pull: { d: date3 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { d: [date1, date2] })
      chai.assert.deepEqual(res, { d: [date1] })
    });
  });

  describe('$pushAll', function () {
    it('{ i: [ 90, 92, 85 ] }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [9, 10] },
        operator: { "$pushAll": { i: [90, 92, 85] } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [9, 10] })
      chai.assert.deepEqual(res, { i: [9, 10, 90, 92, 85] })
    });
  });

  describe('$push', function () {
    it('{ i: 1 }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [8, 9, 10] },
        operator: { "$push": { i: 1 } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [8, 9, 10] })
      chai.assert.deepEqual(res, { i: [8, 9, 10, 1] })
    });
    it('{ i: { $each: [ 90, 92, 85 ] } }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [9, 10] },
        operator: { "$push": { i: { $each: [90, 92, 85] } } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [9, 10] })
      chai.assert.deepEqual(res, { i: [9, 10, 90, 92, 85] })
    });
    it('{ i: { $each: [ 90, 92, 85 ], $position: 0 } }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [9, 10] },
        operator: { "$push": { i: { $each: [90, 92, 85], $position: 0 } } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [9, 10] })
      chai.assert.deepEqual(res, { i: [90, 92, 85, 9, 10] })
    });
    it('{ i: { $each: [ 90, 92, 85 ], $position: 1 } }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [9, 10] },
        operator: { "$push": { i: { $each: [90, 92, 85], $position: 1 } } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [9, 10] })
      chai.assert.deepEqual(res, { i: [9, 90, 92, 85, 10] })
    });
    it('{ $push: { quizzes: {$each: [ { wk: 5, score: 8 }, { wk: 6, score: 7 }, { wk: 7, score: 6 } ],$sort: { score: -1 },$slice: 3}} }', function () {
      let req = {
        type: "update",
        target: 1,
        before: {
          "quizzes": [
            { "wk": 1, "score": 10 },
            { "wk": 2, "score": 8 },
            { "wk": 3, "score": 5 },
            { "wk": 4, "score": 6 }
          ]
        },
        operator: {
          $push: {
            quizzes: {
              $each: [{ wk: 5, score: 8 }, { wk: 6, score: 7 }, { wk: 7, score: 6 }],
              $sort: { score: -1 },
              $slice: 3
            }
          }
        }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, {
        "quizzes": [
          { "wk": 1, "score": 10 },
          { "wk": 2, "score": 8 },
          { "wk": 3, "score": 5 },
          { "wk": 4, "score": 6 }
        ]
      })
      chai.assert.deepEqual(res, {
        "quizzes": [
          { "wk": 1, "score": 10 },
          { "wk": 2, "score": 8 },
          { "wk": 5, "score": 8 }
        ]
      })
    });
  });

  describe('$pullAll', function () {
    it('{ i: [ 0, 5 ] }', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: [ 0, 2, 5, 5, 1, 0 ] },
        operator: { "$pullAll": { i: [ 0, 5 ] } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: [ 0, 2, 5, 5, 1, 0 ] })
      chai.assert.deepEqual(res, { i: [ 2, 1 ] })
    });
  });

  describe('$bit', function () {
    it('{ i: { and: 10 }}', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 13 },
        operator: { "$bit": { i: { and: 10 }  } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 13 })
      chai.assert.deepEqual(res, { i: 8 })
    });
    it('{ i: { or: 5 }}', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 3 },
        operator: { "$bit": { i: { or: 5 }  } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 3 })
      chai.assert.deepEqual(res, { i: 7 })
    });
    it('{ i: { xor: 5 }}', function () {
      let req = {
        type: "update",
        target: 1,
        before: { i: 1 },
        operator: { "$bit": { i: { xor: 5 }  } }
      }
      let res = TransactionRequest.applyOperator(req);
      chai.assert.deepEqual(req.before, { i: 1 })
      chai.assert.deepEqual(res, { i: 4 })
    });
  });
});