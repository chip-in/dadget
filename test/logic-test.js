var chai = require('chai')
var { LogicalOperator } = require('../lib/util/LogicalOperator')

describe('LogicalOperator', () => {
  it('pattern 1', () => {
    let query1 = { "a": { "$not": 2 }, "b": 2 }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": { "$not": 2 }, "b": 2 })
  });

  it('pattern 2', () => {
    let query1 = { "a": 2 }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 2 })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 3', () => {
    let query1 = { "a": 2, "b": 2 }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 2, "b": 2 })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 4', () => {
    let query1 = { "a": 2 }
    let query2 = { "a": 2, "b": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 2 })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "a": 2 }, { "b": { "$not": 2 } }] })
  });

  it('pattern 5', () => {
    let query1 = { "a": { "$not": 2 } }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": { "$not": 2 } })
  });

  it('pattern 6', () => {
    let query1 = { "a": 2 }
    let query2 = { "$or": [{ "a": 2 }, { "a": 3 }] }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"a":2})

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 7', () => {
    let query1 = { "a": {"$gt" : 10} }
    let query2 = { "a": {"$gt" : 0} }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"$and":[{"a":{"$gt":10}},{"a":{"$gt":0}}]})

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 8', () => {
    let query1 = { "a": {"$gt" : 10} }
    let query2 = { "a": {"$gt" : 10} }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"a":{"$gt":10}})

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 9', () => {
    let query1 = { "a": {"$lte" : 10} }
    let query2 = { "a": {"$gte" : 10} }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"$and":[{"a":{"$lte":10}},{"a":{"$gte":10}}]})

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"$and":[{"a":{"$lte":10}},{"a":{"$not":{"$gte":10}}}]})
  });

  it('pattern 10', () => {
    let query1 = { "a": {"$lt" : 10} }
    let query2 = { "a": {"$lt" : 0} }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"$and":[{"a":{"$lt":10}},{"a":{"$lt":0}}]})

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"$and":[{"a":{"$lt":10}},{"a":{"$not":{"$lt":0}}}]})
  });

  it('pattern 11', () => {
    let query1 = {"$and":[{"a":{"$lt":10}},{"a":{"$not":{"$lt":0}}}]}
    let query2 = { "a": {"$gte" : 0} }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, {"$and":[{"a":{"$lt":10}},{"a":{"$not":{"$lt":0}}},{"a":{"$gte":0}}]})

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 12', () => {
    let query1 = { "a": 10 }
    let query2 = { "a": {"$eq" : 10} }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 10 })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 13', () => {
    let query1 = { "a": 10 }
    let query2 = { "a": {"$not": {"$eq" : 10} } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 10 })
  });
});