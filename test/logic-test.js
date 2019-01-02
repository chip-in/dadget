var chai = require('chai')
var { LogicalOperator } = require('../lib/util/LogicalOperator')
var parser = require('mongo-parse')

describe('LogicalOperator', () => {
  it('pattern 1', () => {
    let query1 = { "a": { "$ne": 2 }, "b": 2 }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { $and: [{ "a": { "$ne": 2 } }, { "b": 2 }] })
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
    chai.assert.deepEqual(result, { $and: [{ "a": 2 }, { "b": 2 }] })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 4', () => {
    let query1 = { "a": 2 }
    let query2 = { "a": 2, "b": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { $and: [{ "a": 2 }, { "b": 2 }] })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "a": 2 }, { "b": { "$ne": 2 } }] })
  });

  it('pattern 5', () => {
    let query1 = { "a": { "$ne": 2 } }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": { "$ne": 2 } })
  });

  it('pattern 6', () => {
    let query1 = { "a": 2 }
    let query2 = { "$or": [{ "a": 2 }, { "a": 3 }] }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 2 })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 7', () => {
    let query1 = { "a": { "$gt": 10 } }
    let query2 = { "a": { "$gte": 0 } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": { "$gt": 10 } })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 8', () => {
    let query1 = { "a": { "$gt": 10 } }
    let query2 = { "a": { "$gt": 10 } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": { "$gt": 10 } })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 9', () => {
    let query1 = { "a": { "$lte": 10 } }
    let query2 = { "a": { "$gte": 10 } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "a": { "$lte": 10 } }, { "a": { "$gte": 10 } }] })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "a": { "$lte": 10 } }, { "a": { "$not": { "$gte": 10 } } }] })
  });

  it('pattern 10', () => {
    let query1 = { "a": { "$lt": 10 } }
    let query2 = { "a": { "$lt": 0 } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": { "$lt": 0 } })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "a": { "$lt": 10 } }, { "a": { "$not": { "$lt": 0 } } }] })
  });

  it('pattern 11', () => {
    let query1 = { "$and": [{ "a": { "$lt": 10 } }, { "a": { "$not": { "$lt": 0 } } }] }
    let query2 = { "a": { "$gte": 0 } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "a": { "$lt": 10 } }, { "a": { "$not": { "$lt": 0 } } }, { "a": { "$gte": 0 } }] })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 12', () => {
    let query1 = { "a": 10 }
    let query2 = { "a": { "$eq": 10 } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 10 })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 13', () => {
    let query1 = { "a": 10 }
    let query2 = { "a": { "$not": { "$eq": 10 } } }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "a": 10 })
  });

  it('pattern 14', () => {
    let query1 = { "a": 10 }
    let query2 = {
      "category": {
        "$in": ["EvacuationOrder", "Shelter", "TemporaryStayFacilities"]
      },
      "distributionStatus": {
        "$in": ["Actual", "Exercise"]
      },
      "areaCode": {
        "$regex": "^(11|13|00000|99999)"
      },
      "overwritten": null,
      "expired": {
        "$gt": "2017-06-15T08:47:43.664Z"
      }
    }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "category": { "$in": ["EvacuationOrder", "Shelter", "TemporaryStayFacilities"] } }, { "distributionStatus": { "$in": ["Actual", "Exercise"] } }, { "areaCode": { "$regex": "^(11|13|00000|99999)" } }, { "overwritten": null }, { "expired": { "$gt": "2017-06-15T08:47:43.664Z" } }, { "a": 10 }] })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    //    chai.assert.deepEqual(result, { "a": 10 })
  });

  it('pattern 16', () => {
    let query1 = { "category": "EvacuationOrder" }
    let query2 = {
      "category": {
        "$in": ["EvacuationOrder", "Shelter", "TemporaryStayFacilities"]
      }
    }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "category": "EvacuationOrder" })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)
  });

  it('pattern 17', () => {
    let query1 = {
      "category": {
        "$in": ["EvacuationOrder", "Shelter1"]
      }
    }
    let query2 = {
      "category": {
        "$in": ["EvacuationOrder", "Shelter2", "TemporaryStayFacilities"]
      }
    }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "category": { "$in": ["EvacuationOrder"] } })

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { "$and": [{ "category": { "$in": ["EvacuationOrder", "Shelter1"] } }, { "category": { "$nin": ["EvacuationOrder", "Shelter2", "TemporaryStayFacilities"] } }] })
  });

  it('pattern 18', () => {
    let query1 = { "a": { "$ne": 2 }, "b": 2, "c": { $elemMatch: { "d": 3 } } }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { $and: [{ "a": { "$ne": 2 } }, { "b": 2 }, { "c": { $elemMatch: { "d": 3 } } }] })
  });

  it('pattern 19', () => {
    let query1 = { "a": { "$ne": 2 }, "b": 2, "c": { "a": { "d": 3 } } }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { $and: [{ "a": { "$ne": 2 } }, { "b": 2 }, { "c": { "a": { "d": 3 } } }] })
  });

  it('pattern 20', () => {
    let query1 = { "a": { "$ne": 2 }, "b": 2, "c": { "$and": [{ "d": 3 }, { "e": 4 }] } }
    let query2 = { "a": 2 }

    let result = LogicalOperator.getInsideOfCache(query1, query2)
    chai.assert.deepEqual(result, undefined)

    result = LogicalOperator.getOutsideOfCache(query1, query2)
    chai.assert.deepEqual(result, { $and: [{ "a": { "$ne": 2 } }, { "b": 2 }, { "c": { "$and": [{ "d": 3 }, { "e": 4 }] } }] })
  });
});

describe('mongo-parse', () => {
  it('parser 1', () => {
    let query = { "a": { "$regex": "^(11|13|00000|99999)" } }
    let documents = [{ a: "1111" }, { a: "54334" }]

    let result = parser.search(documents, query)

    chai.assert.deepEqual(result, [{ a: "1111" }])
  });

  it('parser 2', () => {
    let query = { "a": { "$not": { "$regex": "^(11|13|00000|99999)" } } }
    let documents = [{ a: "1111" }, { a: "54334" }]

    let result = parser.search(documents, query)

    chai.assert.deepEqual(result, [{ a: "54334" }])
  });
});
