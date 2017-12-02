var chai = require('chai')
var EJSON = require('mongodb-extended-json')
var Ejson = require('../lib/util/Ejson')

describe('EJSON', function () {
  it('stringify', function () {
    let a = {
      "s": "string",
      "n": 9999,
      datetime: new Date()
    }
    chai.assert.deepEqual(Ejson.stringify(a), EJSON.stringify(a))
  });

  it('parse', function () {
    let a = {
      "s": "string",
      "n": 9999,
      datetime: new Date()
    }
    let s = EJSON.stringify(a)
    console.log(s)
    chai.assert.deepEqual(Ejson.parse(s), EJSON.parse(s))
  });
});