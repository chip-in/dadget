var chai = require('chai')
var EJSON = require('mongodb-extended-json')
var Ejson = require('../lib/util/Ejson')
const fs = require("fs");

describe('EJSON', function () {
  it('stringify', function () {
    let a = {
      "s": "string",
      "n": 9999,
      datetime: new Date(),
      datetime2: [1, new Date()],
      n: null,
    }
    console.log(Ejson.stringify(a))
    chai.assert.deepEqual(Ejson.stringify(a), EJSON.stringify(a))
  });

  it('parse', function () {
    let a = {
      "s": "string",
      "n": 9999,
      datetime: new Date(),
      datetime2: [1, new Date()],
      n: null,
    }
    let s = EJSON.stringify(a)
    console.log(s)
    chai.assert.deepEqual(Ejson.parse(s), EJSON.parse(s))
  });

  it('undefined', function () {
    let a = {
      u: undefined
    }
    let b = {
      u: null
    }
    let s = EJSON.stringify(a)
    console.log(s)
    chai.assert.deepEqual(Ejson.parse(s), b)
  });

  it('bench', function () {
    let a = fs.readFileSync("generated.json", 'utf-8');
    let time = Date.now();
    let b = JSON.parse(a);
    console.log("JSON.parse", Date.now() - time);
    time = Date.now();

    let b2 = Ejson.parse(a);
    console.log("Ejson.parse", Date.now() - time);
    time = Date.now();

    JSON.stringify(b);
    console.log("JSON.stringify", Date.now() - time);
    time = Date.now();

    Ejson.stringify(b);
    console.log("Ejson.stringify", Date.now() - time);
    time = Date.now();
  });

  it('async', () => {
    let a = fs.readFileSync("generated.json", 'utf-8');
    let org = JSON.parse(a);
    org[99].d = {d: [new Date()], t: new Date()};
    org[100].d = {d: [new Date()], t: new Date()};
    org[101].d = {d: [new Date()], t: new Date()};
    org[102].d = {d: [new Date()], t: new Date()};
    const start = new Date()
    let interval = setInterval(() => {
        const interval = new Date()
        console.log(`interval: ${interval - start}`)
    }, 1)

    let time = Date.now();
    return Promise.resolve(org)
      .then((a) => {
        return Ejson.asyncStringify(a)
      })
      .then((b) => {
        console.log("Ejson.asyncStringify", Date.now() - time);
        time = Date.now();
        return Ejson.asyncParse(b)
      })
      .then((r) => {
        console.log("Ejson.asyncParse", Date.now() - time);
        clearInterval(interval);
        return chai.assert.deepEqual(org, r);
      })
  });
});