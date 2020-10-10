var chai = require('chai')
var { Util } = require('../lib/util/Util')

describe('Util', function () {
  describe('project', function () {
    let obj = {
      _id: "dd",
      a: 1,
      b: 2,
      c: 3,
    }
    it('{ }', function () {
      chai.assert.deepEqual(Util.project(obj, { }), { _id: "dd", a: 1, b: 2, c: 3 })
    });
    it('{ _id: 0 }', function () {
      chai.assert.deepEqual(Util.project(obj, { _id: 0 }), { a: 1, b: 2, c: 3 })
    });
    it('{ _id: 1 }', function () {
      chai.assert.deepEqual(Util.project(obj, { _id: 1 }), { _id: "dd"})
    });
    it('{ a: 0 }', function () {
      chai.assert.deepEqual(Util.project(obj, { a: 0 }), { _id: "dd", b: 2, c: 3 })
    });
    it('{ a: 1 }', function () {
      chai.assert.deepEqual(Util.project(obj, { a: 1 }), { _id: "dd", a: 1 })
    });
    it('{ _id: 0, a: 0 }', function () {
      chai.assert.deepEqual(Util.project(obj, { _id: 0,  a: 0 }), { b: 2, c: 3 })
    });
    it('{ _id: 0, a: 1 }', function () {
      chai.assert.deepEqual(Util.project(obj, { _id: 0,  a: 1 }), { a: 1 })
    });
    it('{ _id: 1, a: 0 }', function () {
      // Projection cannot have a mix of inclusion and exclusion.
    });
    it('{ _id: 1, a: 1 }', function () {
      chai.assert.deepEqual(Util.project(obj, { _id: 1,  a: 1 }), { _id: "dd", a: 1 })
    });

    it('{ a: 0, b: 0 }', function () {
      chai.assert.deepEqual(Util.project(obj, { a: 0,  b: 0 }), { _id: "dd", c: 3 })
    });
    it('{ a: 0, b: 1 }', function () {
      // Projection cannot have a mix of inclusion and exclusion.
    });
    it('{ a: 1, b: 0 }', function () {
      // Projection cannot have a mix of inclusion and exclusion.
    });
    it('{ a: 1, b: 1 }', function () {
      chai.assert.deepEqual(Util.project(obj, { a: 1,  b: 1 }), { _id: "dd", a: 1, b: 2 })
    });
  });
});
