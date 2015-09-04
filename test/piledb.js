var PileClient = require('../piledb');
var FakeRedis = require('../fake/redis');
var expect = require('chai').expect;

describe('put data', function () {
  it('should only put a key once', function (done) {
    var db = new PileClient(new FakeRedis());

    db.putData('fred', 'yogurt', function(err) {
      expect(err).to.be.undefined;
      db.putData('fred', 'yogurt', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    })
  });
});