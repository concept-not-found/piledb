var PileClient = require('../piledb');
var FakeRedis = require('../fake/redis');
var expect = require('chai').expect;

describe('pile client', function() {
  describe('put data', function() {
    it('should put a value for a key', function(done) {
      var fakeRedis = new FakeRedis();
      var db = new PileClient(fakeRedis);

      db.putData('fred', 'yogurt', function(err) {
        expect(err).to.be.undefined;
        expect(fakeRedis.storage).to.include.keys('piledb:data:fred');
        expect(fakeRedis.storage['piledb:data:fred']).to.equal('yogurt');
        done();
      });
    });

    it('should only put a key once', function(done) {
      var fakeRedis = new FakeRedis();
      var db = new PileClient(fakeRedis);

      db.putData('fred', 'yogurt', function(err) {
        expect(err).to.be.undefined;
        db.putData('fred', 'yogurt', function(err) {
          expect(err).to.be.an.instanceof(Error);
          done();
        });
      })
    });

    it('should propagate errors from SETNX', function(done) {
      var alwaysFailingSETNX = {
        SETNX: function(key, value, callback) {
          return callback(new Error('oops'));
        }
      };
      var db = new PileClient(alwaysFailingSETNX);

      db.putData('fred', 'yogurt', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });
  });

  describe('get data', function() {
    it('should get a value for a key that exists', function(done) {
      var fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      var db = new PileClient(fakeRedis);

      db.getData('fred', function(err, value) {
        expect(err).to.be.undefined;
        expect(value).to.equal('yogurt');
        done();
      });
    });

    it('should returns an error for a key that does not exists', function(done) {
      var fakeRedis = new FakeRedis();
      var db = new PileClient(fakeRedis);

      db.getData('fred', function(err, value) {
        expect(err).to.be.an.instanceof(Error);
        expect(value).to.be.undefined;
        done();
      });
    });

    it('should propagate errors from GET', function(done) {
      var alwaysFailingGET = {
        GET: function(key, callback) {
          return callback(new Error('oops'));
        }
      };
      var db = new PileClient(alwaysFailingGET);

      db.getData('fred', function(err, value) {
        expect(err).to.be.an.instanceof(Error);
        expect(value).to.be.undefined;
        done();
      });
    });
  });

  describe('put reference', function() {
    it('should put a key for a name', function(done) {
      var fakeRedis = new FakeRedis();
      var db = new PileClient(fakeRedis);

      db.putReference('captain', 'fred', function(err) {
        expect(err).to.be.undefined;
        expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
        expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred']);
        done();
      });
    });

    it('should propagate errors from LPUSH', function(done) {
      var alwaysFailingLPUSH = {
        LPUSH: function(key, value, callback) {
          return callback(new Error('oops'));
        }
      };
      var db = new PileClient(alwaysFailingLPUSH);

      db.putReference('captain', 'fred', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });
  });
});