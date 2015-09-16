'use strict';

const PileClient = require('../piledb').PileClient;
const AlreadySetError = require('../piledb').AlreadySetError;
const NotFoundError = require('../piledb').NotFoundError;
const RedactedDataError = require('../piledb').RedactedDataError;
const FakeRedis = require('../fake/redis');
const expect = require('chai').expect;

describe('pile client', function() {
  describe('put data', function() {
    it('should put a value for a key', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      db.putData('fred', 'yogurt', function(err) {
        expect(err).to.be.undefined;
        expect(fakeRedis.storage).to.include.keys('piledb:data:fred');
        expect(fakeRedis.storage['piledb:data:fred']).to.equal('yogurt');
        done();
      });
    });

    it('should only put a key once', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      db.putData('fred', 'ice cream', function(err) {
        expect(err).to.be.an.instanceof(AlreadySetError);
        done();
      });
    });

    it('should propagate errors from SETNX', function(done) {
      const alwaysFailingSETNX = {
        SETNX: function(key, value, callback) {
          return callback(new Error('oops'));
        }
      };
      const db = new PileClient(alwaysFailingSETNX);

      db.putData('fred', 'yogurt', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });
  });

  describe('get data', function() {
    it('should get a value for a key that exists', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      db.getData('fred', function(err, value) {
        expect(err).to.be.undefined;
        expect(value).to.equal('yogurt');
        done();
      });
    });

    it('should fail for a key that does not exists', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      db.getData('fred', function(err, value) {
        expect(err).to.be.an.instanceof(NotFoundError);
        expect(value).to.be.undefined;
        done();
      });
    });

    it('should fail for a key that has been redacted', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:redaction'] = [
        {
          key: 'bob',
          reason: 'court order 638'
        },
        {
          key: 'fred',
          reason: 'court order 156'
        }
      ];
      const db = new PileClient(fakeRedis);

      db.getData('fred', function(err, value) {
        expect(err).to.be.an.instanceof(RedactedDataError);
        expect(value).to.be.undefined;
        done();
      });
    });

    it('should propagate errors when failing to get redactions', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);
      db.getRedactions = function(callback) {
        return callback(new Error('oops'));
      };

      db.getData('fred', function(err, value) {
        expect(err).to.be.an.instanceof(Error);
        expect(value).to.be.undefined;
        done();
      });
    });

    it('should propagate errors from GET', function(done) {
      const alwaysFailingGET = {
        GET: function(key, callback) {
          return callback(new Error('oops'));
        }
      };
      const db = new PileClient(alwaysFailingGET);

      db.getData('fred', function(err, value) {
        expect(err).to.be.an.instanceof(Error);
        expect(value).to.be.undefined;
        done();
      });
    });
  });

  describe('add reference', function() {
    it('should add a key for a new name', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      db.addReference('captain', 'fred', function(err) {
        expect(err).to.be.undefined;
        expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
        expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred']);
        done();
      });
    });

    it('should add a key for an existing name', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred'];
      const db = new PileClient(fakeRedis);

      db.addReference('captain', 'bob', function(err) {
        expect(err).to.be.undefined;
        expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
        expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred', 'bob']);
        done();
      });
    });

    it('should propagate errors from RPUSH', function(done) {
      const alwaysFailingRPUSH = {
        RPUSH: function(key, value, callback) {
          return callback(new Error('oops'));
        }
      };
      const db = new PileClient(alwaysFailingRPUSH);

      db.addReference('captain', 'fred', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });
  });

  describe('get last reference', function() {
    it('should get the last key for a name that exists', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      db.getLastReference('captain', function(err, key) {
        expect(err).to.be.undefined;
        expect(key).to.equal('bob');
        done();
      });
    });

    it('should fail when name does not exist', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      db.getLastReference('captain', function(err, key) {
        expect(err).to.be.an.instanceof(NotFoundError);
        expect(key).to.be.undefined;
        done();
      });
    });

    it('should propagate errors from LRANGE', function(done) {
      const alwaysFailingLRANGE = {
        LRANGE: function(key, start, end, callback) {
          return callback(new Error('oops'));
        }
      };
      const db = new PileClient(alwaysFailingLRANGE);

      db.getLastReference('captain', function(err, key) {
        expect(err).to.be.an.instanceof(Error);
        expect(key).to.be.undefined;
        done();
      });
    });
  });

  describe('get reference history', function() {
    it('should get all keys for a name that exists', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      db.getReferenceHistory('captain', function(err, key) {
        expect(err).to.be.undefined;
        expect(key).to.eql(['fred', 'bob']);
        done();
      });
    });

    it('should get empty when name does not exist', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      db.getReferenceHistory('captain', function(err, key) {
        expect(err).to.be.undefined;
        expect(key).to.eql([]);
        done();
      });
    });

    it('should propagate errors from LRANGE', function(done) {
      const alwaysFailingLRANGE = {
        LRANGE: function(key, start, end, callback) {
          return callback(new Error('oops'));
        }
      };
      const db = new PileClient(alwaysFailingLRANGE);

      db.getReferenceHistory('captain', function(err, key) {
        expect(err).to.be.an.instanceof(Error);
        expect(key).to.be.undefined;
        done();
      });
    });
  });

  describe('redact data', function() {
    it('should redact data that exists', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      db.redactData('fred', 'court order 156', function(err) {
        expect(err).to.be.undefined;
        expect(fakeRedis.storage).to.not.include.keys('piledb:data:fred');
        done();
      });
    });

    it('should add a redaction when redacting data that exists', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      db.redactData('fred', 'court order 156', function(err) {
        expect(err).to.be.undefined;
        expect(fakeRedis.storage).to.include.keys('piledb:redaction');
        expect(fakeRedis.storage['piledb:redaction']).to.eql([{
          key: 'fred',
          reason: 'court order 156'
        }]);
        done();
      });
    });

    it('should fail when data does not exist', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      db.redactData('fred', 'court order 156', function(err) {
        expect(err).to.be.an.instanceof(NotFoundError);
        done();
      });
    });

    it('should propagate errors from EXISTS', function(done) {
      const alwaysFailingEXISTS = {
        EXISTS: function(key, callback) {
          return callback(new Error('oops'));
        }
      };
      const db = new PileClient(alwaysFailingEXISTS);

      db.redactData('fred', 'court order 156', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });

    it('should propagate errors from RPUSH', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.RPUSH = function(key, value, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      db.redactData('fred', 'court order 156', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });

    it('should propagate errors from DEL', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.DEL = function(key, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      db.redactData('fred', 'court order 156', function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });
  });

  describe('get redactions', function() {
    it('should get all redactions when there are some', function(done) {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:redaction'] = [{
        key: 'fred',
        reason: 'court order 156'
      }];
      const db = new PileClient(fakeRedis);

      db.getRedactions(function(err, redactions) {
        expect(err).to.be.undefined;
        expect(redactions).to.eql([{
          key: 'fred',
          reason: 'court order 156'
        }]);
        done();
      });
    });

    it('should get empty there are no redactions', function(done) {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      db.getRedactions(function(err, redactions) {
        expect(err).to.be.undefined;
        expect(redactions).to.eql([]);
        done();
      });
    });

    it('should propagate errors from LRANGE', function(done) {
      const alwaysFailingLRANGE = {
        LRANGE: function(key, start, end, callback) {
          return callback(new Error('oops'));
        }
      };
      const db = new PileClient(alwaysFailingLRANGE);

      db.getRedactions(function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });
  });
});