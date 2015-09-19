'use strict';

const PileClient = require('../piledb').PileClient;
const AlreadySetError = require('../piledb').AlreadySetError;
const NotFoundError = require('../piledb').NotFoundError;
const RedactedDataError = require('../piledb').RedactedDataError;
const FakeRedis = require('../fake/redis');
const expect = require('chai').expect;

describe('pile client', function() {
  describe('put data', function() {
    it('should put a value for a key', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.putData('fred', 'yogurt')
          .then(function() {
            expect(fakeRedis.storage).to.include.keys('piledb:data:fred');
            expect(fakeRedis.storage['piledb:data:fred']).to.equal('yogurt');
          });
    });

    it('should only put a key once', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.putData('fred', 'ice cream')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(AlreadySetError);
          });
    });

    it('should propagate errors from SETNX', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.SETNX = function(key, value, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.putData('fred', 'yogurt')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });
  });

  describe('get data', function() {
    it('should get a value for a key that exists', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.getData('fred')
          .then(function(value) {
            expect(value).to.equal('yogurt');
          });
    });

    it('should fail for a key that does not exists', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getData('fred')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(NotFoundError);
          });
    });

    it('should fail for a key that has been redacted', function() {
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

      return db.getData('fred')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(RedactedDataError);
          });
    });

    it('should propagate errors when failing to get redactions', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);
      db.getRedactions = function() {
        return Promise.reject(new Error('oops'));
      };

      return db.getData('fred')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });

    it('should propagate errors from GET', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.GET = function(key, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.getData('fred')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });
  });

  describe('add reference', function() {
    it('should add a key for a new name', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.addReference('captain', 'fred')
          .then(function() {
            expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
            expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred']);
          });
    });

    it('should add a key for an existing name', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred'];
      const db = new PileClient(fakeRedis);

      return db.addReference('captain', 'bob')
          .then(function() {
            expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
            expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred', 'bob']);
          });
    });

    it('should propagate errors from RPUSH', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.RPUSH = function(key, value, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.addReference('captain', 'fred')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });
  });

  describe('get last reference', function() {
    it('should get the last key for a name that exists', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      return db.getLastReference('captain')
          .then(function(key) {
            expect(key).to.equal('bob');
          });
    });

    it('should fail when name does not exist', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getLastReference('captain')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(NotFoundError);
          });
    });

    it('should propagate errors from LRANGE', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.LRANGE= function(key, start, end, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.getLastReference('captain')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });
  });

  describe('get reference history', function() {
    it('should get all keys for a name that exists', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      return db.getReferenceHistory('captain')
          .then(function(key) {
            expect(key).to.eql(['fred', 'bob']);
          });
    });

    it('should get empty when name does not exist', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getReferenceHistory('captain')
          .then(function(key) {
            expect(key).to.eql([]);
          });
    });

    it('should propagate errors from LRANGE', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.LRANGE = function(key, start, end, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.getReferenceHistory('captain')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });
  });

  describe('redact data', function() {
    it('should redact data that exists', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .then(function() {
            expect(fakeRedis.storage).to.not.include.keys('piledb:data:fred');
          });
    });

    it('should add a redaction when redacting data that exists', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .then(function() {
            expect(fakeRedis.storage).to.include.keys('piledb:redaction');
            expect(fakeRedis.storage['piledb:redaction']).to.eql([{
              key: 'fred',
              reason: 'court order 156'
            }]);
          });
    });

    it('should fail when data does not exist', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(NotFoundError);
          });
    });

    it('should propagate errors from EXISTS', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.EXISTS = function(key, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });

    it('should propagate errors from RPUSH', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.RPUSH = function(key, value, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });

    it('should propagate errors from DEL', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.DEL = function(key, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });
  });

  describe('get redactions', function() {
    it('should get all redactions when there are some', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:redaction'] = [{
        key: 'fred',
        reason: 'court order 156'
      }];
      const db = new PileClient(fakeRedis);

      return db.getRedactions()
          .then(function(redactions) {
            expect(redactions).to.eql([{
              key: 'fred',
              reason: 'court order 156'
            }]);
          });
    });

    it('should get empty there are no redactions', function() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getRedactions()
          .then(function(redactions) {
            expect(redactions).to.eql([]);
          });
    });

    it('should propagate errors from LRANGE', function() {
      const fakeRedis = new FakeRedis();
      fakeRedis.LRANGE = function(key, start, end, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.getRedactions()
          .catch(function(err) {
            expect(err).to.be.an.instanceof(Error);
          });
    });
  });
});