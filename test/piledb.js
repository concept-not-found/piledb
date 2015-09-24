'use strict';

const PileClient = require('../piledb').PileClient;
const AlreadySetError = require('../piledb').AlreadySetError;
const NotFoundError = require('../piledb').NotFoundError;
const RedactedDataError = require('../piledb').RedactedDataError;
const FakeRedis = require('../fake/redis');
const expect = require('chai').expect;

describe('pile client', () => {
  describe('put data', () => {
    it('should put a value for a key', function *() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      yield db.putData('fred', 'yogurt');
      expect(fakeRedis.storage).to.include.keys('piledb:data:fred');
      expect(fakeRedis.storage['piledb:data:fred']).to.equal('yogurt');
    });

    it('should only put a key once', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      yield db.putData('fred', 'ice cream')
        .catch((err) =>
          expect(err).to.be.an.instanceof(AlreadySetError));
    });
  });

  describe('get data', () => {
    it('should get a value for a key that exists', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      const value = yield db.getData('fred');
      expect(value).to.equal('yogurt');
    });

    it('should fail for a key that does not exists', function *() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      yield db.getData('fred')
        .catch((err) =>
          expect(err).to.be.an.instanceof(NotFoundError));
    });

    it('should fail for a key that has been redacted', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:redaction'] = [
        {
          key: 'fred',
          reason: 'court order 156'
        }
      ];
      const db = new PileClient(fakeRedis);

      yield db.getData('fred')
        .catch((err) =>
          expect(err).to.be.an.instanceof(RedactedDataError));
    });
  });

  describe('add reference', () => {
    it('should add a key for a new name', function *() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      yield db.addReference('captain', 'fred');
      expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
      expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred']);
    });

    it('should add a key for an existing name', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred'];
      const db = new PileClient(fakeRedis);

      yield db.addReference('captain', 'bob');
      expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
      expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred', 'bob']);
    });
  });

  describe('get last reference', () => {
    it('should get the last key for a name that exists', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      const key = yield db.getLastReference('captain');
      expect(key).to.equal('bob');
    });

    it('should fail when name does not exist', function *() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      yield db.getLastReference('captain')
        .catch((err) =>
          expect(err).to.be.an.instanceof(NotFoundError));
    });
  });

  describe('get reference history', () => {
    it('should get all keys for a name that exists', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      const key = yield db.getReferenceHistory('captain');
      expect(key).to.eql(['fred', 'bob']);
    });

    it('should get empty when name does not exist', function *() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      const key = yield db.getReferenceHistory('captain');
      expect(key).to.eql([]);
    });
  });

  describe('redact data', () => {
    it('should redact data that exists', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      yield db.redactData('fred', 'court order 156');
      expect(fakeRedis.storage).to.not.include.keys('piledb:data:fred');
    });

    it('should add a redaction when redacting data that exists', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      yield db.redactData('fred', 'court order 156');
      expect(fakeRedis.storage).to.include.keys('piledb:redaction');
      expect(fakeRedis.storage['piledb:redaction']).to.eql([{
        key: 'fred',
        reason: 'court order 156'
      }]);
    });

    it('should fail when data does not exist', function *() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      yield db.redactData('fred', 'court order 156')
        .catch((err) =>
          expect(err).to.be.an.instanceof(NotFoundError));
    });

    it('should add additonal information on RPUSH failure', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.RPUSH = function(key, value, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      yield db.redactData('fred', 'court order 156')
        .catch((err) => {
          expect(err).to.be.an.instanceof(Error);
          expect(err.message).to.not.equal('oops');
        });
    });

    it('should add additonal information on DEL failure', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.DEL = function(key, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      yield db.redactData('fred', 'court order 156')
        .catch((err) => {
          expect(err).to.be.an.instanceof(Error);
          expect(err.message).to.not.equal('oops');
        });
    });
  });

  describe('get redactions', () => {
    it('should get all redactions when there are some', function *() {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:redaction'] = [{
        key: 'fred',
        reason: 'court order 156'
      }];
      const db = new PileClient(fakeRedis);

      const redactions = yield db.getRedactions();
      expect(redactions).to.eql([{
        key: 'fred',
        reason: 'court order 156'
      }]);
    });

    it('should get empty there are no redactions', function *() {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      const redactions = yield db.getRedactions();
      expect(redactions).to.eql([]);
    });
  });
});
