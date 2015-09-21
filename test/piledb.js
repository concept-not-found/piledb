'use strict';

const PileClient = require('../piledb').PileClient;
const AlreadySetError = require('../piledb').AlreadySetError;
const NotFoundError = require('../piledb').NotFoundError;
const RedactedDataError = require('../piledb').RedactedDataError;
const FakeRedis = require('../fake/redis');
const expect = require('chai').expect;

describe('pile client', () => {
  describe('put data', () => {
    it('should put a value for a key', () => {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.putData('fred', 'yogurt')
          .then(() => {
            expect(fakeRedis.storage).to.include.keys('piledb:data:fred');
            expect(fakeRedis.storage['piledb:data:fred']).to.equal('yogurt');
          });
    });

    it('should only put a key once', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.putData('fred', 'ice cream')
          .catch((err) =>
              expect(err).to.be.an.instanceof(AlreadySetError));
    });
  });

  describe('get data', () => {
    it('should get a value for a key that exists', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.getData('fred')
          .then((value) => {
            expect(value).to.equal('yogurt');
          });
    });

    it('should fail for a key that does not exists', () => {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getData('fred')
          .catch((err) =>
            expect(err).to.be.an.instanceof(NotFoundError));
    });

    it('should fail for a key that has been redacted', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:redaction'] = [
        {
          key: 'fred',
          reason: 'court order 156'
        }
      ];
      const db = new PileClient(fakeRedis);

      return db.getData('fred')
          .catch((err) =>
            expect(err).to.be.an.instanceof(RedactedDataError));
    });
  });

  describe('add reference', () => {
    it('should add a key for a new name', () => {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.addReference('captain', 'fred')
          .then(() => {
            expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
            expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred']);
          });
    });

    it('should add a key for an existing name', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred'];
      const db = new PileClient(fakeRedis);

      return db.addReference('captain', 'bob')
          .then(() => {
            expect(fakeRedis.storage).to.include.keys('piledb:reference:captain');
            expect(fakeRedis.storage['piledb:reference:captain']).to.eql(['fred', 'bob']);
          });
    });
  });

  describe('get last reference', () => {
    it('should get the last key for a name that exists', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      return db.getLastReference('captain')
          .then((key) =>
            expect(key).to.equal('bob'));
    });

    it('should fail when name does not exist', () => {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getLastReference('captain')
          .catch((err) =>
            expect(err).to.be.an.instanceof(NotFoundError));
    });
  });

  describe('get reference history', () => {
    it('should get all keys for a name that exists', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:reference:captain'] = ['fred', 'bob'];
      const db = new PileClient(fakeRedis);

      return db.getReferenceHistory('captain')
          .then((key) =>
            expect(key).to.eql(['fred', 'bob']));
    });

    it('should get empty when name does not exist', () => {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getReferenceHistory('captain')
          .then((key) =>
            expect(key).to.eql([]));
    });
  });

  describe('redact data', () => {
    it('should redact data that exists', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .then(() =>
            expect(fakeRedis.storage).to.not.include.keys('piledb:data:fred'));
    });

    it('should add a redaction when redacting data that exists', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .then(() => {
            expect(fakeRedis.storage).to.include.keys('piledb:redaction');
            expect(fakeRedis.storage['piledb:redaction']).to.eql([{
              key: 'fred',
              reason: 'court order 156'
            }]);
          });
    });

    it('should fail when data does not exist', () => {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .catch((err) =>
            expect(err).to.be.an.instanceof(NotFoundError));
    });

    it('should add additonal information on RPUSH failure', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.RPUSH = function(key, value, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .catch((err) => {
            expect(err).to.be.an.instanceof(Error);
            expect(err.message).to.not.equal('oops');
          });
    });

    it('should add additonal information on DEL failure', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:data:fred'] = 'yogurt';
      fakeRedis.DEL = function(key, callback) {
        return callback(new Error('oops'));
      };
      const db = new PileClient(fakeRedis);

      return db.redactData('fred', 'court order 156')
          .catch((err) => {
            expect(err).to.be.an.instanceof(Error);
            expect(err.message).to.not.equal('oops');
          });
    });
  });

  describe('get redactions', () => {
    it('should get all redactions when there are some', () => {
      const fakeRedis = new FakeRedis();
      fakeRedis.storage['piledb:redaction'] = [{
        key: 'fred',
        reason: 'court order 156'
      }];
      const db = new PileClient(fakeRedis);

      return db.getRedactions()
          .then((redactions) =>
            expect(redactions).to.eql([{
              key: 'fred',
              reason: 'court order 156'
            }]));
    });

    it('should get empty there are no redactions', () => {
      const fakeRedis = new FakeRedis();
      const db = new PileClient(fakeRedis);

      return db.getRedactions()
          .then((redactions) =>
            expect(redactions).to.eql([]));
    });
  });
});
