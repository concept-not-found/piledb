'use strict';

const promisifyRedisClient = require('../library/promisifyRedisClient');
const expect = require('chai').expect;

function redisContract(implementationName, redisClient) {
  const prefix = `redis-contract:${new Date().getTime()}`;
  function getKey(name) {
    return `${prefix}:${name}`;
  }

  const promiseRedisClient = promisifyRedisClient(redisClient);

  describe(`${implementationName} contract`, () => {
    describe('key value', () => {
      it('should GET key value after SETNX', function *() {
        const keyWasSet = yield promiseRedisClient.SETNX(getKey('fred'), 'yogurt');
        expect(keyWasSet).to.equal(1);

        const value = yield promiseRedisClient.GET(getKey('fred'));
        expect(value).to.equal('yogurt');
      });

      it('should fail using SETNX twice', function *() {
        const firstKeyWasSet = yield promiseRedisClient.SETNX(getKey('bob'), 'pizza');
        expect(firstKeyWasSet).to.equal(1);

        const secondKeyWasSet = yield promiseRedisClient.SETNX(getKey('bob'), 'pizza');
        expect(secondKeyWasSet).to.equal(0);
      });

      it('should EXIST once SETNX', function *() {
        const keyWasSet = yield promiseRedisClient.SETNX(getKey('alice'), 'cupcakes');
        expect(keyWasSet).to.equal(1);

        const keyExists = yield promiseRedisClient.EXISTS(getKey('alice'));
        expect(keyExists).to.equal(1);
      });

      it('should not EXIST by default', function *() {
        const keyExists = yield promiseRedisClient.EXISTS(getKey('eva'));
        expect(keyExists).to.equal(0);
      });

      it('should GET undefined by default', function *() {
        const keyExists = yield promiseRedisClient.GET(getKey('eva'));
        expect(keyExists).to.be.null;
      });

      it('should not EXIST after DEL', function *() {
        const keyWasSet = yield promiseRedisClient.SETNX(getKey('betty'), 'fried chicken');
        expect(keyWasSet).to.equal(1);

        yield promiseRedisClient.DEL(getKey('betty'));

        const keyExists = yield promiseRedisClient.EXISTS(getKey('betty'));
        expect(keyExists).to.equal(0);
      });
    });

    describe('list', () => {
      it('should be empty by default', function *() {
        const cadets = yield promiseRedisClient.LRANGE(getKey('cadet'), 0, -1);
        expect(cadets).to.eql([]);
      });

      it('should contain what was RPUSH', function *() {
        yield promiseRedisClient.RPUSH(getKey('captain'), 'fred');

        const captains = yield promiseRedisClient.LRANGE(getKey('captain'), 0, -1);
        expect(captains).to.eql(['fred']);
      });

      it('should RPUSH to the end', function *() {
        yield promiseRedisClient.RPUSH(getKey('cook'), 'sam');
        yield promiseRedisClient.RPUSH(getKey('cook'), 'tammy');
        const cooks = yield promiseRedisClient.LRANGE(getKey('cook'), 0, -1);
        expect(cooks).to.eql(['sam', 'tammy']);
      });

      it('should LRANGE elements', function *() {
        yield promiseRedisClient.RPUSH(getKey('deckhand'), 'john');
        yield promiseRedisClient.RPUSH(getKey('deckhand'), 'james');

        const firstDeckhand = yield promiseRedisClient.LRANGE(getKey('deckhand'), 0, 0);
        expect(firstDeckhand).to.eql(['john']);

        const secondDeckhand = yield promiseRedisClient.LRANGE(getKey('deckhand'), 1, 1);
        expect(secondDeckhand).to.eql(['james']);

        const lastDeckhand = yield promiseRedisClient.LRANGE(getKey('deckhand'), -1, -1);
        expect(lastDeckhand).to.eql(['james']);

        const secondLastDeckhand = yield promiseRedisClient.LRANGE(getKey('deckhand'), -2, -2);
        expect(secondLastDeckhand).to.eql(['john']);

        const thirdDeckhand = yield promiseRedisClient.LRANGE(getKey('deckhand'), 3, 3);
        expect(thirdDeckhand).to.eql([]);
      });
    });
  });
}

module.exports = redisContract;
