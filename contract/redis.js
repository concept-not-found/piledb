'use strict';

const async = require('async');
const expect = require('chai').expect;

function redisContract(name, redisClient) {
  const prefix = `redis-contract:${new Date().getTime()}:`;
  describe(`${name} contract`, () => {
    describe('key value', () => {
      it('should GET key value after SETNX', (done) => {
        const flow = [];
        flow.push((callback) => {
          redisClient.SETNX(`${prefix}fred`, 'yogurt', (err, keyWasSet) => {
            expect(err).to.be.not.ok;
            expect(keyWasSet).to.equal(1);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.GET(`${prefix}fred`, (err, value) => {
            expect(err).to.be.not.ok;
            expect(value).to.equal('yogurt');
            return callback();
          });
        });
        async.series(flow, done);
      });

      it('should fail using SETNX twice', (done) => {
        const flow = [];
        flow.push((callback) => {
          redisClient.SETNX(`${prefix}bob`, 'pizza', (err, keyWasSet) => {
            expect(err).to.be.not.ok;
            expect(keyWasSet).to.equal(1);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.SETNX(`${prefix}bob`, 'pizza', (err, keyWasSet) => {
            expect(err).to.be.not.ok;
            expect(keyWasSet).to.equal(0);
            return callback();
          });
        });
        async.series(flow, done);
      });

      it('should EXIST once SETNX', (done) => {
        const flow = [];
        flow.push((callback) => {
          redisClient.SETNX(`${prefix}alice`, 'cupcakes', (err, keyWasSet) => {
            expect(err).to.be.not.ok;
            expect(keyWasSet).to.equal(1);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.EXISTS(`${prefix}alice`, (err, keyExists) => {
            expect(err).to.be.not.ok;
            expect(keyExists).to.equal(1);
            return callback();
          });
        });
        async.series(flow, done);
      });

      it('should not EXIST by default', (done) => {
        redisClient.EXISTS(`${prefix}eva`, (err, keyExists) => {
          expect(err).to.be.not.ok;
          expect(keyExists).to.equal(0);
          done();
        });
      });

      it('should GET undefined by default', (done) => {
        redisClient.GET(`${prefix}eva`, (err, keyExists) => {
          expect(err).to.be.not.ok;
          expect(keyExists).to.be.null;
          done();
        });
      });

      it('should not EXIST after DEL', (done) => {
        const flow = [];
        flow.push((callback) => {
          redisClient.SETNX(`${prefix}betty`, 'fried chicken', (err, keyWasSet) => {
            expect(err).to.be.not.ok;
            expect(keyWasSet).to.equal(1);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.DEL(`${prefix}betty`, (err) => {
            expect(err).to.be.not.ok;
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.EXISTS(`${prefix}betty`, (err, keyExists) => {
            expect(err).to.be.not.ok;
            expect(keyExists).to.equal(0);
            return callback();
          });
        });
        async.series(flow, done);
      });
    });

    describe('list', () => {
      it('should be empty by default', (done) => {
        redisClient.LRANGE(`${prefix}cadet`, 0, -1, (err, cadets) => {
          expect(err).to.be.not.ok;
          expect(cadets).to.eql([]);
          done();
        });
      });

      it('should contain what was RPUSH', (done) => {
        const flow = [];
        flow.push((callback) => {
          redisClient.RPUSH(`${prefix}captain`, 'fred', (err) => {
            expect(err).to.be.not.ok;
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.LRANGE(`${prefix}captain`, 0, -1, (err, captains) => {
            expect(err).to.be.not.ok;
            expect(captains).to.eql(['fred']);
            return callback();
          });
        });
        async.series(flow, done);
      });

      it('should RPUSH to the end', (done) => {
        const flow = [];
        flow.push((callback) => {
          redisClient.RPUSH(`${prefix}cook`, 'sam', (err) => {
            expect(err).to.be.not.ok;
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.RPUSH(`${prefix}cook`, 'tammy', (err) => {
            expect(err).to.be.not.ok;
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.LRANGE(`${prefix}cook`, 0, -1, (err, cooks) => {
            expect(err).to.be.not.ok;
            expect(cooks).to.eql(['sam', 'tammy']);
            return callback();
          });
        });
        async.series(flow, done);
      });

      it('should LRANGE elements', (done) => {
        const flow = [];
        flow.push((callback) => {
          redisClient.RPUSH(`${prefix}deckhand`, 'john', (err) => {
            expect(err).to.be.not.ok;
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.RPUSH(`${prefix}deckhand`, 'james', (err) => {
            expect(err).to.be.not.ok;
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.LRANGE(`${prefix}deckhand`, 0, 0, (err, deckhands) => {
            expect(err).to.be.not.ok;
            expect(deckhands).to.eql(['john']);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.LRANGE(`${prefix}deckhand`, 1, 1, (err, deckhands) => {
            expect(err).to.be.not.ok;
            expect(deckhands).to.eql(['james']);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.LRANGE(`${prefix}deckhand`, -1, -1, (err, deckhands) => {
            expect(err).to.be.not.ok;
            expect(deckhands).to.eql(['james']);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.LRANGE(`${prefix}deckhand`, -2, -2, (err, deckhands) => {
            expect(err).to.be.not.ok;
            expect(deckhands).to.eql(['john']);
            return callback();
          });
        });
        flow.push((callback) => {
          redisClient.LRANGE(`${prefix}deckhand`, 3, 3, (err, deckhands) => {
            expect(err).to.be.not.ok;
            expect(deckhands).to.eql([]);
            return callback();
          });
        });
        async.series(flow, done);
      });
    });
  });
}

module.exports = redisContract;
