'use strict';

var expect = require('chai').expect;

function redisContract(name, redisClient) {
  var prefix = 'redis-contract:' + new Date().getTime() + ':';
  describe(name + ' contract', function() {
    describe('key value', function() {
      it('should GET key value after SETNX', function(done) {
        redisClient.SETNX(prefix + 'fred', 'yogurt', function(err, keyWasSet) {
          expect(err).to.be.not.ok;
          expect(keyWasSet).to.equal(1);
          redisClient.GET(prefix + 'fred', function(err, value) {
            expect(err).to.be.not.ok;
            expect(value).to.equal('yogurt');
            done();
          });
        });
      });

      it('should fail using SETNX twice', function(done) {
        redisClient.SETNX(prefix + 'bob', 'pizza', function(err, keyWasSet) {
          expect(err).to.be.not.ok;
          expect(keyWasSet).to.equal(1);
          redisClient.SETNX(prefix + 'bob', 'pizza', function(err, keyWasSet) {
            expect(err).to.be.not.ok;
            expect(keyWasSet).to.equal(0);
            done();
          });
        });
      });

      it('should EXIST once SETNX', function(done) {
        redisClient.SETNX(prefix + 'alice', 'cupcakes', function(err, keyWasSet) {
          expect(err).to.be.not.ok;
          expect(keyWasSet).to.equal(1);
          redisClient.EXISTS(prefix + 'alice', function(err, keyExists) {
            expect(err).to.be.not.ok;
            expect(keyExists).to.equal(1);
            done();
          });
        })
      });

      it('should not EXIST by default', function(done) {
        redisClient.EXISTS(prefix + 'eva', function(err, keyExists) {
          expect(err).to.be.not.ok;
          expect(keyExists).to.equal(0);
          done();
        });
      });

      it('should GET undefined by default', function(done) {
        redisClient.GET(prefix + 'eva', function(err, keyExists) {
          expect(err).to.be.not.ok;
          expect(keyExists).to.be.null;
          done();
        });
      });

      it('should not EXIST after DEL', function(done) {
        redisClient.SETNX(prefix + 'betty', 'fried chicken', function(err, keyWasSet) {
          expect(err).to.be.not.ok;
          expect(keyWasSet).to.equal(1);
          redisClient.DEL(prefix + 'betty', function(err) {
            expect(err).to.be.not.ok;
            redisClient.EXISTS(prefix + 'betty', function(err, keyExists) {
              expect(err).to.be.not.ok;
              expect(keyExists).to.equal(0);
              done();
            });
          });
        });
      });
    });

    describe('list', function() {
      it('should be empty by default', function(done) {
        redisClient.LRANGE(prefix + 'cadet', 0, -1, function(err, cadets) {
          expect(err).to.be.not.ok;
          expect(cadets).to.eql([]);
          done();
        });
      });

      it('should contain what was RPUSH', function(done) {
        redisClient.RPUSH(prefix + 'captain', 'fred', function(err) {
          expect(err).to.be.not.ok;
          redisClient.LRANGE(prefix + 'captain', 0, -1, function(err, captains) {
            expect(err).to.be.not.ok;
            expect(captains).to.eql(['fred']);
            done();
          });
        });
      });

      it('should RPUSH to the end', function(done) {
        redisClient.RPUSH(prefix + 'cook', 'sam', function(err) {
          expect(err).to.be.not.ok;
          redisClient.RPUSH(prefix + 'cook', 'tammy', function(err) {
            expect(err).to.be.not.ok;
            redisClient.LRANGE(prefix + 'cook', 0, -1, function(err, cooks) {
              expect(err).to.be.not.ok;
              expect(cooks).to.eql(['sam', 'tammy']);
              done();
            });
          });
        });
      });

      it('should LRANGE elements', function(done) {
        redisClient.RPUSH(prefix + 'deckhand', 'john', function(err) {
          expect(err).to.be.not.ok;
          redisClient.RPUSH(prefix + 'deckhand', 'james', function(err) {
            expect(err).to.be.not.ok;
            redisClient.LRANGE(prefix + 'deckhand', 0, 0, function(err, deckhands) {
              expect(err).to.be.not.ok;
              expect(deckhands).to.eql(['john']);
              redisClient.LRANGE(prefix + 'deckhand', 1, 1, function(err, deckhands) {
                expect(err).to.be.not.ok;
                expect(deckhands).to.eql(['james']);
                redisClient.LRANGE(prefix + 'deckhand', -1, -1, function(err, deckhands) {
                  expect(err).to.be.not.ok;
                  expect(deckhands).to.eql(['james']);
                  redisClient.LRANGE(prefix + 'deckhand', -2, -2, function(err, deckhands) {
                    expect(err).to.be.not.ok;
                    expect(deckhands).to.eql(['john']);
                    redisClient.LRANGE(prefix + 'deckhand', 3, 3, function(err, deckhands) {
                      expect(err).to.be.not.ok;
                      expect(deckhands).to.eql([]);
                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
}

module.exports = redisContract;
