'use strict';

var expect = require('chai').expect;

function redisContract(name, redisClient) {
  describe(name + ' contract', function() {
    describe('key value', function() {
      it('should GET key value after SETNX', function(done) {
        redisClient.SETNX('fred', 'yogurt', function(err, keyWasSet) {
          expect(err).to.be.undefined;
          expect(keyWasSet).to.be.true;
          redisClient.GET('fred', function(err, value) {
            expect(err).to.be.undefined;
            expect(value).to.equal('yogurt');
            done();
          });
        });
      });

      it('should fail using SETNX twice', function(done) {
        redisClient.SETNX('bob', 'pizza', function(err, keyWasSet) {
          expect(err).to.be.undefined;
          expect(keyWasSet).to.be.true;
          redisClient.SETNX('bob', 'pizza', function(err, keyWasSet) {
            expect(err).to.be.undefined;
            expect(keyWasSet).to.be.false;
            done();
          });
        });
      });

      it('should EXIST once SETNX', function(done) {
        redisClient.SETNX('alice', 'cupcakes', function(err, keyWasSet) {
          expect(err).to.be.undefined;
          expect(keyWasSet).to.be.true;
          redisClient.EXISTS('alice', function(err, keyExists) {
            expect(err).to.be.undefined;
            expect(keyExists).to.equal(1);
            done();
          });
        })
      });

      it('should not EXIST by default', function(done) {
        redisClient.EXISTS('eva', function(err, keyExists) {
          expect(err).to.be.undefined;
          expect(keyExists).to.equal(0);
          done();
        });
      });

      it('should GET undefined by default', function(done) {
        redisClient.GET('eva', function(err, keyExists) {
          expect(err).to.be.undefined;
          expect(keyExists).to.be.undefined;
          done();
        });
      });

      it('should not EXIST after DEL', function(done) {
        redisClient.SETNX('betty', 'fried chicken', function(err, keyWasSet) {
          expect(err).to.be.undefined;
          expect(keyWasSet).to.be.true;
          redisClient.DEL('betty', function(err) {
            expect(err).to.be.undefined;
            redisClient.EXISTS('betty', function(err, keyExists) {
              expect(err).to.be.undefined;
              expect(keyExists).to.equal(0);
              done();
            });
          });
        });
      });
    });

    describe('list', function() {
      it('should be empty by default', function(done) {
        redisClient.LRANGE('cadet', 0, -1, function(err, cadets) {
          expect(err).to.be.undefined;
          expect(cadets).to.eql([]);
          done();
        });
      });

      it('should contain what was LPUSH', function(done) {
        redisClient.LPUSH('captain', 'fred', function(err) {
          expect(err).to.be.undefined;
          redisClient.LRANGE('captain', 0, -1, function(err, captains) {
            expect(err).to.be.undefined;
            expect(captains).to.eql(['fred']);
            done();
          });
        });
      });

      it('should LPUSH to the end', function(done) {
        redisClient.LPUSH('cook', 'sam', function(err) {
          expect(err).to.be.undefined;
          redisClient.LPUSH('cook', 'tammy', function(err) {
            expect(err).to.be.undefined;
            redisClient.LRANGE('cook', 0, -1, function(err, cooks) {
              expect(err).to.be.undefined;
              expect(cooks).to.eql(['sam', 'tammy']);
              done();
            });
          });
        });
      });

      it('should LRANGE elements', function(done) {
        redisClient.LPUSH('deckhand', 'john', function(err) {
          expect(err).to.be.undefined;
          redisClient.LPUSH('deckhand', 'james', function(err) {
            expect(err).to.be.undefined;
            redisClient.LRANGE('deckhand', 0, 0, function(err, deckhands) {
              expect(err).to.be.undefined;
              expect(deckhands).to.eql(['john']);
              redisClient.LRANGE('deckhand', 1, 1, function(err, deckhands) {
                expect(err).to.be.undefined;
                expect(deckhands).to.eql(['james']);
                redisClient.LRANGE('deckhand', -1, -1, function(err, deckhands) {
                  expect(err).to.be.undefined;
                  expect(deckhands).to.eql(['james']);
                  redisClient.LRANGE('deckhand', -2, -2, function(err, deckhands) {
                    expect(err).to.be.undefined;
                    expect(deckhands).to.eql(['john']);
                    redisClient.LRANGE('deckhand', 3, 3, function(err, deckhands) {
                      expect(err).to.be.undefined;
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
