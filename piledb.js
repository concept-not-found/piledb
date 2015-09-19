'use strict';

const packageJson = require('./package');
const semver = require('semver');
const promisify = require('es6-promisify');

if (!semver.satisfies(process.version, packageJson.engines.node)) {
  throw new Error('Requires a node version matching ' + packageJson.engines.node);
}

class PileClient {
  constructor(redisClient, namespace) {
    this.namespace = namespace || 'piledb';
    this.promiseRedisClient = {
      SETNX: promisify(redisClient.SETNX.bind(redisClient)),
      GET: promisify(redisClient.GET.bind(redisClient)),
      RPUSH: promisify(redisClient.RPUSH.bind(redisClient)),
      LRANGE: promisify(redisClient.LRANGE.bind(redisClient)),
      EXISTS: promisify(redisClient.EXISTS.bind(redisClient)),
      DEL: promisify(redisClient.DEL.bind(redisClient))
    };
  }

  internalKey(key) {
    return `${this.namespace}:${key}`;
  }

  dataKey(key) {
    return this.internalKey(`data:${key}`);
  }

  referenceKey(key) {
    return this.internalKey(`reference:${key}`);
  }

  redactionKey() {
    return this.internalKey('redaction');
  }

  putData(key, value) {
    return this.promiseRedisClient.SETNX(this.dataKey(key), value)
        .then(function(keyWasSet) {
          if (!keyWasSet) {
            throw new AlreadySetError(key);
          }
        });
  }

  getData(key) {
    var _this = this;
    return this.promiseRedisClient.GET(this.dataKey(key))
        .then(function(value) {
          if (!value) {
            return _this.getRedactions()
                .then(function(redactions) {
                  for (var i = 0; i < redactions.length; i++) {
                    if (redactions[i].key === key) {
                      throw new RedactedDataError(redactions[i]);
                    }
                  }
                  throw new NotFoundError(key);
                });
          } else {
            return value;
          }
        });
  }

  addReference(name, key) {
    return this.promiseRedisClient.RPUSH(this.referenceKey(name), key);
  }

  getLastReference(name) {
    return this.promiseRedisClient.LRANGE(this.referenceKey(name), -1, -1)
        .then(function(latest) {
          if (!latest || latest.length === 0) {
            throw new NotFoundError(name);
          }
          return latest[0];
        });
  }

  getReferenceHistory(name) {
    return this.promiseRedisClient.LRANGE(this.referenceKey(name), 0, -1);
  }

  redactData(key, reason) {
    var _this = this;
    return this.promiseRedisClient.EXISTS(this.dataKey(key))
        .then(function(keyExists) {
          if (!keyExists) {
            throw new NotFoundError(key);
          }

          var redactionLog = {
            key: key,
            reason: reason
          };
          return _this.promiseRedisClient.RPUSH(_this.redactionKey(), redactionLog)
              .then(function() {

                return _this.promiseRedisClient.DEL(_this.dataKey(key))
                    .catch(function(err) {
                      throw new Error('failed to delete redacted data.  left dirty redaction log: ' + err.message);
                    });
              })
              .catch(function(err) {
                throw new Error('failed to log redaction, data not deleted: ' + err.message);
              });
        });
  }

  getRedactions() {
    return this.promiseRedisClient.LRANGE(this.redactionKey(), 0, -1);
  }
}

class AlreadySetError extends Error {
  constructor(key) {
    super();
    this.name = this.constructor.name;
    this.stack = (new Error()).stack;
    this.message = key + ' was already set';
  }
}

class NotFoundError extends Error {
  constructor(key) {
    super();
    this.name = this.constructor.name;
    this.stack = (new Error()).stack;
    this.message = key + ' was not set';
  }
}

class RedactedDataError extends Error {
  constructor(redaction) {
    super();
    this.name = this.constructor.name;
    this.stack = (new Error()).stack;
    this.message = redaction.key + ' was redacted: ' + redaction.reason;
  }
}

module.exports = {
  PileClient,
  AlreadySetError,
  NotFoundError,
  RedactedDataError
};