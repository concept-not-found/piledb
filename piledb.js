'use strict';

const packageJson = require('./package');
const semver = require('semver');
const promisify = require('es6-promisify');
const _ = require('lodash');

/* istanbul ignore if */
if (!semver.satisfies(process.version, packageJson.engines.node)) {
  throw new Error(`Requires a node version matching ${packageJson.engines.node}`);
}

function promisifyMethod(instance, methodName) {
  return promisify(instance[methodName].bind(instance));
}

class PileClient {
  constructor(redisClient, namespace) {
    this.namespace = namespace || 'piledb';
    this.promiseRedisClient = {
      SETNX: promisifyMethod(redisClient, 'SETNX'),
      GET: promisifyMethod(redisClient, 'GET'),
      RPUSH: promisifyMethod(redisClient, 'RPUSH'),
      LRANGE: promisifyMethod(redisClient, 'LRANGE'),
      EXISTS: promisifyMethod(redisClient, 'EXISTS'),
      DEL: promisifyMethod(redisClient, 'DEL')
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
        .then((keyWasSet) => {
          if (!keyWasSet) {
            throw new AlreadySetError(key);
          }
        });
  }

  getData(key) {
    return this.promiseRedisClient.GET(this.dataKey(key))
        .then((value) => {
          return _.find([
            value,
            this.getRedactions()
                .then((redactions) => {
                  const foundRedaction = _.find(redactions, {
                    key
                  });
                  if (foundRedaction) {
                    throw new RedactedDataError(foundRedaction);
                  }
                  throw new NotFoundError(key);
                })
          ]);
        });
  }

  addReference(name, key) {
    return this.promiseRedisClient.RPUSH(this.referenceKey(name), key);
  }

  getLastReference(name) {
    return this.promiseRedisClient.LRANGE(this.referenceKey(name), -1, -1)
        .then((latest) => {
          if (!latest || _.isEmpty(latest)) {
            throw new NotFoundError(name);
          }
          return _.first(latest);
        });
  }

  getReferenceHistory(name) {
    return this.promiseRedisClient.LRANGE(this.referenceKey(name), 0, -1);
  }

  redactData(key, reason) {
    return this.promiseRedisClient.EXISTS(this.dataKey(key))
        .then((keyExists) => {
          if (!keyExists) {
            throw new NotFoundError(key);
          }

          const redactionLog = {
            key,
            reason
          };
          return this.promiseRedisClient.RPUSH(this.redactionKey(), redactionLog)
              .catch((err) => {
                throw new Error(`failed to log redaction, data not deleted: ${err.message}`);
              });
        })
        .then(() => {
          return this.promiseRedisClient.DEL(this.dataKey(key))
              .catch((err) => {
                throw new Error(`failed to delete redacted data.  left dirty redaction log: ${err.message}`);
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
    this.message = `${key} was already set`;
  }
}

class NotFoundError extends Error {
  constructor(key) {
    super();
    this.name = this.constructor.name;
    this.stack = (new Error()).stack;
    this.message = `${key} was not set`;
  }
}

class RedactedDataError extends Error {
  constructor(redaction) {
    super();
    this.name = this.constructor.name;
    this.stack = (new Error()).stack;
    this.message = `${redaction.key} was redacted: ${redaction.reason}`;
  }
}

module.exports = {
  PileClient,
  AlreadySetError,
  NotFoundError,
  RedactedDataError
};
