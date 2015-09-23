'use strict';

const packageJson = require('./package');
const semver = require('semver');
const promisify = require('es6-promisify');
const _ = require('lodash');
const co = require('co');

/* istanbul ignore if */
if (!semver.satisfies(process.version, packageJson.engines.node)) {
  throw new Error(`Requires a node version matching ${packageJson.engines.node}`);
}

class PileClient {
  internalKey(key) {
    return `${this.namespace}:${key}`;
  }

  constructor(redisClient, namespace) {
    this.namespace = namespace || 'piledb';
    const methods = [
      'SETNX',
      'GET',
      'RPUSH',
      'LRANGE',
      'EXISTS',
      'DEL'
    ];
    this.promiseRedisClient = _.zipObject(methods, _.map(methods, (method) =>
        promisify(redisClient[method].bind(redisClient))));
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
    return co(function *() {
      const keyWasSet = yield this.promiseRedisClient.SETNX(this.dataKey(key), value);
      if (!keyWasSet) {
        throw new AlreadySetError(key);
      }
    }.bind(this));
  }

  getData(key) {
    return co(function *() {
      const value = yield this.promiseRedisClient.GET(this.dataKey(key));
      if (value) {
        return value;
      }
      const redactions = yield this.getRedactions();
      const foundRedaction = _.find(redactions, {
        key
      });
      if (foundRedaction) {
        throw new RedactedDataError(foundRedaction);
      }
      throw new NotFoundError(key);
    }.bind(this));
  }

  addReference(name, key) {
    return this.promiseRedisClient.RPUSH(this.referenceKey(name), key);
  }

  getLastReference(name) {
    return co(this.getLastReference2(name));
  }

  *getLastReference2(name) {
    const latest = yield this.promiseRedisClient.LRANGE(this.referenceKey(name), -1, -1);
    if (!latest || _.isEmpty(latest)) {
      throw new NotFoundError(name);
    }
    return _.first(latest);
  }

  getReferenceHistory(name) {
    return this.promiseRedisClient.LRANGE(this.referenceKey(name), 0, -1);
  }

  redactData(key, reason) {
    return co(function *() {
      const keyExists = yield this.promiseRedisClient.EXISTS(this.dataKey(key));
      if (!keyExists) {
        throw new NotFoundError(key);
      }

      const redactionLog = {
        key,
        reason
      };
      yield this.promiseRedisClient.RPUSH(this.redactionKey(), redactionLog)
        .catch((err) => {
          throw new Error(`failed to log redaction, data not deleted: ${err.message}`);
        });
      yield this.promiseRedisClient.DEL(this.dataKey(key))
        .catch((err) => {
          throw new Error(`failed to delete redacted data.  left dirty redaction log: ${err.message}`);
        });
    }.bind(this));
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
