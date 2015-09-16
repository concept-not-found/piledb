'use strict';

const packageJson = require('./package');
const semver = require('semver');

if(!semver.satisfies(process.version, packageJson.engines.node)) {
  throw new Error('Requires a node version matching ' + pkg.engines.node);
}

class PileClient {
  constructor(redisClient, namespace) {
    this.redisClient = redisClient;
    this.namespace = namespace || 'piledb';
  }

  internalKey(key) {
    return this.namespace + ':' + key;
  }

  dataKey(key) {
    return this.internalKey('data:' + key);
  }

  referenceKey(key) {
    return this.internalKey('reference:' + key);
  }

  redactionKey() {
    return this.internalKey('redaction');
  }

  putData(key, value, callback) {
    this.redisClient.SETNX(this.dataKey(key), value, function(err, keyWasSet) {
      if (err) {
        return callback(err);
      }
      if (!keyWasSet) {
        return callback(new AlreadySetError(key));
      }
      return callback();
    });
  }

  getData(key, callback) {
    var _this = this;
    this.redisClient.GET(this.dataKey(key), function(err, value) {
      if (err) {
        return callback(err);
      }
      if (!value) {
        _this.getRedactions(function(err, redactions) {
          if (err) {
            return callback(err);
          }
          for (var i = 0; i < redactions.length; i++) {
            if (redactions[i].key === key) {
              return callback(new RedactedDataError(redactions[i]));
            }
          }
          return callback(new NotFoundError(key));
        });
      } else {
        return callback(undefined, value);
      }
    });
  }

  addReference(name, key, callback) {
    return this.redisClient.RPUSH(this.referenceKey(name), key, callback);
  }

  getLastReference(name, callback) {
    this.redisClient.LRANGE(this.referenceKey(name), -1, -1, function(err, latest) {
      if (err) {
        return callback(err);
      }
      if (!latest || latest.length === 0) {
        return callback(new NotFoundError(name));
      }
      return callback(undefined, latest[0]);
    });
  }

  getReferenceHistory(name, callback) {
    return this.redisClient.LRANGE(this.referenceKey(name), 0, -1, callback);
  }

  redactData(key, reason, callback) {
    var _this = this;
    this.redisClient.EXISTS(this.dataKey(key), function(err, keyExists) {
      if (err) {
        return callback(err);
      }
      if (!keyExists) {
        return callback(new NotFoundError(key));
      }

      var redactionLog = {
        key: key,
        reason: reason
      };
      _this.redisClient.RPUSH(_this.redactionKey(), redactionLog, function(err) {
        if (err) {
          return callback(new Error('failed to log redaction, data not deleted: ' + err.message));
        }
        return _this.redisClient.DEL(_this.dataKey(key), function(err) {
          if (err) {
            return callback(new Error('failed to delete redacted data.  left dirty redaction log: ' + err.message));
          }
          return callback();
        });
      });
    });
  }

  getRedactions(callback) {
    return this.redisClient.LRANGE(this.redactionKey(), 0, -1, callback);
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