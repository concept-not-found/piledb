function PileClient(redisClient, namespace) {
  this.redisClient = redisClient;
  this.namespace = namespace || 'piledb';
}

PileClient.prototype.internalKey = function(key) {
  return this.namespace + ':' + key;
};

PileClient.prototype.dataKey = function(key) {
  return this.internalKey('data:' + key);
};

PileClient.prototype.referenceKey = function(key) {
  return this.internalKey('reference:' + key);
};

PileClient.prototype.redactionKey = function() {
  return this.internalKey('redaction');
};

PileClient.prototype.putData = function(key, value, callback) {
  this.redisClient.SETNX(this.dataKey(key), value, function(err, keyWasSet) {
    if (err) {
      return callback(err);
    }
    if (!keyWasSet) {
      return callback(new Error(key + ' was already set'));
    }
    return callback();
  });
};

PileClient.prototype.getData = function(key, callback) {
  var _this = this;
  var dataKey = this.dataKey(key);
  this.redisClient.GET(dataKey, function(err, value) {
    if (err) {
      return callback(err);
    }
    if (!value) {
      _this.getRedactions(function(err, redactions) {
        if (err) {
          return callback(err);
        }
        for (var i = 0; i < redactions.length; i++) {
          if (redactions[i].key === dataKey) {
            return callback(new Error(key + ' was redacted: ' + redactions[i].reason));
          }
        }
        return callback(new Error(key + ' was not set'));
      });
    } else {
      return callback(undefined, value);
    }
  });
};

PileClient.prototype.addReference = function(name, key, callback) {
  return this.redisClient.LPUSH(this.referenceKey(name), key, callback);
};

PileClient.prototype.getLastReference = function(name, callback) {
  this.redisClient.LRANGE(this.referenceKey(name), -1, -1, function(err, latest) {
    if (err) {
      return callback(err);
    }
    if (!latest || latest.length === 0) {
      return callback(new Error(name + ' was not set'));
    }
    return callback(undefined, latest[0]);
  });
};

PileClient.prototype.getReferenceHistory = function(name, callback) {
  return this.redisClient.LRANGE(this.referenceKey(name), 0, -1, callback);
};

PileClient.prototype.redactData = function(key, reason, callback) {
  var _this = this;
  this.redisClient.EXISTS(this.dataKey(key), function(err, keyExists) {
    if (err) {
      return callback(err);
    }
    if (!keyExists) {
      return callback(new Error(key + ' does not exist'));
    }

    var redactionLog = {
      key: key,
      reason: reason
    };
    _this.redisClient.LPUSH(_this.redactionKey(), redactionLog, function(err) {
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
};

PileClient.prototype.getRedactions = function(callback) {
  return this.redisClient.LRANGE(this.redactionKey(), 0, -1, callback);
};

module.exports = PileClient;