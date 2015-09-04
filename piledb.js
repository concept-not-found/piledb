function PileClient(redisClient, namespace) {
  this.redisClient = redisClient;
  this.namespace = namespace || 'PileClient';
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
    callback();
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
    }
    callback(undefined, value);
  });
};

PileClient.prototype.putReference = function(name, key, callback) {
  this.redisClient.LPUSH(this.referenceKey(name), key, callback);
};

PileClient.prototype.getReference = function(name, callback) {
  this.redisClient.LRANGE(this.referenceKey(name), -1, -1, function(err, latest) {
    if (err) {
      return callback(err);
    }
    if (!latest) {
      return callback(new Error(name + ' was not set'));
    }
    return callback(undefined, latest[0]);
  });
};

PileClient.prototype.getReferenceHistory = function(name, callback) {
  this.redisClient.LRANGE(this.referenceKey(name), 0, -1, callback);
};

PileClient.prototype.redactData = function(key, reason, callback) {
  var _this = this;
  var redactionLog = {
    key: key,
    reason: reason
  };
  this.redisClient.LPUSH(this.redactionKey(), redactionLog, function(err) {
    if (err) {
      return callback(err);
    }
    _this.redisClient.SET(_this.dataKey(key), undefined, callback);
  });
};

PileClient.prototype.getRedactions = function(callback) {
  this.redisClient.LRANGE(this.redactionKey(), 0, -1, callback);
};

module.exports = PileClient;