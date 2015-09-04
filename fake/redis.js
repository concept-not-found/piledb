function FakeRedis() {
  this.storage = {};
}

FakeRedis.prototype.SETNX = function(key, value, callback) {
  if (this.storage[key]) {
    return callback(undefined, false);
  }
  this.storage[key] = value;
  return callback(undefined, true);
};

FakeRedis.prototype.GET = function(key, callback) {
  return callback(undefined, this.storage[key]);
};

FakeRedis.prototype.LRANGE = function(key, start, end, callback) {
  if (!this.storage[key]) {
    return callback(undefined, []);
  }
  return callback(undefined, this.storage[key].slice(start, end));
};

module.exports = FakeRedis;
