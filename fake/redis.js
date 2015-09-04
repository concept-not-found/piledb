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


module.exports = FakeRedis;
