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
  var values = this.storage[key];

  // need to translate redis start/end to slice start/end
  // redis end is inclusive, but slice end is exclusive
  // redis also allows negative indices to mean negative wrap around
  // for example, given an array of length 2
  // redis -> slice
  //  0  0 -> 0 1
  //  0  1 -> 0 2
  //  1  1 -> 1 2
  // -1 -1 -> 1 2
  // -2 -1 -> 0 2

  var redisStartToSliceStart = (values.length - start) % values.length;
  var redisEndToSliceEnd = ((values.length - end) % values.length) + 1;

  return callback(undefined, values.slice(redisStartToSliceStart, redisEndToSliceEnd));
};

FakeRedis.prototype.LPUSH = function(key, value, callback) {
  if (!this.storage[key]) {
    this.storage[key] = [];
  }

  this.storage[key].push(value);
  return callback();
};

module.exports = FakeRedis;
