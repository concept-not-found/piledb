'use strict';

const _ = require('lodash');
const promisify = require('es6-promisify');

function promisifyRedisClient(redisClient) {
  const methods = [
    'SETNX',
    'GET',
    'RPUSH',
    'LRANGE',
    'EXISTS',
    'DEL'
  ];
  return _.zipObject(methods, _.map(methods, (method) =>
    promisify(redisClient[method].bind(redisClient))));
}

module.exports = promisifyRedisClient;
