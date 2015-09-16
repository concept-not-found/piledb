'use strict';

const redisContract = require('../../contract/redis');
const FakeRedis = require('../../fake/redis');

redisContract('fake redis', new FakeRedis());