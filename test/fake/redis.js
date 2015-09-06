'use strict';

var redisContract = require('../../contract/redis');
var FakeRedis = require('../../fake/redis');

redisContract('fake redis', new FakeRedis());