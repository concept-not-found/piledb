'use strict';

const redisContract = require('../../contract/redis');
const redis = require('redis');

redisContract('real redis', redis.createClient());