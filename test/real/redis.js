'use strict';

var redisContract = require('../../contract/redis');
var redis = require('redis');

redisContract('real redis', redis.createClient());