package.path = package.path .. ";../?.lua"

require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local redis = Redis.connect(params)
redis:select_database(15) -- for testing purposes

redis:set('foo', 'bar')
local value = redis:get('foo')

print(value)
