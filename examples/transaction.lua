package.path = package.path .. ";../src/?.lua"

require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local redis = Redis.connect(params)
redis:select(15) -- for testing purposes

local replies = redis:transaction(function(t)
    t:incrby('counter', 10)
    t:incrby('counter', 30)
    t:decrby('counter', 15)
end)

-- check-and-set (CAS)
redis:set('foo', 'bar')
local replies = redis:transaction({ watch = 'foo', cas = true }, function(t)
    --executed after WATCH but before MULTI
    local val = t:get('foo')
    t:multi()
    --executing during MULTI block
    t:set('foo', 'foo' .. val)
    t:get('foo')
end)

for _, reply in pairs(replies) do
    print('*', reply)
end
