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

for _, reply in pairs(replies) do
    print('*', reply)
end
