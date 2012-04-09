package.path = "../src/?.lua;src/?.lua;" .. package.path
pcall(require, "luarocks.require")

local redis = require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local client = redis.connect(params)
client:select(15) -- for testing purposes

local replies = client:pipeline(function(p)
    p:ping()
    p:flushdb()
    p:exists('counter')
    p:incrby('counter', 10)
    p:incrby('counter', 30)
    p:exists('counter')
    p:get('counter')
    p:mset({ foo = 'bar', hoge = 'piyo'})
    p:del('foo', 'hoge')
    p:mget('does_not_exist', 'counter')
    p:info()
end)

for _, reply in pairs(replies) do
    print('*', reply)
end
