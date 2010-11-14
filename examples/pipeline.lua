package.path = package.path .. ";../src/?.lua"

require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local redis = Redis.connect(params)
redis:select(15) -- for testing purposes

local replies = redis:pipeline(function(p)
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
