package.path = package.path .. ";../src/?.lua"

require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local redis = Redis.connect(params)
redis:select(15) -- for testing purposes

local replies = redis:pipeline(function()
    ping()
    flushdb()
    exists('counter')
    incrby('counter', 10)
    incrby('counter', 30)
    exists('counter')
    get('counter')
    mset({ foo = 'bar', hoge = 'piyo'})
    del('foo', 'hoge')
    mget('does_not_exist', 'counter')
    info()
end)

for _, reply in pairs(replies) do
    print('*', reply)
end
