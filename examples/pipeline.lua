package.path = package.path .. ";../?.lua"

require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local redis = Redis.connect(params)
redis:select_database(15) -- for testing purposes

local replies = redis:pipeline(function()
    ping()
    flush_database()
    exists('counter')
    increment_by('counter', 10)
    increment_by('counter', 30)
    exists('counter')
    get('counter')
    set_multiple({ foo = 'bar', hoge = 'piyo'})
    delete('foo', 'hoge')
    get_multiple('does_not_exist', 'counter')
    info()
end)

for _, reply in pairs(replies) do
    print('*', reply)
end
