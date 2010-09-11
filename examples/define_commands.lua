package.path = package.path .. ";../src/?.lua"

require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

-- new commands are defined as multibulk by default
Redis.define_command('hset')

-- you can also specify a response callback to parse raw replies
Redis.define_command('hgetall', {
    response = function(reply, command, ...)
        local new_reply = { }
        for i = 1, #reply, 2 do new_reply[reply[i]] = reply[i + 1] end
        return new_reply
    end
})

local redis = Redis.connect(params)
redis:select(15) -- for testing purposes

redis:hset('user:1000', 'name', 'John Doe')
redis:hset('user:1000', 'nickname', 'anonymous')
redis:hset('user:1000', 'email', 'anything@anywhere.tld')

local user = redis:hgetall('user:1000')
print(string.format('%s is also known as %s and his email address is %s.',
    user.name, user.nickname, user.email
))

--[[
OUTPUT:
John Doe is also known as anonymous and his email address is anything@anywhere.tld.
]]
