package.path = "../src/?.lua;src/?.lua;" .. package.path
pcall(require, "luarocks.require")

local redis = require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

-- commands defined in the redis.commands table are available at module
-- level and are used to populate each new client instance.
redis.commands.hset = redis.command('hset')

-- you can also specify a response callback to parse raw replies
redis.commands.hgetall = redis.command('hgetall', {
    response = function(reply, command, ...)
        local new_reply = { }
        for i = 1, #reply, 2 do new_reply[reply[i]] = reply[i + 1] end
        return new_reply
    end
})

local client = redis.connect(params)
client:select(15) -- for testing purposes

client:hset('user:1000', 'name', 'John Doe')
client:hset('user:1000', 'nickname', 'anonymous')
client:hset('user:1000', 'email', 'anything@anywhere.tld')

local user = client:hgetall('user:1000')
print(string.format('%s is also known as %s and his email address is %s.',
    user.name, user.nickname, user.email
))

--[[
OUTPUT:
John Doe is also known as anonymous and his email address is anything@anywhere.tld.
]]
