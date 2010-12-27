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

--check-and-set
local replies = redis:check_and_set("somekey", function(t)
    --executed after WATCH but before MULTI
    local val = t:get("somekey")
    coroutine.yield()
    --executing during MUTI block
    t:set("somekey", val .. "suffix")
end)
--alternate form
local val
local replies = redis:check_and_set("somekey", function(t)
    val = t:get("somekey")
end, function(t)
    t:set("somekey", val)
end)

for _, reply in pairs(replies) do
    print('*', reply)
end
