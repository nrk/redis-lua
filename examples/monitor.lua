package.path = "../src/?.lua;src/?.lua;" .. package.path
pcall(require, "luarocks.require")

local redis = require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local client = redis.connect(params)
client:select(15) -- for testing purposes

-- Start processing the monitor messages. Open a terminal and use redis-cli to
-- send some commands to the server that will make MONITOR return some entries.

local counter = 0
for msg, abort in client:monitor_messages() do
    counter = counter + 1

    local feedback = string.format("[%d] Received %s on database %d", msg.timestamp, msg.command, msg.database)
    if msg.arguments then
        feedback = string.format('%s with arguments %s', feedback, msg.arguments)
    end

    print(feedback)

    if counter == 5 then
        abort()
    end
end

print(string.format("Closed the MONITOR context after receiving %d commands.", counter))
