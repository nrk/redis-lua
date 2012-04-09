package.path = "../src/?.lua;src/?.lua;" .. package.path
pcall(require, "luarocks.require")

local redis = require 'redis'

local params = {
    host = '127.0.0.1',
    port = 6379,
}

local client = redis.connect(params)
client:select(15) -- for testing purposes

local channels = { 'control_channel', 'notifications' }

-- Start processing the pubsup messages. Open a terminal and use redis-cli
-- to push messages to the channels. Examples:
--   ./redis-cli PUBLISH notifications "this is a test"
--   ./redis-cli PUBLISH control_channel quit_loop

for msg, abort in client:pubsub({ subscribe = channels }) do
    if msg.kind == 'subscribe' then
        print('Subscribed to channel '..msg.channel)
    elseif msg.kind == 'message' then
        if msg.channel == 'control_channel' then
            if msg.payload == 'quit_loop' then
                print('Aborting pubsub loop...')
                abort()
            else
                print('Received an unrecognized command: '..msg.payload)
            end
        else
            print('Received the following message from '..msg.channel.."\n  "..msg.payload.."\n")
        end
    end
end
