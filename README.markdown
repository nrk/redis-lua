# redis-lua #

## About ##

redis-lua is a pure Lua client library for the Redis advanced key-value database.

## Main features ##

- Support for Redis >= 1.2
- Command pipelining
- User-definable commands

## Examples of usage ##

### Include redis-lua in your script ###

    require 'redis'

### Connect to a redis-server instance and send a PING command ###

    local redis = Redis.connect('127.0.0.1', 6379)
    local response = redis:ping()           -- true

### Set keys and get their values ###

    redis:set('usr:nrk', 10)
    redis:set('usr:nobody', 5)
    local value = redis:get('usr:nrk')      -- 10

### Sort list values by using various parameters supported by the server ###

    for _,v in ipairs({ 10,3,2,6,1,4,23 }) do
        redis:rpush('usr:nrk:ids',v)
    end

    local sorted = redis:sort('usr:nrk:ids', {
         sort = 'asc', alpha = true, limit = { 1, 5 }
    })      -- {1=10,2=2,3=23,4=3,5=4}

### Pipeline commands

    local replies = redis:pipeline(function(p)
        p:incrby('counter', 10)
        p:incrby('counter', 30)
        p:get('counter')
    end)

### Leverage Redis MULTI / EXEC transaction (Redis > 2.0)

    local replies = redis:transaction(function(t)
        t:incrby('counter', 10)
        t:incrby('counter', 30)
        t:get('counter')
    end)

### Get useful information from the server ###

    for k,v in pairs(redis:info()) do 
        print(k .. ' => ' .. tostring(v))
    end
    --[[
    redis_git_dirty => 0
    redis_git_sha1 => aaed0894
    process_id => 23115
    vm_enabled => 0
    hash_max_zipmap_entries => 64
    expired_keys => 9
    changes_since_last_save => 2
    role => master
    last_save_time => 1283621624
    used_memory => 537204
    bgsave_in_progress => 0
    redis_version => 2.0.0
    multiplexing_api => epoll
    total_connections_received => 314
    db0 => {keys=3,expires=0}
    pubsub_patterns => 0
    used_memory_human => 524.61K
    pubsub_channels => 0
    uptime_in_seconds => 1033
    connected_slaves => 0
    connected_clients => 1
    bgrewriteaof_in_progress => 0
    blocked_clients => 0
    arch_bits => 32
    total_commands_processed => 3982
    hash_max_zipmap_value => 512
    db15 => {keys=1,expires=0}
    uptime_in_days => 0
    ]]

## Dependencies ##

- [Lua 5.1](http://www.lua.org/) or [LuaJIT 2.0](http://luajit.org/)
- [LuaSocket 2.0](http://www.tecgraf.puc-rio.br/~diego/professional/luasocket/)
- [Telescope](http://telescope.luaforge.net/) (required to run the test suite)

## Links ##

### Project ###
- [Source code](http://github.com/nrk/redis-lua/)
- [Issue tracker](http://github.com/nrk/redis-lua/issues)

### Related ###
- [Redis](http://code.google.com/p/redis/)
- [Git](http://git-scm.com/)

## Authors ##

[Daniele Alessandri](mailto:suppakilla@gmail.com)

## License ##

The code for redis-lua is distributed under the terms of the MIT/X11 license (see LICENSE).
