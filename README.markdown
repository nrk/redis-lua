# redis-lua #

## About ##

redis-lua is a pure Lua client library for the Redis advanced key-value database.

## Main features ##

- Support for Redis up to version 1.2
- Command pipelining (experimental)

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
        redis:push_tail('usr:nrk:ids',v)
    end

    local sorted = redis:sort('usr:nrk:ids', {
         sort = 'asc', alpha = true, limit = { 1, 5 }
    })      -- {1=10,2=2,3=23,4=3,5=4}

### Get useful information from the server ###

    for k,v in pairs(redis:info()) do 
        print(k .. ' => ' .. tostring(v))
    end
    --[[
    changes_since_last_save => 8
    role => master
    last_save_time => 1275138868
    used_memory => 4735076
    bgsave_in_progress => 0
    redis_version => 1.2.6
    multiplexing_api => epoll
    used_memory_human => 4.52M
    uptime_in_seconds => 3451
    connected_slaves => 0
    connected_clients => 1
    bgrewriteaof_in_progress => 0
    db10 => table: 0x864e020
    arch_bits => 32
    total_commands_processed => 601082
    db0 => table: 0x8652030
    total_connections_received => 20
    uptime_in_days => 0
    ]]

## Dependencies ##

- [Lua 5.1](http://www.lua.org/)
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

The code for redis-lua is distributed under the terms of the MIT license (see LICENSE).
