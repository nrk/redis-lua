# redis-lua #

## About ##

redis-lua is a pure Lua client library for the Redis advanced key-value database.

## Main features ##

- Support for Redis >= 1.2
- Command pipelining
- Redis transactions (MULTI/EXEC) with CAS
- User-definable commands
- UNIX domain sockets (when available in LuaSocket)

## Compatibility ##

This library is tested and works with __Lua 5.1__, __Lua 5.2__ (using a compatible
version of LuaSocket) and __LuaJit 2.0__.

## Examples of usage ##

### Include redis-lua in your script ###

Just require the `redis` module assigning it to a variable:

``` lua
local redis = require 'redis'
```

Previous versions of the library defined a global `Redis` alias as soon as the module was
imported by the user. This global alias is still defined but it is considered deprecated
and it will be removed in the next major version.

### Connect to a redis-server instance and send a PING command ###

``` lua
local redis = require 'redis'
local client = redis.connect('127.0.0.1', 6379)
local response = client:ping()           -- true
```

It is also possible to connect to a local redis instance using __UNIX domain sockets__
if LuaSocket has been compiled with them enabled (unfortunately it is not the default):

``` lua
local redis = require 'redis'
local client = redis.connect('unix:///tmp/redis.sock')
```

### Set keys and get their values ###

``` lua
client:set('usr:nrk', 10)
client:set('usr:nobody', 5)
local value = client:get('usr:nrk')      -- 10
```

### Sort list values by using various parameters supported by the server ###

``` lua
for _,v in ipairs({ 10,3,2,6,1,4,23 }) do
    client:rpush('usr:nrk:ids',v)
end

local sorted = client:sort('usr:nrk:ids', {
     sort = 'asc', alpha = true, limit = { 1, 5 }
})      -- {1=10,2=2,3=23,4=3,5=4}
```

### Pipeline commands

``` lua
local replies = client:pipeline(function(p)
    p:incrby('counter', 10)
    p:incrby('counter', 30)
    p:get('counter')
end)
```

### Variadic commands

Some commands such as RPUSH, SADD, SINTER and others have been improved in Redis 2.4
to accept a list of values or keys depending on the nature of the command. Sometimes
it can be useful to pass these arguments as a list in a table, but since redis-lua does
not currently do anything to handle such a case you can use `unpack()` albeit with a
limitation on the maximum number of items which is defined in Lua by LUAI_MAXCSTACK
(the default on Lua 5.1 is set to `8000`, see `luaconf.h`):

```lua
local values = { 'value1', 'value2', 'value3' }
client:rpush('list', unpack(values))

-- the previous line has the same effect of the following one:
client:rpush('list', 'value1', 'value2', 'value3')
```

### Leverage Redis MULTI / EXEC transaction (Redis > 2.0)

``` lua
local replies = client:transaction(function(t)
    t:incrby('counter', 10)
    t:incrby('counter', 30)
    t:get('counter')
end)
```

### Leverage WATCH / MULTI / EXEC for check-and-set (CAS) operations (Redis > 2.2)

``` lua
local options = { watch = "key_to_watch", cas = true, retry = 2 }
local replies = client:transaction(options, function(t)
    local val = t:get("key_to_watch")
    t:multi()
    t:set("akey", val)
    t:set("anotherkey", val)
end)
```

### Get useful information from the server ###

``` lua
for k,v in pairs(client:info()) do
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
```

## Dependencies ##

- [Lua 5.1 and 5.2](http://www.lua.org/) or [LuaJIT 2.0](http://luajit.org/)
- [LuaSocket 2.0](http://www.tecgraf.puc-rio.br/~diego/professional/luasocket/)
- [Telescope](http://telescope.luaforge.net/) (required to run the test suite)

## Links ##

### Project ###
- [Source code](http://github.com/nrk/redis-lua/)
- [Issue tracker](http://github.com/nrk/redis-lua/issues)

### Related ###
- [Redis](http://redis.io/)
- [Git](http://git-scm.com/)

## Authors ##

[Daniele Alessandri](mailto:suppakilla@gmail.com)

### Contributors ###

[Leo Ponomarev](http://github.com/slact/)

## License ##

The code for redis-lua is distributed under the terms of the MIT/X11 license (see LICENSE).
