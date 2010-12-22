module('Redis', package.seeall)

local socket = require('socket')
local uri    = require('socket.url')

local commands = {}
local network, request, response = {}, {}, {}

local defaults = { host = '127.0.0.1', port = 6379, tcp_nodelay = false }
local protocol = {
    newline = '\r\n',
    ok      = 'OK',
    err     = 'ERR',
    queued  = 'QUEUED',
    null    = 'nil'
}

local function parse_boolean(v)
    if v == '1' or v == 'true' or v == 'TRUE' then
        return true
    elseif v == '0' or v == 'false' or v == 'FALSE' then
        return false
    else
        return nil
    end
end

local function toboolean(value) return value == 1 end

local function fire_and_forget(client, command)
    -- let's fire and forget! the connection is closed as soon
    -- as the SHUTDOWN command is received by the server.
    client.network.write(client, command .. protocol.newline)
    return false
end

local function zset_range_parse(reply, command, ...)
    local args = {...}
    if #args == 4 and string.lower(args[4]) == 'withscores' then
        local new_reply = { }
        for i = 1, #reply, 2 do
            table.insert(new_reply, { reply[i], reply[i + 1] })
        end
        return new_reply
    else
        return reply
    end
end

local function zset_store_request(client, command, ...)
    local args, opts = {...}, { }

    if #args >= 1 and type(args[#args]) == 'table' then
        local options = table.remove(args, #args)
        if options.weights and type(options.weights) == 'table' then
            table.insert(opts, 'WEIGHTS')
            for _, weight in ipairs(options.weights) do
                table.insert(opts, weight)
            end
        end
        if options.aggregate then
            table.insert(opts, 'AGGREGATE')
            table.insert(opts, options.aggregate)
        end
    end

    for _, v in pairs(opts) do table.insert(args, v) end
    request.multibulk(client, command, args)
end

local function hmset_filter_args(client, command, ...)
    local args, arguments = {...}, {}
    if (#args == 1 and type(args[1]) == 'table') then
        for k,v in pairs(args[1]) do
            table.insert(arguments, k)
            table.insert(arguments, v)
        end
    else
        arguments = args
    end
    request.multibulk(client, command, arguments)
end

local function load_methods(proto, methods)
    local redis = setmetatable ({}, getmetatable(proto))
    for i, v in pairs(proto) do redis[i] = v end
    for i, v in pairs(methods) do redis[i] = v end
    return redis
end

local function create_client(proto, client_socket, methods)
    local redis = load_methods(proto, methods)
    redis.network = {
        socket = client_socket,
        read   = network.read,
        write  = network.write,
    }
    redis.requests = {
        multibulk = request.multibulk,
    }
    return redis
end

-- ############################################################################

function network.write(client, buffer)
    local _, err = client.network.socket:send(buffer)
    if err then error(err) end
end

function network.read(client, len)
    if len == nil then len = '*l' end
    local line, err = client.network.socket:receive(len)
    if not err then return line else error('connection error: ' .. err) end
end

-- ############################################################################

function response.read(client)
    local res    = client.network.read(client)
    local prefix = res:sub(1, -#res)
    local response_handler = protocol.prefixes[prefix]

    if not response_handler then
        error('unknown response prefix: ' .. prefix)
    else
        return response_handler(client, res)
    end
end

function response.status(client, data)
    local sub = data:sub(2)

    if sub == protocol.ok then
        return true
    elseif sub == protocol.queued then
        return { queued = true }
    else
        return sub
    end
end

function response.error(client, data)
    local err_line = data:sub(2)

    if err_line:sub(1, 3) == protocol.err then
        error('redis error: ' .. err_line:sub(5))
    else
        error('redis error: ' .. err_line)
    end
end

function response.bulk(client, data)
    local str = data:sub(2)
    local len = tonumber(str)

    if not len then
        error('cannot parse ' .. str .. ' as data length.')
    else
        if len == -1 then return nil end
        local next_chunk = client.network.read(client, len + 2)
        return next_chunk:sub(1, -3);
    end
end

function response.multibulk(client, data)
    local str = data:sub(2)
    local list_count = tonumber(str)

    if list_count == -1 then
        return nil
    else
        local list = {}
        if list_count > 0 then
            for i = 1, list_count do
                table.insert(list, i, response.read(client))
            end
        end
        return list
    end
end

function response.integer(client, data)
    local res = data:sub(2)
    local number = tonumber(res)

    if not number then
        if res == protocol.null then
            return nil
        else
            error('cannot parse ' .. res .. ' as numeric response.')
        end
    end

    return number
end

protocol.prefixes = {
    ['+'] = response.status,
    ['-'] = response.error,
    ['$'] = response.bulk,
    ['*'] = response.multibulk,
    [':'] = response.integer,
}

-- ############################################################################

function request.raw(client, buffer)
    local bufferType = type(buffer)

    if bufferType == 'table' then
        client.network.write(client, table.concat(buffer))
    elseif bufferType == 'string' then
        client.network.write(client, buffer)
    else
        error('argument error: ' .. bufferType)
    end
end

function request.multibulk(client, command, ...)
    local args      = {...}
    local args_len  = #args
    local buffer    = { true, true }
    local proto_nl  = protocol.newline

    if args_len == 1 and type(args[1]) == 'table' then
        args_len, args = #args[1], args[1]
    end

    buffer[1] = '*' .. tostring(args_len + 1) .. proto_nl
    buffer[2] = '$' .. #command .. proto_nl .. command .. proto_nl

    for _, argument in pairs(args) do
        s_argument = tostring(argument)
        table.insert(buffer, '$' .. #s_argument .. proto_nl .. s_argument .. proto_nl)
    end

    request.raw(client, buffer)
end

-- ############################################################################

local function custom(command, send, parse)
    return function(client, ...)
        local has_reply = send(client, command, ...)
        if has_reply == false then return end
        local reply = response.read(client)

        if type(reply) == 'table' and reply.queued then
            reply.parser = parse
            return reply
        else
            if parse then
                return parse(reply, command, ...)
            else
                return reply
            end
        end
    end
end

function command(command, opts)
    if opts == nil or type(opts) == 'function' then
        return custom(command, request.multibulk, opts)
    else
        return custom(command, opts.request or request.multibulk, opts.response)
    end
end

function define_command(name, opts)
    local opts = opts or {}
    commands[string.lower(name)] = custom(
        opts.command or string.upper(name),
        opts.request or request.multibulk,
        opts.response or nil
    )
end

function undefine_command(name)
    commands[string.lower(name)] = nil
end

-- ############################################################################

local client_prototype = {}

client_prototype.raw_cmd = function(client, buffer)
    request.raw(client, buffer .. protocol.newline)
    return response.read(client)
end

client_prototype.define_command = function(client, name, opts)
    local opts = opts or {}
    client[string.lower(name)] = custom(
        opts.command or string.upper(name),
        opts.request or request.multibulk,
        opts.response or nil
    )
end

client_prototype.undefine_command = function(client, name)
    client[string.lower(name)] = nil
end

client_prototype.pipeline = function(client, block)
    local simulate_queued = '+' .. protocol.queued
    local requests, replies, parsers = {}, {}, {}
    local __netwrite, __netread = client.network.write, client.network.read

    client.network.write = function(_, buffer)
        table.insert(requests, buffer)
    end

    -- TODO: this hack is necessary to temporarily reuse the current
    --       request -> response handling implementation of redis-lua
    --       without further changes in the code, but it will surely
    --       disappear when the new command-definition infrastructure
    --       will finally be in place.
    client.network.read = function()
        return simulate_queued
    end

    local pipeline = setmetatable({}, {
        __index = function(env, name)
            local cmd = client[name]
            if cmd == nil then
                if _G[name] then
                    return _G[name]
                else
                    error('unknown redis command: ' .. name, 2)
                end
            end
            return function(self, ...)
                local reply = cmd(client, ...)
                table.insert(parsers, #requests, reply.parser)
                return reply
            end
        end
    })

    local success, retval = pcall(block, pipeline)

    client.network.write, client.network.read = __netwrite, __netread
    if not success then error(retval, 0) end

    client.network.write(client, table.concat(requests, ''))

    for i = 1, #requests do
        local raw_reply, parser = response.read(client), parsers[i]
        if parser then
            table.insert(replies, i, parser(raw_reply))
        else
            table.insert(replies, i, raw_reply)
        end
    end

    return replies
end

do
    local function identity(...) return ... end
    local emptytable = {}

    local function initialize_transaction(client, watch_keys, block, queued_parsers)
        local coro = coroutine.create(block)
        for i, key in pairs(watch_keys) do
            client:watch(key)
        end

        local transaction_client = setmetatable({}, {__index=client})
        assert(coroutine.resume(coro, transaction_client))

        transaction_client.discard = function(...)
            local reply = client:discard()
            for i, v in pairs(queued_parsers) do
                queued_parsers[i]=nil
            end
            coro = initialize_transaction(client, watch_keys, block, queued_parsers)
            return reply
        end
        transaction_client.watch = function(...)
            error('WATCH inside MULTI is not allowed')
        end
        setmetatable(transaction_client, { __index = function(t, k)
                local cmd = client[k]
                if type(cmd) == "function" then
                    local function queuey(self, ...)
                        local reply = cmd(client, ...)
                        assert((reply or emptytable).queued == true, 'a QUEUED reply was expected')
                        table.insert(queued_parsers, reply.parser or identity)
                        return reply
                    end
                    t[k]=queuey
                    return queuey
                else
                    return cmd
                end
            end
        })
        client:multi()
        return coro
    end

    local function transaction(client, watch_keys, coroutine_block, retry)
        local queued_parsers, replies = {}, {}
        local coro = initialize_transaction(client, watch_keys, coroutine_block, queued_parsers)

        local success, retval = assert(coroutine.resume(coro))
        
        if #queued_parsers == 0 then 
            client:discard()
            return replies 
        end

        local raw_replies = client:exec()
        if not raw_replies then
            if (retry or 0) <= 0 then
                error "MULTI/EXEC transaction aborted by the server"
            else
                --we're not quite done yet
                return transaction(client, watch_keys, coroutine_block, retry-1)
            end
        end
        
        for i, parser in pairs(queued_parsers) do
            table.insert(replies, i, parser(raw_replies[i]))
        end

        return replies
   end

    client_prototype.transaction = function(client, arg1, arg2)
       local watch_keys, block
        if not arg2 then
            watch_keys, block = {}, arg1
        elseif arg1 then --and arg2, implicitly
            watch_keys, block = type(arg1)=="table" and arg1 or { arg1 }, arg2
        else
            error("Invalid parameters for redis transaction.")
        end
        return nil or transaction(client, watch_keys, function(client, ...)
            coroutine.yield()
            return block(client, ...) --can't wrap this in pcall because we're in a coroutine.
        end)
    end

    client_prototype.check_and_set = function(client, watch_keys, block1, block2)
        local block
        if type(watch_keys) ~= 'table' then
            watch_keys = { watch_keys }
        end
        assert(type(block1)=="function", "Check-and-set operation expects a function parameter")
        if not block2 then
            block = block1
        else --we were given two blocks
            assert(type(block2)=="function", "Check-and-set operation expects third parameter, if present, to be a function.")
            block = function(client)
                local res = { block1(client) }
                coroutine.yield()
                return block2(client, unpack(res))
            end
        end
        return transaction(client, watch_keys, block, 10)
    end
end
-- ############################################################################

function connect(...)
    local args = {...}
    local host, port = defaults.host, defaults.port
    local tcp_nodelay = defaults.tcp_nodelay

    if #args == 1 then
        if type(args[1]) == 'table' then
            host = args[1].host or defaults.host
            port = args[1].port or defaults.port
            if args[1].tcp_nodelay ~= nil then
                tcp_nodelay = args[1].tcp_nodelay == true
            end
        else
            local server = uri.parse(select(1, ...))
            if server.scheme then
                if server.scheme ~= 'redis' then
                    error('"' .. server.scheme .. '" is an invalid scheme')
                end
                host, port = server.host, server.port or defaults.port
                if server.query then
                    for k,v in server.query:gmatch('([-_%w]+)=([-_%w]+)') do
                        if k == 'tcp_nodelay' or k == 'tcp-nodelay' then
                            tcp_nodelay = parse_boolean(v)
                            if tcp_nodelay == nil then
                                tcp_nodelay = defaults.tcp_nodelay
                            end
                        end
                    end
                end
            else
                host, port = server.path, defaults.port
            end
        end
    elseif #args > 1 then
        host, port = unpack(args)
    end

    if host == nil then
        error('please specify the address of running redis instance')
    end

    local client_socket = socket.connect(host, tonumber(port))
    if not client_socket then
        error('could not connect to ' .. host .. ':' .. port)
    end
    client_socket:setoption('tcp-nodelay', tcp_nodelay)

    return create_client(client_prototype, client_socket, commands)
end

-- ############################################################################

commands = {
    -- miscellaneous commands
    ping       = command('PING', {
        response = function(response) return response == 'PONG' end
    }),
    echo       = command('ECHO'),
    auth       = command('AUTH'),

    -- connection handling
    quit       = command('QUIT', { request = fire_and_forget }),

    -- transactions
    multi      = command('MULTI'),
    exec       = command('EXEC'),
    discard    = command('DISCARD'),
    watch      = command('WATCH'),
    unwatch    = command('UNWATCH'),

    -- commands operating on string values
    set        = command('SET'),
    setnx      = command('SETNX', { response = toboolean }),
    setex      = command('SETEX'),          -- >= 2.0
    mset       = command('MSET', { request = hmset_filter_args }),
    msetnx     = command('MSETNX', {
        request = hmset_filter_args,
        response = toboolean
    }),
    get        = command('GET'),
    mget       = command('MGET'),
    getset     = command('GETSET'),
    incr       = command('INCR'),
    incrby     = command('INCRBY'),
    decr       = command('DECR'),
    decrby     = command('DECRBY'),
    exists     = command('EXISTS', { response = toboolean }),
    del        = command('DEL'),
    type       = command('TYPE'),
    append     = command('APPEND'),         -- >= 2.0
    substr     = command('SUBSTR'),         -- >= 2.0

    -- commands operating on the key space
    keys       = command('KEYS', {
        response = function(response)
            if type(response) == 'table' then
                return response
            else
                local keys = {}
                response:gsub('[^%s]+', function(key)
                    table.insert(keys, key)
                end)
                return keys
            end
        end
    }),
    randomkey  = command('RANDOMKEY', {
        response = function(response)
            if response == '' then
                return nil
            else
                return response
            end
        end
    }),
    rename    = command('RENAME'),
    renamenx  = command('RENAMENX', { response = toboolean }),
    expire    = command('EXPIRE', { response = toboolean }),
    expireat  = command('EXPIREAT', { response = toboolean }),
    dbsize    = command('DBSIZE'),
    ttl       = command('TTL'),

    -- commands operating on lists
    rpush            = command('RPUSH'),
    lpush            = command('LPUSH'),
    llen             = command('LLEN'),
    lrange           = command('LRANGE'),
    ltrim            = command('LTRIM'),
    lindex           = command('LINDEX'),
    lset             = command('LSET'),
    lrem             = command('LREM'),
    lpop             = command('LPOP'),
    rpop             = command('RPOP'),
    rpoplpush        = command('RPOPLPUSH'),
    blpop            = command('BLPOP'),
    brpop            = command('BRPOP'),

    -- commands operating on sets
    sadd             = command('SADD', { response = toboolean }),
    srem             = command('SREM', { response = toboolean }),
    spop             = command('SPOP'),
    smove            = command('SMOVE', { response = toboolean }),
    scard            = command('SCARD'),
    sismember        = command('SISMEMBER', { response = toboolean }),
    sinter           = command('SINTER'),
    sinterstore      = command('SINTERSTORE'),
    sunion           = command('SUNION'),
    sunionstore      = command('SUNIONSTORE'),
    sdiff            = command('SDIFF'),
    sdiffstore       = command('SDIFFSTORE'),
    smembers         = command('SMEMBERS'),
    srandmember      = command('SRANDMEMBER'),

    -- commands operating on sorted sets
    zadd             = command('ZADD', { response = toboolean }),
    zincrby          = command('ZINCRBY'),
    zrem             = command('ZREM', { response = toboolean }),
    zrange           = command('ZRANGE', { response = zset_range_parse }),
    zrevrange        = command('ZREVRANGE', { response = zset_range_parse }),
    zrangebyscore    = command('ZRANGEBYSCORE'),
    zunionstore      = command('ZUNIONSTORE', { request = zset_store_request }),
    zinterstore      = command('ZINTERSTORE', { request = zset_store_request }),
    zcount           = command('ZCOUNT'),
    zcard            = command('ZCARD'),
    zscore           = command('ZSCORE'),
    zremrangebyscore = command('ZREMRANGEBYSCORE'),
    zrank            = command('ZRANK'),
    zrevrank         = command('ZREVRANK'),
    zremrangebyrank  = command('ZREMRANGEBYRANK'),

    -- commands operating on hashes
    hset             = command('HSET', { response = toboolean }),
    hsetnx           = command('HSETNX', { response = toboolean }),
    hmset            = command('HMSET', {
        request = function(client, command, ...)
            local args, arguments = {...}, { }
            if #args == 2 then
                table.insert(arguments, args[1])
                for k, v in pairs(args[2]) do
                    table.insert(arguments, k)
                    table.insert(arguments, v)
                end
            else
                arguments = args
            end
            request.multibulk(client, command, arguments)
        end,
    }),
    hincrby          = command('HINCRBY'),
    hget             = command('HGET'),
    hmget            = command('HMGET', {
        request = function(client, command, ...)
            local args, arguments = {...}, { }
            if #args == 2 then
                table.insert(arguments, args[1])
                for _, v in ipairs(args[2]) do
                    table.insert(arguments, v)
                end
            else
                arguments = args
            end
            request.multibulk(client, command, arguments)
        end,
    }),
    hdel             = command('HDEL', { response = toboolean }),
    hexists          = command('HEXISTS', { response = toboolean }),
    hlen             = command('HLEN'),
    hkeys            = command('HKEYS'),
    hvals            = command('HVALS'),
    hgetall          = command('HGETALL', {
        response = function(reply, command, ...)
            local new_reply = { }
            for i = 1, #reply, 2 do new_reply[reply[i]] = reply[i + 1] end
            return new_reply
        end
    }),

    -- publish - subscribe
    subscribe        = command('SUBSCRIBE'),
    unsubscribe      = command('UNSUBSCRIBE'),
    psubscribe       = command('PSUBSCRIBE'),
    punsubscribe     = command('PUNSUBSCRIBE'),
    publish          = command('PUBLISH'),

    -- multiple databases handling commands
    select           = command('SELECT'),
    move             = command('MOVE', { response = toboolean }),
    flushdb          = command('FLUSHDB'),
    flushall         = command('FLUSHALL'),

    -- sorting
    --[[ params = {
            by    = 'weight_*',
            get   = 'object_*',
            limit = { 0, 10 },
            sort  = 'desc',
            alpha = true,
        }
    --]]
    sort             = command('SORT', {
        request = function(client, command, key, params)
            local query = { key }

            if params then
                if params.by then
                    table.insert(query, 'BY')
                    table.insert(query, params.by)
                end

                if type(params.limit) == 'table' then
                    -- TODO: check for lower and upper limits
                    table.insert(query, 'LIMIT')
                    table.insert(query, params.limit[1])
                    table.insert(query, params.limit[2])
                end

                if params.get then
                    table.insert(query, 'GET')
                    table.insert(query, params.get)
                end

                if params.sort then
                    table.insert(query, params.sort)
                end

                if params.alpha == true then
                    table.insert(query, 'ALPHA')
                end

                if params.store then
                    table.insert(query, 'STORE')
                    table.insert(query, params.store)
                end
            end

            request.multibulk(client, command, query)
        end
    }),

    -- persistence control commands
    save             = command('SAVE'),
    bgsave           = command('BGSAVE'),
    lastsave         = command('LASTSAVE'),
    shutdown         = command('SHUTDOWN', { request = fire_and_forget }),
    bgrewriteaof     = command('BGREWRITEAOF'),

    -- remote server control commands
    info             = command('INFO', {
        response = function(response)
            local info = {}
            response:gsub('([^\r\n]*)\r\n', function(kv)
                local k,v = kv:match(('([^:]*):([^:]*)'):rep(1))
                if (k:match('db%d+')) then
                    info[k] = {}
                    v:gsub(',', function(dbkv)
                        local dbk,dbv = kv:match('([^:]*)=([^:]*)')
                        info[k][dbk] = dbv
                    end)
                else
                    info[k] = v
                end
            end)
            return info
        end
    }),
    slaveof          = command('SLAVEOF'),
    config           = command('CONFIG'),
}
