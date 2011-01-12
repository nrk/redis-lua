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

local function zset_range_request(client, command, ...)
    local args, opts = {...}, { }

    if #args >= 1 and type(args[#args]) == 'table' then
        local options = table.remove(args, #args)
        if options.withscores then
            table.insert(opts, 'WITHSCORES')
        end
    end

    for _, v in pairs(opts) do table.insert(args, v) end
    request.multibulk(client, command, args)
end

local function zset_range_byscore_request(client, command, ...)
    local args, opts = {...}, { }

    if #args >= 1 and type(args[#args]) == 'table' then
        local options = table.remove(args, #args)
        if options.limit then
            table.insert(opts, 'LIMIT')
            table.insert(opts, options.limit.offset or options.limit[1])
            table.insert(opts, options.limit.count or options.limit[2])
        end
        if options.withscores then
            table.insert(opts, 'WITHSCORES')
        end
    end

    for _, v in pairs(opts) do table.insert(args, v) end
    request.multibulk(client, command, args)
end

local function zset_range_reply(reply, command, ...)
    local args = {...}
    local opts = args[4]
    if opts and (opts.withscores or string.lower(tostring(opts)) == 'withscores') then
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
    local res = client.network.read(client)
    local prefix  = res:sub(1, -#res)
    local handler = assert(protocol.prefixes[prefix], 'unknown response prefix: '..prefix)
    return handler(client, res)
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
    assert(len, 'cannot parse ' .. str .. ' as data length')

    if len == -1 then return nil end
    local next_chunk = client.network.read(client, len + 2)
    return next_chunk:sub(1, -3);
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

local define_command_impl = function(target, name, opts)
    local opts = opts or {}
    target[string.lower(name)] = custom(
        opts.command or string.upper(name),
        opts.request or request.multibulk,
        opts.response or nil
    )
end

function define_command(name, opts)
    define_command_impl(commands, name, opts)
end

local undefine_command_impl = function(target, name)
    target[string.lower(name)] = nil
end

function undefine_command(name)
    undefine_command_impl(commands, name)
end

-- ############################################################################

local client_prototype = {}

client_prototype.raw_cmd = function(client, buffer)
    request.raw(client, buffer .. protocol.newline)
    return response.read(client)
end

client_prototype.define_command = function(client, name, opts)
    define_command_impl(client, name, opts)
end

client_prototype.undefine_command = function(client, name)
    undefine_command_impl(client, name)
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

    local function initialize_transaction(client, options, block, queued_parsers)
        local coro = coroutine.create(block)

        if options.watch then
            local watch_keys = {}
            for _, key in pairs(options.watch) do
                table.insert(watch_keys, key)
            end
            if #watch_keys > 0 then
                client:watch(unpack(watch_keys))
            end
        end

        local transaction_client = setmetatable({}, {__index=client})
        transaction_client.exec  = function(...)
            error('cannot use EXEC inside a transaction block')
        end
        transaction_client.multi = function(...)
            coroutine.yield()
        end
        transaction_client.commands_queued = function()
            return #queued_parsers
        end

        assert(coroutine.resume(coro, transaction_client))

        transaction_client.multi = nil
        transaction_client.discard = function(...)
            local reply = client:discard()
            for i, v in pairs(queued_parsers) do
                queued_parsers[i]=nil
            end
            coro = initialize_transaction(client, options, block, queued_parsers)
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

    local function transaction(client, options, coroutine_block, attempts)
        local queued_parsers, replies = {}, {}
        local retry = tonumber(attempts) or tonumber(options.retry) or 2
        local coro = initialize_transaction(client, options, coroutine_block, queued_parsers)

        local success, retval
        if coroutine.status(coro) == 'suspended' then
            success, retval = coroutine.resume(coro)
        else
            -- do not fail if the coroutine has not been resumed (missing t:multi() with CAS)
            success, retval = true, 'empty transaction'
        end
        if #queued_parsers == 0 or not success then
            client:discard()
            assert(success, retval)
            return replies, 0
        end

        local raw_replies = client:exec()
        if not raw_replies then
            if (retry or 0) <= 0 then
                error("MULTI/EXEC transaction aborted by the server")
            else
                --we're not quite done yet
                return transaction(client, options, coroutine_block, retry - 1)
            end
        end

        for i, parser in pairs(queued_parsers) do
            table.insert(replies, i, parser(raw_replies[i]))
        end

        return replies, #queued_parsers
    end

    client_prototype.transaction = function(client, arg1, arg2)
        local options, block
        if not arg2 then
            options, block = {}, arg1
        elseif arg1 then --and arg2, implicitly
            options, block = type(arg1)=="table" and arg1 or { arg1 }, arg2
        else
            error("Invalid parameters for redis transaction.")
        end

        if not options.watch then
            watch_keys = { }
            for i, v in pairs(options) do
                if tonumber(i) then
                    table.insert(watch_keys, v)
                    options[i] = nil
                end
            end
            options.watch = watch_keys
        elseif not (type(options.watch) == 'table') then
            options.watch = { options.watch }
        end

        if not options.cas then
            local tx_block = block
            block = function(client, ...)
                client:multi()
                return tx_block(client, ...) --can't wrap this in pcall because we're in a coroutine.
            end
        end

        return transaction(client, options, block)
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
                assert(server.scheme == 'redis', '"'..server.scheme..'" is an invalid scheme')
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

    assert(host, 'please specify the address of running redis instance')
    local client_socket = socket.connect(host, tonumber(port))
    assert(client_socket, 'could not connect to ' .. host .. ':' .. port)
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
    watch      = command('WATCH'),          -- >= 2.2
    unwatch    = command('UNWATCH'),        -- >= 2.2

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
    strlen     = command('STRLEN'),         -- >= 2.2
    setrange   = command('SETRANGE'),       -- >= 2.2
    getrange   = command('GETRANGE'),       -- >= 2.2
    setbit     = command('SETBIT'),         -- >= 2.2
    getbit     = command('GETBIT'),         -- >= 2.2

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
    persist   = command('PERSIST', { response = toboolean }),     -- >= 2.2

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
    rpushx           = command('RPUSHX'),           -- >= 2.2
    lpushx           = command('LPUSHX'),           -- >= 2.2
    linsert          = command('LINSERT'),          -- >= 2.2
    brpoplpush       = command('BRPOPLPUSH'),       -- >= 2.2

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
    zrange           = command('ZRANGE', {
        request  = zset_range_request,
        response = zset_range_reply,
    }),
    zrevrange        = command('ZREVRANGE', {
        request  = zset_range_request,
        response = zset_range_reply,
    }),
    zrangebyscore    = command('ZRANGEBYSCORE', {
        request  = zset_range_byscore_request,
        response = zset_range_reply,
    }),
    zrevrangebyscore = command('ZREVRANGEBYSCORE', {              -- >= 2.2
        request  = zset_range_byscore_request,
        response = zset_range_reply,
    }),
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
