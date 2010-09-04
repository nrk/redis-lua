module('Redis', package.seeall)

local socket = require('socket')
local uri    = require('socket.url')

local redis_commands = {}
local network, request, response = {}, {}, {}

local defaults = { host = '127.0.0.1', port = 6379 }
local protocol = {
    newline = '\r\n',
    ok      = 'OK',
    err     = 'ERR',
    queued  = 'QUEUED',
    null    = 'nil'
}

local function toboolean(value) return value == 1 end

local function fire_and_forget(client, command) 
    -- let's fire and forget! the connection is closed as soon 
    -- as the SHUTDOWN command is received by the server.
    network.write(client, command .. protocol.newline)
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

-- ############################################################################

function network.write(client, buffer)
    local _, err = client.socket:send(buffer)
    if err then error(err) end
end

function network.read(client, len)
    if len == nil then len = '*l' end
    local line, err = client.socket:receive(len)
    if not err then return line else error('connection error: ' .. err) end
end

-- ############################################################################

function response.read(client)
    local res    = network.read(client)
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
        local next_chunk = network.read(client, len + 2)
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

    if bufferType == 'string' then
        network.write(client, buffer)
    elseif bufferType == 'table' then
        network.write(client, table.concat(buffer))
    else
        error('argument error: ' .. bufferType)
    end
end

function request.multibulk(client, command, ...)
    local args      = {...}
    local buffer    = { }
    local arguments = nil
    local args_len  = nil

    if #args == 1 and type(args[1]) == 'table' then
        arguments = args[1]
        args_len  = #args[1]
    else
        arguments = args
        args_len  = #args
    end
 
    table.insert(buffer, '*' .. tostring(args_len + 1) .. protocol.newline)
    table.insert(buffer, '$' .. #command .. protocol.newline .. command .. protocol.newline)

    for _, argument in pairs(arguments) do
        s_argument = tostring(argument)
        table.insert(buffer, '$' .. #s_argument .. protocol.newline .. s_argument .. protocol.newline)
    end

    request.raw(client, buffer)
end

-- ############################################################################

local function custom(command, send, parse)
    return function(self, ...)
        local has_reply = send(self, command, ...)
        if has_reply == false then return end
        local reply = response.read(self)

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

local function command(command, opts)
    if opts == nil or type(opts) == 'function' then
        return custom(command, request.multibulk, opts)
    else
        return custom(command, opts.request or request.multibulk, opts.response)
    end
end

-- ############################################################################

function connect(...)
    local args = {...}
    local host, port = defaults.host, defaults.port

    if #args == 1 then
        if type(args[1]) == 'table' then
            host = args[1].host or defaults.host
            port = args[1].port or defaults.port
        else
            local server = uri.parse(select(1, ...))
            if server.scheme then
                if server.scheme ~= 'redis' then 
                    error('"' .. server.scheme .. '" is an invalid scheme')
                end
                host, port = server.host, server.port or defaults.port
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

    local redis_client = {
        socket  = client_socket, 
        raw_cmd = function(self, buffer)
            request.raw(self, buffer .. protocol.newline)
            return response.read(self)
        end, 
        requests = {
            multibulk = request.multibulk,
        },
        add_command = function(self, name, opts)
            local opts = opts or {}
            redis_commands[name] = custom(
                opts.command or string.upper(name),
                opts.request or request.multibulk,
                opts.response or nil
            )
            self[name] = redis_commands[name]
        end, 
        pipeline = function(self, block)
            local simulate_queued = '+' .. protocol.queued
            local requests, replies, parsers = {}, {}, {}
            local __netwrite, __netread = network.write, network.read

            network.write = function(_, buffer)
                table.insert(requests, buffer)
            end

            -- TODO: this hack is necessary to temporarily reuse the current 
            --       request -> response handling implementation of redis-lua 
            --       without further changes in the code, but it will surely 
            --       disappear when the new command-definition infrastructure 
            --       will finally be in place.
            network.read = function()
                return simulate_queued
            end

            local pipeline_mt = setmetatable({}, { 
                __index = function(env, name) 
                    local cmd = redis_commands[name]
                    if cmd == nil then 
                        error('unknown redis command: ' .. name, 2)
                    end
                    return function(...) 
                        local reply = cmd(self, ...)
                        table.insert(parsers, #requests, reply.parser)
                        return reply
                    end
                end 
            })

            local success, retval = pcall(setfenv(block, pipeline_mt), _G)

            network.write, network.read = __netwrite, __netread
            if not success then error(retval, 0) end

            network.write(self, table.concat(requests, ''))

            for i = 1, #requests do
                local parser = parsers[i]
                if parser then
                    table.insert(replies, parser(response.read(self)))
                else
                    table.insert(replies, response.read(self))
                end
            end

            return replies
        end,
    }

    return load_methods(redis_client, redis_commands)
end

-- ############################################################################

redis_commands = {
    -- miscellaneous commands
    ping       = command('PING', {
        response = function(response) return response == 'PONG' end
    }),
    echo       = command('ECHO'),  
    auth       = command('AUTH'), 

    -- connection handling
    quit       = command('QUIT', { request = fire_and_forget }), 

    -- commands operating on string values
    set        = command('SET'), 
    setnx      = command('SETNX', { response = toboolean }), 
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
    zcard            = command('ZCARD'), 
    zscore           = command('ZSCORE'), 
    zremrangebyscore = command('ZREMRANGEBYSCORE'), 

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
}
