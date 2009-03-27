local _G = _G
local require, error, type, print = require, error, type, print
local table, pairs, tostring, tonumber = table, pairs, tostring, tonumber

module('Redis')

local socket = require('socket')       -- requires LuaSocket as a dependency

local client_socket  = nil
local redis_commands = {}
local network, request, response, utils = {}, {}, {}, {}, {}

local protocol = { newline = '\r\n', ok = 'OK', err = 'ERR', null = 'nil' }

local function toboolean(value) return value == 1 end

local function load_methods(proto, methods)
    local redis = table.clone(proto)
    for i, v in pairs(methods) do redis[i] = v end
    return redis
end

-- ############################################################################

function network.write(buffer)
    local _, err = client_socket:send(buffer)
    if err then error(err) end
end

function network.read(len)
    if len == nil then len = '*l' end
    local line, err = client_socket:receive(len)
    if not err then return line else error('Connection error: ' .. err) end
end

-- ############################################################################

function response.read()
    if options and options.close == true then return end

    local res    = network.read()
    local prefix = res:sub(1, -#res)
    local response_handler = protocol.prefixes[prefix]

    if not response_handler then 
        error("Unknown response prefix: " .. prefix)
    else
        return response_handler(res)
    end
end

function response.status(data)
    local sub = data:sub(2)
    if sub == protocol.ok then return true else return sub end
end

function response.error(data)
    local err_line = data:sub(2)

    if err_line:sub(1, 3) == protocol.err then
        error("Redis error: " .. err_line:sub(5))
    else
        error("Redis error: " .. err_line)
    end
end

function response.bulk(data)
    local str = data:sub(2)
    local len = tonumber(str)

    if not len then 
        error('Cannot parse ' .. str .. ' as data length.')
    else
        if len == -1 then return nil end
        local next_chunk = network.read(len + 2)
        return next_chunk:sub(1, -3);
    end
end

function response.multibulk(data)
    local str = data:sub(2)

    -- TODO: add a check if the returned value is indeed a number
    local list_count = tonumber(str)

    if list_count == -1 then 
        return nil
    else
        local list = {}

        if list_count > 0 then 
            for i = 1, list_count do
                table.insert(list, i, response.bulk(network.read()))
            end
        end

        return list
    end
end

function response.integer(data)
    local res = data:sub(2)
    local number = tonumber(res)

    if not number then
        if res == protocol.null then
            return nil
        else
            error('Cannot parse ' .. res .. ' as numeric response.')
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

function request.raw(buffer)
    -- TODO: optimize
    local bufferType = type(buffer)

    if bufferType == 'string' then
        network.write(buffer)
    elseif bufferType == 'table' then
        network.write(table.concat(buffer))
    else
        error('Argument error: ' .. bufferType)
    end

    return response.read()
end

function request.inline(command, ...)
    if arg.n == 0 then
        network.write(command .. protocol.newline)
    else
        local arguments = arg
        arguments.n = nil

        if #arguments > 0 then 
            arguments = table.concat(arguments, ' ')
        else 
            arguments = ''
        end

        network.write(command .. ' ' .. arguments .. protocol.newline)
    end

    return response.read()
end

function request.bulk(command, ...)
    local arguments = arg
    local data      = tostring(table.remove(arguments))
    arguments.n = nil

    -- TODO: optimize
    if #arguments > 0 then 
        arguments = table.concat(arguments, ' ')
    else 
        arguments = ''
    end

    return request.raw({ 
        command, ' ', arguments, ' ', #data, protocol.newline, data, protocol.newline 
    })
end

-- ############################################################################

local function custom(command, send, parse)
    return function(_, ...)
        local reply = send(command, ...)
        -- TODO: set adapter to default on creation if nil
        if parse then
            return parse(reply, command, ...)
        else
            return reply
        end
    end
end

local function bulk(command, reader)
    return custom(command, request.bulk, reader)
end

local function inline(command, reader)
    return custom(command, request.inline, reader)
end

-- ############################################################################

function connect(host, port)
    client_socket = socket.connect(host, port)
    if not client_socket then
        error('Could not connect to ' .. host .. ':' .. port)
    end

    local redis_client = {
        socket  = client_socket, 
        raw_cmd = function(self, buffer)
            return request.raw(buffer .. protocol.newline)
        end, 
    }

    return load_methods(redis_client, redis_commands)
end

-- ############################################################################

redis_commands = {
    -- miscellaneous commands
    ping  = inline('PING', 
        function(response) 
            if response == 'PONG' then return true else return false end
        end
    ), 
    echo  = bulk('ECHO'),  
    -- TODO: the server returns an empty -ERR on authentication failure
    auth  = inline('AUTH'), 

    -- connection handling
    quit  = custom('QUIT', 
        function(command) 
            -- let's fire and forget! the connection is closed as soon 
            -- as the QUIT command is received by the server.
            network.write(command .. protocol.newline)
        end
    ), 

    -- commands operating on string values
    set           = bulk('SET'), 
    set_preserve  = bulk('SETNX', toboolean), 
    get           = inline('GET'), 
    get_multiple  = inline('MGET'), 
    increment     = inline('INCR'), 
    increment_by  = inline('INCRBY'), 
    decrement     = inline('DECR'), 
    decrement_by  = inline('DECRBY'), 
    exists        = inline('EXISTS', toboolean), 
    delete        = inline('DEL', toboolean), 
    type          = inline('TYPE'), 

    -- commands operating on the key space
    keys          = inline('KEYS', 
        function(response) 
            local keys = {}
            response:gsub('%w+', function(key) 
                table.insert(keys, key)
            end)
            return keys
        end
    ),
    random_key       = inline('RANDOMKEY'), 
    rename           = inline('RENAME'), 
    rename_preserve  = inline('RENAMENX'), 
    database_size    = inline('DBSIZE'), 

    -- commands operating on lists
    push_tail    = bulk('RPUSH'), 
    push_head    = bulk('LPUSH'), 
    list_length  = inline('LLEN'), 
    list_range   = inline('LRANGE'), 
    list_trim    = inline('LTRIM'), 
    list_index   = inline('LINDEX'), 
    list_set     = bulk('LSET'), 
    list_remove  = bulk('LREM'), 
    pop_first    = inline('LPOP'), 
    pop_last     = inline('RPOP'), 

    -- commands operating on sets
    set_add                 = inline('SADD'), 
    set_remove              = inline('SREM'), 
    set_cardinality         = inline('SCARD'), 
    set_is_member           = inline('SISMEMBER'), 
    set_intersection        = inline('SINTER'), 
    set_intersection_store  = inline('SINTERSTORE'), 
    set_members             = inline('SMEMBERS'), 

    -- multiple databases handling commands
    select_database  = inline('SELECT'), 
    move_key         = inline('MOVE'), 
    flush_database   = inline('FLUSHDB'), 
    flush_databases  = inline('FLUSHALL'), 

    -- sorting
    --[[
        TODO: should we pass sort parameters as a table? e.g: 
                params = { 
                    by    = 'weight_*', 
                    get   = 'object_*', 
                    limit = { 0, 10 },
                    sort  = { 'desc', 'alpha' }
                }
    --]]
    sort  = custom('SORT', 
        function(command, params)
            -- TODO: here we will put the logic needed to serialize the params 
            --       table to be sent as the argument of the SORT command.
            return request.inline(command, params)
        end
    ), 

    -- persistence control commands
    save             = inline('SAVE'), 
    background_save  = inline('BGSAVE'), 
    last_save        = inline('LASTSAVE'), 
    shutdown         = custom('SHUTDOWN',
        function(command) 
            -- let's fire and forget! the connection is closed as soon 
            -- as the SHUTDOWN command is received by the server.
            network.write(command .. protocol.newline)
        end
    ), 

    -- remote server control commands
    info  = inline('INFO', 
        function(response) 
            local info = {}
            response:gsub('([^\r\n]*)\r\n', function(kv) 
                local k,v = kv:match(('([^:]*):([^:]*)'):rep(1))
                info[k] = v
            end)
            return info
        end
    ),
}
