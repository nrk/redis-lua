module('Redis', package.seeall)

require('socket')

-- ########################################################################### --

local protocol = {
    newline = '\r\n', ok = 'OK', err = 'ERR', null = 'nil', 

    commands = {
        -- connection handling
        quit = 'QUIT', 

        -- commands operating on string values
        set = 'SET', get = 'GET', mget = 'MGET', setnx = 'SETNX', incr = 'INCR', 
        incrby = 'INCRBY', decr = 'DECR', decrby = 'DECRBY', exists = 'EXISTS', 
        del = 'DEL', type = 'TYPE', 

        -- commands operating on the key space
        keys = 'KEYS', randomkey = 'RANDOMKEY', rename = 'RENAME', 
        renamenx = 'RENAMENX', dbsize = 'DBSIZE', 

        -- commands operating on lists
        rpush = 'RPUSH', lpush = 'LPUSH', llen = 'LLEN', lrange = 'LRANGE', 
        ltrim = 'LTRIM', lindex = 'LINDEX', lset = 'LSET', lrem = 'LREM', 
        lpop = 'LPOP', rpop = 'RPOP', 

        -- commands operating on sets
        sadd = 'SADD', srem = 'SREM', scard = 'SCARD', sismember = 'SISMEMBER', 
        sinter = 'SINTER', sinterstore = 'SINTERSTORE', smembers = 'SMEMBERS',

        -- multiple databases handling commands
        select = 'SELECT', move = 'MOVE', flushdb = 'FLUSHDB', flushall = 'FLUSHALL', 

        -- sorting
        sort = 'SORT', 

        -- persistence control commands
        save = 'SAVE', bgsave = 'BGSAVE', lastsave = 'LASTSAVE', shutdown = 'SHUTDOWN', 

        -- remote server control commands
        info = 'INFO', ping = 'PING', echo = 'ECHO', 
    }, 
}

-- ########################################################################### --

local function toboolean(value)
    -- plain and simple
    if value == 1 then return true else return false end
end

local function _write(client, buffer)
    client.socket:send(buffer)
end

local function _read(client, len)
    if len == nil then len = '*l' end
    local line, err = client.socket:receive(len)
    if not err then return line end
end

-- ########################################################################### --

local function _read_response(client, options)
    local res    = _read(client)
    local prefix = res:sub(1, -#res)
    local response_handler = protocol.prefixes[prefix]

    if not response_handler then 
        error("Unknown response prefix: " .. prefix)
    else
        return response_handler(client, res, options)
    end
end


local function _send_raw(client, buffer, options)
    -- TODO: optimize
    local bufferType = type(buffer)

    if bufferType == 'string' then
        _write(client, buffer)
    elseif bufferType == 'table' then
        _write(client, table.concat(buffer))
    else
        error('Argument error: ' .. bufferType)
    end

    return _read_response(client, options)
end

local function _send_inline(client, command, args, options)
    if args == nil then
        _write(client, command .. protocol.newline)
    else
        -- TODO: optimize
        local argsType = type(args)

        if argsType == 'string' then
            _write(client, command .. ' ' .. args .. protocol.newline)
        elseif argsType == 'table' then
            _write(client, command .. ' ' .. table.concat(args, ' ') .. protocol.newline)
        else
            error('Invalid type for arguments: ' .. argsType)
        end
    end

    return _read_response(client, options)
end

local function _send_bulk(client, command, args, data, options)
    return _send_raw(client, { command, ' ', #data, protocol.newline, data, protocol.newline })
end


local function _read_line(client, response, options)
    return response:sub(2)
end

local function _read_error(client, response, options)
    local err_line = response:sub(2)

    if err_line:sub(1, 3) == protocol.err then
        error(err_line:sub(5))
    else
        error(err_line)
    end
end

local function _read_bulk(client, response, options) 
    local str = response:sub(2)
    local len = tonumber(str)

    if not len then 
        error('Cannot parse ' .. str .. ' as data length.')
    else
        if len == -1 then return nil end
        local data = _read(client, len + 2)
        return data:sub(1, -3);
    end
end

local function _read_multibulk(client, response, options)
    local str = response:sub(2)

    -- TODO: add a check if the returned value is indeed a number
    local list_count = tonumber(str)

    if list_count == -1 then 
        return nil
    else
        local list = {}

        if list_count > 0 then 
            for i = 1, list_count do
                table.insert(list, i, _read_bulk(client, _read(client), options))
            end
        end

        return list
    end
end

local function _read_number(client, response, options)
    local res = response:sub(2)
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


-- ########################################################################### --

protocol.prefixes = {
    ['+'] = _read_line, 
    ['-'] = _read_error, 
    ['$'] = _read_bulk, 
    ['*'] = _read_multibulk, 
    [':'] = _read_number, 
}

-- ########################################################################### --

local function raw_cmd(client, buffer)
    return _send_raw(client, buffer .. protocol.newline)
end

local function ping(client)
    return _send_inline(client, protocol.commands.ping)
end

local function echo(client, data)
    return _send_raw(client, {
        protocol.commands.echo, ' ' , #data, protocol.newline, 
        data, protocol.newline
    })
end

local function _set(client, command, key, value)
    return _send_raw(client, {
        command, ' ' , key, ' ', #value, protocol.newline, 
        value, protocol.newline
    })
end

local function set(client, key, value)
    return _set(client, protocol.commands.set, key, value)
end

local function set_preserve(client, key, value)
    return _set(client, protocol.commands.setnx, key, value)
end

local function get(client, key)
    return _send_inline(client, protocol.commands.get, key)
end

local function mget(client, keys)
    return _send_inline(client, protocol.commands.mget, keys)
end

local function incr(client, key)
    return _send_inline(client, protocol.commands.incr, key)
end

local function incr_by(client, key, step)
    return _send_inline(client, protocol.commands.incrby, { key, step })
end

local function decr(client, key)
    return _send_inline(client, protocol.commands.decr, key)
end

local function decr_by(client, key, step)
    return _send_inline(client, protocol.commands.decrby, { key, step })
end

local function exists(client, key)
    local exists = _send_inline(client, protocol.commands.exists, key)
    return toboolean(exists)
end

local function delete(client, key)
    local deleted = _send_inline(client, protocol.commands.del, key)
    return toboolean(deleted)
end

local function type(client, key)
    return _send_inline(client, protocol.commands.type, key)
end

local function keys(client, pattern)
    return _send_inline(client, protocol.commands.keys, pattern)
end

-- ########################################################################### --

function connect(host, port)
    local client_socket = socket.connect(host, port)

    if not client_socket then
        error('Could not connect to ' .. host .. ':' .. port)
    end

    return {
        socket       = client_socket, 
        raw_cmd      = raw_cmd, 
        ping         = ping,
        echo         = echo, 
        set          = set, 
        get          = get, 
        mget         = mget, 
        incr         = incr, 
        incr_by      = incr_by, 
        decr         = decr, 
        decr_by      = decr_by, 
        exists       = exists, 
        delete       = delete, 
        type       = type, 
        keys         = keys, 
        set_preserve = set_preserve, 
    }
end
