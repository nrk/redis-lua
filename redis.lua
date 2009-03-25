module('Redis', package.seeall)

require('socket')

-- ############################################################################

local protocol = {
    newline = '\r\n', ok = 'OK', err = 'ERR', null = 'nil', 

    commands = {
        -- connection handling
        quit = 'QUIT', 

        -- commands operating on string values
        set = 'SET', get = 'GET', mget = 'MGET', setnx = 'SETNX', 
        incr = 'INCR', incrby = 'INCRBY', decr = 'DECR', decrby = 'DECRBY', 
        exists = 'EXISTS', del = 'DEL', type = 'TYPE', 

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
        select = 'SELECT', move = 'MOVE', flushdb = 'FLUSHDB', 
        flushall = 'FLUSHALL', 

        -- sorting
        sort = 'SORT', 

        -- persistence control commands
        save = 'SAVE', bgsave = 'BGSAVE', lastsave = 'LASTSAVE', 
        shutdown = 'SHUTDOWN', 

        -- remote server control commands
        info = 'INFO', ping = 'PING', echo = 'ECHO', 
    }, 
}

-- ############################################################################

local function toboolean(value)
    -- plain and simple
    if value == 1 then return true else return false end
end

local function _write(self, buffer)
    self.socket:send(buffer)
end

local function _read(self, len)
    if len == nil then len = '*l' end
    local line, err = self.socket:receive(len)
    if not err then return line else error('Connection error: ' .. err) end
end

-- ############################################################################

local function _read_response(self, options)
    if options and options.close == true then return end

    local res    = _read(self)
    local prefix = res:sub(1, -#res)
    local response_handler = protocol.prefixes[prefix]

    if not response_handler then 
        error("Unknown response prefix: " .. prefix)
    else
        return response_handler(self, res, options)
    end
end


local function _send_raw(self, buffer, options)
    -- TODO: optimize
    local bufferType = type(buffer)

    if bufferType == 'string' then
        _write(self, buffer)
    elseif bufferType == 'table' then
        _write(self, table.concat(buffer))
    else
        error('Argument error: ' .. bufferType)
    end

    return _read_response(self, options)
end

local function _send_inline(self, command, args, options)
    if args == nil then
        _write(self, command .. protocol.newline)
    else
        -- TODO: optimize
        local argsType = type(args)

        if argsType == 'string' then
            _write(self, command .. ' ' .. args .. protocol.newline)
        elseif argsType == 'table' then
            _write(self, command .. ' ' .. table.concat(args, ' ') .. protocol.newline)
        else
            error('Invalid type for arguments: ' .. argsType)
        end
    end

    return _read_response(self, options)
end

local function _send_bulk(self, command, args, data, options)
    -- TODO: optimize, and ensure that the type of data is string
    if type(args) == 'table' then
        args = table.concat(args, ' ')
    elseif args == nil then
        args = ' '
    end

    return _send_raw(self, 
        { command, ' ', args, ' ', #data, protocol.newline, data, protocol.newline }, 
    options)
end


local function _read_line(self, response, options)
    return response:sub(2)
end

local function _read_error(self, response, options)
    local err_line = response:sub(2)

    if err_line:sub(1, 3) == protocol.err then
        error(err_line:sub(5))
    else
        error(err_line)
    end
end

local function _read_bulk(self, response, options) 
    local str = response:sub(2)
    local len = tonumber(str)

    if not len then 
        error('Cannot parse ' .. str .. ' as data length.')
    else
        if len == -1 then return nil end
        local data = _read(self, len + 2)
        return data:sub(1, -3);
    end
end

local function _read_multibulk(self, response, options)
    local str = response:sub(2)

    -- TODO: add a check if the returned value is indeed a number
    local list_count = tonumber(str)

    if list_count == -1 then 
        return nil
    else
        local list = {}

        if list_count > 0 then 
            for i = 1, list_count do
                table.insert(list, i, _read_bulk(self, _read(self), options))
            end
        end

        return list
    end
end

local function _read_integer(self, response, options)
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

-- ############################################################################

protocol.prefixes = {
    ['+'] = _read_line, 
    ['-'] = _read_error, 
    ['$'] = _read_bulk, 
    ['*'] = _read_multibulk, 
    [':'] = _read_integer, 
}

-- ############################################################################

local function raw_cmd(self, buffer)
    return _send_raw(self, buffer .. protocol.newline)
end

local function ping(self)
    return _send_inline(self, protocol.commands.ping)
end

local function echo(self, data)
    return _send_bulk(self, protocol.commands.echo, nil, tostring(data))
end

local function set(self, key, value)
    return _send_bulk(self, protocol.commands.set, { key }, tostring(value))
end

local function set_preserve(self, key, value)
    return _send_bulk(self, protocol.commands.setnx, { key }, tostring(value))
end

local function get(self, key)
    return _send_inline(self, protocol.commands.get, key)
end

local function mget(self, keys)
    return _send_inline(self, protocol.commands.mget, keys)
end

local function incr(self, key)
    return _send_inline(self, protocol.commands.incr, key)
end

local function incr_by(self, key, step)
    return _send_inline(self, protocol.commands.incrby, { key, step })
end

local function decr(self, key)
    return _send_inline(self, protocol.commands.decr, key)
end

local function decr_by(self, key, step)
    return _send_inline(self, protocol.commands.decrby, { key, step })
end

local function exists(self, key)
    return toboolean(_send_inline(self, protocol.commands.exists, key))
end

local function delete(self, key)
    return toboolean(_send_inline(self, protocol.commands.del, key))
end

local function type(self, key)
    return _send_inline(self, protocol.commands.type, key)
end

local function keys(self, pattern)
    -- TODO: should return an array of keys (split the string by " ")
    return _send_inline(self, protocol.commands.keys, pattern)
end

local function randomkey(self, pattern)
    return _send_inline(self, protocol.commands.randomkey)
end

local function rename(self, oldname, newname)
    return _send_inline(self, protocol.commands.rename, { oldname, newname })
end

local function renamenx(self, oldname, newname)
    return _send_inline(self, protocol.commands.renamenx, { oldname, newname })
end

local function dbsize(self, oldname, newname)
    return _send_inline(self, protocol.commands.dbsize)
end

local function rpush(self, key, value)
    return _send_bulk(self, protocol.commands.rpush, { key }, tostring(value))
end

local function lpush(self, key, value)
    return _send_bulk(self, protocol.commands.lpush, { key }, tostring(value))
end

local function llen(self, key)
    return _send_inline(self, protocol.commands.llen, key)
end

local function lrange(self, key, start, last)
    return _send_inline(self, protocol.commands.lrange, { key, start, last })
end

local function ltrim(self, key, start, last)
    return _send_inline(self, protocol.commands.ltrim, { key, start, last })
end

local function lindex(self, key, index)
    return _send_inline(self, protocol.commands.lindex, { key, index })
end

local function lset(self, key, index, value)
    return _send_bulk(self, protocol.commands.lset, { key, index }, tostring(value))
end

local function lrem(self, key, count, value)
    return _send_bulk(self, protocol.commands.lrem, { key, count }, tostring(value))
end

local function lpop(self, key)
    return _send_inline(self, protocol.commands.lpop, key)
end

local function rpop(self, key)
    return _send_inline(self, protocol.commands.rpop, key)
end

local function sadd(self, key, member)
    return _send_inline(self, protocol.commands.sadd, { key, member })
end

local function srem(self, key, member)
    return _send_inline(self, protocol.commands.srem, { key, member })
end

local function scard(self, key)
    return _send_inline(self, protocol.commands.scard, key)
end

local function sismember(self, key, member)
    return _send_inline(self, protocol.commands.sismember, { key, member })
end

local function sinter(self, keys)
    return _send_inline(self, protocol.commands.sinter, keys)
end

local function sinterstore(self, keys)
    return _send_inline(self, protocol.commands.sinter, keys)
end

local function smembers(self, key)
    return _send_inline(self, protocol.commands.smembers, key)
end

local function select(self, index)
    return _send_inline(self, protocol.commands.select, tostring(index))
end

local function move(self, key, dbindex)
    return _send_inline(self, protocol.commands.move, { key, dbindex })
end

local function flushdb(self)
    return _send_inline(self, protocol.commands.flushdb)
end

local function flushall(self)
    return _send_inline(self, protocol.commands.flushall)
end

local function save(self)
    return _send_inline(self, protocol.commands.save)
end

local function bgsave(self)
    return _send_inline(self, protocol.commands.bgsave)
end

local function lastsave(self)
    return _send_inline(self, protocol.commands.lastsave)
end

local function shutdown(self) 
    -- TODO: specs says that redis reply with a status code on error, 
    -- but we are closing the connection soon after having sent the command.
    _send_inline(self, protocol.commands.shutdown, nil, {close = true})
end

local function info(self)
    local response, info = _send_inline(self, protocol.commands.info), {}
    response:gsub('([^\r\n]*)\r\n', function(kv) 
        local k,v = kv:match(('([^:]*):([^:]*)'):rep(1))
        info[k] = v
    end)
    return info
end

local function quit(self)
    _send_inline(self, protocol.commands.quit, nil, {close = true})
end

-- ############################################################################

function connect(host, port)
    local client_socket = socket.connect(host, port)

    if not client_socket then
        error('Could not connect to ' .. host .. ':' .. port)
    end

    -- TODO: way too ugly
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
        type         = type, 
        keys         = keys, 
        randomkey    = randomkey, 
        rename       = rename, 
        renamenx     = renamenx, 
        dbsize       = dbsize, 
        rpush        = rpush, 
        lpush        = lpush, 
        llen         = llen, 
        lrange       = lrange, 
        ltrim        = ltrim, 
        lindex       = lindex, 
        lset         = lset, 
        lrem         = lrem, 
        lpop         = lpop, 
        rpop         = rpop, 
        set_preserve = set_preserve, 
        sadd         = sadd,
        srem         = srem, 
        scard        = scard, 
        sismember    = sismember,
        sinter       = sinter, 
        sinterstore  = sinterstore, 
        smembers     = smembers, 
        select       = select, 
        move         = move, 
        flushdb      = flushdb, 
        flushall     = flushall,
        save         = save, 
        bgsave       = bgsave, 
        lastsave     = lastsave, 
        shutdown     = shutdown, 
        info         = info, 
        quit         = quit, 
    }
end
