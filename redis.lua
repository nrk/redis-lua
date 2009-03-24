module('Redis', package.seeall)

require('socket')

-- ########################################################################### --

local protocol = {
    newline  = '\r\n', 
    ok       = '+OK', 
    err      = '-ERR', 
    null     = 'nil', 

    commands = {
        ping    = 'PING', 
        echo    = 'ECHO', 
        set     = 'SET', 
        setnx   = 'SETNX', 
    }, 
}

-- ########################################################################### --

local function _send(client, buffer)
    local bufferType = type(buffer)

    if bufferType == 'string' then
        client.socket:send(buffer)
    elseif bufferType == 'table' then
        for _, chunk in pairs(buffer) do
            client.socket:send(chunk)
        end
    else
        error('Argument error for buffer: ' .. bufferType)
    end
end

local function _receive(client)
    local line, err = client.socket:receive('*l')
    if not err then return line end
end

local function _receive_len(client, len)
    local buffer, err = client.socket:receive(len)
    if not err then return buffer end
end

-- ########################################################################### --

local function _read_response(client, options)
    local res = _receive(client)
    local prefix = res:sub(1, -#res)

    local response_handler = protocol.prefixes[prefix]

    if not response_handler then 
        error("Unknown response prefix: " .. prefix)
    else
        return response_handler(client, res, options)
    end
end

local function _get_generic(client, response, options)
    return response:sub(2)
end

local function _get_error(client, response, options)
    return response:sub(2)
end

local function _get_value(client, response, options) 
    local str = response:sub(2)
    local len = tonumber(str)

    if not len then 
        error('Cannot parse ' .. str .. ' as data length.')
    else
        local data, err = client.socket:receive(len + 2)
        if not err then return data:sub(1, -3) end
    end
end

local function _get_list(client, response, options)
    local str = response:sub(2)
    local list_count = tonumber(str)    -- add a check if the returned value is indeed a number

    list = {}

    while list_count > 0 do
        table.concat(list, _get_value(client, _receive(client), options))
    end

    return list
end

local function _get_number(client, response, options)
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
    ['+'] = _get_generic, 
    ['-'] = _get_error, 
    ['$'] = _get_value, 
    ['*'] = _get_list, 
    [':'] = _get_number, 
}

-- ########################################################################### --

local function raw_cmd(client, buffer)
    _send(client, buffer)
end

local function ping(client)
    _send(client, protocol.commands.ping .. protocol.newline)
    return _read_response(client)
end

local function echo(client, value)
    _send(client, {
        protocol.commands.echo, ' ', #str, protocol.newline,
        str, protocol.newline
    })
    return _read_response(client)
end

local function _set(client, command, key, value)
    _send(client, {
        command, ' ' , key, ' ', #value, protocol.newline, 
        value, protocol.newline
    })
end

local function set(client, key, value)
    _set(client, protocol.commands.set, key, value)
    return _read_response(client)
end

local function set_preserve(client, key, value)
    _set(client, protocol.commands.setnx, key, value)
    return _read_response(client)
end

-- ########################################################################### --

function connect(host, port)
    local client_socket = socket.connect(host, port)

    if not client_socket then
        error('Could not connect to ' .. host .. ':' .. port)
    end

    return {
        socket  = client_socket, 

        raw_cmd = raw_cmd, 

        ping    = ping,
        echo    = echo, 
        set     = set, 
        set_preserve = set_preserve, 
    }
end
