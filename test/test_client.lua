package.path = "../src/?.lua;src/?.lua;" .. package.path

pcall(require, "luarocks.require")

local unpack = _G.unpack or table.unpack

local tsc = require "telescope"
local redis = require "redis"

local settings = {
    host     = '127.0.0.1',
    port     = 6379,
    database = 14,
    password = nil,
}

function table.merge(self, tbl2)
    local new_table = {}
    for k,v in pairs(self) do new_table[k] = v end
    for k,v in pairs(tbl2) do new_table[k] = v end
    return new_table
end

function table.keys(self)
    local keys = {}
    for k, _ in pairs(self) do table.insert(keys, k) end
    return keys
end

function table.values(self)
    local values = {}
    for _, v in pairs(self) do table.insert(values, v) end
    return values
end

function table.contains(self, value)
    for _, v in pairs(self) do
        if v == value then return true end
    end
    return false
end

function table.slice(self, first, length)
    -- TODO: must be improved
    local new_table = {}
    for i = first, first + length - 1 do
        table.insert(new_table, self[i])
    end
    return new_table
end

function table.compare(self, other)
    -- NOTE: the body of this function was taken and slightly adapted from
    --       Penlight (http://github.com/stevedonovan/Penlight)
    if #self ~= #other then return false end
    local visited = {}
    for i = 1, #self do
        local val, gotcha = self[i], nil
        for j = 1, #other do
            if not visited[j] then
                if (type(val) == 'table') then
                    if (table.compare(val, other[j])) then
                        gotcha = j
                        break
                    end
                else
                    if val == other[j] then
                        gotcha = j
                        break
                    end
                end
            end
        end
        if not gotcha then return false end
        visited[gotcha] = true
    end
    return true
end

function parse_version(version_str)
    local major, minor, patch, status = version_str:match('^(%d+)%.(%d+)%.(%d+)%-?(%w-)$')

    local info = {
        string  = version_str,
        compare =  function(self, other)
            if type(other) == 'string' then
                other = parse_version(other)
            end
            if self.unrecognized or other.unrecognized then
                error('Cannot compare versions')
            end

            for _, part in ipairs({ 'major', 'minor', 'patch' }) do
                if self[part] < other[part] then
                    return -1
                end
                if self[part] > other[part] then
                    return 1
                end
            end

            return 0
        end,
        is = function(self, op, other)
            local comparation = self:compare(other);
            if op == '<' then return comparation < 0 end
            if op == '<=' then return comparation <= 0 end
            if op == '=' then return comparation == 0 end
            if op == '>=' then return comparation >= 0 end
            if op == '>' then return comparation > 0 end

            error('Invalid comparison operator: '..op)
        end,
    }

    if major and minor and patch then
        info.major  = tonumber(major)
        info.minor  = tonumber(minor)
        info.patch  = tonumber(patch)
        if status then
            info.status = status
        end
    else
        info.unrecognized = true
    end

    return info
end

local utils = {
    create_client = function(parameters)
        if parameters == nil then
            parameters = settings
        end

        local client = redis.connect(parameters.host, parameters.port)
        if parameters.password then client:auth(parameters.password) end
        if parameters.database then client:select(parameters.database) end
        client:flushdb()

        local info = client:info()
        local version = parse_version(info.redis_version or info.server.redis_version)

        if version:is('<', '1.2.0') then
            error("redis-lua does not support Redis < 1.2.0 (current: "..version.string..")")
        end

        return client, version
    end,
    rpush_return = function(client, key, values, wipe)
        if wipe then client:del(key) end
        for _, v in ipairs(values) do
            client:rpush(key, v)
        end
        return values
    end,
    sadd_return = function(client, key, values, wipe)
        if wipe then client:del(key) end
        for _, v in ipairs(values) do
            client:sadd(key, v)
        end
        return values
    end,
    zadd_return = function(client, key, values, wipe)
        if wipe then client:del(key) end
        for k, v in pairs(values) do
            client:zadd(key, v, k)
        end
        return values
    end,
    sleep = function(sec)
        socket.select(nil, nil, sec)
    end,
}

local shared = {
    kvs_table = function()
        return {
            foo    = 'bar',
            hoge   = 'piyo',
            foofoo = 'barbar',
        }
    end,
    kvs_ns_table = function()
        return {
            ['metavars:foo']    = 'bar',
            ['metavars:hoge']   = 'piyo',
            ['metavars:foofoo'] = 'barbar',
        }
    end,
    lang_table = function()
        return {
            italian  = "ciao",
            english  = "hello",
            japanese = "こんいちは！",
        }
    end,
    numbers = function()
        return { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' }
    end,
    zset_sample = function()
        return { a = -10, b = 0, c = 10, d = 20, e = 20, f = 30 }
    end,
}

tsc.make_assertion("table_values", "'%s' to have the same values as '%s'", table.compare)
tsc.make_assertion("response_queued", "to be queued", function(response)
    if type(response) == 'table' and response.queued == true then
        return true
    else
        return false
    end
end)
tsc.make_assertion("error_message", "result to be an error with the expected message", function(msg, f)
    local ok, err = pcall(f)
    return not ok and err:match(msg)
end)

-- ------------------------------------------------------------------------- --

context("Client initialization", function()
    test("Can connect successfully", function()
        local client = redis.connect(settings.host, settings.port)
        assert_type(client, 'table')
        assert_true(table.contains(table.keys(client.network), 'socket'))

        client.network.socket:send("PING\r\n")
        assert_equal(client.network.socket:receive('*l'), '+PONG')
    end)

    test("Can handle connection failures", function()
        assert_error_message("could not connect to .*:%d+ %[connection refused%]", function()
            redis.connect(settings.host, settings.port + 100)
        end)
    end)

    test("Accepts an URI for connection parameters", function()
        local uri = 'redis://'..settings.host..':'..settings.port
        local client = redis.connect(uri)
        assert_type(client, 'table')
    end)

    test("Accepts a table for connection parameters", function()
        local client = redis.connect(settings)
        assert_type(client, 'table')
    end)

    test("Can use an already connected socket", function()
        local connection = require('socket').tcp()
        connection:connect(settings.host, settings.port)

        local client = redis.connect({ socket = connection })
        assert_type(client, 'table')
        assert_true(client:ping())
    end)

    test("Can specify a timeout for connecting", function()
        local time, timeout = os.time(), 2;

        assert_error_message("could not connect to .*:%d+ %[timeout%]", function()
            redis.connect({ host = '169.254.255.255', timeout = timeout })
        end)

        assert_equal(time + timeout, os.time())
    end)
end)

context("Client features", function()
    before(function()
        client = utils.create_client(settings)
    end)

    test("Send raw commands", function()
        assert_equal(client:raw_cmd("PING\r\n"), 'PONG')
        assert_true(client:raw_cmd("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
        assert_equal(client:raw_cmd("GET foo\r\n"), 'bar')
    end)

    test("Create a new unbound command object", function()
        local cmd = redis.command('doesnotexist')
        assert_nil(client.doesnotexist)
        assert_error(function() cmd(client) end)

        local cmd = redis.command('ping', {
            response = function(response) return response == 'PONG' end
        })
        assert_equal(cmd(client), true)
    end)

    test("Define commands at module level", function()
        redis.commands.doesnotexist = redis.command('doesnotexist')
        local client2 = utils.create_client(settings)

        redis.commands.doesnotexist = nil
        local client3 = utils.create_client(settings)

        assert_nil(client.doesnotexist)
        assert_not_nil(client2.doesnotexist)
        assert_nil(client3.doesnotexist)
    end)

    test("Define commands at module level (OLD)", function()
        redis.define_command('doesnotexist')
        local client2 = utils.create_client(settings)

        redis.undefine_command('doesnotexist')
        local client3 = utils.create_client(settings)

        assert_nil(client.doesnotexist)
        assert_not_nil(client2.doesnotexist)
        assert_nil(client3.doesnotexist)
    end)

    test("Define new commands at client instance level", function()
        client.doesnotexist = redis.command('doesnotexist')
        assert_not_nil(client.doesnotexist)
        assert_error(function() client:doesnotexist() end)

        client.doesnotexist = nil
        assert_nil(client.doesnotexist)

        client.ping = redis.command('ping')
        assert_not_nil(client.ping)
        assert_equal(client:ping(), 'PONG')

        client.ping = redis.command('ping', {
            request = client.requests.multibulk
        })
        assert_not_nil(client.ping)
        assert_equal(client:ping(), 'PONG')

        client.ping = redis.command('ping', {
            request  = client.requests.multibulk,
            response = function(reply) return reply == 'PONG' end
        })
        assert_not_nil(client.ping)
        assert_true(client:ping())
    end)

    test("Define new commands at client instance level (OLD)", function()
        client:define_command('doesnotexist')
        assert_not_nil(client.doesnotexist)
        assert_error(function() client:doesnotexist() end)

        client:undefine_command('doesnotexist')
        assert_nil(client.doesnotexist)

        client:define_command('ping')
        assert_not_nil(client.ping)
        assert_equal(client:ping(), 'PONG')

        client:define_command('ping', {
            request = client.requests.multibulk
        })
        assert_not_nil(client.ping)
        assert_equal(client:ping(), 'PONG')

        client:define_command('ping', {
            request  = client.requests.multibulk,
            response = function(reply) return reply == 'PONG' end
        })
        assert_not_nil(client.ping)
        assert_true(client:ping())
    end)

    test("Pipelining commands", function()
        local replies, count = client:pipeline(function(p)
            p:ping()
            p:exists('counter')
            p:incrby('counter', 10)
            p:incrby('counter', 30)
            p:exists('counter')
            p:get('counter')
            p:mset({ foo = 'bar', hoge = 'piyo'})
            p:del('foo', 'hoge')
            p:mget('does_not_exist', 'counter')
            p:info()
            p:get('nilkey')
        end)

        assert_type(replies, 'table')
        assert_equal(count, 11)
        assert_equal(#replies, 10)
        assert_true(replies[1])
        assert_type(replies[9], 'table')
        assert_equal(replies[9][2], '40')
        assert_type(replies[10], 'table')
    end)

    after(function()
        client:quit()
    end)
end)

context("Redis commands", function()
    before(function()
        client, version = utils.create_client(settings)
    end)

    after(function()
        client:quit()
    end)

    context("Connection related commands", function()
        test("PING (client:ping)", function()
            assert_true(client:ping())
        end)

        test("ECHO (client:echo)", function()
            local str_ascii, str_utf8 = "Can you hear me?", "聞こえますか？"

            assert_equal(client:echo(str_ascii), str_ascii)
            assert_equal(client:echo(str_utf8), str_utf8)
        end)

        test("SELECT (client:select)", function()
            if not settings.database then return end

            assert_true(client:select(0))
            assert_true(client:select(settings.database))
            assert_error(function() client:select(100) end)
            assert_error(function() client:select(-1) end)
        end)
    end)

    context("Commands operating on the key space", function()
        test("KEYS (client:keys)", function()
            local kvs_prefixed   = shared.kvs_ns_table()
            local kvs_unprefixed = { aaa = 1, aba = 2, aca = 3 }
            local kvs_all = table.merge(kvs_prefixed, kvs_unprefixed)

            client:mset(kvs_all)

            assert_empty(client:keys('nokeys:*'))
            assert_table_values(
                table.values(client:keys('*')),
                table.keys(kvs_all)
            )
            assert_table_values(
                table.values(client:keys('metavars:*')),
                table.keys(kvs_prefixed)
            )
            assert_table_values(
                table.values(client:keys('a?a')),
                table.keys(kvs_unprefixed)
            )
        end)

        test("EXISTS (client:exists)", function()
            client:set('foo', 'bar')

            assert_true(client:exists('foo'))
            assert_false(client:exists('hoge'))
        end)

        test("DEL (client:del)", function()
            client:mset(shared.kvs_table())

            assert_equal(client:del('doesnotexist'), 0)
            assert_equal(client:del('foofoo'), 1)
            assert_equal(client:del('foo', 'hoge', 'doesnotexist'), 2)
        end)

        test("TYPE (client:type)", function()
            assert_equal(client:type('doesnotexist'), 'none')

            client:set('fooString', 'bar')
            assert_equal(client:type('fooString'), 'string')

            client:rpush('fooList', 'bar')
            assert_equal(client:type('fooList'), 'list')

            client:sadd('fooSet', 'bar')
            assert_equal(client:type('fooSet'), 'set')

            client:zadd('fooZSet', 0, 'bar')
            assert_equal(client:type('fooZSet'), 'zset')

            if version:is('>=', '2.0.0') then
                client:hset('fooHash', 'value', 'bar')
                assert_equal('hash', client:type('fooHash'))
            end
        end)

        test("RANDOMKEY (client:randomkey)", function()
            local kvs = shared.kvs_table()

            assert_nil(client:randomkey())
            client:mset(kvs)
            assert_true(table.contains(table.keys(kvs), client:randomkey()))
        end)

        test("RENAME (client:rename)", function()
            local kvs = shared.kvs_table()
            client:mset(kvs)

            assert_true(client:rename('hoge', 'hogehoge'))
            assert_false(client:exists('hoge'))
            assert_equal(client:get('hogehoge'), 'piyo')

            -- rename overwrites existing keys
            assert_true(client:rename('foo', 'foofoo'))
            assert_false(client:exists('foo'))
            assert_equal(client:get('foofoo'), 'bar')

            -- rename fails when the key does not exist
            assert_error(function()
                client:rename('doesnotexist', 'fuga')
            end)
        end)

        test("RENAMENX (client:renamenx)", function()
            local kvs = shared.kvs_table()
            client:mset(kvs)

            assert_true(client:renamenx('hoge', 'hogehoge'))
            assert_false(client:exists('hoge'))
            assert_equal(client:get('hogehoge'), 'piyo')

            -- rename overwrites existing keys
            assert_false(client:renamenx('foo', 'foofoo'))
            assert_true(client:exists('foo'))

            -- rename fails when the key does not exist
            assert_error(function()
                client:renamenx('doesnotexist', 'fuga')
            end)
        end)

        test("TTL (client:ttl)", function()
            client:set('foo', 'bar')
            assert_equal(client:ttl('foo'), -1)

            assert_true(client:expire('foo', 5))
            assert_lte(client:ttl('foo'), 5)
        end)

        test("PTTL (client:pttl)", function()
            if version:is('<', '2.5.0') then return end

            client:set('foo', 'bar')
            assert_equal(client:pttl('foo'), -1)

            local ttl = 5
            assert_true(client:expire('foo', ttl))
            assert_lte(client:pttl('foo'), 5 * 1000)
            assert_gte(client:pttl('foo'), 5 * 1000 - 500)
        end)

        test("EXPIRE (client:expire)", function()
            client:set('foo', 'bar')
            assert_true(client:expire('foo', 2))
            assert_true(client:exists('foo'))
            assert_lte(client:ttl('foo'), 2)
            utils.sleep(3)
            assert_false(client:exists('foo'))

            client:set('foo', 'bar')
            assert_true(client:expire('foo', 100))
            utils.sleep(3)
            assert_lte(client:ttl('foo'), 97)

            assert_true(client:expire('foo', -100))
            assert_false(client:exists('foo'))
        end)

        test("PEXPIRE (client:pexpire)", function()
            if version:is('<', '2.5.0') then return end

            local ttl = 1
            client:set('foo', 'bar')
            assert_true(client:pexpire('foo', ttl * 1000))
            assert_true(client:exists('foo'))
            assert_lte(client:pttl('foo'), ttl * 1000)
            assert_gte(client:pttl('foo'), ttl * 1000 - 500)
            utils.sleep(ttl)
            assert_false(client:exists('foo'))
        end)

        test("EXPIREAT (client:expireat)", function()
            client:set('foo', 'bar')
            assert_true(client:expireat('foo', os.time() + 2))
            assert_lte(client:ttl('foo'), 2)
            utils.sleep(3)
            assert_false(client:exists('foo'))

            client:set('foo', 'bar')
            assert_true(client:expireat('foo', os.time() - 100))
            assert_false(client:exists('foo'))
        end)

        test("PEXPIREAT (client:pexpireat)", function()
            if version:is('<', '2.5.0') then return end

            local ttl = 2
            client:set('foo', 'bar')
            assert_true(client:pexpireat('foo', os.time() + ttl * 1000))
            assert_lte(client:pttl('foo'), ttl * 1000)
            utils.sleep(ttl + 1)
            assert_false(client:exists('foo'))

            client:set('foo', 'bar')
            assert_true(client:pexpireat('foo', os.time() - 100 * 1000))
            assert_false(client:exists('foo'))
        end)

        test("MOVE (client:move)", function()
            if not settings.database then return end

            local other_db = settings.database + 1
            client:set('foo', 'bar')
            client:select(other_db)
            client:flushdb()
            client:select(settings.database)

            assert_true(client:move('foo', other_db))
            assert_false(client:move('foo', other_db))
            assert_false(client:move('doesnotexist', other_db))

            client:set('hoge', 'piyo')
            assert_error(function() client:move('hoge', 100) end)
        end)

        test("DBSIZE (client:dbsize)", function()
            assert_equal(client:dbsize(), 0)
            client:mset(shared.kvs_table())
            assert_greater_than(client:dbsize(), 0)
        end)

        test("PERSIST (client:persist)", function()
            if version:is('<', '2.1.0') then return end

            client:set('foo', 'bar')

            assert_true(client:expire('foo', 1))
            assert_equal(client:ttl('foo'), 1)
            assert_true(client:persist('foo'))
            assert_equal(client:ttl('foo'), -1)

            assert_false(client:persist('foo'))
            assert_false(client:persist('foobar'))
        end)
    end)

    context("Commands operating on the key space - SORT", function()
        -- TODO: missing tests for params GET and BY

        before(function()
            -- TODO: code duplication!
            list01, list01_values = "list01", { "4","2","3","5","1" }
            for _,v in ipairs(list01_values) do client:rpush(list01,v) end

            list02, list02_values = "list02", { "1","10","2","20","3","30" }
            for _,v in ipairs(list02_values) do client:rpush(list02,v) end
        end)

        test("SORT (client:sort)", function()
            local sorted = client:sort(list01)
            assert_table_values(sorted, { "1","2","3","4","5" })
        end)

        test("SORT (client:sort) with parameter ASC/DESC", function()
            assert_table_values(client:sort(list01, { sort = 'asc'}),  { "1","2","3","4","5" })
            assert_table_values(client:sort(list01, { sort = 'desc'}), { "5","4","3","2","1" })
        end)

        test("SORT (client:sort) with parameter LIMIT", function()
            assert_table_values(client:sort(list01, { limit = { 0,3 } }), { "1","2", "3" })
            assert_table_values(client:sort(list01, { limit = { 3,2 } }), { "4","5" })
        end)

        test("SORT (client:sort) with parameter ALPHA", function()
            assert_table_values(client:sort(list02, { alpha = false }), { "1","2","3","10","20","30" })
            assert_table_values(client:sort(list02, { alpha = true }),  { "1","10","2","20","3","30" })
        end)

        test("SORT (client:sort) with parameter GET", function()
            client:rpush('uids', 1003)
            client:rpush('uids', 1001)
            client:rpush('uids', 1002)
            client:rpush('uids', 1000)
            local sortget = {
                ['uid:1000'] = 'foo',  ['uid:1001'] = 'bar',
                ['uid:1002'] = 'hoge', ['uid:1003'] = 'piyo',
            }
            client:mset(sortget)

            assert_table_values(client:sort('uids', { get = 'uid:*' }), table.values(sortget))
            assert_table_values(client:sort('uids', { get = { 'uid:*' } }), table.values(sortget))
        end)

        test("SORT (client:sort) with multiple parameters", function()
            assert_table_values(client:sort(list02, {
                alpha = false,
                sort  = 'desc',
                limit = { 1, 4 }
            }), { "20","10","3","2" })
        end)

        test("SORT (client:sort) with parameter STORE", function()
            assert_equal(client:sort(list01, { store = 'list01_ordered' }), 5)
            assert_true(client:exists('list01_ordered'))
        end)
    end)

    context("Commands operating on string values", function()
        test("SET (client:set)", function()
            assert_true(client:set('foo', 'bar'))
            assert_equal(client:get('foo'), 'bar')
        end)

        test("GET (client:get)", function()
            client:set('foo', 'bar')

            assert_equal(client:get('foo'), 'bar')
            assert_nil(client:get('hoge'))

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:get('metavars')
            end)
        end)

        test("SETNX (client:setnx)", function()
            assert_true(client:setnx('foo', 'bar'))
            assert_false(client:setnx('foo', 'baz'))
            assert_equal(client:get('foo'), 'bar')
        end)

        test("SETEX (client:setex)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:setex('foo', 10, 'bar'))
            assert_true(client:exists('foo'))
            assert_lte(client:ttl('foo'), 10)

            assert_true(client:setex('hoge', 1, 'piyo'))
            utils.sleep(2)
            assert_false(client:exists('hoge'))

            assert_error(function() client:setex('hoge', 2.5, 'piyo') end)
            assert_error(function() client:setex('hoge', 0, 'piyo') end)
            assert_error(function() client:setex('hoge', -10, 'piyo') end)
        end)

        test("PSETEX (client:psetex)", function()
            if version:is('<', '2.5.0') then return end

            local ttl = 10 * 1000
            assert_true(client:psetex('foo', ttl, 'bar'))
            assert_true(client:exists('foo'))
            assert_lte(client:pttl('foo'), ttl)
            assert_gte(client:pttl('foo'), ttl - 500)

            assert_true(client:psetex('hoge', 1 * 1000, 'piyo'))
            utils.sleep(2)
            assert_false(client:exists('hoge'))

            assert_error(function() client:psetex('hoge', 2.5, 'piyo') end)
            assert_error(function() client:psetex('hoge', 0, 'piyo') end)
            assert_error(function() client:psetex('hoge', -10, 'piyo') end)
        end)

        test("MSET (client:mset)", function()
            local kvs = shared.kvs_table()

            assert_true(client:mset(kvs))
            for k,v in pairs(kvs) do
                assert_equal(client:get(k), v)
            end

            assert_true(client:mset('a', '1', 'b', '2', 'c', '3'))
            assert_equal(client:get('a'), '1')
            assert_equal(client:get('b'), '2')
            assert_equal(client:get('c'), '3')
        end)

        test("MSETNX (client:msetnx)", function()
           assert_true(client:msetnx({ a = '1', b = '2' }))
           assert_false(client:msetnx({ c = '3', a = '100'}))
           assert_equal(client:get('a'), '1')
           assert_equal(client:get('b'), '2')
        end)

        test("MGET (client:mget)", function()
            local kvs = shared.kvs_table()
            local keys, values = table.keys(kvs), table.values(kvs)

            assert_true(client:mset(kvs))
            assert_table_values(client:mget(unpack(keys)), values)
        end)

        test("GETSET (client:getset)", function()
            assert_nil(client:getset('foo', 'bar'))
            assert_equal(client:getset('foo', 'barbar'), 'bar')
            assert_equal(client:getset('foo', 'baz'), 'barbar')
        end)

        test("INCR (client:incr)", function()
            assert_equal(client:incr('foo'), 1)
            assert_equal(client:incr('foo'), 2)

            assert_true(client:set('hoge', 'piyo'))

            if version:is('<', '2.0.0') then
                assert_equal(client:incr('hoge'), 1)
            else
                assert_error(function()
                    client:incr('hoge')
                end)
            end
        end)

        test("INCRBY (client:incrby)", function()
            client:set('foo', 2)
            assert_equal(client:incrby('foo', 20), 22)
            assert_equal(client:incrby('foo', -12), 10)
            assert_equal(client:incrby('foo', -110), -100)
        end)

        test("INCRBYFLOAT (client:incrbyfloat)", function()
            if version:is('<', '2.5.0') then return end

            client:set('foo', 2)
            assert_equal(client:incrbyfloat('foo', 20.123), 22.123)
            assert_equal(client:incrbyfloat('foo', -12.123), 10)
            assert_equal(client:incrbyfloat('foo', -110.01), -100.01)
        end)

        test("DECR (client:decr)", function()
            assert_equal(client:decr('foo'), -1)
            assert_equal(client:decr('foo'), -2)

            assert_true(client:set('hoge', 'piyo'))
            if version:is('<', '2.0.0') then
                assert_equal(client:decr('hoge'), -1)
            else
                assert_error(function()
                    client:decr('hoge')
                end)
            end
        end)

        test("DECRBY (client:decrby)", function()
            client:set('foo', -2)
            assert_equal(client:decrby('foo', 20), -22)
            assert_equal(client:decrby('foo', -12), -10)
            assert_equal(client:decrby('foo', -110), 100)
        end)

        test("APPEND (client:append)", function()
            if version:is('<', '2.0.0') then return end

            client:set('foo', 'bar')
            assert_equal(client:append('foo', '__'), 5)
            assert_equal(client:append('foo', 'bar'), 8)
            assert_equal(client:get('foo'), 'bar__bar')

            assert_equal(client:append('hoge', 'piyo'), 4)
            assert_equal(client:get('hoge'), 'piyo')

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:append('metavars', 'bar')
            end)
        end)

        test("SUBSTR (client:substr)", function()
            if version:is('<', '2.0.0') then return end

            client:set('var', 'foobar')
            assert_equal(client:substr('var', 0, 2), 'foo')
            assert_equal(client:substr('var', 3, 5), 'bar')
            assert_equal(client:substr('var', -3, -1), 'bar')

            assert_equal(client:substr('var', 5, 0), '')

            client:set('numeric', 123456789)
            assert_equal(client:substr('numeric', 0, 4), '12345')

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:substr('metavars', 0, 3)
            end)
        end)

        test("STRLEN (client:strlen)", function()
            if version:is('<', '2.1.0') then return end

            client:set('var', 'foobar')
            assert_equal(client:strlen('var'), 6)
            assert_equal(client:append('var', '___'), 9)
            assert_equal(client:strlen('var'), 9)

            assert_error(function()
                client:rpush('metavars', 'foo')
                qclient:strlen('metavars')
            end)
        end)

        test("SETRANGE (client:setrange)", function()
            if version:is('<', '2.1.0') then return end

            assert_equal(client:setrange('var', 0, 'foobar'), 6)
            assert_equal(client:get('var'), 'foobar')
            assert_equal(client:setrange('var', 3, 'foo'), 6)
            assert_equal(client:get('var'), 'foofoo')
            assert_equal(client:setrange('var', 10, 'barbar'), 16)
            assert_equal(client:get('var'), "foofoo\0\0\0\0barbar")

            assert_error(function()
                client:setrange('var', -1, 'bogus')
            end)

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:setrange('metavars', 0, 'hoge')
            end)
        end)

        test("GETRANGE (client:getrange)", function()
            if version:is('<', '2.1.0') then return end

            client:set('var', 'foobar')
            assert_equal(client:getrange('var', 0, 2), 'foo')
            assert_equal(client:getrange('var', 3, 5), 'bar')
            assert_equal(client:getrange('var', -3, -1), 'bar')

            assert_equal(client:substr('var', 5, 0), '')

            client:set('numeric', 123456789)
            assert_equal(client:getrange('numeric', 0, 4), '12345')

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:getrange('metavars', 0, 3)
            end)
        end)

        test("SETBIT (client:setbit)", function()
            if version:is('<', '2.1.0') then return end

            assert_equal(client:setbit('binary', 31, 1), 0)
            assert_equal(client:setbit('binary', 0, 1), 0)
            assert_equal(client:strlen('binary'), 4)
            assert_equal(client:get('binary'), "\128\0\0\1")

            assert_equal(client:setbit('binary', 0, 0), 1)
            assert_equal(client:setbit('binary', 0, 0), 0)
            assert_equal(client:get('binary'), "\0\0\0\1")

            assert_error(function()
              client:setbit('binary', -1, 1)
            end)

            assert_error(function()
                client:setbit('binary', 'invalid', 1)
            end)

            assert_error(function()
                client:setbit('binary', 'invalid', 1)
            end)

            assert_error(function()
                client:setbit('binary', 15, 255)
            end)

            assert_error(function()
                client:setbit('binary', 15, 'invalid')
            end)

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:setbit('metavars', 0, 1)
            end)
        end)

        test("GETBIT (client:getbit)", function()
            if version:is('<', '2.1.0') then return end

            client:set('binary', "\128\0\0\1")

            assert_equal(client:getbit('binary', 0), 1)
            assert_equal(client:getbit('binary', 15), 0)
            assert_equal(client:getbit('binary', 31), 1)
            assert_equal(client:getbit('binary', 63), 0)

            assert_error(function()
              client:getbit('binary', -1)
            end)

            assert_error(function()
              client:getbit('binary', 'invalid')
            end)

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:getbit('metavars', 0)
            end)
        end)

        test("BITOP (client:bitop)", function()
            if version:is('<', '2.5.10') then return end

            client:set('foo', 'a')
            client:set('bar', 'b')

            client:bitop('AND', 'foo&bar', 'foo', 'bar')
            client:bitop('OR', 'foo|bar', 'foo', 'bar')
            client:bitop('XOR', 'foo^bar', 'foo', 'bar')
            client:bitop('NOT', '-foo', 'foo')

            assert_equal(client:get('foo&bar'), '\96')
            assert_equal(client:get('foo|bar'), '\99')
            assert_equal(client:get('foo^bar'), '\3')
            assert_equal(client:get('-foo'), '\158')
        end)

        test("BITCOUNT (client:bitcount)", function()
            if version:is('<', '2.5.10') then return end

            client:set('foo', 'abcde')

            assert_equal(client:bitcount('foo', 1, 3), 10)
            assert_equal(client:bitcount('foo', 0, -1), 17)
        end)
    end)

    context("Commands operating on lists", function()
        test("RPUSH (client:rpush)", function()
            if version:is('<', '2.0.0') then
                assert_true(client:rpush('metavars', 'foo'))
                assert_true(client:rpush('metavars', 'hoge'))
            else
                assert_equal(client:rpush('metavars', 'foo'), 1)
                assert_equal(client:rpush('metavars', 'hoge'), 2)
            end
            assert_error(function()
                client:set('foo', 'bar')
                client:rpush('foo', 'baz')
            end)
        end)

        test("RPUSHX (client:rpushx)", function()
            if version:is('<', '2.1.0') then return end

            assert_equal(client:rpushx('numbers', 1), 0)
            assert_equal(client:rpush('numbers', 2), 1)
            assert_equal(client:rpushx('numbers', 3), 2)
            assert_equal(client:llen('numbers'), 2)
            assert_table_values(client:lrange('numbers', 0, -1), { '2', '3' })

            assert_error(function()
                client:set('foo', 'bar')
                client:rpushx('foo', 'baz')
            end)
        end)

        test("LPUSH (client:lpush)", function()
            if version:is('<', '2.0.0') then
                assert_true(client:lpush('metavars', 'foo'))
                assert_true(client:lpush('metavars', 'hoge'))
            else
                assert_equal(client:lpush('metavars', 'foo'), 1)
                assert_equal(client:lpush('metavars', 'hoge'), 2)
            end
            assert_error(function()
                client:set('foo', 'bar')
                client:lpush('foo', 'baz')
            end)
        end)

        test("LPUSHX (client:lpushx)", function()
            if version:is('<', '2.1.0') then return end

            assert_equal(client:lpushx('numbers', 1), 0)
            assert_equal(client:lpush('numbers', 2), 1)
            assert_equal(client:lpushx('numbers', 3), 2)

            assert_equal(client:llen('numbers'), 2)
            assert_table_values(client:lrange('numbers', 0, -1), { '3', '2' })

            assert_error(function()
                client:set('foo', 'bar')
                client:lpushx('foo', 'baz')
            end)
        end)

        test("LLEN (client:llen)", function()
            local kvs = shared.kvs_table()
            for _, v in pairs(kvs) do
                client:rpush('metavars', v)
            end

            assert_equal(client:llen('metavars'), 3)
            assert_equal(client:llen('doesnotexist'), 0)
            assert_error(function()
                client:set('foo', 'bar')
                client:llen('foo')
            end)
        end)

        test("LRANGE (client:lrange)", function()
            local numbers = utils.rpush_return(client, 'numbers', shared.numbers())

            assert_table_values(client:lrange('numbers', 0, 3), table.slice(numbers, 1, 4))
            assert_table_values(client:lrange('numbers', 4, 8), table.slice(numbers, 5, 5))
            assert_table_values(client:lrange('numbers', 0, 0), table.slice(numbers, 1, 1))
            assert_empty(client:lrange('numbers', 1, 0))
            assert_table_values(client:lrange('numbers', 0, -1), numbers)
            assert_table_values(client:lrange('numbers', 5, -5), { '5' })
            assert_empty(client:lrange('numbers', 7, -5))
            assert_table_values(client:lrange('numbers', -5, -2), table.slice(numbers, 6, 4))
            assert_table_values(client:lrange('numbers', -100, 100), numbers)
        end)

        test("LTRIM (client:ltrim)", function()
            local numbers = utils.rpush_return(client, 'numbers', shared.numbers(), true)
            assert_true(client:ltrim('numbers', 0, 2))
            assert_table_values(client:lrange('numbers', 0, -1), table.slice(numbers, 1, 3))

            local numbers = utils.rpush_return(client, 'numbers', shared.numbers(), true)
            assert_true(client:ltrim('numbers', 5, 9))
            assert_table_values(client:lrange('numbers', 0, -1), table.slice(numbers, 6, 5))

            local numbers = utils.rpush_return(client, 'numbers', shared.numbers(), true)
            assert_true(client:ltrim('numbers', 0, -6))
            assert_table_values(client:lrange('numbers', 0, -1), table.slice(numbers, 1, 5))

            local numbers = utils.rpush_return(client, 'numbers', shared.numbers(), true)
            assert_true(client:ltrim('numbers', -5, -3))
            assert_table_values(client:lrange('numbers', 0, -1), table.slice(numbers, 6, 3))

            local numbers = utils.rpush_return(client, 'numbers', shared.numbers(), true)
            assert_true(client:ltrim('numbers', -100, 100))
            assert_table_values(client:lrange('numbers', 0, -1), numbers)

            assert_error(function()
                client:set('foo', 'bar')
                client:ltrim('foo', 0, 1)
            end)
        end)

        test("LINDEX (client:lindex)", function()
            local numbers = utils.rpush_return(client, 'numbers', shared.numbers())

            assert_equal(client:lindex('numbers', 0), numbers[1])
            assert_equal(client:lindex('numbers', 5), numbers[6])
            assert_equal(client:lindex('numbers', 9), numbers[10])
            assert_nil(client:lindex('numbers', 100))

            assert_equal(client:lindex('numbers', -0), numbers[1])
            assert_equal(client:lindex('numbers', -1), numbers[10])
            assert_equal(client:lindex('numbers', -3), numbers[8])
            assert_nil(client:lindex('numbers', -100))

            assert_error(function()
                client:set('foo', 'bar')
                client:lindex('foo', 0)
            end)
        end)

        test("LSET (client:lset)", function()
            utils.rpush_return(client, 'numbers', shared.numbers())

            assert_true(client:lset('numbers', 5, -5))
            assert_equal(client:lindex('numbers', 5), '-5')

            assert_error(function()
                client:lset('numbers', 99, 99)
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:lset('foo', 0, 0)
            end)
        end)

        test("LREM (client:lrem)", function()
            local mixed = { '0', '_', '2', '_', '4', '_', '6', '_' }

            utils.rpush_return(client, 'mixed', mixed, true)
            assert_equal(client:lrem('mixed', 2, '_'), 2)
            assert_table_values(client:lrange('mixed', 0, -1), { '0', '2', '4', '_', '6', '_' })

            utils.rpush_return(client, 'mixed', mixed, true)
            assert_equal(client:lrem('mixed', 0, '_'), 4)
            assert_table_values(client:lrange('mixed', 0, -1), { '0', '2', '4', '6' })

            utils.rpush_return(client, 'mixed', mixed, true)
            assert_equal(client:lrem('mixed', -2, '_'), 2)
            assert_table_values(client:lrange('mixed', 0, -1), { '0', '_', '2', '_', '4', '6' })

            utils.rpush_return(client, 'mixed', mixed, true)
            assert_equal(client:lrem('mixed', 2, '|'), 0)
            assert_table_values(client:lrange('mixed', 0, -1), mixed)

            assert_equal(client:lrem('doesnotexist', 2, '_'), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:lrem('foo', 0, 0)
            end)
        end)

        test("LPOP (client:lpop)", function()
            local numbers = utils.rpush_return(client, 'numbers', { '0', '1', '2', '3', '4' })

            assert_equal(client:lpop('numbers'), numbers[1])
            assert_equal(client:lpop('numbers'), numbers[2])
            assert_equal(client:lpop('numbers'), numbers[3])

            assert_table_values(client:lrange('numbers', 0, -1), { '3', '4' })

            client:lpop('numbers')
            client:lpop('numbers')
            assert_nil(client:lpop('numbers'))

            assert_nil(client:lpop('doesnotexist'))

            assert_error(function()
                client:set('foo', 'bar')
                client:lpop('foo')
            end)
        end)

        test("RPOP (client:rpop)", function()
            local numbers = utils.rpush_return(client, 'numbers', { '0', '1', '2', '3', '4' })

            assert_equal(client:rpop('numbers'), numbers[5])
            assert_equal(client:rpop('numbers'), numbers[4])
            assert_equal(client:rpop('numbers'), numbers[3])

            assert_table_values(client:lrange('numbers', 0, -1), { '0', '1' })

            client:rpop('numbers')
            client:rpop('numbers')
            assert_nil(client:rpop('numbers'))

            assert_nil(client:rpop('doesnotexist'))

            assert_error(function()
                client:set('foo', 'bar')
                client:rpop('foo')
            end)
        end)

        test("RPOPLPUSH (client:rpoplpush)", function()
            local numbers = utils.rpush_return(client, 'numbers', { '0', '1', '2' }, true)
            assert_equal(client:llen('temporary'), 0)
            assert_equal(client:rpoplpush('numbers', 'temporary'), '2')
            assert_equal(client:rpoplpush('numbers', 'temporary'), '1')
            assert_equal(client:rpoplpush('numbers', 'temporary'), '0')
            assert_equal(client:llen('numbers'), 0)
            assert_equal(client:llen('temporary'), 3)

            local numbers = utils.rpush_return(client, 'numbers', { '0', '1', '2' }, true)
            client:rpoplpush('numbers', 'numbers')
            client:rpoplpush('numbers', 'numbers')
            client:rpoplpush('numbers', 'numbers')
            assert_table_values(client:lrange('numbers', 0, -1), numbers)

            assert_nil(client:rpoplpush('doesnotexist1', 'doesnotexist2'))

            assert_error(function()
                client:set('foo', 'bar')
                client:rpoplpush('foo', 'hoge')
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:rpoplpush('temporary', 'foo')
            end)
        end)

        test("BLPOP (client:blpop)", function()
            if version:is('<', '2.0.0') then return end
            -- TODO: implement tests
        end)

        test("BRPOP (client:brpop)", function()
            if version:is('<', '2.0.0') then return end
            -- TODO: implement tests
        end)

        test("BRPOPLPUSH (client:brpoplpush)", function()
            if version:is('<', '2.1.0') then return end
            -- TODO: implement tests
        end)

        test("LINSERT (client:linsert)", function()
            if version:is('<', '2.1.0') then return end

            utils.rpush_return(client, 'numbers', shared.numbers(), true)

            assert_equal(client:linsert('numbers', 'before', 0, -2), 11)
            assert_equal(client:linsert('numbers', 'after', -2, -1), 12)
            assert_table_values(client:lrange('numbers', 0, 3), { '-2', '-1', '0', '1' });

            assert_equal(client:linsert('numbers', 'before', 100, 200), -1)
            assert_equal(client:linsert('numbers', 'after', 100, 50), -1)

            assert_error(function()
                client:set('foo', 'bar')
                client:linsert('foo', 0, 0)
            end)
        end)
    end)

    context("Commands operating on sets", function()
        test("SADD (client:sadd)", function()
            assert_equal(client:sadd('set', 0), 1)
            assert_equal(client:sadd('set', 1), 1)
            assert_equal(client:sadd('set', 0), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:sadd('foo', 0)
            end)
        end)

        test("SREM (client:srem)", function()
            utils.sadd_return(client, 'set', { '0', '1', '2', '3', '4' })

            assert_equal(client:srem('set', 0), 1)
            assert_equal(client:srem('set', 4), 1)
            assert_equal(client:srem('set', 10), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:srem('foo', 0)
            end)
        end)

        test("SPOP (client:spop)", function()
            local set = utils.sadd_return(client, 'set', { '0', '1', '2', '3', '4' })

            assert_true(table.contains(set, client:spop('set')))
            assert_nil(client:spop('doesnotexist'))

            assert_error(function()
                client:set('foo', 'bar')
                client:spop('foo')
            end)
        end)

        test("SMOVE (client:smove)", function()
            utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5' })
            utils.sadd_return(client, 'setB', { '5', '6', '7', '8', '9', '10' })

            assert_true(client:smove('setA', 'setB', 0))
            assert_equal(client:srem('setA', 0), 0)
            assert_equal(client:srem('setB', 0), 1)

            assert_true(client:smove('setA', 'setB', 5))
            assert_false(client:smove('setA', 'setB', 100))

            assert_error(function()
                client:set('foo', 'bar')
                client:smove('foo', 'setB', 5)
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:smove('setA', 'foo', 5)
            end)
        end)

        test("SCARD (client:scard)", function()
            utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5' })

            assert_equal(client:scard('setA'), 6)

            -- empty set
            client:sadd('setB', 0)
            client:spop('setB')
            assert_equal(client:scard('doesnotexist'), 0)

            -- non-existent set
            assert_equal(client:scard('doesnotexist'), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:scard('foo')
            end)
        end)

        test("SISMEMBER (client:sismember)", function()
            utils.sadd_return(client, 'set', { '0', '1', '2', '3', '4', '5' })

            assert_true(client:sismember('set', 3))
            assert_false(client:sismember('set', 100))
            assert_false(client:sismember('doesnotexist', 0))

            assert_error(function()
                client:set('foo', 'bar')
                client:sismember('foo', 0)
            end)
        end)

        test("SMEMBERS (client:smembers)", function()
            local set = utils.sadd_return(client, 'set', { '0', '1', '2', '3', '4', '5' })

            assert_table_values(client:smembers('set'), set)

            if version:is('<', '2.0.0') then
                assert_nil(client:smembers('doesnotexist'))
            else
                assert_table_values(client:smembers('doesnotexist'), {})
            end

            assert_error(function()
                client:set('foo', 'bar')
                client:smembers('foo')
            end)
        end)

        test("SINTER (client:sinter)", function()
            local setA = utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.sadd_return(client, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_table_values(client:sinter('setA'), setA)
            assert_table_values(client:sinter('setA', 'setB'), { '3', '4', '6', '1' })

            if version:is('<', '2.0.0') then
                assert_nil(client:sinter('setA', 'doesnotexist'))
            else
                assert_table_values(client:sinter('setA', 'doesnotexist'), {})
            end

            assert_error(function()
                client:set('foo', 'bar')
                client:sinter('foo')
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:sinter('setA', 'foo')
            end)
        end)

        test("SINTERSTORE (client:sinterstore)", function()
            local setA = utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.sadd_return(client, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_equal(client:sinterstore('setC', 'setA'), #setA)
            assert_table_values(client:smembers('setC'), setA)

            client:del('setC')
            -- this behaviour has changed in redis 2.0
            assert_equal(client:sinterstore('setC', 'setA', 'setB'), 4)
            assert_table_values(client:smembers('setC'), { '1', '3', '4', '6' })

            client:del('setC')
            assert_equal(client:sinterstore('setC', 'doesnotexist'), 0)
            assert_false(client:exists('setC'))

            -- existing keys are replaced by SINTERSTORE
            client:set('foo', 'bar')
            assert_equal(client:sinterstore('foo', 'setA'), #setA)

            assert_error(function()
                client:set('foo', 'bar')
                client:sinterstore('setA', 'foo')
            end)
        end)

        test("SUNION (client:sunion)", function()
            local setA = utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.sadd_return(client, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_table_values(client:sunion('setA'), setA)
            assert_table_values(
                client:sunion('setA', 'setB'),
                { '0', '1', '10', '2', '3', '4', '5', '6', '9' }
            )

            -- this behaviour has changed in redis 2.0
            assert_table_values(client:sunion('setA', 'doesnotexist'), setA)

            assert_error(function()
                client:set('foo', 'bar')
                client:sunion('foo')
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:sunion('setA', 'foo')
            end)
        end)

        test("SUNIONSTORE (client:sunionstore)", function()
            local setA = utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.sadd_return(client, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_equal(client:sunionstore('setC', 'setA'), #setA)
            assert_table_values(client:smembers('setC'), setA)

            client:del('setC')
            assert_equal(client:sunionstore('setC', 'setA', 'setB'), 9)
            assert_table_values(
                client:smembers('setC'),
                { '0' ,'1' , '10', '2', '3', '4', '5', '6', '9' }
            )

            client:del('setC')
            assert_equal(client:sunionstore('setC', 'doesnotexist'), 0)
            if version:is('<', '2.0.0') then
                assert_true(client:exists('setC'))
            else
                assert_false(client:exists('setC'))
            end
            assert_equal(client:scard('setC'), 0)

            -- existing keys are replaced by SUNIONSTORE
            client:set('foo', 'bar')
            assert_equal(client:sunionstore('foo', 'setA'), #setA)

            assert_error(function()
                client:set('foo', 'bar')
                client:sunionstore('setA', 'foo')
            end)
        end)

        test("SDIFF (client:sdiff)", function()
            local setA = utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.sadd_return(client, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_table_values(client:sdiff('setA'), setA)
            assert_table_values(client:sdiff('setA', 'setB'), { '5', '0', '2' })
            assert_table_values(client:sdiff('setA', 'doesnotexist'), setA)

            assert_error(function()
                client:set('foo', 'bar')
                client:sdiff('foo')
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:sdiff('setA', 'foo')
            end)
        end)

        test("SDIFFSTORE (client:sdiffstore)", function()
            local setA = utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.sadd_return(client, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_equal(client:sdiffstore('setC', 'setA'), #setA)
            assert_table_values(client:smembers('setC'), setA)

            client:del('setC')
            assert_equal(client:sdiffstore('setC', 'setA', 'setB'), 3)
            assert_table_values(client:smembers('setC'), { '5', '0', '2' })

            client:del('setC')
            assert_equal(client:sdiffstore('setC', 'doesnotexist'), 0)
            if version:is('<', '2.0.0') then
                assert_true(client:exists('setC'))
            else
                assert_false(client:exists('setC'))
            end
            assert_equal(client:scard('setC'), 0)

            -- existing keys are replaced by SDIFFSTORE
            client:set('foo', 'bar')
            assert_equal(client:sdiffstore('foo', 'setA'), #setA)

            assert_error(function()
                client:set('foo', 'bar')
                client:sdiffstore('setA', 'foo')
            end)
        end)

        test("SRANDMEMBER (client:srandmember)", function()
            local setA = utils.sadd_return(client, 'setA', { '0', '1', '2', '3', '4', '5', '6' })

            assert_true(table.contains(setA, client:srandmember('setA')))
            assert_nil(client:srandmember('doesnotexist'))

            assert_error(function()
                client:set('foo', 'bar')
                client:srandmember('foo')
            end)
        end)
    end)

    context("Commands operating on zsets", function()
        test("ZADD (client:zadd)", function()
            assert_equal(client:zadd('zset', 0, 'a'), 1)
            assert_equal(client:zadd('zset', 1, 'b'), 1)
            assert_equal(client:zadd('zset', -1, 'c'), 1)

            assert_equal(client:zadd('zset', 2, 'b'), 0)
            assert_equal(client:zadd('zset', -22, 'b'), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:zadd('foo', 0, 'a')
            end)
        end)

        test("ZINCRBY (client:zincrby)", function()
            assert_equal(client:zincrby('doesnotexist', 1, 'foo'), '1')
            assert_equal(client:type('doesnotexist'), 'zset')

            utils.zadd_return(client, 'zset', shared.zset_sample())
            assert_equal(client:zincrby('zset', 5, 'a'), '-5')
            assert_equal(client:zincrby('zset', 1, 'b'), '1')
            assert_equal(client:zincrby('zset', 0, 'c'), '10')
            assert_equal(client:zincrby('zset', -20, 'd'), '0')
            assert_equal(client:zincrby('zset', 2, 'd'), '2')
            assert_equal(client:zincrby('zset', -30, 'e'), '-10')
            assert_equal(client:zincrby('zset', 1, 'x'), '1')

            assert_error(function()
                client:set('foo', 'bar')
                client:zincrby('foo', 1, 'a')
            end)
        end)

        test("ZREM (client:zrem)", function()
            utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_equal(client:zrem('zset', 'a'), 1)
            assert_equal(client:zrem('zset', 'x'), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:zrem('foo', 'bar')
            end)
        end)

        test("ZRANGE (client:zrange)", function()
            local zset = utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_table_values(client:zrange('zset', 0, 3), { 'a', 'b', 'c', 'd' })
            assert_table_values(client:zrange('zset', 0, 0), { 'a' })
            assert_empty(client:zrange('zset', 1, 0))
            assert_table_values(client:zrange('zset', 0, -1), table.keys(zset))
            assert_table_values(client:zrange('zset', 3, -3), { 'd' })
            assert_empty(client:zrange('zset', 5, -3))
            assert_table_values(client:zrange('zset', -100, 100), table.keys(zset))

            assert_table_values(
                client:zrange('zset', 0, 2, 'withscores'),
                  { { 'a', '-10' }, { 'b', '0' }, { 'c', '10' } }
            )

            assert_table_values(
                client:zrange('zset', 0, 2, { withscores = true }),
                  { { 'a', '-10' }, { 'b', '0' }, { 'c', '10' } }
            )

            assert_error(function()
                client:set('foo', 'bar')
                client:zrange('foo', 0, -1)
            end)
        end)

        test("ZREVRANGE (client:zrevrange)", function()
            local zset = utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_table_values(client:zrevrange('zset', 0, 3), { 'f', 'e', 'd', 'c' })
            assert_table_values(client:zrevrange('zset', 0, 0), { 'f' })
            assert_empty(client:zrevrange('zset', 1, 0))
            assert_table_values(client:zrevrange('zset', 0, -1), table.keys(zset))
            assert_table_values(client:zrevrange('zset', 3, -3), { 'c' })
            assert_empty(client:zrevrange('zset', 5, -3))
            assert_table_values(client:zrevrange('zset', -100, 100), table.keys(zset))

            assert_table_values(
                client:zrevrange('zset', 0, 2, 'withscores'),
                { { 'f', '30' }, { 'e', '20' }, { 'd', '20' } }
            )

            assert_table_values(
                client:zrevrange('zset', 0, 2, { withscores = true }),
                { { 'f', '30' }, { 'e', '20' }, { 'd', '20' } }
            )

            assert_error(function()
                client:set('foo', 'bar')
                client:zrevrange('foo', 0, -1)
            end)
        end)

        test("ZRANGEBYSCORE (client:zrangebyscore)", function()
            local zset = utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_table_values(client:zrangebyscore('zset', -10, -10), { 'a' })
            assert_table_values(client:zrangebyscore('zset', 10, 30), { 'c', 'd', 'e', 'f' })
            assert_table_values(client:zrangebyscore('zset', 20, 20), { 'd', 'e' })
            assert_empty(client:zrangebyscore('zset', 30, 0))

            assert_table_values(
                client:zrangebyscore('zset', 10, 20, 'withscores'),
                { { 'c', '10' }, { 'd', '20' }, { 'e', '20' } }
            )

            assert_table_values(
                client:zrangebyscore('zset', 10, 20, { withscores = true }),
                { { 'c', '10' }, { 'd', '20' }, { 'e', '20' } }
            )

            assert_table_values(
                client:zrangebyscore('zset', 10, 20, { limit = { 1, 2 } }),
                { 'd', 'e' }
            )

            assert_table_values(
                client:zrangebyscore('zset', 10, 20, {
                    limit = { offset = 1, count = 2 }
                }),
                { 'd', 'e' }
            )

            assert_table_values(
                client:zrangebyscore('zset', 10, 20, {
                    limit = { offset = 1, count = 2 },
                    withscores = true
                }),
                { { 'd', '20' }, { 'e', '20' } }
            )

            assert_error(function()
                client:set('foo', 'bar')
                client:zrangebyscore('foo', 0, -1)
            end)
        end)

        test("ZREVRANGEBYSCORE (client:zrevrangebyscore)", function()
            local zset = utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_table_values(client:zrevrangebyscore('zset', -10, -10), { 'a' })
            assert_table_values(client:zrevrangebyscore('zset', 0, -10), { 'b', 'a' })
            assert_table_values(client:zrevrangebyscore('zset', 20, 20), { 'e', 'd' })
            assert_table_values(client:zrevrangebyscore('zset', 30, 0), { 'f', 'e', 'd', 'c', 'b' })

            assert_table_values(
                client:zrevrangebyscore('zset', 20, 10, 'withscores'),
                { { 'e', '20' }, { 'd', '20' }, { 'c', '10' } }
            )

            assert_table_values(
                client:zrevrangebyscore('zset', 20, 10, { limit = { 1, 2 } }),
                { 'd', 'c' }
            )

            assert_table_values(
                client:zrevrangebyscore('zset', 20, 10, {
                    limit = { offset = 1, count = 2 }
                }),
                { 'd', 'c' }
            )

            assert_table_values(
                client:zrevrangebyscore('zset', 20, 10, {
                    limit = { offset = 1, count = 2 },
                    withscores = true
                }),
                { { 'd', '20' }, { 'c', '10' } }
            )

            assert_error(function()
                client:set('foo', 'bar')
                client:zrevrangebyscore('foo', 0, -1)
            end)
        end)


        test("ZUNIONSTORE (client:zunionstore)", function()
            if version:is('<', '2.0.0') then return end

            utils.zadd_return(client, 'zseta', { a = 1, b = 2, c = 3 })
            utils.zadd_return(client, 'zsetb', { b = 1, c = 2, d = 3 })

            -- basic ZUNIONSTORE
            assert_equal(client:zunionstore('zsetc', 2, 'zseta', 'zsetb'), 4)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'a', '1' }, { 'b', '3' }, { 'd', '3' }, { 'c', '5' } }
            )

            assert_equal(client:zunionstore('zsetc', 2, 'zseta', 'zsetbNull'), 3)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'a', '1' }, { 'b', '2' }, { 'c', '3' }}
            )

            assert_equal(client:zunionstore('zsetc', 2, 'zsetaNull', 'zsetb'), 3)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'b', '1' }, { 'c', '2' }, { 'd', '3' }}
            )

            assert_equal(client:zunionstore('zsetc', 2, 'zsetaNull', 'zsetbNull'), 0)

            -- with WEIGHTS
            local opts =  { weights = { 2, 3 } }
            assert_equal(client:zunionstore('zsetc', 2, 'zseta', 'zsetb', opts), 4)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'a', '2' }, { 'b', '7' }, { 'd', '9' }, { 'c', '12' } }
            )

            -- with AGGREGATE (min)
            local opts =  { aggregate = 'min' }
            assert_equal(client:zunionstore('zsetc', 2, 'zseta', 'zsetb', opts), 4)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'a', '1' }, { 'b', '1' }, { 'c', '2' }, { 'd', '3' } }
            )

            -- with AGGREGATE (max)
            local opts =  { aggregate = 'max' }
            assert_equal(client:zunionstore('zsetc', 2, 'zseta', 'zsetb', opts), 4)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'a', '1' }, { 'b', '2' }, { 'c', '3' }, { 'd', '3' } }
            )

            assert_error(function()
                client:set('zsetFake', 'fake')
                client:zunionstore('zsetc', 2, 'zseta', 'zsetFake')
            end)
        end)

        test("ZINTERSTORE (client:zinterstore)", function()
            if version:is('<', '2.0.0') then return end

            utils.zadd_return(client, 'zseta', { a = 1, b = 2, c = 3 })
            utils.zadd_return(client, 'zsetb', { b = 1, c = 2, d = 3 })

            -- basic ZUNIONSTORE
            assert_equal(client:zinterstore('zsetc', 2, 'zseta', 'zsetb'), 2)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'b', '3' }, { 'c', '5' } }
            )

            assert_equal(client:zinterstore('zsetc', 2, 'zseta', 'zsetbNull'), 0)
            assert_equal(client:zinterstore('zsetc', 2, 'zsetaNull', 'zsetb'), 0)
            assert_equal(client:zinterstore('zsetc', 2, 'zsetaNull', 'zsetbNull'), 0)

            -- with WEIGHTS
            local opts =  { weights = { 2, 3 } }
            assert_equal(client:zinterstore('zsetc', 2, 'zseta', 'zsetb', opts), 2)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'b', '7' }, { 'c', '12' } }
            )

            -- with AGGREGATE (min)
            local opts =  { aggregate = 'min' }
            assert_equal(client:zinterstore('zsetc', 2, 'zseta', 'zsetb', opts), 2)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'b', '1' }, { 'c', '2' } }
            )

            -- with AGGREGATE (max)
            local opts =  { aggregate = 'max' }
            assert_equal(client:zinterstore('zsetc', 2, 'zseta', 'zsetb', opts), 2)
            assert_table_values(
                client:zrange('zsetc', 0, -1, 'withscores'),
                { { 'b', '2' }, { 'c', '3' } }
            )

            assert_error(function()
                client:set('zsetFake', 'fake')
                client:zinterstore('zsetc', 2, 'zseta', 'zsetFake')
            end)
        end)

        test("ZCOUNT (client:zcount)", function()
            if version:is('<', '2.0.0') then return end

            utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_equal(client:zcount('zset', 50, 100), 0)
            assert_equal(client:zcount('zset', -100, 100), 6)
            assert_equal(client:zcount('zset', 10, 20), 3)
            assert_equal(client:zcount('zset', '(10', 20), 2)
            assert_equal(client:zcount('zset', 10, '(20'), 1)
            assert_equal(client:zcount('zset', '(10', '(20'), 0)
            assert_equal(client:zcount('zset', '(0', '(30'), 3)

            assert_error(function()
                client:set('foo', 'bar')
                client:zcount('foo', 0, 0)
            end)
        end)

        test("ZCARD (client:zcard)", function()
            local zset = utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_equal(client:zcard('zset'), #table.keys(zset))

            client:zrem('zset', 'a')
            assert_equal(client:zcard('zset'), #table.keys(zset) - 1)

            client:zadd('zsetB', 0, 'a')
            client:zrem('zsetB', 'a')
            assert_equal(client:zcard('zsetB'), 0)

            assert_equal(client:zcard('doesnotexist'), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:zcard('foo')
            end)
        end)

        test("ZSCORE (client:zscore)", function()
            utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_equal(client:zscore('zset', 'a'), '-10')
            assert_equal(client:zscore('zset', 'c'), '10')
            assert_equal(client:zscore('zset', 'e'), '20')

            assert_nil(client:zscore('zset', 'x'))
            assert_nil(client:zscore('doesnotexist', 'a'))

            assert_error(function()
                client:set('foo', 'bar')
                client:zscore('foo', 'a')
            end)
        end)

        test("ZREMRANGEBYSCORE (client:zremrangebyscore)", function()
            utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_equal(client:zremrangebyscore('zset', -10, 0), 2)
            assert_table_values(client:zrange('zset', 0, -1), { 'c', 'd', 'e', 'f' })

            assert_equal(client:zremrangebyscore('zset', 10, 10), 1)
            assert_table_values(client:zrange('zset', 0, -1), { 'd', 'e', 'f' })

            assert_equal(client:zremrangebyscore('zset', 100, 100), 0)

            assert_equal(client:zremrangebyscore('zset', 0, 100), 3)
            assert_equal(client:zremrangebyscore('zset', 0, 100), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:zremrangebyscore('foo', 0, 0)
            end)
        end)

        test("ZRANK (client:zrank)", function()
            if version:is('<', '2.0.0') then return end

            utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_equal(client:zrank('zset', 'a'), 0)
            assert_equal(client:zrank('zset', 'b'), 1)
            assert_equal(client:zrank('zset', 'e'), 4)

            client:zrem('zset', 'd')
            assert_equal(client:zrank('zset', 'e'), 3)

            assert_nil(client:zrank('zset', 'x'))

            assert_error(function()
                client:set('foo', 'bar')
                client:zrank('foo', 'a')
            end)
        end)

        test("ZREVRANK (client:zrevrank)", function()
            if version:is('<', '2.0.0') then return end

            utils.zadd_return(client, 'zset', shared.zset_sample())

            assert_equal(client:zrevrank('zset', 'a'), 5)
            assert_equal(client:zrevrank('zset', 'b'), 4)
            assert_equal(client:zrevrank('zset', 'e'), 1)

            client:zrem('zset', 'e')
            assert_equal(client:zrevrank('zset', 'd'), 1)

            assert_nil(client:zrevrank('zset', 'x'))

            assert_error(function()
                client:set('foo', 'bar')
                client:zrevrank('foo', 'a')
            end)
        end)

        test("ZREMRANGEBYRANK (client:zremrangebyrank)", function()
            if version:is('<', '2.0.0') then return end

            utils.zadd_return(client, 'zseta', shared.zset_sample())
            assert_equal(client:zremrangebyrank('zseta', 0, 2), 3)
            assert_table_values(client:zrange('zseta', 0, -1), { 'd', 'e', 'f' })
            assert_equal(client:zremrangebyrank('zseta', 0, 0), 1)
            assert_table_values(client:zrange('zseta', 0, -1), { 'e', 'f' })

            utils.zadd_return(client, 'zsetb', shared.zset_sample())
            assert_equal(client:zremrangebyrank('zsetb', -3, -1), 3)
            assert_table_values(client:zrange('zsetb', 0, -1), { 'a', 'b', 'c' })
            assert_equal(client:zremrangebyrank('zsetb', -1, -1), 1)
            assert_table_values(client:zrange('zsetb', 0, -1), { 'a', 'b' })
            assert_equal(client:zremrangebyrank('zsetb', -2, -1), 2)
            assert_table_values(client:zrange('zsetb', 0, -1), { })
            assert_false(client:exists('zsetb'))

            assert_equal(client:zremrangebyrank('zsetc', 0, 0), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:zremrangebyrank('foo', 0, 1)
            end)
        end)
    end)

    context("Commands operating on hashes", function()
        test("HSET (client:hset)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:hset('metavars', 'foo', 'bar'))
            assert_true(client:hset('metavars', 'hoge', 'piyo'))
            assert_equal(client:hget('metavars', 'foo'), 'bar')
            assert_equal(client:hget('metavars', 'hoge'), 'piyo')

            assert_error(function()
                client:set('test', 'foobar')
                client:hset('test', 'hoge', 'piyo')
            end)
        end)

        test("HGET (client:hget)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:hset('metavars', 'foo', 'bar'))
            assert_equal(client:hget('metavars', 'foo'), 'bar')
            assert_nil(client:hget('metavars', 'hoge'))
            assert_nil(client:hget('hashDoesNotExist', 'field'))

            assert_error(function()
                client:rpush('metavars', 'foo')
                client:hget('metavars', 'foo')
            end)
        end)

        test("HEXISTS (client:hexists)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:hset('metavars', 'foo', 'bar'))
            assert_true(client:hexists('metavars', 'foo'))
            assert_false(client:hexists('metavars', 'hoge'))
            assert_false(client:hexists('hashDoesNotExist', 'field'))

            assert_error(function()
                client:set('foo', 'bar')
                client:hexists('foo')
            end)
        end)

        test("HDEL (client:hdel)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:hset('metavars', 'foo', 'bar'))
            assert_true(client:hexists('metavars', 'foo'))
            assert_equal(client:hdel('metavars', 'foo'), 1)
            assert_false(client:hexists('metavars', 'foo'))

            assert_equal(client:hdel('metavars', 'hoge'), 0)
            assert_equal(client:hdel('hashDoesNotExist', 'field'), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:hdel('foo', 'field')
            end)
        end)

        test("HLEN (client:hlen)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:hset('metavars', 'foo', 'bar'))
            assert_true(client:hset('metavars', 'hoge', 'piyo'))
            assert_true(client:hset('metavars', 'foofoo', 'barbar'))
            assert_true(client:hset('metavars', 'hogehoge', 'piyopiyo'))

            assert_equal(client:hlen('metavars'), 4)
            client:hdel('metavars', 'foo')
            assert_equal(client:hlen('metavars'), 3)
            assert_equal(client:hlen('hashDoesNotExist'), 0)

            assert_error(function()
                client:set('foo', 'bar')
                client:hlen('foo')
            end)
        end)

        test("HSETNX (client:hsetnx)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:hsetnx('metavars', 'foo', 'bar'))
            assert_false(client:hsetnx('metavars', 'foo', 'barbar'))
            assert_equal(client:hget('metavars', 'foo'), 'bar')

            assert_error(function()
                client:set('test', 'foobar')
                client:hsetnx('test', 'hoge', 'piyo')
            end)
        end)

        test("HMSET / HMGET (client:hmset, client:hmget)", function()
            if version:is('<', '2.0.0') then return end

            local hashKVs = { foo = 'bar', hoge = 'piyo' }

            -- key => value pairs via table
            assert_true(client:hmset('metavars', hashKVs))
            local retval = client:hmget('metavars', table.keys(hashKVs))
            assert_table_values(retval, table.values(hashKVs))

            -- key => value pairs via function arguments
            client:del('metavars')
            assert_true(client:hmset('metavars', 'foo', 'bar', 'hoge', 'piyo'))
            assert_table_values(retval, table.values(hashKVs))
        end)

        test("HINCRBY (client:hincrby)", function()
            if version:is('<', '2.0.0') then return end

            assert_equal(client:hincrby('hash', 'counter', 10), 10)
            assert_equal(client:hincrby('hash', 'counter', 10), 20)
            assert_equal(client:hincrby('hash', 'counter', -20), 0)

            assert_error(function()
                client:hset('hash', 'field', 'string_value')
                client:hincrby('hash', 'field', 10)
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:hincrby('foo', 'bar', 1)
            end)
        end)

        test("HINCRBYFLOAT (client:hincrbyfloat)", function()
            if version:is('<', '2.5.0') then return end

            assert_equal(client:hincrbyfloat('hash', 'counter', 10.1), 10.1)
            assert_equal(client:hincrbyfloat('hash', 'counter', 10.4), 20.5)
            assert_equal(client:hincrbyfloat('hash', 'counter', -20.000), 0.5)

            assert_error(function()
                client:hset('hash', 'field', 'string_value')
                client:hincrbyfloat('hash', 'field', 10.10)
            end)

            assert_error(function()
                client:set('foo', 'bar')
                client:hincrbyfloat('foo', 'bar', 1.10)
            end)
        end)

        test("HKEYS (client:hkeys)", function()
            if version:is('<', '2.0.0') then return end

            local hashKVs = { foo = 'bar', hoge = 'piyo' }
            assert_true(client:hmset('metavars', hashKVs))

            assert_table_values(client:hkeys('metavars'), table.keys(hashKVs))
            assert_table_values(client:hkeys('hashDoesNotExist'), { })

            assert_error(function()
                client:set('foo', 'bar')
                client:hkeys('foo')
            end)
        end)

        test("HVALS (client:hvals)", function()
            if version:is('<', '2.0.0') then return end

            local hashKVs = { foo = 'bar', hoge = 'piyo' }
            assert_true(client:hmset('metavars', hashKVs))

            assert_table_values(client:hvals('metavars'), table.values(hashKVs))
            assert_table_values(client:hvals('hashDoesNotExist'), { })

            assert_error(function()
                client:set('foo', 'bar')
                client:hvals('foo')
            end)
        end)

        test("HGETALL (client:hgetall)", function()
            if version:is('<', '2.0.0') then return end

            local hashKVs = { foo = 'bar', hoge = 'piyo' }
            assert_true(client:hmset('metavars', hashKVs))

            assert_true(table.compare(client:hgetall('metavars'), hashKVs))
            assert_true(table.compare(client:hgetall('hashDoesNotExist'), { }))

            assert_error(function()
                client:set('foo', 'bar')
                client:hgetall('foo')
            end)
        end)
    end)

    context("Remote server control commands", function()
        test("INFO (client:info)", function()
            local info = client:info()
            assert_type(info, 'table')
            assert_not_nil(info.redis_version or info.server.redis_version)
        end)

        test("CONFIG GET (client:config)", function()
            if version:is('<', '2.0.0') then return end

            local config = client:config('get', '*')
            assert_type(config, 'table')
            assert_not_nil(config['list-max-ziplist-entries'])
            if version:is('>=', '2.4.0') then
                assert_not_nil(config.loglevel)
            end

            local config = client:config('get', '*max-*-entries*')
            assert_type(config, 'table')
            assert_not_nil(config['list-max-ziplist-entries'])
            if version:is('>=', '2.4.0') then
                assert_nil(config.loglevel)
            end
        end)

        test("CONFIG SET (client:config)", function()
            if version:is('<', '2.4.0') then return end

            local new, previous = 'notice', client:config('get', 'loglevel').loglevel

            assert_type(previous, 'string')

            assert_true(client:config('set', 'loglevel', new))
            assert_equal(client:config('get', 'loglevel').loglevel, new)

            assert_true(client:config('set', 'loglevel', previous))
        end)

        test("CONFIG RESETSTAT (client:config)", function()
            assert_true(client:config('resetstat'))
        end)

        test("SLOWLOG RESET (client:slowlog)", function()
            if version:is('<', '2.2.12') then return end

            assert_true(client:slowlog('reset'))
        end)

        test("SLOWLOG GET (client:slowlog)", function()
            if version:is('<', '2.2.12') then return end

            local previous = client:config('get', 'slowlog-log-slower-than')['slowlog-log-slower-than']

            client:config('set', 'slowlog-log-slower-than', 0)
            client:set('foo', 'bar')
            client:del('foo')

            local log = client:slowlog('get')
            assert_type(log, 'table')
            assert_greater_than(#log, 0)

            assert_type(log[1], 'table')
            assert_greater_than(log[1].id, 0)
            assert_greater_than(log[1].timestamp, 0)
            assert_greater_than(log[1].duration, 0)
            assert_type(log[1].command, 'table')

            local log = client:slowlog('get', 1)
            assert_type(log, 'table')
            assert_equal(#log, 1)

            client:config('set', 'slowlog-log-slower-than', previous or 10000)
        end)

        test("TIME (client:time)", function()
            if version:is('<', '2.5.0') then return end

            local redis_time = client:time()
            assert_type(redis_time, 'table')
            assert_not_nil(redis_time[1])
            assert_not_nil(redis_time[2])
        end)

        test("CLIENT (client:client)", function()
            if version:is('<', '2.4.0') then return end
            -- TODO: implement tests
        end)

        test("LASTSAVE (client:lastsave)", function()
            assert_not_nil(client:lastsave())
        end)

        test("FLUSHDB (client:flushdb)", function()
            assert_true(client:flushdb())
        end)
    end)

    context("Transactions", function()
        test("MULTI / EXEC (client:multi, client:exec)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:multi())
            assert_response_queued(client:ping())
            assert_response_queued(client:echo('hello'))
            assert_response_queued(client:echo('redis'))
            assert_table_values(client:exec(), { 'PONG', 'hello', 'redis' })

            assert_true(client:multi())
            assert_table_values(client:exec(), {})

            -- should raise an error when trying to EXEC without having previously issued MULTI
            assert_error(function() client:exec() end)
        end)
        test("DISCARD (client:discard)", function()
            if version:is('<', '2.0.0') then return end

            assert_true(client:multi())
            assert_response_queued(client:set('foo', 'bar'))
            assert_response_queued(client:set('hoge', 'piyo'))
            assert_true(client:discard())

            -- should raise an error when trying to EXEC after a DISCARD
            assert_error(function() client:exec() end)

            assert_false(client:exists('foo'))
            assert_false(client:exists('hoge'))
        end)

        test("WATCH", function()
            if version:is('<', '2.1.0') then return end

            local client2 = utils.create_client(settings)
            assert_true(client:set('foo', 'bar'))
            assert_true(client:watch('foo'))
            assert_true(client:multi())
            assert_response_queued(client:get('foo'))
            assert_true(client2:set('foo', 'hijacked'))
            assert_nil(client:exec())
        end)

        test("UNWATCH", function()
            if version:is('<', '2.1.0') then return end

            local client2 = utils.create_client(settings)
            assert_true(client:set('foo', 'bar'))
            assert_true(client:watch('foo'))
            assert_true(client:unwatch())
            assert_true(client:multi())
            assert_response_queued(client:get('foo'))
            assert_true(client2:set('foo', 'hijacked'))
            assert_table_values(client:exec(), { 'hijacked' })
        end)

        test("MULTI / EXEC / DISCARD abstraction", function()
            if version:is('<', '2.0.0') then return end

            local replies, processed

            replies, processed = client:transaction(function(t)
                -- empty transaction
            end)
            assert_table_values(replies, { })
            assert_equal(processed, 0)

            replies, processed = client:transaction(function(t)
                t:discard()
            end)
            assert_table_values(replies, { })
            assert_equal(processed, 0)

            replies, processed = client:transaction(function(t)
                assert_response_queued(t:set('foo', 'bar'))
                assert_true(t:discard())
                assert_response_queued(t:ping())
                assert_response_queued(t:echo('hello'))
                assert_response_queued(t:echo('redis'))
                assert_response_queued(t:exists('foo'))
            end)
            assert_table_values(replies, { true, 'hello', 'redis', false })
            assert_equal(processed, 4)

            -- clean up transaction after client-side errors
            assert_error(function()
                client:transaction(function(t)
                    t:lpush('metavars', 'foo')
                    error('whoops!')
                    t:lpush('metavars', 'hoge')
                end)
            end)
            assert_false(client:exists('metavars'))
        end)

        test("WATCH / MULTI / EXEC abstraction", function()
            if version:is('<', '2.1.0') then return end

            local redis2 = utils.create_client(settings)
            local watch_keys = { 'foo' }

            local replies, processed = client:transaction(watch_keys, function(t)
                -- empty transaction
            end)
            assert_table_values(replies, { })
            assert_equal(processed, 0)

            assert_error(function()
                client:transaction(watch_keys, function(t)
                    t:set('foo', 'bar')
                    redis2:set('foo', 'hijacked')
                    t:get('foo')
                end)
            end)
        end)

        test("WATCH / MULTI / EXEC with check-and-set (CAS) abstraction", function()
            if version:is('<', '2.1.0') then return end

            local opts, replies, processed

            opts = { cas = 'foo' }
            replies, processed = client:transaction(opts, function(t)
                -- empty transaction (with missing call to t:multi())
            end)
            assert_table_values(replies, { })
            assert_equal(processed, 0)

            opts = { watch = 'foo', cas = true }
            replies, processed = client:transaction(opts, function(t)
                t:multi()
                -- empty transaction
            end)
            assert_table_values(replies, { })
            assert_equal(processed, 0)

            local redis2 = utils.create_client(settings)        
            local n = 5
            opts = { watch = 'foobarr', cas = true, retry = 5 }
            replies, processed = client:transaction(opts, function(t)
                t:set('foobar', 'bazaar')
                local val = t:get('foobar')
                t:multi()
                assert_response_queued(t:set('discardable', 'bar'))
                assert_equal(t:commands_queued(), 1)
                assert_true(t:discard())
                assert_response_queued(t:ping())
                assert_equal(t:commands_queued(), 1)
                assert_response_queued(t:echo('hello'))
                assert_response_queued(t:echo('redis'))
                assert_equal(t:commands_queued(), 3)
                if n>0 then
                    n = n-1
                    redis2:set("foobarr", n)
                end
                assert_response_queued(t:exists('foo'))
                assert_response_queued(t:get('foobar'))
                assert_response_queued(t:get('foobarr'))
            end)
            assert_table_values(replies, { true, 'hello', 'redis', false, "bazaar", '0' })
            assert_equal(processed, 6)
        end)

        test("Abstraction options", function()
            -- TODO: more in-depth tests (proxy calls to WATCH)
            local opts, replies, processed
            local tx_empty = function(t) end
            local tx_cas_empty = function(t) t:multi() end

            replies, processed = client:transaction(tx_empty)
            assert_table_values(replies, { })

            assert_error(function()
                client:transaction(opts, tx_empty)
            end)

            opts = 'foo'
            replies, processed = client:transaction(opts, tx_empty)
            assert_table_values(replies, { })
            assert_equal(processed, 0)

            opts = { 'foo', 'bar' }
            replies, processed = client:transaction(opts, tx_empty)
            assert_equal(processed, 0)

            opts = { watch = 'foo' }
            replies, processed = client:transaction(opts, tx_empty)
            assert_equal(processed, 0)

            opts = { watch = { 'foo', 'bar' } }
            replies, processed = client:transaction(opts, tx_empty)
            assert_equal(processed, 0)

            opts = { cas = true }
            replies, processed = client:transaction(opts, tx_cas_empty)
            assert_equal(processed, 0)

            opts = { 'foo', 'bar', cas = true }
            replies, processed = client:transaction(opts, tx_cas_empty)
            assert_equal(processed, 0)

            opts = { 'foo', nil, 'bar', cas = true }
            replies, processed = client:transaction(opts, tx_cas_empty)
            assert_equal(processed, 0)

            opts = { watch = { 'foo', 'bar' }, cas = true }
            replies, processed = client:transaction(opts, tx_cas_empty)
            assert_equal(processed, 0)

            opts = { nil, cas = true }
            replies, processed = client:transaction(opts, tx_cas_empty)
            assert_equal(processed, 0)
        end)
    end)

    context("Pub/Sub", function()
        test('PUBLISH (client:publish)', function()
            assert_equal(client:publish('redis-lua-publish', 'test'), 0)
        end)

        test('SUBSCRIBE (client:subscribe)', function()
            client:subscribe('redis-lua-publish')

            -- we have one subscriber
            data = 'data' .. tostring(math.random(1000))
            publisher = utils.create_client(settings)
            assert_equal(publisher:publish('redis-lua-publish', data), 1)
            -- we have data
            response = client:subscribe('redis-lua-publish')
            -- {"message","redis-lua-publish","testXXX"}
            assert_true(table.contains(response, 'message'))
            assert_true(table.contains(response, 'redis-lua-publish'))
            assert_true(table.contains(response, data))

            client:unsubscribe('redis-lua-publish')
        end)
    end)

    context("Scripting", function()
        test('EVAL (client:eval)', function()
            if version:is('<', '2.5.0') then return end
        end)

        test('EVALSHA (client:evalsha)', function()
            if version:is('<', '2.5.0') then return end
        end)

        test('SCRIPT (client:script)', function()
            if version:is('<', '2.5.0') then return end
        end)
    end)
end)
