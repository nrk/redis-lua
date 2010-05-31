package.path = package.path .. ";../?.lua"

require "luarocks.require"
require 'base'
require "telescope"
require "redis"

local settings = {
    host     = '127.0.0.1',
    port     = 6379,
    database = 14,
    password = nil,
}

make_assertion("numeric", "'%s' to be a numeric value", function(a) 
    return type(tonumber(a)) == "number"
end)

make_assertion("table_values", "'%s' to have the same values as '%s'", function(a,b)
    -- NOTE: the body of this function was taken and slightly adapted from 
    --       Penlight (http://github.com/stevedonovan/Penlight)
    if #a ~= #b then return false end
    local visited = {}
    for i = 1,#a do
        local val, gotcha = a[i], nil
        for j = 1,#b do
            if not visited[j] then
                if val == b[j] then
                    gotcha = j
                    break
                end
            end
        end
        if not gotcha then return false end
        visited[gotcha] = true
    end
    return true
end)

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

local utils = {
    push_tail_return = function(client, key, values, wipe)
        if wipe then client:delete(key) end
        for _, v in ipairs(values) do
            client:push_tail(key, v)
        end
        return values
    end,
    set_add_return = function(client, key, values, wipe)
        if wipe then client:delete(key) end
        for _, v in ipairs(values) do
            client:set_add(key, v)
        end
        return values
    end,
    zset_add_return = function(client, key, values, wipe)
        if wipe then client:delete(key) end
        for k, v in pairs(values) do
            client:zset_add(key, v, k)
        end
        return values
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

context("Redis commands", function() 
    before(function()
        redis = Redis.connect(settings.host, settings.port)
        if settings.password then redis:auth(settings.password) end
        if settings.database then redis:select_database(settings.database) end
        redis:flush_database()
    end)

    after(function()
        redis:quit()
    end)

    context("Miscellaneous commands", function() 
        test("PING (redis:ping)", function() 
            assert_true(redis:ping())
        end)

        test("ECHO (redis:echo)", function() 
            local str_ascii, str_utf8 = "Can you hear me?", "聞こえますか？"

            assert_equal(redis:echo(str_ascii), str_ascii)
            assert_equal(redis:echo(str_utf8), str_utf8)
        end)
    end)

    context("Commands operating on string values", function() 
        test("SET (redis:set)", function() 
            assert_true(redis:set('foo', 'bar'))
            assert_equal(redis:get('foo'), 'bar')
        end)

        test("GET (redis:get)", function() 
            redis:set('foo', 'bar')

            assert_equal(redis:get('foo'), 'bar')
            assert_nil(redis:get('hoge'))

            assert_error(function()
                redis:push_tail('metavars', 'foo')
                redis:get('metavars')
            end)
        end)

        test("EXISTS (redis:exists)", function() 
            redis:set('foo', 'bar')

            assert_true(redis:exists('foo'))
            assert_false(redis:exists('hoge'))
        end)

        test("SETNX (redis:set_preserve)", function() 
            assert_true(redis:set_preserve('foo', 'bar'))
            assert_false(redis:set_preserve('foo', 'baz'))
            assert_equal(redis:get('foo'), 'bar')
        end)

        test("MSET (redis:set_multiple)", function()
            local kvs = shared.kvs_table()

            assert_true(redis:set_multiple(kvs))
            for k,v in pairs(kvs) do 
                assert_equal(redis:get(k), v)
            end

            assert_true(redis:set_multiple('a', '1', 'b', '2', 'c', '3'))
            assert_equal(redis:get('a'), '1')
            assert_equal(redis:get('b'), '2')
            assert_equal(redis:get('c'), '3')
        end)

        test("MSETNX (redis:set_multiple_preserve)", function()
           assert_true(redis:set_multiple_preserve({ a = '1', b = '2' }))
           assert_false(redis:set_multiple_preserve({ c = '3', a = '100'}))
           assert_equal(redis:get('a'), '1')
           assert_equal(redis:get('b'), '2')
        end)

        test("MGET (redis:get_multiple)", function() 
            local kvs = shared.kvs_table()
            local keys, values = table.keys(kvs), table.values(kvs)

            assert_true(redis:set_multiple(kvs))
            assert_table_values(redis:get_multiple(unpack(keys)), values)
        end)

        test("GETSET (redis:get_set)", function() 
            assert_nil(redis:get_set('foo', 'bar'))
            assert_equal(redis:get_set('foo', 'barbar'), 'bar')
            assert_equal(redis:get_set('foo', 'baz'), 'barbar')
        end)

        test("INCR (redis:increment)", function() 
            assert_equal(redis:increment('foo'), 1)
            assert_equal(redis:increment('foo'), 2)

            assert_true(redis:set('hoge', 'piyo'))
            assert_equal(redis:increment('hoge'), 1)
        end)

        test("INCRBY (redis:increment_by)", function() 
            redis:set('foo', 2)
            assert_equal(redis:increment_by('foo', 20), 22)
            assert_equal(redis:increment_by('foo', -12), 10)
            assert_equal(redis:increment_by('foo', -110), -100)
        end)

        test("DECR (redis:decrement)", function()  
            assert_equal(redis:decrement('foo'), -1)
            assert_equal(redis:decrement('foo'), -2)

            assert_true(redis:set('hoge', 'piyo'))
            assert_equal(redis:decrement('hoge'), -1)
        end)

        test("DECRBY (redis:decrement_by)", function() 
            redis:set('foo', -2)
            assert_equal(redis:decrement_by('foo', 20), -22)
            assert_equal(redis:decrement_by('foo', -12), -10)
            assert_equal(redis:decrement_by('foo', -110), 100)
        end)

        test("DEL (redis:delete)", function() 
            redis:set_multiple(shared.kvs_table())

            assert_equal(redis:delete('doesnotexist'), 0)
            assert_equal(redis:delete('foofoo'), 1)
            assert_equal(redis:delete('foo', 'hoge', 'doesnotexist'), 2)
        end)

        test("TYPE (redis:type)", function() 
            assert_equal(redis:type('doesnotexist'), 'none')

            redis:set('fooString', 'bar')
            assert_equal(redis:type('fooString'), 'string')

            redis:push_tail('fooList', 'bar')
            assert_equal(redis:type('fooList'), 'list')

            redis:set_add('fooSet', 'bar')
            assert_equal(redis:type('fooSet'), 'set')

            redis:zset_add('fooZSet', 0, 'bar')
            assert_equal(redis:type('fooZSet'), 'zset')
        end)
    end)

    context("Commands operating on the key space", function() 
        test("KEYS (redis:keys)", function() 
            local kvs_prefixed   = shared.kvs_ns_table()
            local kvs_unprefixed = { aaa = 1, aba = 2, aca = 3 }
            local kvs_all = table.merge(kvs_prefixed, kvs_unprefixed)

            redis:set_multiple(kvs_all)

            assert_empty(redis:keys('nokeys:*'))
            assert_table_values(
                table.values(redis:keys('*')), 
                table.keys(kvs_all)
            )
            assert_table_values(
                table.values(redis:keys('metavars:*')), 
                table.keys(kvs_prefixed)
            )
            assert_table_values(
                table.values(redis:keys('a?a')), 
                table.keys(kvs_unprefixed)
            )
        end)

        test("RANDOMKEY (redis:random_key)", function() 
            local kvs = shared.kvs_table()

            assert_nil(redis:random_key())
            redis:set_multiple(kvs)
            assert_true(table.contains(table.keys(kvs), redis:random_key()))
        end)

        test("RENAME (redis:rename)", function() 
            local kvs = shared.kvs_table()
            redis:set_multiple(kvs)

            assert_true(redis:rename('hoge', 'hogehoge'))
            assert_false(redis:exists('hoge'))
            assert_equal(redis:get('hogehoge'), 'piyo')

            -- rename overwrites existing keys
            assert_true(redis:rename('foo', 'foofoo'))
            assert_false(redis:exists('foo'))
            assert_equal(redis:get('foofoo'), 'bar')

            -- rename fails when the key does not exist
            assert_error(function()
                redis:rename('doesnotexist', 'fuga')
            end)
        end)

        test("RENAMENX (redis:rename_preserve)", function() 
            local kvs = shared.kvs_table()
            redis:set_multiple(kvs)

            assert_true(redis:rename_preserve('hoge', 'hogehoge'))
            assert_false(redis:exists('hoge'))
            assert_equal(redis:get('hogehoge'), 'piyo')

            -- rename overwrites existing keys
            assert_false(redis:rename_preserve('foo', 'foofoo'))
            assert_true(redis:exists('foo'))

            -- rename fails when the key does not exist
            assert_error(function()
                redis:rename_preserve('doesnotexist', 'fuga')
            end)
        end)

        test("EXPIRE (redis:expire)", function() 
            -- TODO: cannot sleep with standard lua functions
        end)

        test("EXPIREAT (redis:expire_at)", function() 
            -- TODO: cannot sleep with standard lua functions
        end)

        test("TTL (redis:ttl)", function() 
            -- TODO: cannot sleep with standard lua functions
        end)

        test("DBSIZE (redis:database_size)", function() 
            assert_equal(redis:database_size(), 0)
            redis:set_multiple(shared.kvs_table())
            assert_greater_than(redis:database_size(), 0)
        end)
    end)

    context("Commands operating on lists", function() 
        test("RPUSH (redis:push_tail)", function() 
            assert_true(redis:push_tail('metavars', 'foo'))
            assert_true(redis:push_tail('metavars', 'hoge'))
            assert_error(function()
                redis:set('foo', 'bar')
                redis:push_tail('foo', 'baz')
            end)
        end)

        test("LPUSH (redis:push_head)", function() 
            assert_true(redis:push_head('metavars', 'foo'))
            assert_true(redis:push_head('metavars', 'hoge'))
            assert_error(function()
                redis:set('foo', 'bar')
                redis:push_head('foo', 'baz')
            end)
        end)

        test("LLEN (redis:list_length)", function() 
            local kvs = shared.kvs_table()
            for _, v in pairs(kvs) do
                redis:push_tail('metavars', v)
            end

            assert_equal(redis:list_length('metavars'), 3)
            assert_equal(redis:list_length('doesnotexist'), 0)
            assert_error(function()
                redis:set('foo', 'bar')
                redis:list_length('foo')
            end)
        end)

        test("LRANGE (redis:list_range)", function() 
            local numbers = utils.push_tail_return(redis, 'numbers', shared.numbers())

            assert_table_values(redis:list_range('numbers', 0, 3), table.slice(numbers, 1, 4))
            assert_table_values(redis:list_range('numbers', 4, 8), table.slice(numbers, 5, 5))
            assert_table_values(redis:list_range('numbers', 0, 0), table.slice(numbers, 1, 1))
            assert_empty(redis:list_range('numbers', 1, 0))
            assert_table_values(redis:list_range('numbers', 0, -1), numbers)
            assert_table_values(redis:list_range('numbers', 5, -5), { '5' })
            assert_empty(redis:list_range('numbers', 7, -5))
            assert_table_values(redis:list_range('numbers', -5, -2), table.slice(numbers, 6, 4))
            assert_table_values(redis:list_range('numbers', -100, 100), numbers)
        end)

        test("LTRIM (redis:list_trim)", function() 
            local numbers = utils.push_tail_return(redis, 'numbers', shared.numbers(), true)
            assert_true(redis:list_trim('numbers', 0, 2))
            assert_table_values(redis:list_range('numbers', 0, -1), table.slice(numbers, 1, 3))

            local numbers = utils.push_tail_return(redis, 'numbers', shared.numbers(), true)
            assert_true(redis:list_trim('numbers', 5, 9))
            assert_table_values(redis:list_range('numbers', 0, -1), table.slice(numbers, 6, 5))

            local numbers = utils.push_tail_return(redis, 'numbers', shared.numbers(), true)
            assert_true(redis:list_trim('numbers', 0, -6))
            assert_table_values(redis:list_range('numbers', 0, -1), table.slice(numbers, 1, 5))

            local numbers = utils.push_tail_return(redis, 'numbers', shared.numbers(), true)
            assert_true(redis:list_trim('numbers', -5, -3))
            assert_table_values(redis:list_range('numbers', 0, -1), table.slice(numbers, 6, 3))

            local numbers = utils.push_tail_return(redis, 'numbers', shared.numbers(), true)
            assert_true(redis:list_trim('numbers', -100, 100))
            assert_table_values(redis:list_range('numbers', 0, -1), numbers)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:list_trim('foo', 0, 1)
            end)
        end)

        test("LINDEX (redis:list_index)", function() 
            local numbers = utils.push_tail_return(redis, 'numbers', shared.numbers())

            assert_equal(redis:list_index('numbers', 0), numbers[1])
            assert_equal(redis:list_index('numbers', 5), numbers[6])
            assert_equal(redis:list_index('numbers', 9), numbers[10])
            assert_nil(redis:list_index('numbers', 100))

            assert_equal(redis:list_index('numbers', -0), numbers[1])
            assert_equal(redis:list_index('numbers', -1), numbers[10])
            assert_equal(redis:list_index('numbers', -3), numbers[8])
            assert_nil(redis:list_index('numbers', -100))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:list_index('foo', 0)
            end)
        end)

        test("LSET (redis:list_set)", function() 
            utils.push_tail_return(redis, 'numbers', shared.numbers())

            assert_true(redis:list_set('numbers', 5, -5))
            assert_equal(redis:list_index('numbers', 5), '-5')

            assert_error(function()
                redis:list_set('numbers', 99, 99)
            end)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:list_set('foo', 0, 0)
            end)
        end)

        test("LREM (redis:list_remove)", function() 
            local mixed = { '0', '_', '2', '_', '4', '_', '6', '_' }

            utils.push_tail_return(redis, 'mixed', mixed, true)
            assert_equal(redis:list_remove('mixed', 2, '_'), 2)
            assert_table_values(redis:list_range('mixed', 0, -1), { '0', '2', '4', '_', '6', '_' })

            utils.push_tail_return(redis, 'mixed', mixed, true)
            assert_equal(redis:list_remove('mixed', 0, '_'), 4)
            assert_table_values(redis:list_range('mixed', 0, -1), { '0', '2', '4', '6' })

            utils.push_tail_return(redis, 'mixed', mixed, true)
            assert_equal(redis:list_remove('mixed', -2, '_'), 2)
            assert_table_values(redis:list_range('mixed', 0, -1), { '0', '_', '2', '_', '4', '6' })

            utils.push_tail_return(redis, 'mixed', mixed, true)
            assert_equal(redis:list_remove('mixed', 2, '|'), 0)
            assert_table_values(redis:list_range('mixed', 0, -1), mixed)

            assert_equal(redis:list_remove('doesnotexist', 2, '_'), 0)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:list_remove('foo', 0, 0)
            end)
        end)

        test("LPOP (redis:pop_first)", function() 
            local numbers = utils.push_tail_return(redis, 'numbers', { '0', '1', '2', '3', '4' })

            assert_equal(redis:pop_first('numbers'), numbers[1])
            assert_equal(redis:pop_first('numbers'), numbers[2])
            assert_equal(redis:pop_first('numbers'), numbers[3])

            assert_table_values(redis:list_range('numbers', 0, -1), { '3', '4' })

            redis:pop_first('numbers')
            redis:pop_first('numbers')
            assert_nil(redis:pop_first('numbers'))

            assert_nil(redis:pop_first('doesnotexist'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:pop_first('foo')
            end)
        end)

        test("RPOP (redis:pop_last)", function() 
            local numbers = utils.push_tail_return(redis, 'numbers', { '0', '1', '2', '3', '4' })

            assert_equal(redis:pop_last('numbers'), numbers[5])
            assert_equal(redis:pop_last('numbers'), numbers[4])
            assert_equal(redis:pop_last('numbers'), numbers[3])

            assert_table_values(redis:list_range('numbers', 0, -1), { '0', '1' })

            redis:pop_last('numbers')
            redis:pop_last('numbers')
            assert_nil(redis:pop_last('numbers'))

            assert_nil(redis:pop_last('doesnotexist'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:pop_last('foo')
            end)
        end)

        test("RPOPLPUSH (redis:pop_last_push_head)", function() 
            local numbers = utils.push_tail_return(redis, 'numbers', { '0', '1', '2' }, true)
            assert_equal(redis:list_length('temporary'), 0)
            assert_equal(redis:pop_last_push_head('numbers', 'temporary'), '2')
            assert_equal(redis:pop_last_push_head('numbers', 'temporary'), '1')
            assert_equal(redis:pop_last_push_head('numbers', 'temporary'), '0')
            assert_equal(redis:list_length('numbers'), 0)
            assert_equal(redis:list_length('temporary'), 3)

            local numbers = utils.push_tail_return(redis, 'numbers', { '0', '1', '2' }, true)
            redis:pop_last_push_head('numbers', 'numbers')
            redis:pop_last_push_head('numbers', 'numbers')
            redis:pop_last_push_head('numbers', 'numbers')
            assert_table_values(redis:list_range('numbers', 0, -1), numbers)

            assert_nil(redis:pop_last_push_head('doesnotexist1', 'doesnotexist2'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:pop_last_push_head('foo', 'hoge')
            end)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:pop_last_push_head('temporary', 'foo')
            end)
        end)
    end)

    context("Commands operating on sets", function() 
        test("SADD (redis:set_add)", function() 
            assert_true(redis:set_add('set', 0))
            assert_true(redis:set_add('set', 1))
            assert_false(redis:set_add('set', 0))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_add('foo', 0)
            end)
        end)

        test("SREM (redis:set_remove)", function() 
            utils.set_add_return(redis, 'set', { '0', '1', '2', '3', '4' })

            assert_true(redis:set_remove('set', 0))
            assert_true(redis:set_remove('set', 4))
            assert_false(redis:set_remove('set', 10))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_remove('foo', 0)
            end)
        end)

        test("SPOP (redis:set_pop)", function() 
            local set = utils.set_add_return(redis, 'set', { '0', '1', '2', '3', '4' })

            assert_true(table.contains(set, redis:set_pop('set')))
            assert_nil(redis:set_pop('doesnotexist'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_pop('foo')
            end)
        end)

        test("SMOVE (redis:set_move)", function() 
            utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5' })
            utils.set_add_return(redis, 'setB', { '5', '6', '7', '8', '9', '10' })

            assert_true(redis:set_move('setA', 'setB', 0))
            assert_false(redis:set_remove('setA', 0))
            assert_true(redis:set_remove('setB', 0))

            assert_true(redis:set_move('setA', 'setB', 5))
            assert_false(redis:set_move('setA', 'setB', 100))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_move('foo', 'setB', 5)
            end)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_move('setA', 'foo', 5)
            end)
        end)

        test("SCARD (redis:set_cardinality)", function() 
            utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5' })

            assert_equal(redis:set_cardinality('setA'), 6)

            -- empty set
            redis:set_add('setB', 0)
            redis:set_pop('setB')
            assert_equal(redis:set_cardinality('doesnotexist'), 0)

            -- non-existent set
            assert_equal(redis:set_cardinality('doesnotexist'), 0)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_cardinality('foo')
            end)
        end)

        test("SISMEMBER (redis:set_is_member)", function() 
            utils.set_add_return(redis, 'set', { '0', '1', '2', '3', '4', '5' })

            assert_true(redis:set_is_member('set', 3))
            assert_false(redis:set_is_member('set', 100))
            assert_false(redis:set_is_member('doesnotexist', 0))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_is_member('foo', 0)
            end)
        end)

        test("SMEMBERS (redis:set_members)", function() 
            local set = utils.set_add_return(redis, 'set', { '0', '1', '2', '3', '4', '5' })

            assert_table_values(redis:set_members('set'), set)
            -- this behaviour has changed in redis 2.0
            assert_nil(redis:set_members('doesnotexist'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_members('foo')
            end)
        end)

        test("SINTER (redis:set_intersection)", function() 
            local setA = utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.set_add_return(redis, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_table_values(redis:set_intersection('setA'), setA)
            assert_table_values(redis:set_intersection('setA', 'setB'), { '3', '4', '6', '1' })

            -- this behaviour has changed in redis 2.0
            assert_nil(redis:set_intersection('setA', 'doesnotexist'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_intersection('foo')
            end)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_intersection('setA', 'foo')
            end)
        end)

        test("SINTERSTORE (redis:set_intersection_store)", function() 
            local setA = utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.set_add_return(redis, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_equal(redis:set_intersection_store('setC', 'setA'), #setA)
            assert_table_values(redis:set_members('setC'), setA)

            redis:delete('setC')
            -- this behaviour has changed in redis 2.0
            assert_equal(redis:set_intersection_store('setC', 'setA', 'setB'), 4)
            assert_table_values(redis:set_members('setC'), { '1', '3', '4', '6' })

            redis:delete('setC')
            assert_equal(redis:set_intersection_store('setC', 'doesnotexist'), 0)
            assert_false(redis:exists('setC'))

            -- existing keys are replaced by SINTERSTORE
            redis:set('foo', 'bar')
            assert_equal(redis:set_intersection_store('foo', 'setA'), #setA)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_intersection_store('setA', 'foo')
            end)
        end)

        test("SUNION (redis:set_union)", function() 
            local setA = utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.set_add_return(redis, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_table_values(redis:set_union('setA'), setA)
            assert_table_values(
                redis:set_union('setA', 'setB'), 
                { '0', '1', '10', '2', '3', '4', '5', '6', '9' }
            )

            -- this behaviour has changed in redis 2.0
            assert_table_values(redis:set_union('setA', 'doesnotexist'), setA)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_union('foo')
            end)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_union('setA', 'foo')
            end)
        end)

        test("SUNIONSTORE (redis:set_union_store)", function() 
            local setA = utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.set_add_return(redis, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_equal(redis:set_union_store('setC', 'setA'), #setA)
            assert_table_values(redis:set_members('setC'), setA)

            redis:delete('setC')
            assert_equal(redis:set_union_store('setC', 'setA', 'setB'), 9)
            assert_table_values(
                redis:set_members('setC'), 
                { '0' ,'1' , '10', '2', '3', '4', '5', '6', '9' }
            )

            redis:delete('setC')
            assert_equal(redis:set_union_store('setC', 'doesnotexist'), 0)
            -- this behaviour has changed in redis 2.0
            assert_true(redis:exists('setC'))
            assert_equal(redis:set_cardinality('setC'), 0)

            -- existing keys are replaced by SUNIONSTORE
            redis:set('foo', 'bar')
            assert_equal(redis:set_union_store('foo', 'setA'), #setA)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_union_store('setA', 'foo')
            end)
        end)

        test("SDIFF (redis:set_difference)", function() 
            local setA = utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.set_add_return(redis, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_table_values(redis:set_difference('setA'), setA)
            assert_table_values(redis:set_difference('setA', 'setB'), { '5', '0', '2' })
            assert_table_values(redis:set_difference('setA', 'doesnotexist'), setA)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_difference('foo')
            end)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_difference('setA', 'foo')
            end)
        end)

        test("SDIFFSTORE (redis:set_difference_store)", function() 
            local setA = utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5', '6' })
            local setB = utils.set_add_return(redis, 'setB', { '1', '3', '4', '6', '9', '10' })

            assert_equal(redis:set_difference_store('setC', 'setA'), #setA)
            assert_table_values(redis:set_members('setC'), setA)

            redis:delete('setC')
            assert_equal(redis:set_difference_store('setC', 'setA', 'setB'), 3)
            assert_table_values(redis:set_members('setC'), { '5', '0', '2' })

            redis:delete('setC')
            assert_equal(redis:set_difference_store('setC', 'doesnotexist'), 0)
            -- this behaviour has changed in redis 2.0
            assert_true(redis:exists('setC'))
            assert_equal(redis:set_cardinality('setC'), 0)

            -- existing keys are replaced by SDIFFSTORE
            redis:set('foo', 'bar')
            assert_equal(redis:set_difference_store('foo', 'setA'), #setA)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_difference_store('setA', 'foo')
            end)
        end)

        test("SRANDMEMBER (redis:set_random_member)", function() 
            local setA = utils.set_add_return(redis, 'setA', { '0', '1', '2', '3', '4', '5', '6' })

            assert_true(table.contains(setA, redis:set_random_member('setA')))
            assert_nil(redis:set_random_member('doesnotexist'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:set_random_member('foo')
            end)
        end)
    end)

    context("Commands operating on zsets", function() 
        test("ZADD (redis:zset_add)", function() 
            assert_true(redis:zset_add('zset', 0, 'a'))
            assert_true(redis:zset_add('zset', 1, 'b'))
            assert_true(redis:zset_add('zset', -1, 'c'))

            assert_false(redis:zset_add('zset', 2, 'b'))
            assert_false(redis:zset_add('zset', -22, 'b'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:zset_add('foo', 0, 'a')
            end)
        end)

        test("ZINCRBY (redis:zset_increment_by)", function() 
            assert_equal(redis:zset_increment_by('doesnotexist', 1, 'foo'), '1')
            assert_equal(redis:type('doesnotexist'), 'zset')

            utils.zset_add_return(redis, 'zset', shared.zset_sample())
            assert_equal(redis:zset_increment_by('zset', 5, 'a'), '-5')
            assert_equal(redis:zset_increment_by('zset', 1, 'b'), '1')
            assert_equal(redis:zset_increment_by('zset', 0, 'c'), '10')
            assert_equal(redis:zset_increment_by('zset', -20, 'd'), '0')
            assert_equal(redis:zset_increment_by('zset', 2, 'd'), '2')
            assert_equal(redis:zset_increment_by('zset', -30, 'e'), '-10')
            assert_equal(redis:zset_increment_by('zset', 1, 'x'), '1')

            assert_error(function()
                redis:set('foo', 'bar')
                redis:zset_increment_by('foo', 1, 'a')
            end)
        end)

        test("ZREM (redis:zset_remove)", function() 
            utils.zset_add_return(redis, 'zset', shared.zset_sample())

            assert_true(redis:zset_remove('zset', 'a'))
            assert_false(redis:zset_remove('zset', 'x'))

            assert_error(function()
                redis:set('foo', 'bar')
                redis:zset_remove('foo', 'bar')
            end)
        end)

        test("ZRANGE (redis:zset_range)", function() 
            local zset = utils.zset_add_return(redis, 'zset', shared.zset_sample())

            assert_table_values(redis:zset_range('zset', 0, 3), { 'a', 'b', 'c', 'd' })
            assert_table_values(redis:zset_range('zset', 0, 0), { 'a' })
            assert_empty(redis:zset_range('zset', 1, 0))
            assert_table_values(redis:zset_range('zset', 0, -1), table.keys(zset))
            assert_table_values(redis:zset_range('zset', 3, -3), { 'd' })
            assert_empty(redis:zset_range('zset', 5, -3))
            assert_table_values(redis:zset_range('zset', -100, 100), table.keys(zset))

            -- TODO: should return a kind of tuple when using 'withscores'
            assert_table_values(
                redis:zset_range('zset', 0, 2, 'withscores'),
                { 'a', '-10', 'b', '0', 'c', '10' }
            )

            assert_error(function()
                redis:set('foo', 'bar')
                redis:zset_range('foo', 0, -1)
            end)
        end)

        test("ZREVRANGE (redis:zset_reverse_range)", function() 
            -- TODO
        end)

        test("ZRANGEBYSCORE (redis:zset_range_by_score)", function() 
            -- TODO
        end)

        test("ZCARD (redis:zset_cardinality)", function() 
            local zset = utils.zset_add_return(redis, 'zset', shared.zset_sample())

            assert_equal(redis:zset_cardinality('zset'), #table.keys(zset))

            redis:zset_remove('zset', 'a')
            assert_equal(redis:zset_cardinality('zset'), #table.keys(zset) - 1)

            redis:zset_add('zsetB', 0, 'a')
            redis:zset_remove('zsetB', 'a')
            assert_equal(redis:zset_cardinality('zsetB'), 0)

            assert_equal(redis:zset_cardinality('doesnotexist'), 0)

            assert_error(function()
                redis:set('foo', 'bar')
                redis:zset_cardinality('foo')
            end)
        end)

        test("ZSCORE (redis:zset_score)", function() 
            -- TODO
        end)

        test("ZREMRANGEBYSCORE (redis:zset_remove_range_by_score)", function() 
            -- TODO
        end)
    end)

    context("Sorting", function() 
        -- TODO: missing tests for params GET and BY

        before(function()
            -- TODO: code duplication!
            list01, list01_values = "list01", { "4","2","3","5","1" }
            for _,v in ipairs(list01_values) do redis:push_tail(list01,v) end

            list02, list02_values = "list02", { "1","10","2","20","3","30" }
            for _,v in ipairs(list02_values) do redis:push_tail(list02,v) end
        end)

        test("SORT (redis:sort)", function() 
            local sorted = redis:sort(list01)
            assert_table_values(sorted, { "1","2","3","4","5" })
        end)

        test("SORT (redis:sort) with parameter ASC/DESC", function() 
            assert_table_values(redis:sort(list01, { sort = 'asc'}),  { "1","2","3","4","5" })
            assert_table_values(redis:sort(list01, { sort = 'desc'}), { "5","4","3","2","1" })
        end)

        test("SORT (redis:sort) with parameter LIMIT", function() 
            assert_table_values(redis:sort(list01, { limit = { 0,3 } }), { "1","2", "3" })
            assert_table_values(redis:sort(list01, { limit = { 3,2 } }), { "4","5" })
        end)

        test("SORT (redis:sort) with parameter ALPHA", function() 
            assert_table_values(redis:sort(list02, { alpha = false }), { "1","2","3","10","20","30" })
            assert_table_values(redis:sort(list02, { alpha = true }),  { "1","10","2","20","3","30" })
        end)

        test("SORT (redis:sort) with multiple parameters", function() 
            assert_table_values(redis:sort(list02, { 
                alpha = false, 
                sort  = 'desc', 
                limit = { 1, 4 }
            }), { "20","10","3","2" })
        end)

        test("SORT (redis:sort) with parameter STORE", function() 
            assert_equal(redis:sort(list01, { store = 'list01_ordered' }), 5)
            assert_true(redis:exists('list01_ordered'))
        end)
    end)

    context("Multiple databases handling commands", function() 
        test("SELECT (redis:select_database)", function() 
            if not settings.database then return end

            assert_true(redis:select_database(0))
            assert_true(redis:select_database(settings.database))
            assert_error(function() redis:select_database(100) end)
            assert_error(function() redis:select_database(-1) end)
        end)

        test("FLUSHDB (redis:flush_database)", function() 
            assert_true(redis:flush_database())
        end)

        test("MOVE (redis:move_key)", function() 
            if not settings.database then return end

            local other_db = settings.database + 1
            redis:set('foo', 'bar')
            redis:select_database(other_db)
            redis:flush_database()
            redis:select_database(settings.database)

            assert_true(redis:move_key('foo', other_db))
            assert_false(redis:move_key('foo', other_db))
            assert_false(redis:move_key('doesnotexist', other_db))

            redis:set('hoge', 'piyo')
            assert_error(function() redis:move_key('hoge', 100) end)
        end)
    end)

    context("Remote server control commands", function() 
        test("INFO (redis:info)", function() 
            local server_info = redis:info()
            assert_not_nil(server_info.redis_version)
            assert_type(server_info, 'table')
            assert_greater_than(tonumber(server_info.uptime_in_seconds), 0)
            assert_greater_than(tonumber(server_info.total_connections_received), 0)
        end)

        test("SLAVEOF (redis:slave_of)", function() 
            local master_host, master_port = 'www.google.com', 80

            assert_true(redis:slave_of(master_host, master_port))
            local server_info = redis:info()
            assert_equal(server_info.role, 'slave')
            assert_equal(server_info.master_host, master_host)
            assert_equal(server_info.master_port, tostring(master_port))

            -- SLAVE OF NO ONE (explicit)
            assert_true(redis:slave_of('NO', 'ONE'))
            local server_info = redis:info()
            assert_equal(server_info.role, 'master')
        end)
    end)

    context("Persistence control commands", function() 
        test("SAVE (redis:save)", function() 
            assert_true(redis:save())
        end)

        test("BGSAVE (redis:background_save)", function() 
            assert_equal(redis:background_save(), 'Background saving started')
        end)

        test("BGREWRITEAOF (redis:background_rewrite_aof)", function() 
            assert_equal(redis:background_rewrite_aof(), 'Background append only file rewriting started')
        end)

        test("LASTSAVE (redis:last_save)", function() 
            assert_greater_than(tonumber(redis:last_save()), 0)
        end)
    end)
end)
