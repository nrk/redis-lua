package.path = package.path .. ";../?.lua"

require "luarocks.require"
require 'base'
require "telescope"
require "redis"

local settings = {
    host     = '127.0.0.1',
    port     = 6379,
    database = 15,
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

    --[[  TODO: 
      - commands operating on lists
      - commands operating on sets
      - multiple databases handling commands
      - persistence control commands
      - remote server control commands
    ]]
end)
