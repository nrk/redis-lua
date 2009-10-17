require "luarocks.require"
require "telescope"
require "redis"

local settings = {
    host    = "127.0.0.1",
    port    = 6379, 
    version = 1.001,
    multiple_dbs = false, 
}

make_assertion("true", "'%s' to be true", function(a) return a == true end)
make_assertion("false", "'%s' to be false", function(a) return a == false end)
make_assertion("table_values", "'%s' to have the same values as '%s'", function(a,b)
    if #a ~= #b then return false end
    for i,k in ipairs(a) do if k ~= b[i] then return false end end
    return true
end)

context("Redis client", function() 
    it("Connects to " .. settings.host .. ":" .. settings.port, function()
        redis = Redis.connect(settings.host, settings.port)
        -- 
        assert_not_nil(redis)
        assert_not_nil(redis.socket)
    end)

    it("Matches the target version " .. string.format("%.3f", settings.version), function()
        local info = redis:info()
        assert_not_empty(info)
        assert_gte(tonumber(info.redis_version), settings.version)
    end)

    context("Miscellaneous commands", function() 
        test("PING (redis:ping)", function() 
            assert_true(redis:ping())
        end)

        test("ECHO (redis:echo)", function() 
            local echo_ascii = "Can you hear me?"
            assert_equal(redis:echo(echo_ascii), echo_ascii)

            local echo_utf8  = "聞こえますか？"
            assert_equal(redis:echo(echo_utf8), echo_utf8)
        end)

        --TODO: auth
    end)

    context("Commands operating on string values", function() 
        before(function()
            redis:flush_databases()
        end)

        test("SET (redis:set)", function() 
            local k1 = "k1"

            assert_true(redis:set(k1, 1))
            assert_true(redis:set(k1, 2))
        end)

        test("SETNX (redis:set_preserve)", function() 
            local k1 = "k1"

            assert_true(redis:set_preserve(k1, 1))
            assert_false(redis:set_preserve(k1, 2))
        end)

        test("MSET (redis:multiple_set)", function()
            local kvs = { 
                italian  = "ciao", 
                english  = "hello", 
                japanese = "こんいちは！", 
            }

            assert_true(redis:multiple_set(kvs))
            assert_true(redis:multiple_set('a', 1, 'b', 2, 'c', 3))
        end)

        test("MSETNX (redis:multiple_set_preserve)", function()
           assert_true(redis:multiple_set_preserve({ a = 1, b = 2, c = 3 }))
           assert_false(redis:multiple_set_preserve('d', 4, 'a', 'dup', 'e', 5))
        end)

        test("GET (redis:get)", function() 
            local k1, v1= "k1", "v1"

            assert_nil(redis:get(k1))
            assert_true(redis:set(k1, v1))
            assert_equal(redis:get(k1), v1)
        end)

        test("MGET (redis:get_multiple)", function() 
            local keys   = { "italian", "english", "japanese" }
            local values = { "ciao!", "hello!", "こんいちは！" }

            for i,k in ipairs(keys) do redis:set(k,values[i]) end
            local mget_values = redis:get_multiple(unpack(keys))

            assert_table_values(mget_values, values)
        end)

        test("GETSET (redis:get_set)", function() 
            local k1, v1, v2 = "k1", "v1", "v2"

            assert_nil(redis:get_set(k1, v1))
            assert_equal(redis:get_set(k1, v2), v1)
            assert_equal(redis:get_set(k1, v1), v2)
        end)

        test("INCR (redis:increment)", function() 
            local k1 = "k1"

            assert_true(redis:set(k1, -2))
            assert_equal(redis:increment(k1), -1)
            assert_equal(redis:increment(k1), 0)
            assert_equal(redis:increment(k1), 1)
        end)

        test("INCRBY (redis:increment_by)", function() 
            local k1 = "k1"

            assert_true(redis:set(k1, 0))
            assert_equal(redis:increment_by(k1, 10), 10)
            assert_equal(redis:increment_by(k1, 20), 30)
            assert_equal(redis:increment_by(k1, -50), -20)
        end)

        test("DECR (redis:decrement)", function()  
            local k1 = "k1"

            assert_true(redis:set(k1, 1))
            assert_equal(redis:decrement(k1), 0)
            assert_equal(redis:decrement(k1), -1)
            assert_equal(redis:decrement(k1), -2)
        end)

        test("DECRBY (redis:decrement_by)", function() 
            local k1 = "k1"

            assert_true(redis:set(k1, 0))
            assert_equal(redis:decrement_by(k1, 10), -10)
            assert_equal(redis:decrement_by(k1, 20), -30)
            assert_equal(redis:decrement_by(k1, -50), 20)
        end)

        test("EXISTS (redis:exists)", function() 
            local k1, k2 = "k1", "k2"

            assert_true(redis:set(k1, 0))
            assert_true(redis:exists(k1))
            assert_false(redis:exists(k2))
        end)

        test("DEL (redis:delete)", function() 
            local k1, k2 = "k1", "k2"

            assert_true(redis:set(k1, 0))
            assert_true(redis:delete(k1))
            assert_false(redis:delete(k2))
        end)

        test("TYPE (redis:type)", function() 
            local k1, k2, k3, k4 = "k1", "k2", "k3", "k4"

            assert_true(redis:set(k1, "string"))
            assert_true(redis:push_tail(k2, 0))
            assert_equal(redis:set_add(k3, 0), 1)

            assert_equal(redis:type(k1), "string")
            assert_equal(redis:type(k2), "list")
            assert_equal(redis:type(k3), "set")
            assert_equal(redis:type(k4), "none")
        end)
    end)

    context("Sorting", function() 
        -- TODO: missing tests for params GET and BY

        before(function()
            redis:flush_databases()

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
    end)

    --[[  TODO: 
      - commands operating on the key space
      - commands operating on lists
      - commands operating on sets
      - multiple databases handling commands
      - persistence control commands
      - remote server control commands
    ]]

    test("QUIT (redis:quit) closes the connection to the server", function() 
        assert_nil(redis:quit())
    end)
end)
