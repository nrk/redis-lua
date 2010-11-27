package = "redis-lua"
version = "1.0.1-0"

source = {
   url = "http://download.github.com/nrk-redis-lua-v1.0.1-0-g36cb1d2.tar.gz",
   md5 = "6fba15da590b4cbaffbc48a9359543f4"
}

description = {
   summary = "A Lua client library for the redis key value storage system.",
   detailed = [[
      A Lua client library for the redis key value storage system.
   ]],
   homepage = "http://github.com/nrk/redis-lua",
   license = "MIT/X11"
}

dependencies = {
   "lua >= 5.1",
   "luasocket"
}

build = {
   type = "none",
   install = {
      lua = {
         "redis.lua"
      }
   }
}
