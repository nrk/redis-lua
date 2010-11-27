package = "redis-lua"
version = "2.0.0-0"

source = {
   url = "http://cloud.github.com/downloads/nrk/redis-lua/redis-lua-2.0.0-0.tar.gz",
   md5 = "db1f9a74d13158c1b551a4fa054a92ba"
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
         redis = "src/redis.lua"
      }
   }
}
