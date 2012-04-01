package = "redis-lua"
version = "2.0.3-0"

source = {
   url = "http://cloud.github.com/downloads/nrk/redis-lua/redis-lua-2.0.3-0.tar.gz",
   md5 = "28b370247c63a9cfd9e346e57c1a7f7a"
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
