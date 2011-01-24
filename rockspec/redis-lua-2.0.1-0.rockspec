package = "redis-lua"
version = "2.0.1-0"

source = {
   url = "http://cloud.github.com/downloads/nrk/redis-lua/redis-lua-2.0.1-0.tar.gz",
   md5 = "824c9bd4e98b919747c6f1f3be322196"
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
