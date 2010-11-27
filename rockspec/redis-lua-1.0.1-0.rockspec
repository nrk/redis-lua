package = "redis-lua"
version = "1.0.1-0"

source = {
   url = "http://cloud.github.com/downloads/nrk/redis-lua/redis-lua-1.0.1-0.tar.gz",
   md5 = "0e00178a8bc7d68d463007eec49117d5"
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
