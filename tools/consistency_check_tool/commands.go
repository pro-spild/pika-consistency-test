package main

var getcommandForWrite = map[string]string{
	"SET":   "GET",
	"MSET":  "MGET",
	"HSET":  "HGET",
	"HMSET": "HMGET",
	"LPUSH": "LRANGE",
	"SADD":  "SMEMBERS",
	"ZADD":  "ZRANGE",
	"XADD":  "XRANGE",
}

var delCommand = []string{"DEL", "HDEL", "LREM", "SREM", "ZREM", "XDEL"}
