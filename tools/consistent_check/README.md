## 介绍
consistent_check 用来进行数据一致性校验，目前支持了 string 类型的 set/get/mset/mget 和 hash 类型的 hset/hget/hmset/hmget/hgetall 的校验。

## 参数
所有的命令行参数如下所示：
```
  -count (request counts per thread) type: int32 default: 100000
  -dbs (dbs name, eg: 0,1,2) type: string default: "0"
  -element_count (elements number in hash/list/set/zset) type: int32 default: 1
  -host (target server's host) type: string default: "127.0.0.1"
  -key_size (key size int bytes) type: int32 default: 50
  -password (password) type: string default: ""
  -port (target server's listen port) type: int32 default: 9221
  -thread_num (concurrent thread num) type: int32 default: 10
  -timeout (request timeout) type: int32 default: 1000
  -value_size (value size in bytes) type: int32 default: 100
  -is_batch, false, "check with batch command or not"
  -type, "string", "data type for consistent check"
  -max_retries, 10, "max retry times when value mismatch"
```

## 使用方式
需要先执行generate方式生成待请求的key，如：
```
./consistent_check --type=generate --count=2 --element_count=10 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1
```
执行完成后会在当前目录生成两个consistent_file_*文件（每个线程生成一个），每个文件行数为20（element_count * count），每个key长度为10.

每个 key 由三部分构成，8字节的主机名_进程ID_随机字符串，保证不同客户端机器，不同进程之间数据不重复。 每个value为一个自增的整数，初始为 0。当进行数据一致性校验时，会首先从consistent_file_* 中读取所有的 key，然后调用 Redis API 将数据写入，写入成功之后将数据读出进行一致性校验，考虑到 Pika 的异步复制可能读到旧数据， 当返回的数据不符合预期时，consistent_check 会进行有限次数的重试，重试次数可以通过 max_retries 设置，当重试次数达到设定值结果仍然不符合预期时，进程退出，打印错误日志。

当校验 string 类型单 key 接口的一致性时，执行：
```
./consistent_check --string=string --count=2 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --is_batch=false
```

当校验 string 类型批量接口的一致性时，执行：
```
./consistent_check --string=string --count=2 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --is_batch=true
```

当校验 hash 类型单 key 接口的一致性时，执行：
```
./consistent_check --string=hash --count=2 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --is_batch=false
```

当校验 hash 类型批量接口的一致性时，执行：
```
./consistent_check --string=hash --count=2 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --is_batch=true
```