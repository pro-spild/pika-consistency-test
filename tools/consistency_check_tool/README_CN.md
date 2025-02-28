# 一致性测试工具
使用方法为*先执行写入操作，再执行读取操作*，最后执行一致性检查操作。两次操作的k-v参数必须完全一致，随机种子也必须一致。
使用场景为检测主从同步一致性，以及重启/升级前后数据一致性。
检测原理为先写入数据，从主从节点分别读取数据，最后比较两次读取的数据是否一致。或者比较生成的写数据和读取的数据是否一致。

## 编译
```shell
go build
```

## 支持的参数
- `-mh`：目标主机的 IP 地址；
- `-mp`：目标主机的端口号；
- `mu` : 目标主机用户名；
- `mpw` : 目标主机密码；
- `-sh`：从节点的 IP 地址；
- `-sp`：从节点的端口号；
- `su` : 从节点用户名；
- `spw` : 从节点密码；
- `-r`：生成随机key的长度；
- `-d`：生成随机value的长度；
- `-n`: 总共生成到的command数量；
- `-log-file`：日志文件名；
- `-checkMode`：检查模式；
- `randomSeed`：随机种子；
- `c` : 客户端数量；
- `ARGS`：为写入的操作的参数
从节点的IP和端口号可以有多个，但是必须一一对应。
从节点的用户名和密码可以有多个，但是必须一一对应。
从节点的用户名和密码可以少于从节点的IP和端口号，匹配规则为前面的的正常配对，后面缺少的按空处理，即不使用

## 写入操作
```shell
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 60 -d 60 -n 1000 -log-file SET-12345.log SET __key__ __data__
```

## 读取操作
```shell
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -sh 127.0.0.1 -sp 9241 -sh 127.0.0.1 -sp 9251 -r 60 -d 60 -n 1000 -checkMode 2 -log-file SET-12345-check.log SET __key__ __data__
```


### 检查模式
- 0：（默认），此时为写入模式，写入失败会产生logError。
- 1：（检查模式，不存在写入），可以用作主从一致性检查，检测原理为先从主节点读取数据，再从从节点读取数据，最后比较两次读取的数据是否一致。如果前面的写入失败导致值为空，因为主从仍然是一致的，所以在check时不会产生logError。
- 2：（检查模式，不存在写入），用作重启/升级前写入埋点数据，重启/升级后检查数据是否一致。此时master和slave都为需要检测的客户端，分别读取数据，并检查是否与随机产生的写入数据一致。因为写入和检查使用的是一样的随机种子。这个时候如果存在节点数据与写入不一致，哪怕主从一致，也会产生logError。

### Args
直接把写入的命令和参数写在后面，如SET __key__ __data__，写入的key均以__key__替代，写入的value均以__data__替代。
命令只支持大写，如SET、DEL、HSET等。
eg:
*write*:
```shell
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 60 -d 60 -n 1000 -log-file SET-12345.log SET __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 59 -d 59 -n 1000 -log-file HSET-12345.log HSET __key__ __key__ __data__  
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 58 -d 58 -n 1000 -log-file LPUSH-12345.log LPUSH __key__ __key__ __key__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 57 -d 57 -n 1000 -log-file SADD-12345.log SADD __key__ __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 56 -d 56 -n 1000 -log-file ZADD-12345.log ZADD __key__ 10 __key__ 9 __key__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 55 -d 55 -n 1000 -log-file XADD-12345.log XADD __key__ 1 __key__ __data__ 
```

*check*:
```shell
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -r 60 -d 60 -n 1000 -checkMode 2 -log-file SET-12345-check.log SET __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -r 59 -d 59 -n 1000 -checkMode 2 -log-file HSET-12345-check.log HSET __key__ __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -r 58 -d 58 -n 1000 -checkMode 2 -log-file LPUSH-12345-check.log LPUSH __key__ __key__ __key__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -r 57 -d 57 -n 1000 -checkMode 2 -log-file SADD-12345-check.log SADD __key__ __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -r 56 -d 56 -n 1000 -checkMode 2 -log-file ZADD-12345-check.log ZADD __key__ 10 __key__ 9 __key__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -r 55 -d 55 -n 1000 -checkMode 2 -log-file XADD-12345-check.log XADD __key__ 1 __key__ __data__ 
```

### 删除测试写入的所有key
*delete*:
```shell
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 60 -d 60 -n 1000 -log-file DEL-12345.log DEL __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 59 -d 59 -n 1000 -log-file HDEL-12345.log HDEL __key__ __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 58 -d 58 -n 1000 -log-file LPOP-12345.log LPOP __key__ __key__ __key__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 57 -d 57 -n 1000 -log-file SREM-12345.log SREM __key__ __key__ __data__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 56 -d 56 -n 1000 -log-file ZREM-12345.log ZREM __key__ __key__ __key__ 
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -r 55 -d 55 -n 1000 -log-file XDEL-12345.log XDEL __key__ 1 __key__ __data__ __key__ __data__
```