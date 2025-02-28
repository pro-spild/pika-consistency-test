#!/bin/bash

./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -sh 127.0.0.1 -sp 9241 -sh 127.0.0.1 -sp 9251 -r 60 -d 60 -n 1000 -checkMode 2 -log-file SET-12345-check.log SET __key__ __data__ &
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -sh 127.0.0.1 -sp 9241 -sh 127.0.0.1 -sp 9251 -r 59 -d 59 -n 1000 -checkMode 2 -log-file HSET-12345-check.log HSET __key__ __key__ __data__ &
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -sh 127.0.0.1 -sp 9241 -sh 127.0.0.1 -sp 9251 -r 58 -d 58 -n 1000 -checkMode 2 -log-file LPUSH-12345-check.log LPUSH __key__ __key__ __key__ &
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -sh 127.0.0.1 -sp 9241 -sh 127.0.0.1 -sp 9251 -r 57 -d 57 -n 1000 -checkMode 2 -log-file SADD-12345-check.log SADD __key__ __key__ __data__ &
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -sh 127.0.0.1 -sp 9241 -sh 127.0.0.1 -sp 9251 -r 56 -d 56 -n 1000 -checkMode 2 -log-file ZADD-12345-check.log ZADD __key__ 10 __key__ 9 __key__ &
./consistency_check_tool -mh 127.0.0.1 -mp 9221 -sh 127.0.0.1 -sp 9231 -sh 127.0.0.1 -sp 9241 -sh 127.0.0.1 -sp 9251 -r 55 -d 55 -n 1000 -checkMode 2 -log-file XADD-12345-check.log XADD __key__ 1 __key__ __data__ 
