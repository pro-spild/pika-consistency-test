package main

import (
	"log"
	"math/rand"
	"strings"
)

const charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type datapoint struct {
	cmdSuccess   bool
	durationMS   int64
	isConsistent bool
}

func stringWithCharset(length int, charset string, r *rand.Rand) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func keyBuildLogic(keyPos []int, dataPos []int, datasize, keyspacelen uint64, cmdS []string, charset string, r *rand.Rand) (newCmdS []string, key string) {
	newCmdS = make([]string, len(cmdS))
	copy(newCmdS, cmdS)
	for i := range keyPos {
		keyV := stringWithCharset(int(keyspacelen), charset, r)
		key = strings.Replace(newCmdS[keyPos[i]], "__key__", keyV, -1)
		newCmdS[keyPos[i]] = key
	}
	for i := range dataPos {
		newCmdS[dataPos[i]] = stringWithCharset(int(datasize), charset, r)
	}
	// only reserve keyPos in newCmdS
	if isDelCommand(newCmdS[0]) && newCmdS[0] != "XDEL" {
		delCommand := make([]string, len(keyPos)+1)
		delCommand[0] = newCmdS[0]
		for i := range keyPos {
			delCommand[i+1] = newCmdS[keyPos[i]]
		}
		return delCommand, key
	}
	return newCmdS, key
}

func getplaceholderpos(args []string) (keyPlaceOlderPos []int, dataPlaceOlderPos []int) {
	keyPlaceOlderPos = make([]int, 0)
	dataPlaceOlderPos = make([]int, 0)
	for pos, arg := range args {
		if arg == "__data__" {
			dataPlaceOlderPos = append(dataPlaceOlderPos, pos)
		}

		if strings.Contains(arg, "__key__") {
			keyPlaceOlderPos = append(keyPlaceOlderPos, pos)
		}
	}
	return
}

func getReadCmdsFromWriteCmds(writeCmds []string) (readCmds, result []string) {
	readCmd := getcommandForWrite[writeCmds[0]]
	if readCmd == "" {
		// error print
		log.Fatalf("No read command found for write command %s", writeCmds[0])
	}
	switch readCmd {
	case "GET":
		// SET key value
		// writeCmds[SET, key, value]
		// readCmds[GET, key]
		readCmds = []string{"GET", writeCmds[1]}
		result = []string{writeCmds[2]}
	case "MGET":
		// MSET key value [key value ...]
		// writeCmds[MSET, key, value, key, value]
		// readCmds[MGET, key, key]
		readCmds = []string{"MGET"}
		for i := 1; i < len(writeCmds); i += 2 {
			readCmds = append(readCmds, writeCmds[i])
		}
		var values []string
		for i := 2; i < len(writeCmds); i += 2 {
			values = append(values, writeCmds[i])
		}
		result = append(result, values...)
	case "HGET":
		// HSET key filed value
		// writeCmds[HSET, key, field, value]
		// readCmds[HGET, key, field]
		readCmds = []string{"HGET"}
		readCmds = append(readCmds, writeCmds[1], writeCmds[2])
		result = []string{writeCmds[3]}
	case "HMGET":
		// HMSET key field value [field value ...]
		// writeCmds[HMSET, key, field, value, field, value]
		var fields []string
		for i := 2; i < len(writeCmds); i += 2 {
			fields = append(fields, writeCmds[i])
		}
		var values []string
		for i := 3; i < len(writeCmds); i += 2 {
			values = append(values, writeCmds[i])
		}
		readCmds = []string{"HMGET", writeCmds[1]}
		readCmds = append(readCmds, fields...)
		result = append(result, values...)
	case "LRANGE":
		// LPUSH key value [value ...]
		// writeCmds[LPUSH, key, value, value]
		readCmds = []string{"LRANGE", writeCmds[1], "0", "-1"}
		result = writeCmds[2:]
	case "SMEMBERS":
		// SADD key member [member ...]
		// writeCmds[SADD, key, member, member]
		readCmds = []string{"SMEMBERS", writeCmds[1]}
		result = writeCmds[2:]
	case "ZRANGE":
		// ZADD key score member [score member ...]
		// writeCmds[ZADD, key, score, member]
		readCmds = []string{"ZRANGE", writeCmds[1], "0", "-1"}
		var members []string
		for i := 3; i < len(writeCmds); i += 2 {
			members = append(members, writeCmds[i])
		}
		result = append(result, members...)
	case "XRANGE":
		// XADD key id field value [field value ...]
		// writeCmds[XADD, key, id, field, value]
		readCmds = []string{"XRANGE", writeCmds[1], "-", "+"}
		result = writeCmds[3:]
	}
	return
}

func isDelCommand(cmd string) bool {
	for _, delCmd := range delCommand {
		if cmd == delCmd {
			return true
		}
	}
	return false
}
