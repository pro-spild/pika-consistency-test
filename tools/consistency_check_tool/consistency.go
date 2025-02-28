package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type StringSlice []string

var checkMode int64

func (s *StringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *StringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var slaves StringSlice
	var slavesPort StringSlice
	var slavesUser StringSlice
	var slavesPasswd StringSlice

	master := flag.String("mh", "", "Master hostName")
	masterPort := flag.String("mp", "", "Master port")
	masterUser := flag.String("mu", "", "Master user")
	masterPassword := flag.String("mpw", "", "Master password")
	flag.Var(&slaves, "sh", "Slave hostName")
	flag.Var(&slavesPort, "sp", "Slave port")
	flag.Var(&slavesUser, "su", "Slave user")
	flag.Var(&slavesPasswd, "spw", "Slave password")
	consistencyCheck := flag.Int64("checkMode", 0, "0: just write to master;\n 1: Check consistency of master and slaves(might write failded in master;\n 2: Check that all nodes(master and slaves) are consistent with the written value previously")
	logFile := flag.String("log-file", "", "Log file. If empty will not save.")
	seed := flag.Int64("random-seed", 12345, "random seed to be used.")
	clients := flag.Uint64("c", 50, "number of clients.")
	keyspacelen := flag.Uint64("r", 100000, "keyspace length. The benchmark will expand the string __key__ inside an argument with a number in the specified range from 0 to keyspacelen-1. The substitution changes every time a command is executed.")
	datasize := flag.Uint64("d", 30, "Data size of the expanded string __data__ value in bytes. The benchmark will expand the string __data__ inside an argument with a charset with length specified by this parameter. The substitution changes every time a command is executed.")
	numberRequests := flag.Uint64("n", 10000000, "Total number of requests")

	flag.Parse()
	cmd := flag.Args()

	// check if the number of slaves and slave ports are equal
	if len(slaves) != len(slavesPort) {
		panic("The number of slaves and slave ports should be equal")
	}

	// check if the user and password
	if len(slavesUser) != len(slavesPasswd) {
		panic("The number of slaves user and password should be equal")
	}

	if len(slavesPasswd) < len(slaves) {
		for i := len(slavesPasswd); i < len(slaves); i++ {
			slavesPasswd = append(slavesPasswd, "")
			slavesUser = append(slavesUser, "")
		}
	}

	if len(cmd) < 2 {
		log.Fatalf("You need to specify a command after the flag command arguments. The commands requires a minimum size of 2 ( command name and key )")
	}

	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	checkMode = *consistencyCheck
	cmdKeyplaceHolderPos, cmdDataplaceHolderPos := getplaceholderpos(cmd)
	samplesPerClient := *numberRequests / *clients
	masterOpts := redis.Options{}
	if *masterUser != "" {
		masterOpts.Username = *masterUser
	}
	if *masterPassword != "" {
		masterOpts.Password = *masterPassword
	}
	masterOpts.Addr = *master + ":" + *masterPort
	slavesOpts := make([]redis.Options, len(slaves))

	for i, slave := range slaves {
		slavesOpts[i].Addr = slave + ":" + slavesPort[i]
		if slavesUser[i] != "" {
			slavesOpts[i].Username = slavesUser[i]
		}
		if slavesPasswd[i] != "" {
			slavesOpts[i].Password = slavesPasswd[i]
		}
	}

	stopChan := make(chan struct{})
	datapointsChan := make(chan datapoint, *clients)

	wg := sync.WaitGroup{}
	var sourceClient *redis.Client
	var targetClient []*redis.Client
	for clientID := 1; clientID <= int(*clients); clientID++ {
		wg.Add(1)
		sourceClient = redis.NewClient(&masterOpts)
		targetClient = make([]*redis.Client, len(slaves))
		for i, slave := range slavesOpts {
			targetClient[i] = redis.NewClient(&slave)
		}
		go checkRoutine(sourceClient, targetClient, cmd, *keyspacelen, *datasize, samplesPerClient, cmdKeyplaceHolderPos, cmdDataplaceHolderPos, *seed+int64(clientID), &wg, datapointsChan)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	tick := time.NewTicker(1 * time.Second)
	closed, totalCommands, totalErrors, totalDiffCommand := updateCLI(*numberRequests, *tick, c, datapointsChan)
	if !closed {
		tick.Stop()
	}
	if checkMode == 0 {
		log.Printf("Total commands: %d\nTotal errors: %d\n", totalCommands, totalErrors)
	} else {
		log.Printf("Total commands: %d\nTotal errors: %d\nTotal diff commands: %d\n", totalCommands, totalErrors, totalDiffCommand)
	}
	close(stopChan)
	wg.Wait()
	os.Exit(0)
	fmt.Printf("\nTest finished\n")
}

func checkRoutine(sourceClient *redis.Client, targetClient []*redis.Client, cmds []string, keySpaceLen, dataSize, samplesPerClient uint64, keyPlace, dataPlace []int, seed int64, wg *sync.WaitGroup, dataPointsChan chan datapoint) {
	defer wg.Done()
	r := rand.New(rand.NewSource(seed))
	for i := uint64(0); i < samplesPerClient; i++ {
		writeCmds, _ := keyBuildLogic(keyPlace, dataPlace, dataSize, keySpaceLen, cmds, charSet, r)
		if writeCmds[0] == "LPOP" {
			writeCmds = writeCmds[:2]
		} else if writeCmds[0] == "XDEL" {
			writeCmds = writeCmds[:3]
		}
		sendCmd(sourceClient, targetClient, writeCmds, dataPointsChan)
	}
}

func sendCmd(sourceClient *redis.Client, targetClients []*redis.Client, writeCmdS []string, dataPointsChan chan datapoint) {
	ctx := context.TODO()
	var err error
	startT := time.Now()
	isConsistent := true

	if checkMode == 0 {
		// send write command to master
		args := make([]interface{}, len(writeCmdS))
		for i, v := range writeCmdS {
			args[i] = v
		}
		err = sourceClient.Do(ctx, args...).Err()
		// log.Printf("Source write command is: %v", writeCmdS)

		if err != nil {
			log.Printf("Received an error with the following command(s): %v, error: %v", writeCmdS, err)
		}

	} else if checkMode == 1 {
		// check master and all slave consistency

		// get read command from write command
		readCmds, _ := getReadCmdsFromWriteCmds(writeCmdS)
		readArgs := make([]interface{}, len(readCmds))
		for i, v := range readCmds {
			readArgs[i] = v
		}
		sourceResult := sourceClient.Do(ctx, readArgs...).Val()
		// send read command to all slave node
		for _, targetClient := range targetClients {
			targetResult := targetClient.Do(ctx, readArgs...).Val()
			if readCmds[0] == "XRANGE" {
				isConsistent = compareXStreamResults(sourceResult, targetResult)
			} else {
				isConsistent = compareResults(sourceResult, targetResult)
			}
			if isConsistent {
				// log.Printf("Target read command is: %v\nSuccess:\nMaster result: %v\nSlave result: %v\n", readCmds, sourceResult, targetResult)
			} else {
				isConsistent = false
				log.Printf("Target:%s read command is: %v\nFailed:\nMaster result: %v\nSlave result: %v\n", targetClient.Options().Addr, readCmds, sourceResult, targetResult)
			}
		}
	} else {
		// check that all nodes are consistent with the written value previously
		readCmds, result := getReadCmdsFromWriteCmds(writeCmdS)
		readArgs := make([]interface{}, len(readCmds))
		expectResult := make([]interface{}, len(result))
		for i, v := range readCmds {
			readArgs[i] = v
		}
		for i, v := range result {
			expectResult[i] = v
		}
		targetClients = append(targetClients, sourceClient)
		// send read command to all slave node
		for _, targetClient := range targetClients {
			targetResult := targetClient.Do(ctx, readArgs...).Val()
			if readCmds[0] == "XRANGE" {
				isConsistent = compareXStreamResults(expectResult, targetResult)
			} else {
				isConsistent = compareResults(expectResult, targetResult)
			}
			if isConsistent {
				// log.Printf("Target read command is: %v\nSuccess:\nMaster result: %v\nSlave result: %v\n", readCmds, sourceResult, targetResult)
			} else {
				log.Printf("Target:%s read command is: %v\nFailed:\nMaster result: %v\nSlave result: %v\n", targetClient.Options().Addr, readCmds, expectResult, targetResult)
			}
		}
	}

	endT := time.Now()
	duration := endT.Sub(startT)
	dataPointsChan <- datapoint{cmdSuccess: err == nil, durationMS: duration.Microseconds(), isConsistent: isConsistent}
}

func updateCLI(message_limit uint64, tick time.Ticker, c chan os.Signal, datadatapointsChan chan datapoint) (closed bool, totalCommands uint64, totalErrors uint64, totalDiffCommand uint64) {
	var currentErr uint64 = 0
	var currentCount uint64 = 0
	var diffComandCount uint64 = 0
	start := time.Now()
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	var dp datapoint
	if checkMode == 0 {
		fmt.Printf("%26s %25s %7s %25s %7s %25s\n", "Test time", "Total Commands", " ", "Total Errors", " ", "Command Rate")
	} else {
		fmt.Printf("%26s %25s %7s %25s %7s %25s %25s\n", "Test time", "Total Commands", " ", "Total Errors", " ", "Total Diff Commands", "Command Rate")
	}
	for {
		select {
		case dp = <-datadatapointsChan:
			{
				if !dp.cmdSuccess {
					currentErr++
				}
				if !dp.isConsistent {
					diffComandCount++
				}
				currentCount++
			}
		case <-tick.C:
			{
				totalCommands += currentCount
				totalErrors += currentErr
				totalDiffCommand += diffComandCount
				currentErr = 0
				currentCount = 0
				diffComandCount = 0
				now := time.Now()
				took := now.Sub(prevTime)
				messageRate := float64(totalCommands-prevMessageCount) / float64(took.Seconds())
				completionPercent := float64(totalCommands) / float64(message_limit) * 100
				errorPercent := float64(totalErrors) / float64(totalCommands) * 100
				if prevMessageCount == 0 && totalCommands != 0 {
					start = time.Now()
				}

				prevMessageCount = totalCommands
				prevTime = now
				if checkMode == 0 {
					fmt.Printf("%25.0fs %25d [%3.2f%%] %25d [%3.2f%%] %25.0f\t", time.Since(start).Seconds(), totalCommands, completionPercent, totalErrors, errorPercent, messageRate)
				} else {
					fmt.Printf("%25.0fs %25d [%3.2f%%] %25d [%3.2f%%] %25d  %25.0f\t", time.Since(start).Seconds(), totalCommands, completionPercent, totalErrors, errorPercent, totalDiffCommand, messageRate)
				}
				fmt.Printf("\n")
				if messageRate > 0 && totalCommands >= uint64(message_limit) {
					return true, totalCommands, totalErrors, totalDiffCommand
				}
			}
		case <-c:
			log.Printf("\nreceived signal to stop\n")
			return true, totalCommands, totalErrors, totalDiffCommand
		}
	}
}

func compareResults(expectResult, targetResult interface{}) bool {
	expectStrSlice := convertToStringSlice(expectResult)
	targetStrSlice := convertToStringSlice(targetResult)
	sort.Strings(expectStrSlice)
	sort.Strings(targetStrSlice)
	return reflect.DeepEqual(expectStrSlice, targetStrSlice)
}

func convertToStringSlice(result interface{}) []string {
	var strSlice []string
	switch v := result.(type) {
	case []interface{}:
		for _, item := range v {
			strSlice = append(strSlice, fmt.Sprintf("%v", item))
		}
	case []string:
		strSlice = v
	case string:
		strSlice = append(strSlice, v)
	default:
		strSlice = append(strSlice, fmt.Sprintf("%v", v))
	}
	return strSlice
}

func compareXStreamResults(expectResult, targetResult interface{}) bool {
	expectValues := extractXStreamValues(expectResult)
	targetValues := extractXStreamValues(targetResult)
	sort.Strings(expectValues)
	sort.Strings(targetValues)
	return reflect.DeepEqual(expectValues, targetValues)
}

func extractXStreamValues(result interface{}) []string {
	var values []string
	switch v := result.(type) {
	case []interface{}:
		for _, item := range v {
			switch item := item.(type) {
			case []interface{}:
				if len(item) > 1 {
					if innerValues, ok := item[1].([]interface{}); ok {
						for _, innerValue := range innerValues {
							values = append(values, fmt.Sprintf("%v", innerValue))
						}
					}
				}
			default:
				values = append(values, fmt.Sprintf("%v", item))
			}
		}
	case []string:
		values = v
	case string:
		values = append(values, v)
	default:
		values = append(values, fmt.Sprintf("%v", v))
	}
	return values
}
