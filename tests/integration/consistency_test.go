package pika_integration

import (
	"context"
	"fmt"
	"log"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

// func cleanEnv(ctx context.Context, clientMaster, clientSlave *redis.Client) {
// 	Expect(clientMaster.Do(ctx, "flushdb").Err()).NotTo(HaveOccurred())
// 	r := clientSlave.Do(ctx, "slaveof", "no", "one")
// 	Expect(r.Err()).NotTo(HaveOccurred())
// 	Expect(r.Val()).To(Equal("OK"))
// 	Expect(clientSlave.Do(ctx, "clearreplicationid").Err()).NotTo(HaveOccurred())
// 	Expect(clientMaster.Do(ctx, "clearreplicationid").Err()).NotTo(HaveOccurred())
// }

func doSlave(ctx context.Context, clientSlave *redis.Client, clientMaster *redis.Client) {
	infoRes := clientSlave.Info(ctx, "replication")
	Expect(infoRes.Err()).NotTo(HaveOccurred())
	Expect(infoRes.Val()).To(ContainSubstring("role:master"))
	infoRes = clientMaster.Info(ctx, "replication")
	Expect(infoRes.Err()).NotTo(HaveOccurred())
	Expect(infoRes.Val()).To(ContainSubstring("role:master"))
	Expect(clientSlave.Do(ctx, "slaveof", LOCALHOST, SLAVEPORT).Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))

	var count = 0
	for {
		res := trySlave(ctx, clientSlave, LOCALHOST, MASTERPORT)
		if res {
			break
		} else if count > 4 {
			break
		} else {
			cleanEnv(ctx, clientMaster, clientSlave)
			count++
		}
	}

	infoRes = clientSlave.Info(ctx, "replication")
	Expect(infoRes.Err()).NotTo(HaveOccurred())
	Expect(infoRes.Val()).To(ContainSubstring("master_link_status:up"))

	infoRes = clientMaster.Info(ctx, "replication")
	Expect(infoRes.Err()).NotTo(HaveOccurred())
	Expect(infoRes.Val()).To(ContainSubstring("connected_slaves:1"))

	// test slave read only, master read and write
	slaveWrite := clientSlave.Set(ctx, "foo", "bar", 0)
	Expect(slaveWrite.Err()).To(MatchError("ERR READONLY You can't write against a read only replica."))
}

type redisCommandFunc func(ctx context.Context, client *redis.Client) (interface{}, error)

// Pika guarantees read and write consistency in seconds, it waits for 1 second before reading data from the slave.
func compareValues(ctx context.Context, clientMaster *redis.Client, clientSlave *redis.Client, command redisCommandFunc) {

	time.Sleep(1 * time.Second)
	masterRes, err := command(ctx, clientMaster)
	Expect(err).NotTo(HaveOccurred())
	slaveRes, err := command(ctx, clientSlave)
	Expect(err).NotTo(HaveOccurred())
	Expect(slaveRes).To(Equal(masterRes))
}

func generateStringArray(strLength int, arrayLength int) []string {
	var res []string
	for i := 0; i < arrayLength; i++ {
		res = append(res, randomString(strLength))
	}
	return res
}

func generateZSetArray(maxScore int, strLength int, arrayLength int) []redis.Z {
	var res []redis.Z
	for i := 0; i < arrayLength; i++ {
		res = append(res, redis.Z{Score: float64(randomInt(maxScore)), Member: randomString(strLength)})
	}
	return res
}

func generateGeoArray(arrayLength int) []*redis.GeoLocation {
	var res []*redis.GeoLocation
	for i := 0; i < arrayLength; i++ {
		res = append(res, &redis.GeoLocation{Longitude: float64(randomInt(180)), Latitude: float64(randomInt(90)), Name: randomString(10)})
	}
	return res
}

func getResult(cmd redis.Cmder) (interface{}, error) {
	switch cmd := cmd.(type) {
	case *redis.StringCmd:
		return cmd.Result()
	case *redis.IntCmd:
		return cmd.Result()
	case *redis.SliceCmd:
		return cmd.Result()
	case *redis.StringSliceCmd:
		return cmd.Result()
	case *redis.GeoLocationCmd:
		return cmd.Result()
	case *redis.XMessageSliceCmd:
		return cmd.Result()
	case *redis.MapStringStringCmd:
		return cmd.Result()
	case *redis.Cmd:
		return cmd.Result()
	default:
		return nil, fmt.Errorf("unsupported command type: %T", cmd)
	}
}

var _ = Describe("should replication ", func() {
	Describe("all replication test", func() {
		ctx := context.TODO()
		var clientSlave *redis.Client
		var clientMaster *redis.Client

		BeforeEach(func() {
			clientMaster = redis.NewClient(PikaOption(MASTERADDR))
			clientSlave = redis.NewClient(PikaOption(SLAVEADDR))
			cleanEnv(ctx, clientMaster, clientSlave)
			if GlobalBefore != nil {
				GlobalBefore(ctx, clientMaster)
				GlobalBefore(ctx, clientSlave)
			}
			time.Sleep(3 * time.Second)
		})
		AfterEach(func() {
			cleanEnv(ctx, clientMaster, clientSlave)
			Expect(clientSlave.Close()).NotTo(HaveOccurred())
			Expect(clientMaster.Close()).NotTo(HaveOccurred())
			log.Println("Replication test case done")
		})

		XIt("Slave and master are continuously connected:  ", func() {
			doSlave(ctx, clientSlave, clientMaster)
			time.Sleep(20 * time.Second)
			// key test
			// err := clientMaster.SetEx(ctx, "key", randomString(10), 5*time.Second).Err()
			// Expect(err).NotTo(HaveOccurred())
			// Consistently(func() string {
			// 	return clientSlave.Get(ctx, "key").Val()
			// }, 5*time.Second, 100*time.Millisecond).Should(Equal(clientMaster.Get(ctx, "key").Val()))

			// ======================== string ========================
			// set
			By("test string_set")
			Expect(clientMaster.Set(ctx, "key_set", randomString(10), 0).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.Get(ctx, "key_set").Result()
			})

			// append
			By("test string_append")
			Expect(clientMaster.Set(ctx, "key_append", randomString(10), 0).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.Append(ctx, "key_append", randomString(10)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.Get(ctx, "key_append").Result()
			})

			// mset
			By("test string_mset")
			mSetKeys := generateStringArray(100, 100)
			mSetKeys[0] = "key_mset"
			mSetValues := generateStringArray(100, 100)
			mSetMap := make(map[string]interface{})
			for i := 0; i < 100; i++ {
				mSetMap[mSetKeys[i]] = mSetValues[i]
			}
			Expect(clientMaster.MSet(ctx, mSetMap).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.MGet(ctx, mSetKeys...).Result()
			})

			By("test string_bitop")
			Expect(clientMaster.Set(ctx, "bitkey1", "1", 0).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.Set(ctx, "bitkey2", "0", 0).Err()).NotTo(HaveOccurred())
			// and
			Expect(clientMaster.BitOpAnd(ctx, "bitkey_out_and", "bitkey1", "bitkey2").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.Get(ctx, "bitkey_out_and").Result()
			})

			// or
			Expect(clientMaster.BitOpOr(ctx, "bitkey_out_or", "bitkey1", "bitkey2").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.Get(ctx, "bitkey_out_or").Result()
			})
			// xor
			Expect(clientMaster.BitOpXor(ctx, "bitkey_out_xor", "bitkey1", "bitkey2").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.Get(ctx, "bitkey_out_xor").Result()
			})

			// not
			Expect(clientMaster.BitOpNot(ctx, "bitkey_out_not", "bitkey1").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.Get(ctx, "bitkey_out_not").Result()
			})
			// ======================== hash ========================
			// hset
			By("test hash_hset")
			Expect(clientMaster.HSet(ctx, "hash_hset", "field", randomString(10)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.HGet(ctx, "hash_hset", "field").Result()
			})

			// hdel
			By("test hash_hdel")
			Expect(clientMaster.HSet(ctx, "hash_hdel", "field", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.HSet(ctx, "hash_hdel", "field1", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.HDel(ctx, "hash_hdel", "field").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.HGetAll(ctx, "hash_hdel").Result()
			})

			// hset_multi
			By("test hash_hset_multi")
			Expect(clientMaster.HMSet(ctx, "hash_hset_multi", map[string]interface{}{"field1": randomString(10), "field2": randomString(10)}).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.HGetAll(ctx, "hash_hset_multi").Result()
			})

			// hincrby
			By("test hash_hincrby")
			Expect(clientMaster.HSet(ctx, "hash_hincrby", "field", "1").Err()).NotTo(HaveOccurred())
			Expect(clientMaster.HIncrBy(ctx, "hash_hincrby", "field", 1).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.HGetAll(ctx, "hash_hincrby").Result()
			})

			// ======================== list ========================
			// lpush
			By("test list_lpush")
			Expect(clientMaster.LPush(ctx, "list_lpush", randomString(10)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.LRange(ctx, "list_lpush", 0, -1).Result()
			})

			By("test list_mlpush")
			Expect(clientMaster.LPush(ctx, "list_mlpush", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.LRange(ctx, "list_mlpush", 0, -1).Result()
			})

			// lset
			By("test list_lset")
			Expect(clientMaster.LPush(ctx, "list_lset", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.LSet(ctx, "list_lset", 0, randomString(10)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.LRange(ctx, "list_lset", 0, -1).Result()
			})

			// rpoplpush
			By("test list_rpoplpush")
			Expect(clientMaster.LPush(ctx, "list_rpoplpush", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.LPush(ctx, "list_rpoplpush_src", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.RPopLPush(ctx, "list_rpoplpush_src", "list_rpoplpush").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.LRange(ctx, "list_rpoplpush", 0, -1).Result()
			})

			// linsert
			By("test list_linsert")
			Expect(clientMaster.LPush(ctx, "list_linsert", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())
			value := clientMaster.LIndex(ctx, "list_linsert", 50).Val()
			Expect(clientMaster.LInsert(ctx, "list_linsert", "BEFORE", value, randomString(10)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.LRange(ctx, "list_linsert", 0, -1).Result()
			})

			// lrem
			By("test list_lrem")
			Expect(clientMaster.LPush(ctx, "list_lrem", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.LRem(ctx, "list_lrem", 0, value).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.LRange(ctx, "list_lrem", 0, -1).Result()
			})

			// ======================== set ========================
			set1Value := generateStringArray(100, 100)
			set2Value := generateStringArray(100, 100)
			set3Value := generateStringArray(100, 100)

			// sadd
			By("test set_sadd")
			Expect(clientMaster.SAdd(ctx, "set_sadd", randomString(10)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.SMembers(ctx, "set_sadd").Result()
			})

			// sadd_muti
			By("test set_sadd_muti")
			Expect(clientMaster.SAdd(ctx, "set_sadd_muti", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.SMembers(ctx, "set_sadd_muti").Result()
			})

			// spop
			// 报错：redis: can't parse reply="*1" reading string
			// Expect(clientMaster.SPop(ctx, "set").Err()).NotTo(HaveOccurred())

			Expect(clientMaster.SAdd(ctx, "set1", append(set1Value, set2Value...)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.SAdd(ctx, "set2", append(set2Value, set3Value...)).Err()).NotTo(HaveOccurred())

			// sdiffstore
			By("test set_diffStore")
			Expect(clientMaster.SDiffStore(ctx, "set_diffStore", "set1", "set2").Val()).To(Equal(int64(len(set2Value))))
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.SMembers(ctx, "set_diffStore").Result()
			})

			// sinterstore
			By("test set_interStore")
			Expect(clientMaster.SInterStore(ctx, "set_interStore", "set1", "set2").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.SMembers(ctx, "set_interStore").Result()
			})

			// sunionstore
			By("test set_unionStore")
			Expect(clientMaster.SUnionStore(ctx, "set_unionStore", "set1", "set2").Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.SMembers(ctx, "set_unionStore").Result()
			})

			// smove
			By("test set_smove")
			Expect(clientMaster.SMove(ctx, "set1", "set2", set1Value[50]).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.SMembers(ctx, "set2").Result()
			})

			// srem
			By("test set_srem")
			Expect(clientMaster.SAdd(ctx, "set_srem", append(set1Value, set2Value...)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.SRem(ctx, "set_srem", set2Value).Val()).To(Equal(int64(100)))
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.SMembers(ctx, "set_srem").Result()
			})

			// ======================== zset ========================
			z1 := generateZSetArray(100, 10, 100)
			z2 := generateZSetArray(100, 10, 100)
			z3 := generateZSetArray(100, 10, 100)
			zSetValue := redis.Z{Score: float64(randomInt(10)), Member: randomString(10)}
			zSet1value := append(z1, z2...)
			zSet2value := append(z2, z3...)

			// zadd
			By("test zset_zadd")
			Expect(clientMaster.ZAdd(ctx, "zset_zadd", zSetValue).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.ZRange(ctx, "zset_zadd", 0, -1).Result()
			})

			// zadd_muti
			By("test zset_zadd_multi")
			Expect(clientMaster.ZAdd(ctx, "zset_zadd_multi", zSet1value...).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.ZRange(ctx, "zset_zadd_multi", 0, -1).Result()
			})

			Expect(clientMaster.ZAdd(ctx, "zset1", zSet1value...).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.ZAdd(ctx, "zset2", zSet2value...).Err()).NotTo(HaveOccurred())

			// zincrby
			By("test zset_zincrby")
			Expect(clientMaster.ZAdd(ctx, "zset_zincrby", zSetValue).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.ZIncrBy(ctx, "zset_zincrby", 1, zSetValue.Member.(string)).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.ZRange(ctx, "zset_zincrby", 0, -1).Result()
			})

			// unionstore
			By("test zset_zsetunion")
			Expect(clientMaster.ZUnionStore(ctx, "zset_zsetunion", &redis.ZStore{Keys: []string{"zset1", "zset2"}, Weights: []float64{1, 1}}).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.ZRange(ctx, "zset_zsetunion", 0, -1).Result()
			})

			// interstore
			By("test zset_zsetinter")
			Expect(clientMaster.ZInterStore(ctx, "zset_zsetinter", &redis.ZStore{Keys: []string{"zset1", "zset2"}, Weights: []float64{1, 1}}).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.ZRange(ctx, "zset_zsetinter", 0, -1).Result()
			})

			// zrem
			By("test zset_rem")
			delMembers := make([]string, len(z2))
			for i := 0; i < len(z2); i++ {
				delMembers[i] = z2[i].Member.(string)
			}
			Expect(clientMaster.ZAdd(ctx, "zset_rem", zSet1value...).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.ZRem(ctx, "zset_rem", delMembers).Err()).NotTo(HaveOccurred())
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.ZRange(ctx, "zset_rem", 0, -1).Result()
			})
			// ======================== geo ========================
			// geoadd
			By("test geo")
			Expect(clientMaster.GeoAdd(ctx, "geo", generateGeoArray(100)...).Val()).To(Equal(int64(100)))
			compareValues(ctx, clientMaster, clientSlave, func(ctx context.Context, client *redis.Client) (interface{}, error) {
				return client.Do(ctx, "GEORADIUS", "geo", 0, 0, 100, "km", "WITHDIST", "WITHCOORD").Result()
			})

			// ======================== stream ========================
			Expect(clientMaster.XAdd(ctx, &redis.XAddArgs{Stream: "stream", Values: map[string]interface{}{"key": randomString(10)}}).Err()).NotTo(HaveOccurred())
		})

		XIt("Slave first connect after master do all operations:  ", func() {
			// key test
			// err := clientMaster.SetEx(ctx, "key", randomString(10), 5*time.Second).Err()
			// Expect(err).NotTo(HaveOccurred())
			// Consistently(func() string {
			// 	return clientSlave.Get(ctx, "key").Val()
			// }, 5*time.Second, 100*time.Millisecond).Should(Equal(clientMaster.Get(ctx, "key").Val()))

			// ======================== string ========================
			// set
			Expect(clientMaster.Set(ctx, "key_set", randomString(10), 0).Err()).NotTo(HaveOccurred())

			// append
			Expect(clientMaster.Set(ctx, "key_append", randomString(10), 0).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.Append(ctx, "key_append", randomString(10)).Err()).NotTo(HaveOccurred())

			mSetKeys := generateStringArray(100, 100)
			mSetKeys[0] = "key_mset"
			mSetValues := generateStringArray(100, 100)
			mSetMap := make(map[string]interface{})
			for i := 0; i < 100; i++ {
				mSetMap[mSetKeys[i]] = mSetValues[i]
			}
			Expect(clientMaster.MSet(ctx, mSetMap).Err()).NotTo(HaveOccurred())

			Expect(clientMaster.Set(ctx, "bitkey1", "1", 0).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.Set(ctx, "bitkey2", "0", 0).Err()).NotTo(HaveOccurred())
			// and
			Expect(clientMaster.BitOpAnd(ctx, "bitkey_out_and", "bitkey1", "bitkey2").Err()).NotTo(HaveOccurred())
			// or
			Expect(clientMaster.BitOpOr(ctx, "bitkey_out_or", "bitkey1", "bitkey2").Err()).NotTo(HaveOccurred())
			// xor
			Expect(clientMaster.BitOpXor(ctx, "bitkey_out_xor", "bitkey1", "bitkey2").Err()).NotTo(HaveOccurred())
			// not
			Expect(clientMaster.BitOpNot(ctx, "bitkey_out_not", "bitkey1").Err()).NotTo(HaveOccurred())

			// ======================== hash ========================
			// hset
			Expect(clientMaster.HSet(ctx, "hash_hset", "field", randomString(10)).Err()).NotTo(HaveOccurred())

			// hdel
			Expect(clientMaster.HSet(ctx, "hash_hdel", "field", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.HSet(ctx, "hash_hdel", "field1", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.HDel(ctx, "hash_hdel", "field").Err()).NotTo(HaveOccurred())

			// hset_multi
			Expect(clientMaster.HMSet(ctx, "hash_hset_multi", map[string]interface{}{"field1": randomString(10), "field2": randomString(10)}).Err()).NotTo(HaveOccurred())

			// ======================== list ========================
			// lpush
			Expect(clientMaster.LPush(ctx, "list_lpush", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.LPush(ctx, "list_mlpush", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())

			// lset
			Expect(clientMaster.LPush(ctx, "list_lset", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.LSet(ctx, "list_lset", 0, randomString(10)).Err()).NotTo(HaveOccurred())

			// rpoplpush
			Expect(clientMaster.LPush(ctx, "list_rpoplpush", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.LPush(ctx, "list_rpoplpush_src", randomString(10)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.RPopLPush(ctx, "list_rpoplpush_src", "list_rpoplpush").Err()).NotTo(HaveOccurred())

			// linsert
			Expect(clientMaster.LPush(ctx, "list_linsert", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())
			value := clientMaster.LIndex(ctx, "list_linsert", 50).Val()
			Expect(clientMaster.LInsert(ctx, "list_linsert", "BEFORE", value, randomString(10)).Err()).NotTo(HaveOccurred())

			// lrem
			Expect(clientMaster.LPush(ctx, "list_lrem", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.LRem(ctx, "list_lrem", 0, value).Err()).NotTo(HaveOccurred())

			// ======================== set ========================
			set1Value := generateStringArray(100, 100)
			set2Value := generateStringArray(100, 100)
			set3Value := generateStringArray(100, 100)

			// sadd
			Expect(clientMaster.SAdd(ctx, "set_sadd", randomString(10)).Err()).NotTo(HaveOccurred())
			// sadd_muti
			Expect(clientMaster.SAdd(ctx, "set_sadd_muti", generateStringArray(100, 100)).Err()).NotTo(HaveOccurred())

			// spop
			// 报错：redis: can't parse reply="*1" reading string
			// Expect(clientMaster.SPop(ctx, "set").Err()).NotTo(HaveOccurred())

			Expect(clientMaster.SAdd(ctx, "set1", append(set1Value, set2Value...)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.SAdd(ctx, "set2", append(set2Value, set3Value...)).Err()).NotTo(HaveOccurred())

			// sdiffstore
			Expect(clientMaster.SDiffStore(ctx, "set_diffStore", "set1", "set2").Val()).To(Equal(int64(len(set2Value))))
			// sinterstore
			Expect(clientMaster.SInterStore(ctx, "set_interStore", "set1", "set2").Err()).NotTo(HaveOccurred())
			// sunionstore
			Expect(clientMaster.SUnionStore(ctx, "set_unionStore", "set1", "set2").Err()).NotTo(HaveOccurred())
			// smove
			Expect(clientMaster.SMove(ctx, "set1", "set2", set1Value[50]).Err()).NotTo(HaveOccurred())
			// srem
			Expect(clientMaster.SAdd(ctx, "set_srem", append(set1Value, set2Value...)).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.SRem(ctx, "set_srem", set2Value).Val()).To(Equal(int64(100)))

			// ======================== zset ========================
			z1 := generateZSetArray(100, 10, 100)
			z2 := generateZSetArray(100, 10, 100)
			z3 := generateZSetArray(100, 10, 100)
			zSetValue := redis.Z{Score: float64(randomInt(10)), Member: randomString(10)}
			zSet1value := append(z1, z2...)
			zSet2value := append(z2, z3...)
			// zadd
			Expect(clientMaster.ZAdd(ctx, "zset_zadd", zSetValue).Err()).NotTo(HaveOccurred())

			// zadd_muti
			Expect(clientMaster.ZAdd(ctx, "zset_zadd_multi", zSet1value...).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.ZAdd(ctx, "zset1", zSet1value...).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.ZAdd(ctx, "zset2", zSet2value...).Err()).NotTo(HaveOccurred())

			// zincrby
			Expect(clientMaster.ZAdd(ctx, "zset_zincrby", zSetValue).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.ZIncrBy(ctx, "zset_zincrby", 1, zSetValue.Member.(string)).Err()).NotTo(HaveOccurred())

			// unionstore
			Expect(clientMaster.ZUnionStore(ctx, "zset_zsetunion", &redis.ZStore{Keys: []string{"zset1", "zset2"}, Weights: []float64{1, 1}}).Err()).NotTo(HaveOccurred())

			// interstore
			Expect(clientMaster.ZInterStore(ctx, "zset_zsetinter", &redis.ZStore{Keys: []string{"zset1", "zset2"}, Weights: []float64{1, 1}}).Err()).NotTo(HaveOccurred())

			// zrem
			delMembers := make([]string, len(z2))
			for i := 0; i < len(z2); i++ {
				delMembers[i] = z2[i].Member.(string)
			}
			Expect(clientMaster.ZAdd(ctx, "zset_rem", zSet1value...).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.ZRem(ctx, "zset_rem", delMembers).Err()).NotTo(HaveOccurred())

			// ======================== geo ========================
			// geoadd
			Expect(clientMaster.GeoAdd(ctx, "geo", generateGeoArray(100)...).Val()).To(Equal(int64(100)))

			// ======================== stream ========================
			Expect(clientMaster.XAdd(ctx, &redis.XAddArgs{Stream: "stream", Values: map[string]interface{}{"key": randomString(10)}}).Err()).NotTo(HaveOccurred())

			// recorver slave
			doSlave(ctx, clientSlave, clientMaster)

			// get all result
			slaveResults, _ := clientSlave.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Get(ctx, "key_set")
				pipe.Get(ctx, "key_append")
				pipe.MGet(ctx, mSetKeys...)
				pipe.Get(ctx, "bitkey_out_and")
				pipe.Get(ctx, "bitkey_out_or")
				pipe.Get(ctx, "bitkey_out_xor")
				pipe.Get(ctx, "bitkey_out_not")
				pipe.HGet(ctx, "hash_hset", "field")
				pipe.HGetAll(ctx, "hash_hdel")
				pipe.HGetAll(ctx, "hash_hset_multi")
				pipe.LRange(ctx, "list_lpush", 0, -1)
				pipe.LRange(ctx, "list_mlpush", 0, -1)
				pipe.LRange(ctx, "list_lset", 0, -1)
				pipe.LRange(ctx, "list_rpoplpush", 0, -1)
				pipe.LRange(ctx, "list_linsert", 0, -1)
				pipe.LRange(ctx, "list_lrem", 0, -1)
				pipe.SMembers(ctx, "set_sadd")
				pipe.SMembers(ctx, "set_sadd_muti")
				pipe.SMembers(ctx, "set_diffStore")
				pipe.SMembers(ctx, "set_interStore")
				pipe.SMembers(ctx, "set_unionStore")
				pipe.SMembers(ctx, "set_srem")
				pipe.ZRange(ctx, "zset_zadd", 0, -1)
				pipe.ZRange(ctx, "zset_zadd_multi", 0, -1)
				pipe.ZRange(ctx, "zset_zincrby", 0, -1)
				pipe.ZRange(ctx, "zset_zsetunion", 0, -1)
				pipe.ZRange(ctx, "zset_zsetinter", 0, -1)
				pipe.ZRange(ctx, "zset_rem", 0, -1)
				pipe.Do(ctx, "GEORADIUS", "geo", 0, 0, 100, "km", "WITHDIST", "WITHCOORD")
				// pipe.XRange(ctx, "stream", "-", "+")
				return nil
			})

			masterResults, _ := clientMaster.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Get(ctx, "key_set")
				pipe.Get(ctx, "key_append")
				pipe.MGet(ctx, mSetKeys...)
				pipe.Get(ctx, "bitkey_out_and")
				pipe.Get(ctx, "bitkey_out_or")
				pipe.Get(ctx, "bitkey_out_xor")
				pipe.Get(ctx, "bitkey_out_not")
				pipe.HGet(ctx, "hash_hset", "field")
				pipe.HGetAll(ctx, "hash_hdel")
				pipe.HGetAll(ctx, "hash_hset_multi")
				pipe.LRange(ctx, "list_lpush", 0, -1)
				pipe.LRange(ctx, "list_mlpush", 0, -1)
				pipe.LRange(ctx, "list_lset", 0, -1)
				pipe.LRange(ctx, "list_rpoplpush", 0, -1)
				pipe.LRange(ctx, "list_linsert", 0, -1)
				pipe.LRange(ctx, "list_lrem", 0, -1)
				pipe.SMembers(ctx, "set_sadd")
				pipe.SMembers(ctx, "set_sadd_muti")
				pipe.SMembers(ctx, "set_diffStore")
				pipe.SMembers(ctx, "set_interStore")
				pipe.SMembers(ctx, "set_unionStore")
				pipe.SMembers(ctx, "set_srem")
				pipe.ZRange(ctx, "zset_zadd", 0, -1)
				pipe.ZRange(ctx, "zset_zadd_multi", 0, -1)
				pipe.ZRange(ctx, "zset_zincrby", 0, -1)
				pipe.ZRange(ctx, "zset_zsetunion", 0, -1)
				pipe.ZRange(ctx, "zset_zsetinter", 0, -1)
				pipe.ZRange(ctx, "zset_rem", 0, -1)
				pipe.Do(ctx, "GEORADIUS", "geo", 0, 0, 100, "km", "WITHDIST", "WITHCOORD")
				// pipe.XRange(ctx, "stream", "-", "+")
				return nil
			})

			// compare each command result
			for i, slaveResult := range slaveResults {
				By("test " + slaveResult.Args()[1].(string))
				slaveResult, slaveErr := getResult(slaveResult)
				masterResult, masterErr := getResult(masterResults[i])
				Expect(masterErr).NotTo(HaveOccurred())
				Expect(slaveErr).NotTo(HaveOccurred())
				Expect(slaveResult).To(Equal(masterResult))
			}
		})
	})

})
