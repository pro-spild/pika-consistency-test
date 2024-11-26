#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <set>
#include <ctime>
#include <chrono>
#include <random>
#include <vector>
#include <iostream>
#include <functional>
#include <unordered_map>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "hiredis/hiredis.h"

#include "pstd/include/env.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"

DEFINE_string(host, "127.0.0.1", "target server's host");
DEFINE_int32(port, 9221, "target server's listen port");
DEFINE_int32(timeout, 1000, "request timeout");
DEFINE_string(password, "", "password");
DEFINE_int32(key_size, 50, "key size int bytes");
DEFINE_int32(value_size, 100, "value size in bytes");
DEFINE_int32(count, 100000, "request counts");
DEFINE_int32(thread_num, 10, "concurrent thread num");
DEFINE_string(dbs, "0", "dbs name, eg: 0,1,2");
DEFINE_int32(element_count, 1, "elements number in hash/list/set/zset");
DEFINE_bool(is_batch, false, "check with batch command or not");
DEFINE_int32(batch_size, 10, "request key number in batch command");
DEFINE_string(type, "string", "data type for consistent check");
DEFINE_int32(max_retries, 10, "max retry times when value mismatch");

using std::default_random_engine;
using pstd::Status;

struct ThreadArg {
  ThreadArg(pthread_t t, const std::string& tn, int i)
      : idx(i), value_version(0), tid(t), table_name(tn) {}
  int idx;
  uint64_t value_version;
  pthread_t tid;
  std::string table_name;
};

thread_local int last_seed = 0;
std::vector<ThreadArg> thread_args;
std::vector<std::string> tables;

redisContext* Prepare(ThreadArg* arg);
void FreeAndReconnect(redisContext*& c, ThreadArg* arg) {
  LOG(INFO) << "request timeout, reconnect";
  redisFree(c);
  c = nullptr;
  while (!c) {
    c = Prepare(arg);
  }
}

void DoSet(std::vector<const char*>& argv, std::vector<size_t>& argvlen, ThreadArg* arg, redisContext*& c, int expect_type = REDIS_REPLY_STATUS) {
  redisReply* res = nullptr;
  while (true) {
    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, argv.size(), &(argv[0]), &(argvlen[0])));
    if (!res) {
      FreeAndReconnect(c, arg);
      continue;
    }

    if (res->type != expect_type ||
        (res->type == REDIS_REPLY_STATUS && strcasecmp(res->str, "OK"))) {
      LOG(INFO) << "set response error, type: " << res->type << " str: " << res->str;
      freeReplyObject(res);
      continue;
    }

    freeReplyObject(res);
    break;
  }
}

bool CompareValue(const std::vector<std::string>& expect, const redisReply* reply) {
  if (reply == nullptr ||
      reply->type != REDIS_REPLY_ARRAY ||
      reply->elements != expect.size()) {
    return false;
  }
  for (size_t i = 0; i < reply->elements; i++) {
    std::string value(reply->element[i]->str, reply->element[i]->len);
    if (value != expect[i]) {
      return false;
    }
  }
  return true;
}

bool CompareValue(std::set<std::string> expect, const redisReply* reply) {
  if (reply == nullptr ||
      reply->type != REDIS_REPLY_ARRAY ||
      reply->elements != expect.size()) {
    return false;
  }
  for (size_t i = 0; i < reply->elements; i++) {
    std::string key = reply->element[i]->str;
    auto it = expect.find(key);
    if (it == expect.end()) {
      return false;
    }
    expect.erase(key);
  }
  return expect.size() == 0;
}

// for hash type
bool CompareValue(std::unordered_map<std::string, std::string> expect, const redisReply* reply) {
  if (reply == nullptr ||
      reply->type != REDIS_REPLY_ARRAY ||
      reply->elements != expect.size() * 2) {
    return false;
  }
  std::unordered_map<std::string, std::string> actual;
  for (size_t i = 0; i < reply->elements; i++) {
    std::string key = reply->element[i]->str;
    std::string value = reply->element[++i]->str;
    auto it = expect.find(key);
    if (it == expect.end()) {
      return false;
    } else if (it->second != value) {
      return false;
    }
    expect.erase(key);
  }
  return expect.size() == 0;
}

// for string type
bool CompareValue(const std::string& expect, const std::string& actual) {
  return expect == actual;
}

void PrepareKeys(int suffix, std::vector<std::string>* keys) {
  keys->resize(FLAGS_count * FLAGS_element_count);
  std::string filename = "consistent_file_" + std::to_string(suffix);
  FILE* fp = fopen(filename.c_str(), "r");
  for (int idx = 0; idx < FLAGS_count * FLAGS_element_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, FLAGS_key_size + 2, fp);
    key[FLAGS_key_size] = '\0';
    (*keys)[idx] = std::string(key, FLAGS_key_size);
    delete []key;
  }
  fclose(fp);
}

void PreparePkeyMembers(int suffix, std::vector<std::pair<std::string, std::set<std::string>>>* keys) {
  keys->resize(FLAGS_count);
  std::string filename = "consistent_file_" + std::to_string(suffix);
  FILE* fp = fopen(filename.c_str(), "r");
  for (int idx = 0; idx < FLAGS_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, FLAGS_key_size + 2, fp);
    key[FLAGS_key_size] = '\0';
    std::set<std::string> elements;
    elements.insert(std::string(key));
    for (int idy = 1; idy < FLAGS_element_count; ++idy) {
      char* element = new char[FLAGS_key_size + 2];
      fgets(element, FLAGS_key_size + 2, fp);
      element[FLAGS_key_size] = '\0';
      elements.insert(std::string(element));
      delete []element;
    }
    (*keys)[idx] = std::make_pair(std::string(key), elements);
    delete []key;
  }
  fclose(fp);
}

void GenerateRandomString(int32_t len, std::string* target) {
  target->clear();
  char c_map[67] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'g', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
                    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                    'I', 'G', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
                    'Z', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '=', '_', '+'};

  default_random_engine e;
  for (int i = 0; i < len; i++) {
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 67;
    target->push_back(c_map[rand_num]);
  }
}

void PrintInfo(const std::time_t& now) {
  std::cout << "=================== Consistent Check ===================" << std::endl;
  std::cout << "Server host name: " << FLAGS_host << std::endl;
  std::cout << "Server port: " << FLAGS_port << std::endl;
  std::cout << "Thread num : " << FLAGS_thread_num << std::endl;
  std::cout << "Payload size : " << FLAGS_value_size << std::endl;
  std::cout << "Number of request : " << FLAGS_count << std::endl;
  std::cout << "Collection of dbs: " << FLAGS_dbs << std::endl;
  std::cout << "Elements num: " << FLAGS_element_count << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now)) << std::endl;
  std::cout << "========================================================" << std::endl;
}

void RunGenerate(int index) {
  int pid = (int)getpid();
  char hostname[255];
  if (gethostname(hostname, sizeof(hostname)) == -1) {
    LOG(WARNING) << "get hostname failed";
    exit(1);
  }


  std::string prefix(hostname, 8);
  prefix.append("_");
  prefix.append(std::to_string(pid));
  prefix.append("_");
  prefix.append(std::to_string(index));
  prefix.append("_");

  std::string filename = "consistent_file_" + std::to_string(index);
  FILE* fp = fopen(filename.c_str(), "w+");
  if (fp == nullptr) {
    LOG(INFO) << "open file error";
    return;
  }
  for (int i = 0; i < FLAGS_count * FLAGS_element_count; i++) {
    std::string key;
    GenerateRandomString(FLAGS_key_size - prefix.size(), &key);
    key.append("\n");
    fwrite(prefix.data(), sizeof(char), prefix.size(), fp);
    fwrite(key.data(), sizeof(char), key.size(), fp);
  }
  fclose(fp);
}

redisContext* Prepare(ThreadArg* arg) {
  int index = arg->idx;
  std::string table = arg->table_name;
  struct timeval timeout = {FLAGS_timeout / 1000, (FLAGS_timeout % 1000) * 1000};
  redisContext* c = redisConnectWithTimeout(FLAGS_host.data(), FLAGS_port, timeout);
  if (!c || c->err) {
    if (c) {
      printf("Table: %s Thread %d, Connection error: %s\n",
             table.c_str(), index, c->errstr);
      redisFree(c);
    } else {
      printf("Table %s Thread %d, Connection error: "
             "can't allocate redis context\n", table.c_str(), index);
    }
    return nullptr;
  }

  if (!FLAGS_password.empty()) {
    const char* auth_argv[2] = {"AUTH", FLAGS_password.data()};
    size_t auth_argv_len[2] = {4, FLAGS_password.size()};
    auto res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 2, reinterpret_cast<const char**>(auth_argv),
                         reinterpret_cast<const size_t*>(auth_argv_len)));
    if (!res) {
      printf("Table %s Thread %d Auth Failed: Get reply Error\n", table.c_str(), index);
      freeReplyObject(res);
      redisFree(c);
      return nullptr;
    } else {
      if (!strcasecmp(res->str, "OK")) {
      } else {
        printf("Table %s Thread %d Auth Failed: %s\n", table.c_str(), index, res->str);
        freeReplyObject(res);
        redisFree(c);
        return nullptr;
      }
    }
    freeReplyObject(res);
  }

  const char* select_argv[2] = {"SELECT", arg->table_name.data()};
  size_t select_argv_len[2] = {6, arg->table_name.size()};
  auto res = reinterpret_cast<redisReply*>(
      redisCommandArgv(c, 2, reinterpret_cast<const char**>(select_argv),
                       reinterpret_cast<const size_t*>(select_argv_len)));
  if (!res) {
    printf("Thread %d Select Table %s Failed, Get reply Error\n", index, table.c_str());
    redisFree(c);
    return nullptr;
  } else {
    if (!strcasecmp(res->str, "OK")) {
      freeReplyObject(res);
      printf("Table %s Thread %d Select DB Success, start to write data...\n",
             table.c_str(), index);
    } else {
      printf("Table %s Thread %d Select DB Failed: %s, thread exit...\n",
             table.c_str(), index, res->str);
      freeReplyObject(res);
      redisFree(c);
      return nullptr;
    }
  }
  return c;
}

Status RunSingleStringCheck(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::string> keys;
  PrepareKeys(arg->idx, &keys);
  uint64_t value_version = arg->value_version++;
  std::string value = std::to_string(value_version);

  for (int idx = 0; idx < FLAGS_count * FLAGS_element_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " consistent check";
    }

    std::string key = keys[idx].c_str();
    std::vector<const char*> set_argv(3);
    std::vector<size_t> set_argvlen(3);
    set_argv[0] = "set";
    set_argvlen[0] = 3;
    set_argv[1] = key.c_str();
    set_argvlen[1] = key.size();
    set_argv[2] = value.c_str();
    set_argvlen[2] = value.size();
    DoSet(set_argv, set_argvlen, arg, c);

    std::vector<const char*> get_argv(2);
    std::vector<size_t> get_argvlen(2);
    get_argv[0] = "get";
    get_argvlen[0] = 3;
    get_argv[1] = key.c_str();
    get_argvlen[1] = key.size();

    int retry_times = 0;
    while (true) {
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, get_argv.size(), &(get_argv[0]), &(get_argvlen[0])));

      // nullptr res, reconnect
      if (!res) {
        FreeAndReconnect(c, arg);
        continue;
      }

      // success
      if (res->type == REDIS_REPLY_STRING && CompareValue(value, std::string(res->str))) {
        freeReplyObject(res);
        break;
      }

      // can retry
      if (++retry_times < FLAGS_max_retries) {
        freeReplyObject(res);
        usleep(5000);
        continue;
      }

      // log and terminate process
      LOG(ERROR) << "string single key consistent_check failed, key: " << key
                 << " expect type : " << REDIS_REPLY_STRING
                 << " actual type : " << res->type
                 << " expect value: " << value
                 << " actual value: " << res->str;

      freeReplyObject(res);
      exit(1);
    }
  }
  return Status::OK();
}

Status RunBatchStringCheck(redisContext*& c, ThreadArg* arg) {
  std::vector<std::string> keys;
  PrepareKeys(arg->idx, &keys);
  uint64_t value_version = arg->value_version++;
  std::string value = std::to_string(value_version);

  int total = FLAGS_count * FLAGS_element_count;
  for (int idx = 0; idx < total / FLAGS_batch_size; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " consistent check";
    }

    std::vector<const char*> set_argv(2 * FLAGS_batch_size + 1);
    std::vector<size_t> set_argvlen(2 * FLAGS_batch_size + 1);
    set_argv[0] = "mset";
    set_argvlen[0] = 4;

    for (int i = 0; i < FLAGS_batch_size; ++i) {
      set_argv[2 * i + 1] = keys[idx * FLAGS_batch_size + i].c_str();
      set_argvlen[2 * i + 1] = keys[idx * FLAGS_batch_size + i].size();
      set_argv[2 * i + 2] = value.c_str();
      set_argvlen[2 * i + 2] = value.size();
    }
    DoSet(set_argv, set_argvlen, arg, c);

    std::vector<std::string> expect_values(FLAGS_batch_size, value);
    std::vector<const char*> get_argv(FLAGS_batch_size + 1);
    std::vector<size_t> get_argvlen(FLAGS_batch_size + 1);
    get_argv[0] = "mget";
    get_argvlen[0] = 4;
    for (int i = 0; i < FLAGS_batch_size; ++i) {
      get_argv[i + 1] = keys[idx * FLAGS_batch_size + i].c_str();
      get_argvlen[i + 1] = keys[idx * FLAGS_batch_size + i].size();
    }

    int retry_times = 0;
    while (true) {
      redisReply* res = nullptr;
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, get_argv.size(), &(get_argv[0]), &(get_argvlen[0])));

      // nullptr res, reconnect
      if (!res) {
        FreeAndReconnect(c, arg);
        continue;
      }

      // success
      if (res->type == REDIS_REPLY_ARRAY && CompareValue(expect_values, res)) {
        freeReplyObject(res);
        break;
      }

      // can retry
      if (++retry_times < FLAGS_max_retries) {
        freeReplyObject(res);
        usleep(5000);
        continue;
      }

      // log and terminate process
      LOG(ERROR) << "string batch key consistent_check failed,"
                 << " expect type : " << REDIS_REPLY_ARRAY
                 << " actual type : " << res->type;
      LOG(ERROR) << "request keys: ";
      for (size_t i = 1; i < get_argv.size(); i++) {
        LOG(ERROR) << "key: " << std::string(get_argv[i], get_argvlen[i]);
      }

      LOG(ERROR) << "expect vs actual value: ";
      for (size_t i = 0; i < res->elements; i++) {
        LOG(ERROR) << "expect: " << value << " actual: "
                   << std::string(res->element[i]->str, res->element[i]->len);
      }
      freeReplyObject(res);
      exit(1);
    }
  }
  return Status::OK();
}

Status RunSingleHashCheck(redisContext*& c, ThreadArg* arg) {
  Status s;
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);
  uint64_t value_version = arg->value_version++;
  std::string value = std::to_string(value_version);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " single hash consistent check";
    }

    std::string pkey = keys[idx].first;
    std::set<std::string> members = std::move(keys[idx].second);
    auto member_iter = members.cbegin();
    while (member_iter != members.cend()) {
      std::vector<const char*> set_argv(4);
      std::vector<size_t> set_argvlen(4);
      set_argv[0] = "hset";
      set_argvlen[0] = 4;
      set_argv[1] = pkey.c_str();
      set_argvlen[1] = pkey.size();
      set_argv[2] = member_iter->c_str();
      set_argvlen[2] = member_iter->size();
      set_argv[3] = value.c_str();
      set_argvlen[3] = value.size();
      DoSet(set_argv, set_argvlen, arg, c, REDIS_REPLY_INTEGER);

      std::vector<const char*> get_argv(3);
      std::vector<size_t> get_argvlen(3);
      get_argv[0] = "hget";
      get_argvlen[0] = 4;
      get_argv[1] = pkey.c_str();
      get_argvlen[1] = pkey.size();
      get_argv[2] = member_iter->c_str();
      get_argvlen[2] = member_iter->size();

      int retry_times = 0;
      while (true) {
        res = reinterpret_cast<redisReply*>(
            redisCommandArgv(c, get_argv.size(), &(get_argv[0]), &(get_argvlen[0])));

        // nullptr res, reconnect
        if (!res) {
          FreeAndReconnect(c, arg);
          continue;
        }

        // success
        if (res->type == REDIS_REPLY_STRING && CompareValue(value, std::string(res->str, res->len))) {
          freeReplyObject(res);
          break;
        }

        // can retry
        if (++retry_times < FLAGS_max_retries) {
          freeReplyObject(res);
          usleep(5000);
          continue;
        }

        // log and terminate process
        LOG(ERROR) << "hash single key consistent_check failed,"
                   << " pkey: " << pkey
                   << " expect type : " << REDIS_REPLY_ARRAY
                   << " actual type : " << res->type
                   << " request key : " << (*member_iter)
                   << " expect value : " << value
                   << " actual value : " << std::string(res->str, res->len);
        freeReplyObject(res);
        exit(1);
      }
      member_iter++;
    }
  }

  return s;
}

Status RunBatchHashCheck(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);
  uint64_t value_version = arg->value_version++;
  std::string value = std::to_string(value_version);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " batch hash consistent check";
    }

    std::string pkey = keys[idx].first;
    std::set<std::string> members = std::move(keys[idx].second);
    std::unordered_map<std::string, std::string> member2value;
    for (const auto& member : members) {
      member2value[member] = value;
    }
    auto member_iter = members.cbegin();
    while (member_iter != members.cend()) {
      std::vector<const char*> set_argv(2 * FLAGS_batch_size + 2);
      std::vector<size_t> set_argvlen(2 * FLAGS_batch_size + 2);
      set_argv[0] = "hmset";
      set_argvlen[0] = 5;
      set_argv[1] = pkey.c_str();
      set_argvlen[1] = pkey.size();

      int i = 0;
      for (i = 0; i < FLAGS_batch_size && member_iter != members.cend(); i++) {
        set_argv[2 * i + 2] = member_iter->c_str();
        set_argvlen[2 * i + 2] = member_iter->size();
        set_argv[2 * i + 3] = value.c_str();
        set_argvlen[2 * i + 3] = value.size();
        member_iter++;
      }
      if (i != FLAGS_batch_size) {
        set_argv.resize(2 + i * 2);
      }
      DoSet(set_argv, set_argvlen, arg, c);
    }

    member_iter = members.cbegin();
    while (member_iter != members.cend()) {
      std::vector<const char*> get_argv(FLAGS_batch_size + 2);
      std::vector<size_t> get_argvlen(FLAGS_batch_size + 2);
      get_argv[0] = "hmget";
      get_argvlen[0] = 5;
      get_argv[1] = pkey.c_str();
      get_argvlen[1] = pkey.size();

      int i = 0;
      std::vector<std::string> expect_value(FLAGS_batch_size, value);
      for (i = 0; i < FLAGS_batch_size && member_iter != members.cend(); i++) {
        get_argv[i + 2] = member_iter->c_str();
        get_argvlen[i + 2] = member_iter->size();
        member_iter++;
      }
      if (i != FLAGS_batch_size) {
        get_argv.resize(2 + i);
      }

      int retry_times = 0;
      while (true) {
        res = reinterpret_cast<redisReply*>(
            redisCommandArgv(c, get_argv.size(), &(get_argv[0]), &(get_argvlen[0])));

        // nullptr res, reconnect
        if (!res) {
          FreeAndReconnect(c, arg);
          continue;
        }

        // success
        if (res->type == REDIS_REPLY_ARRAY && CompareValue(expect_value, res)) {
          freeReplyObject(res);
          break;
        }

        // can retry
        if (++retry_times < FLAGS_max_retries) {
          freeReplyObject(res);
          usleep(5000);
          continue;
        }

        // log and terminate process
        LOG(ERROR) << "hash batch key consistent_check failed,"
                   << " pkey: " << pkey
                   << " expect type : " << REDIS_REPLY_ARRAY
                   << " actual type : " << res->type;
        LOG(ERROR) << "request members: ";
        for (size_t i = 2; i < get_argv.size(); i++) {
          LOG(ERROR) << "member: " << std::string(get_argv[i], get_argvlen[i]);
        }
        LOG(ERROR) << "expect vs actual value: ";
        for (size_t i = 0; i < res->elements; i++) {
          LOG(ERROR) << "expect: " << value << " actual: "
                     << std::string(res->element[i]->str, res->element[i]->len);
        }
        freeReplyObject(res);
        exit(1);
      }
    }

    std::vector<const char*> getall_argv;
    std::vector<size_t> getall_argvlen;
    getall_argv.push_back("hgetall");
    getall_argvlen.push_back(7);
    getall_argv.push_back(pkey.c_str());
    getall_argvlen.push_back(pkey.size());

    int retry_times = 0;
    while (retry_times < FLAGS_max_retries) {
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, getall_argv.size(), &(getall_argv[0]), &(getall_argvlen[0])));

      // nullptr res, reconnect
      if (!res) {
        FreeAndReconnect(c, arg);
        continue;
      }

      // success
      if (res->type == REDIS_REPLY_ARRAY && CompareValue(member2value, res)) {
        freeReplyObject(res);
        break;
      }

      // can retry
      if (++retry_times < FLAGS_max_retries) {
        freeReplyObject(res);
        usleep(5000);
        continue;
      }

      // log and terminate process
      LOG(ERROR) << "hash batch key hgetall consistent_check failed,"
                 << " expect type : " << REDIS_REPLY_ARRAY
                 << " actual type : " << res->type
                 << " pkey: " << pkey;
      LOG(ERROR) << "keys: ";
      for (const auto& it : member2value) {
        LOG(ERROR) << "key: " << it.first;
      }

      LOG(ERROR) << "expect vs actual value: ";
      for (size_t i = 0; i < res->elements; i++) {
        LOG(ERROR) << "expect: " << value << " actual: "
                   << std::string(res->element[i]->str, res->element[i]->len);
      }
      freeReplyObject(res);
      exit(1);
    }
  }
  return Status::OK();
}

void* ThreadMain(void* arg) {
  ThreadArg* ta = reinterpret_cast<ThreadArg*>(arg);
  last_seed = ta->idx;

  if (FLAGS_type == "generate") {
    RunGenerate(ta->idx);
    return nullptr;
  }

  redisContext* c = Prepare(ta);
  if (!c) {
    return nullptr;
  }

  Status s;
  while (s.ok()) {
    if (FLAGS_type == "string" && FLAGS_is_batch) {
      s = RunBatchStringCheck(c, ta);
    } else if (FLAGS_type == "string" && !FLAGS_is_batch) {
      s = RunSingleStringCheck(c, ta);
    } else if (FLAGS_type == "hash" && FLAGS_is_batch) {
      s = RunBatchHashCheck(c, ta);
    } else if (FLAGS_type == "hash" && !FLAGS_is_batch) {
      s = RunSingleHashCheck(c, ta);
    }
  }

  if (!s.ok()) {
    std::string thread_info = "Table " + ta->table_name + ", Thread " + std::to_string(ta->idx);
    printf("%s, %s, thread exit...\n", thread_info.c_str(), s.ToString().c_str());
  }
  redisFree(c);
  return nullptr;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  pstd::StringSplit(FLAGS_dbs, ',', tables);
  if (tables.empty()) {
    exit(-1);
  }

  FLAGS_logtostderr = true;
  FLAGS_minloglevel = 0;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("consistent_check");

  if (FLAGS_element_count <= FLAGS_batch_size) {
    LOG(ERROR) << "element_count should larger than batch_size";
    exit(1);
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  for (const auto& table : tables) {
    for (int idx = 0; idx < FLAGS_thread_num; ++idx) {
      thread_args.push_back({0, table, idx});
    }
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_create(&thread_args[idx].tid, nullptr, ThreadMain, &thread_args[idx]);
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_join(thread_args[idx].tid, nullptr);
  }

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

  std::cout << "Total Time Cost : " << hours << " hours " << minutes % 60 << " minutes " << seconds % 60 << " seconds "
            << std::endl;
  return 0;
}