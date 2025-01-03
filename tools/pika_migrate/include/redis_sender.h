#ifndef REDIS_SENDER_H_
#define REDIS_SENDER_H_

#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <queue>

#include "pika_repl_bgworker.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

class RedisSender : public net::Thread {
 public:
  RedisSender(int id, std::string ip, int64_t port, std::string password);
  virtual ~RedisSender();
  void Stop(void);
  int64_t elements() {
    return elements_;
  }

  void SendRedisCommand(const std::string &command);

 private:
  int SendCommand(std::string &command);
  void ConnectRedis();
  size_t commandQueueSize() {
    std::lock_guard l(keys_mutex_);
    return commands_queue_.size();
  }

 private:
  int id_;
  std::shared_ptr<net::NetCli> cli_;
  pstd::CondVar rsignal_;
  pstd::CondVar wsignal_;
  pstd::Mutex signal_mutex_;
  pstd::Mutex keys_mutex_;
  std::queue<std::string> commands_queue_;
  std::string ip_;
  int port_;
  std::string password_;
  bool should_exit_;
  int32_t cnt_;
  int64_t elements_;
  std::atomic<time_t> last_write_time_;

  virtual void *ThreadMain();
};

#endif
