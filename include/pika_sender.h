#ifndef PIKA_SENDER_H_
#define PIKA_SENDER_H_

#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <queue>

#include "net/include/bg_thread.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

class PikaSender : public net::Thread {
public:
  PikaSender(std::string ip, int64_t port, std::string password);
  virtual ~PikaSender();
  void LoadKey(const std::string &cmd);
  void Stop();

  int64_t elements() { return elements_; }

  void SendCommand(std::string &command, const std::string &key);
  int QueueSize();
  void ConnectRedis();

private:
  net::NetCli *cli_;
  pstd::CondVar wsignal_;
  pstd::CondVar rsignal_;
  std::mutex signal_mutex;
  std::mutex keys_queue_mutex_;
  std::queue<std::string> keys_queue_;
  std::string ip_;
  int port_;
  std::string password_;
  std::atomic<bool> should_exit_;
  int64_t elements_;

  virtual void *ThreadMain();
};

#endif
