// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include "include/redis_sender.h"

#include <time.h>
#include <unistd.h>

#include <glog/logging.h>

static time_t kCheckDiff = 5;

RedisSender::RedisSender(int id, std::string ip, int64_t port, std::string password):
  id_(id),
  cli_(NULL),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  cnt_(0),
  elements_(0) {

  last_write_time_ = ::time(NULL);
  cli_ = std::shared_ptr<net::NetCli>(net::NewRedisCli());
  cli_->set_connect_timeout(1000);
  cli_->set_recv_timeout(10000);
  cli_->set_send_timeout(10000);
}

RedisSender::~RedisSender() {
}

void RedisSender::ConnectRedis() {
  while (true) {
    pstd::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      LOG(WARNING) << "Can not connect to " << ip_ << ":" << port_ << ", status: " << s.ToString();
      continue;
    } else {
      // Connect success
      LOG(INFO) << "RedisSender thread " << id_ << "Connect to redis(" << ip_ << ":" << port_ << ") success";
      if (!Authenticate()) {
        cli_->Close();
        continue;
      }
      break;
    }
  }
}

bool RedisSender::Authenticate() {
  if (!password_.empty()) {
    net::RedisCmdArgsType argv, resp;
    std::string cmd;

    argv.push_back("AUTH");
    argv.push_back(password_);
    net::SerializeRedisCommand(argv, &cmd);
    pstd::Status s = cli_->Send(&cmd);

    if (s.ok()) {
      s = cli_->Recv(&resp);
      if (resp[0] == "OK") {
        return true;
      } else {
        LOG(ERROR) << "Connect to redis(" << ip_ << ":" << port_ << ") Invalid password";
        return false;
      }
    } else {
      LOG(WARNING) << "send auth failed: " << s.ToString();
      return false;
    }
  } else {
    net::RedisCmdArgsType argv, resp;
    std::string cmd;

    argv.push_back("PING");
    net::SerializeRedisCommand(argv, &cmd);
    pstd::Status s = cli_->Send(&cmd);

    if (s.ok()) {
      s = cli_->Recv(&resp);
      if (s.ok() && resp[0] == "NOAUTH Authentication required.") {
        LOG(ERROR) << "Ping redis(" << ip_ << ":" << port_ << ") NOAUTH Authentication required";
        return false;
      }
    } else {
      LOG(WARNING) << s.ToString();
      return false;
    }
  }
  return true;
}

void RedisSender::Stop() {
  set_should_stop();
  should_exit_ = true;
  rsignal_.notify_all();
  wsignal_.notify_all();
}

void RedisSender::SendRedisCommand(const std::string &command) {
  std::unique_lock lock(signal_mutex_);
  wsignal_.wait(lock, [this]() { return commandQueueSize() < 100000; });
  if (!should_exit_) {
    std::lock_guard l(keys_mutex_);
    commands_queue_.push(command);
    rsignal_.notify_one();
  }
}

int RedisSender::SendCommand(std::string &command) {
  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
      cli_->Close();
      ConnectRedis();
    last_write_time_ = now;
  }

  // Send command
  int idx = 0;
  do {
    pstd::Status s = cli_->Send(&command);
    if (s.ok()) {
      cli_->Recv(NULL);
      return 0;
    }

    cli_->Close();
    ConnectRedis();
  } while(++idx < 3);
  LOG(ERROR) << "RedisSender " << id_ << " fails to send redis command " << command << ", times: " << idx << ", error: " << "send command failed";
  return -1;
}

void *RedisSender::ThreadMain() {
  LOG(INFO) << "Start redis sender " << id_ << " thread...";
  // sleep(15);

  ConnectRedis();

  while (!should_exit_) {
    std::unique_lock lock(signal_mutex_);
    while (commandQueueSize() == 0 && !should_exit_) {
      rsignal_.wait_for(lock, std::chrono::milliseconds(100));
    }

    if (should_exit_) {
      break;
    }

    if (commandQueueSize() == 0) {
      continue;
    }

    // get redis command
    std::string command;
    {
      std::lock_guard l(keys_mutex_);
      command = commands_queue_.front();
      elements_++;
      commands_queue_.pop();
    }

    wsignal_.notify_one();
    SendCommand(command);

  }

  LOG(INFO) << "RedisSender thread " << id_ << " complete";
  cli_->Close();
  return NULL;
}

