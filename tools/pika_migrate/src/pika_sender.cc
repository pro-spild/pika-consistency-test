// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_sender.h"

#include <glog/logging.h>

PikaSender::PikaSender(std::string ip, int64_t port, std::string password):
  cli_(NULL),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  elements_(0)
  {
  }

PikaSender::~PikaSender() {
}

int PikaSender::QueueSize() {
  std::lock_guard<std::mutex> lock(keys_queue_mutex_);
  return keys_queue_.size();
}

void PikaSender::Stop() {
  should_exit_.store(true);
  wsignal_.notify_all();
  rsignal_.notify_all();
}

void PikaSender::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1000);
    pstd::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      delete cli_;
      cli_ = NULL;
      LOG(WARNING) << "Can not connect to " << ip_ << ":" << port_ << ", status: " << s.ToString();
      continue;
    } else {
      // Connect success

      // Authentication
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
          } else {
            LOG(FATAL) << "Connect to redis(" << ip_ << ":" << port_ << ") Invalid password";
            cli_->Close();
            delete cli_;
            cli_ = NULL;
            should_exit_ = true;
            return;
          }
        } else {
          LOG(WARNING) << "send auth failed: " << s.ToString();
          cli_->Close();
          delete cli_;
          cli_ = NULL;
          continue;
        }
      } else {
        // If forget to input password
        net::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        net::SerializeRedisCommand(argv, &cmd);
        pstd::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              LOG(FATAL) << "Ping redis(" << ip_ << ":" << port_ << ") NOAUTH Authentication required";
              cli_->Close();
              delete cli_;
              cli_ = NULL;
              should_exit_ = true;
              return;
            }
          } else {
            LOG(WARNING) << "Recv failed: " << s.ToString();
            cli_->Close();
            delete cli_;
            cli_ = NULL;
          }
        }
      }
    }
  }
}

void PikaSender::LoadKey(const std::string &key) {
  std::unique_lock lock(signal_mutex);
  wsignal_.wait(lock, [this]() { return keys_queue_.size() < 100000 || should_exit_; });
  if(!should_exit_) {
    std::lock_guard<std::mutex> lock(keys_queue_mutex_);
    keys_queue_.push(key);
    rsignal_.notify_one();
  } 
}

void PikaSender::SendCommand(std::string &command, const std::string &key) {
  // Send command
  pstd::Status s = cli_->Send(&command);
  if (!s.ok()) {
    elements_--;
    LoadKey(key);
    cli_->Close();
    LOG(INFO) <<  s.ToString().data();
    delete cli_;
    cli_ = NULL;
    ConnectRedis();
  }else {
    cli_->Recv(nullptr);
  }
}

void *PikaSender::ThreadMain() {

  if (cli_ == NULL) {
    ConnectRedis();
  }

  while (!should_exit_ || QueueSize() != 0) {
    std::string command;

    std::unique_lock lock(signal_mutex);
    rsignal_.wait(lock, [this]() { return !QueueSize() == 0 || should_exit_; });
    if (QueueSize() == 0 && should_exit_) {
      return NULL;
    }
    lock.unlock();

    std::string key;
    {
      std::lock_guard<std::mutex> lock(keys_queue_mutex_);
      key = keys_queue_.front();
      elements_++;
      keys_queue_.pop();
    }
    wsignal_.notify_one();
    SendCommand(key, key);
  }

  if (cli_) {
    cli_->Close();
    delete cli_;
    cli_ = NULL;
  }
  return NULL;
}

