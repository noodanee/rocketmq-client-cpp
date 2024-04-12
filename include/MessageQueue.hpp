/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ROCKETMQ_MESSAGEQUEUE_HPP_
#define ROCKETMQ_MESSAGEQUEUE_HPP_

#include <cstdint>  // int32_t

#include <sstream>  // std::stringstream
#include <string>   // std::string
#include <utility>  // std::move

#include "RocketMQClient.h"

namespace rocketmq {

/**
 * MessageQueue(Topic, BrokerName, QueueId)
 */
class ROCKETMQCLIENT_API MessageQueue {
 public:
  MessageQueue(std::string topic, std::string broker_name, int32_t queue_id)
      : topic_(std::move(topic)), broker_name_(std::move(broker_name)), queue_id_(queue_id) {}

  MessageQueue() = default;
  ~MessageQueue() = default;

  MessageQueue(const MessageQueue&) = default;
  MessageQueue& operator=(const MessageQueue&) = default;

  MessageQueue(MessageQueue&&) noexcept = default;
  MessageQueue& operator=(MessageQueue&&) noexcept = default;

  bool operator==(const MessageQueue& other) const {
    return this == &other ||
           (queue_id_ == other.queue_id_ && broker_name_ == other.broker_name_ && topic_ == other.topic_);
  }
  bool operator!=(const MessageQueue& other) const { return !operator==(other); }

  bool operator<(const MessageQueue& other) const { return Compare(other) < 0; }

  int Compare(const MessageQueue& other) const {
    int result = topic_.compare(other.topic_);
    if (result != 0) {
      return result;
    }

    result = broker_name_.compare(other.broker_name_);
    if (result != 0) {
      return result;
    }

    return queue_id_ - other.queue_id_;
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "MessageQueue [topic=" << topic_ << ", brokerName=" << broker_name_ << ", queueId=" << queue_id_ << "]";
    return ss.str();
  }

  const std::string& topic() const noexcept { return topic_; }
  void set_topic(std::string topic) noexcept { topic_ = std::move(topic); }

  const std::string& broker_name() const noexcept { return broker_name_; }
  void set_broker_name(std::string broker_name) noexcept { broker_name_ = std::move(broker_name); }

  int32_t queue_id() const noexcept { return queue_id_; }
  void set_queue_id(int32_t queue_id) noexcept { queue_id_ = queue_id; }

 private:
  std::string topic_;
  std::string broker_name_;
  int32_t queue_id_{-1};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_MESSAGEQUEUE_HPP_
