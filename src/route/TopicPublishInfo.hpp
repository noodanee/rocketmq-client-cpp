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
#ifndef ROCKETMQ_ROUTE_TOPICPUBLISHINFO_HPP_
#define ROCKETMQ_ROUTE_TOPICPUBLISHINFO_HPP_

#include <atomic>  // std::atomic
#include <memory>  // std::shared_ptr
#include <mutex>   // std::mutex
#include <vector>  // std::vector

#include "MQException.h"
#include "MessageQueue.hpp"
#include "logging/Logging.hpp"
#include "protocol/body/TopicRouteData.hpp"

namespace rocketmq {

// struct TopicRouteData;

class TopicPublishInfo;
using TopicPublishInfoPtr = std::shared_ptr<const TopicPublishInfo>;

class TopicPublishInfo {
 public:
  TopicPublishInfo() = default;

  TopicPublishInfo(const std::string& topic, std::shared_ptr<TopicRouteData> topic_route_data);

  bool isOrderTopic() const { return order_topic_; }

  bool ok() const { return !message_queues_.empty(); }

  const std::vector<MessageQueue>& getMessageQueueList() const { return message_queues_; }

  std::atomic<std::size_t>& getSendWhichQueue() const { return send_which_queue_; }

  const MessageQueue& selectOneMessageQueue(const std::string& lastBrokerName) const {
    if (!lastBrokerName.empty()) {
      auto mqSize = message_queues_.size();
      if (mqSize <= 1) {
        if (mqSize == 0) {
          LOG_ERROR_NEW("[BUG] messageQueueList is empty");
          THROW_MQEXCEPTION(MQClientException, "messageQueueList is empty", -1);
        }
        return message_queues_[0];
      }

      // NOTE: If it possible, mq in same broker is nonadjacent.
      auto index = send_which_queue_.fetch_add(1);
      for (size_t i = 0; i < 2; i++) {
        auto pos = index++ % message_queues_.size();
        const auto& mq = message_queues_[pos];
        if (mq.broker_name() != lastBrokerName) {
          return mq;
        }
      }
      return message_queues_[(index - 2) % message_queues_.size()];
    }
    return selectOneMessageQueue();
  }

  const MessageQueue& selectOneMessageQueue() const {
    auto index = send_which_queue_.fetch_add(1);
    auto pos = index % message_queues_.size();
    return message_queues_[pos];
  }

  int getQueueIdByBroker(const std::string& broker_name) const {
    for (const auto& queue_data : topic_route_data_->queue_datas) {
      if (queue_data.broker_name == broker_name) {
        return queue_data.write_queue_nums;
      }
    }
    return -1;
  }

 private:
  bool order_topic_{false};

  const std::vector<MessageQueue> message_queues_;  // no change after build
  mutable std::atomic<std::size_t> send_which_queue_{0};

  std::shared_ptr<TopicRouteData> topic_route_data_;  // no change after set
};

}  // namespace rocketmq

#endif  // ROCKETMQ_ROUTE_TOPICPUBLISHINFO_HPP_
