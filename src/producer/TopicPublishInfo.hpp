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
#ifndef ROCKETMQ_PRODUCER_TOPICPUBLISHINFO_HPP_
#define ROCKETMQ_PRODUCER_TOPICPUBLISHINFO_HPP_

#include <cstdlib>  // std::atoi

#include <algorithm>  // std::sort
#include <atomic>     // std::atomic
#include <memory>     // std::shared_ptr
#include <mutex>      // std::mutex
#include <utility>    // std::move

#include "MQException.h"
#include "MessageQueue.hpp"
#include "common/PermName.h"
#include "common/UtilAll.h"
#include "protocol/body/TopicRouteData.hpp"

namespace rocketmq {

class TopicPublishInfo;
using TopicPublishInfoPtr = std::shared_ptr<const TopicPublishInfo>;

class TopicPublishInfo {
 public:
  using QueuesVec = std::vector<MessageQueue>;

 public:
  TopicPublishInfo() = default;

  TopicPublishInfo(const std::string& topic, TopicRouteDataPtr topic_route_data)
      : order_topic_(!topic_route_data->order_topic_conf.empty()), topic_route_data_(std::move(topic_route_data)) {
    if (order_topic_) {
      InitForOrderTopic(topic);
    } else {
      InitForPlainTopic(topic);
    }
  }

  bool isOrderTopic() const { return order_topic_; }

  void setOrderTopic(bool orderTopic) { order_topic_ = orderTopic; }

  bool ok() const { return !message_queue_list_.empty(); }

  const QueuesVec& getMessageQueueList() const { return message_queue_list_; }

  std::atomic<std::size_t>& getSendWhichQueue() const { return send_which_queue_; }

  bool isHaveTopicRouterInfo() const { return have_topic_router_info_; }

  void setHaveTopicRouterInfo(bool haveTopicRouterInfo) { have_topic_router_info_ = haveTopicRouterInfo; }

  const MessageQueue& selectOneMessageQueue(const std::string& lastBrokerName) const {
    if (!lastBrokerName.empty()) {
      auto mqSize = message_queue_list_.size();
      if (mqSize <= 1) {
        if (mqSize == 0) {
          LOG_ERROR_NEW("[BUG] messageQueueList is empty");
          THROW_MQEXCEPTION(MQClientException, "messageQueueList is empty", -1);
        }
        return message_queue_list_[0];
      }

      // NOTE: If it possible, mq in same broker is nonadjacent.
      auto index = send_which_queue_.fetch_add(1);
      for (size_t i = 0; i < 2; i++) {
        auto pos = index++ % message_queue_list_.size();
        const auto& mq = message_queue_list_[pos];
        if (mq.broker_name() != lastBrokerName) {
          return mq;
        }
      }
      return message_queue_list_[(index - 2) % message_queue_list_.size()];
    }
    return selectOneMessageQueue();
  }

  const MessageQueue& selectOneMessageQueue() const {
    auto index = send_which_queue_.fetch_add(1);
    auto pos = index % message_queue_list_.size();
    return message_queue_list_[pos];
  }

  int getQueueIdByBroker(const std::string& brokerName) const {
    for (const auto& queueData : topic_route_data_->queue_datas) {
      if (queueData.broker_name == brokerName) {
        return queueData.write_queue_nums;
      }
    }

    return -1;
  }

 private:
  void InitForOrderTopic(const std::string& topic) {
    // example: broker-a:8;broker-b:8
    std::string order_topic_conf = topic_route_data_->order_topic_conf;

    std::vector<std::string> brokers;
    UtilAll::Split(brokers, order_topic_conf, ';');

    for (const auto& broker : brokers) {
      std::vector<std::string> item;
      UtilAll::Split(item, broker, ':');
      int nums = std::atoi(item[1].c_str());
      for (int i = 0; i < nums; i++) {
        message_queue_list_.emplace_back(topic, item[0], i);
      }
    }
  }

  void InitForPlainTopic(const std::string& topic) {
    for (const auto& queue_data : topic_route_data_->queue_datas) {
      if (!PermName::isWriteable(queue_data.perm)) {
        continue;
      }

      const auto* broker_data = GetBrokerData(queue_data.broker_name);

      if (nullptr == broker_data) {
        LOG_WARN_NEW("broker:{} of topic:{} have not data", queue_data.broker_name, topic);
        continue;
      }

      if (broker_data->broker_addrs.find(MASTER_ID) == broker_data->broker_addrs.end()) {
        LOG_WARN_NEW("broker:{} of topic:{} have not master node", queue_data.broker_name, topic);
        continue;
      }

      for (int i = 0; i < queue_data.write_queue_nums; i++) {
        message_queue_list_.emplace_back(topic, queue_data.broker_name, i);
      }
    }

    // sort, make broker_name is staggered.
    std::sort(message_queue_list_.begin(), message_queue_list_.end(), [](const MessageQueue& a, const MessageQueue& b) {
      auto result = a.queue_id() - b.queue_id();
      if (result == 0) {
        result = a.broker_name().compare(b.broker_name());
      }
      return result < 0;
    });
  }

  const BrokerData* GetBrokerData(const std::string& broker_name) const {
    for (const auto& broker_data : topic_route_data_->broker_datas) {
      if (broker_data.broker_name == broker_name) {
        return &broker_data;
      }
    }
    return nullptr;
  }

 private:
  bool order_topic_{false};
  bool have_topic_router_info_{false};

  QueuesVec message_queue_list_;  // no change after build
  mutable std::atomic<std::size_t> send_which_queue_{0};

  TopicRouteDataPtr topic_route_data_;  // no change after set
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PRODUCER_TOPICPUBLISHINFO_HPP_
