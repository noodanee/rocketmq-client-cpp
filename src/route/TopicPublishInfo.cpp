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
#include "route/TopicPublishInfo.hpp"

#include <cstdlib>  // std::atoi

#include <algorithm>  // std::sort
#include <utility>    // std::move

#include "common/PermName.h"
#include "common/UtilAll.h"

namespace rocketmq {

namespace {

std::vector<MessageQueue> MakeMessageQueuesForOrderTopic(const std::string& topic, const TopicRouteData& route) {
  std::vector<MessageQueue> message_queues;

  // example: broker-a:8;broker-b:8
  std::string order_topic_conf = route.order_topic_conf;

  std::vector<std::string> brokers;
  UtilAll::Split(brokers, order_topic_conf, ';');

  for (const auto& broker : brokers) {
    std::vector<std::string> item;
    UtilAll::Split(item, broker, ':');
    int nums = std::atoi(item[1].c_str());
    for (int i = 0; i < nums; i++) {
      message_queues.emplace_back(topic, item[0], i);
    }
  }

  return message_queues;
}

const BrokerData* GetBrokerData(const std::vector<BrokerData>& broker_datas, const std::string& broker_name) {
  for (const auto& broker_data : broker_datas) {
    if (broker_data.broker_name == broker_name) {
      return &broker_data;
    }
  }
  return nullptr;
}

std::vector<MessageQueue> MakeMessageQueuesForPlainTopic(const std::string& topic, const TopicRouteData& route) {
  std::vector<MessageQueue> message_queues;

  for (const auto& queue_data : route.queue_datas) {
    if (!PermName::isWriteable(queue_data.perm)) {
      continue;
    }

    const auto* broker_data = GetBrokerData(route.broker_datas, queue_data.broker_name);

    if (nullptr == broker_data) {
      LOG_WARN_NEW("broker:{} of topic:{} have not data", queue_data.broker_name, topic);
      continue;
    }

    if (broker_data->broker_addrs.find(MASTER_ID) == broker_data->broker_addrs.end()) {
      LOG_WARN_NEW("broker:{} of topic:{} have not master node", queue_data.broker_name, topic);
      continue;
    }

    for (int i = 0; i < queue_data.write_queue_nums; i++) {
      message_queues.emplace_back(topic, queue_data.broker_name, i);
    }
  }

  // sort, make broker_name is staggered.
  std::sort(message_queues.begin(), message_queues.end(), [](const MessageQueue& a, const MessageQueue& b) {
    auto result = a.queue_id() - b.queue_id();
    if (result == 0) {
      result = a.broker_name().compare(b.broker_name());
    }
    return result < 0;
  });

  return message_queues;
}

std::vector<MessageQueue> MakeMessageQueues(const std::string& topic, const TopicRouteData& route) {
  if (!route.order_topic_conf.empty()) {
    return MakeMessageQueuesForOrderTopic(topic, route);
  }
  return MakeMessageQueuesForPlainTopic(topic, route);
}

}  // namespace

TopicPublishInfo::TopicPublishInfo(const std::string& topic, std::shared_ptr<TopicRouteData> topic_route_data)
    : order_topic_(!topic_route_data->order_topic_conf.empty()),
      message_queues_(MakeMessageQueues(topic, *topic_route_data)),
      topic_route_data_(std::move(topic_route_data)) {}

}  // namespace rocketmq
