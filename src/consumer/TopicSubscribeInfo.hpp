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
#ifndef ROCKETMQ_PRODUCER_TOPICSUBSCRIBEINFO_HPP_
#define ROCKETMQ_PRODUCER_TOPICSUBSCRIBEINFO_HPP_

#include <string>
#include <vector>

#include "MessageQueue.hpp"
#include "common/PermName.h"
#include "protocol/body/TopicRouteData.hpp"

namespace rocketmq {

std::vector<MessageQueue> MakeTopicSubscribeInfo(const std::string& topic, const TopicRouteData& route) {
  std::vector<MessageQueue> message_queues;
  for (const auto& queue_data : route.queue_datas) {
    if (PermName::isReadable(queue_data.perm)) {
      for (int i = 0; i < queue_data.read_queue_nums; i++) {
        message_queues.emplace_back(topic, queue_data.broker_name, i);
      }
    }
  }
  return message_queues;
}

}  // namespace rocketmq

#endif  // ROCKETMQ_PRODUCER_TOPICSUBSCRIBEINFO_HPP_
