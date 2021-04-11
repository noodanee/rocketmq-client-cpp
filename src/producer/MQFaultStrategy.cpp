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
#include "MQFaultStrategy.h"

namespace rocketmq {

const MQMessageQueue& MQFaultStrategy::SelectOneMessageQueue(const TopicPublishInfo* topic_publish_info,
                                                             const std::string& last_broker_name) {
  if (enable_) {
    {
      auto index = topic_publish_info->getSendWhichQueue().fetch_add(1);
      const auto& message_queue_list = topic_publish_info->getMessageQueueList();
      for (size_t i = 0; i < message_queue_list.size(); i++) {
        auto pos = index++ % message_queue_list.size();
        const auto& message_queue = message_queue_list[pos];
        if (latency_fault_tolerance_.IsAvailable(message_queue.broker_name())) {
          return message_queue;
        }
      }
    }

    auto not_best_broker = latency_fault_tolerance_.PickOneAtLeast();
    int write_queue_nums = topic_publish_info->getQueueIdByBroker(not_best_broker);
    if (write_queue_nums > 0) {
      // FIXME: why modify origin mq object, not return a new one?
      static thread_local MQMessageQueue message_queue;
      message_queue = topic_publish_info->selectOneMessageQueue();
      if (!not_best_broker.empty()) {
        message_queue.set_broker_name(not_best_broker);
        message_queue.set_queue_id(topic_publish_info->getSendWhichQueue().fetch_add(1) % write_queue_nums);
      }
      return message_queue;
    } else {
      latency_fault_tolerance_.Remove(not_best_broker);
    }

    return topic_publish_info->selectOneMessageQueue();
  }

  return topic_publish_info->selectOneMessageQueue(last_broker_name);
}

void MQFaultStrategy::UpdateFaultItem(const std::string& broker_name, long current_latency, bool isolation) {
  if (enable_) {
    long duration = ComputeNotAvailableDuration(isolation ? 30000 : current_latency);
    latency_fault_tolerance_.UpdateFaultItem(broker_name, current_latency, duration);
  }
}

long MQFaultStrategy::ComputeNotAvailableDuration(long current_latency) {
  for (size_t i = latency_max_.size(); i > 0; i--) {
    if (current_latency >= latency_max_[i - 1]) {
      return not_available_duration_[i - 1];
    }
  }
  return 0;
}

}  // namespace rocketmq
