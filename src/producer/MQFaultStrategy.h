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
#ifndef ROCKETMQ_PRODUCER_MQFAULTSTRATEGY_H_
#define ROCKETMQ_PRODUCER_MQFAULTSTRATEGY_H_

#include <string>
#include <vector>

#include "LatencyFaultTolerancyImpl.h"
#include "MessageQueue.hpp"
#include "TopicPublishInfo.hpp"

namespace rocketmq {

class MQFaultStrategy {
 public:
  const MessageQueue& SelectOneMessageQueue(const TopicPublishInfo* topic_publish_info,
                                            const std::string& last_broker_name);

  void UpdateFaultItem(const std::string& broker_name, long current_latency, bool isolation);

 private:
  long ComputeNotAvailableDuration(long current_latency);

 public:
  bool enable() const { return enable_; }
  void set_enable(bool enable) { enable_ = enable; }

  const std::vector<long>& latency_max() const { return latency_max_; }
  void set_latency_max(const std::vector<long>& latency_max) { latency_max_ = latency_max; }

  const std::vector<long>& not_available_duration() const { return not_available_duration_; }
  void set_not_available_duration(std::vector<long> not_available_duration) {
    not_available_duration_ = std::move(not_available_duration);
  }

 private:
  bool enable_{false};
  LatencyFaultTolerancyImpl latency_fault_tolerance_;

  std::vector<long> latency_max_{50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
  std::vector<long> not_available_duration_{0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PRODUCER_MQFAULTSTRATEGY_H_
