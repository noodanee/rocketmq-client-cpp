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
#ifndef ROCKETMQ_PROTOCOL_HEADER_PULLMESSAGEREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_PULLMESSAGEREQUESTHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct PullMessageRequestHeader : public CommandCustomHeader {
  std::string consumer_group;
  std::string topic;
  int32_t queue_id{0};
  int64_t queue_offset{0};
  int32_t max_message_nums{0};
  int32_t system_flag{0};
  int64_t commit_offset{0};
  int64_t suspend_timeout_millis{0};
  std::string subscription;  // nullable
  int64_t subscription_version{0};
  std::string expression_type;  // nullable

  void Encode(Json::Value& extend_fields) override {
    extend_fields["consumerGroup"] = consumer_group;
    extend_fields["topic"] = topic;
    extend_fields["queueId"] = queue_id;
    extend_fields["queueOffset"] = UtilAll::to_string(queue_offset);
    extend_fields["maxMsgNums"] = max_message_nums;
    extend_fields["sysFlag"] = system_flag;
    extend_fields["commitOffset"] = UtilAll::to_string(commit_offset);
    extend_fields["suspendTimeoutMillis"] = UtilAll::to_string(suspend_timeout_millis);
    if (!subscription.empty()) {
      extend_fields["subscription"] = subscription;
    }
    extend_fields["subVersion"] = UtilAll::to_string(subscription_version);
    if (!expression_type.empty()) {
      extend_fields["expressionType"] = expression_type;
    }
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("consumerGroup", consumer_group);
    request_map.emplace("topic", topic);
    request_map.emplace("queueId", UtilAll::to_string(queue_id));
    request_map.emplace("queueOffset", UtilAll::to_string(queue_offset));
    request_map.emplace("maxMsgNums", UtilAll::to_string(max_message_nums));
    request_map.emplace("sysFlag", UtilAll::to_string(system_flag));
    request_map.emplace("commitOffset", UtilAll::to_string(commit_offset));
    request_map.emplace("suspendTimeoutMillis", UtilAll::to_string(suspend_timeout_millis));
    if (!subscription.empty()) {
      request_map.emplace("subscription", subscription);
    }
    request_map.emplace("subVersion", UtilAll::to_string(subscription_version));
    if (!expression_type.empty()) {
      request_map.emplace("expressionType", expression_type);
    }
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_PULLMESSAGEREQUESTHEADER_HPP_
