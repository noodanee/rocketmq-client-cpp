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
#ifndef ROCKETMQ_PROTOCOL_HEADER_CONSUMERSENDMSGBACKREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_CONSUMERSENDMSGBACKREQUESTHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <string>
#include <utility>  // std::move

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct ConsumerSendMsgBackRequestHeader : public CommandCustomHeader {
  int64_t offset{0};
  std::string group;
  int32_t delay_level{0};
  std::string origin_message_id;  // nullable
  std::string origin_topic;       // nullable
  bool unit_mode{false};
  int32_t max_reconsume_times{-1};  // nullable

  ConsumerSendMsgBackRequestHeader() = default;

  ConsumerSendMsgBackRequestHeader(int64_t offset,
                                   std::string group,
                                   int32_t delay_level,
                                   std::string origin_message_id,
                                   std::string origin_topic,
                                   int32_t max_consume_retry_times)
      : offset(offset),
        group(std::move(group)),
        delay_level(delay_level),
        origin_message_id(std::move(origin_message_id)),
        origin_topic(std::move(origin_topic)),
        max_reconsume_times(max_consume_retry_times) {}

  void Encode(Json::Value& extend_fields) override {
    extend_fields["offset"] = UtilAll::to_string(offset);
    extend_fields["group"] = group;
    extend_fields["delayLevel"] = UtilAll::to_string(delay_level);
    if (!origin_message_id.empty()) {
      extend_fields["originMsgId"] = origin_message_id;
    }
    if (!origin_topic.empty()) {
      extend_fields["originTopic"] = origin_topic;
    }
    extend_fields["unitMode"] = UtilAll::to_string(unit_mode);
    if (max_reconsume_times != -1) {
      extend_fields["maxReconsumeTimes"] = UtilAll::to_string(max_reconsume_times);
    }
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("offset", UtilAll::to_string(offset));
    request_map.emplace("group", group);
    request_map.emplace("delayLevel", UtilAll::to_string(delay_level));
    if (!origin_message_id.empty()) {
      request_map.emplace("originMsgId", origin_message_id);
    }
    if (!origin_topic.empty()) {
      request_map.emplace("originTopic", origin_topic);
    }
    request_map.emplace("unitMode", UtilAll::to_string(unit_mode));
    if (max_reconsume_times != -1) {
      request_map.emplace("maxReconsumeTimes", UtilAll::to_string(max_reconsume_times));
    }
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_CONSUMERSENDMSGBACKREQUESTHEADER_HPP_
