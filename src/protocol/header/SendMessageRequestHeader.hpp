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
#ifndef ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGEREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGEREQUESTHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct SendMessageRequestHeader : public CommandCustomHeader {
  std::string producer_group;
  std::string topic;
  std::string default_topic;
  int32_t default_topic_queue_nums{0};
  int32_t queue_id{0};
  int32_t system_flag{0};
  int64_t born_timestamp{0};
  int32_t flag{0};
  std::string properties;          // nullable
  int32_t reconsume_times{-1};     // nullable
  bool unit_mode{false};           // nullable
  bool batch{false};               // nullable
  int32_t max_reconsume_times{1};  // nullable

  void Encode(Json::Value& extend_fields) override {
    extend_fields["producerGroup"] = producer_group;
    extend_fields["topic"] = topic;
    extend_fields["defaultTopic"] = default_topic;
    extend_fields["defaultTopicQueueNums"] = UtilAll::to_string(default_topic_queue_nums);
    extend_fields["queueId"] = UtilAll::to_string(queue_id);
    extend_fields["sysFlag"] = UtilAll::to_string(system_flag);
    extend_fields["bornTimestamp"] = UtilAll::to_string(born_timestamp);
    extend_fields["flag"] = UtilAll::to_string(flag);
    if (!properties.empty()) {
      extend_fields["properties"] = properties;
    }
    if (reconsume_times != -1) {
      extend_fields["reconsumeTimes"] = UtilAll::to_string(reconsume_times);
    }
    extend_fields["unitMode"] = UtilAll::to_string(unit_mode);
    extend_fields["batch"] = UtilAll::to_string(batch);
    if (max_reconsume_times != -1) {
      extend_fields["maxReconsumeTimes"] = UtilAll::to_string(max_reconsume_times);
    }
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("producerGroup", producer_group);
    request_map.emplace("topic", topic);
    request_map.emplace("defaultTopic", default_topic);
    request_map.emplace("defaultTopicQueueNums", UtilAll::to_string(default_topic_queue_nums));
    request_map.emplace("queueId", UtilAll::to_string(queue_id));
    request_map.emplace("sysFlag", UtilAll::to_string(system_flag));
    request_map.emplace("bornTimestamp", UtilAll::to_string(born_timestamp));
    request_map.emplace("flag", UtilAll::to_string(flag));
    if (!properties.empty()) {
      request_map.emplace("properties", properties);
    }
    if (reconsume_times != -1) {
      request_map.emplace("reconsumeTimes", UtilAll::to_string(reconsume_times));
    }
    request_map.emplace("unitMode", UtilAll::to_string(unit_mode));
    request_map.emplace("batch", UtilAll::to_string(batch));
    if (max_reconsume_times != -1) {
      request_map.emplace("maxReconsumeTimes", UtilAll::to_string(max_reconsume_times));
    }
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGEREQUESTHEADER_HPP_
