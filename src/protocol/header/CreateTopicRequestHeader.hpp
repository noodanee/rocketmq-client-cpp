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
#ifndef ROCKETMQ_PROTOCOL_HEADER_CREATETOPICREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_CREATETOPICREQUESTHEADER_HPP_

#include <cstdint>  // int32_t

#include <map>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct CreateTopicRequestHeader : public CommandCustomHeader {
  std::string topic;
  std::string default_topic;
  int32_t read_queue_nums{0};
  int32_t write_queue_nums{0};
  int32_t perm{0};
  std::string topic_filter_type;
  int32_t topic_system_flag{-1};  // nullable
  bool order{false};

  CreateTopicRequestHeader() = default;

  CreateTopicRequestHeader(std::string topic,
                           std::string default_topic,
                           int32_t read_queue_nums,
                           int32_t write_queue_nums,
                           int32_t perm,
                           std::string topic_filter_type)
      : topic(std::move(topic)),
        default_topic(std::move(default_topic)),
        read_queue_nums(read_queue_nums),
        write_queue_nums(write_queue_nums),
        perm(perm),
        topic_filter_type(std::move(topic_filter_type)) {}

  void Encode(Json::Value& extend_fields) override {
    extend_fields["topic"] = topic;
    extend_fields["defaultTopic"] = default_topic;
    extend_fields["readQueueNums"] = UtilAll::to_string(read_queue_nums);
    extend_fields["writeQueueNums"] = UtilAll::to_string(write_queue_nums);
    extend_fields["perm"] = UtilAll::to_string(perm);
    extend_fields["topicFilterType"] = topic_filter_type;
    if (topic_system_flag != -1) {
      extend_fields["topicSysFlag"] = UtilAll::to_string(topic_system_flag);
    }
    extend_fields["order"] = UtilAll::to_string(order);
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("topic", topic);
    request_map.emplace("defaultTopic", default_topic);
    request_map.emplace("readQueueNums", UtilAll::to_string(read_queue_nums));
    request_map.emplace("writeQueueNums", UtilAll::to_string(write_queue_nums));
    request_map.emplace("perm", UtilAll::to_string(perm));
    request_map.emplace("topicFilterType", topic_filter_type);
    if (topic_system_flag != -1) {
      request_map.emplace("topicSysFlag", UtilAll::to_string(topic_system_flag));
    }
    request_map.emplace("order", UtilAll::to_string(order));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_CREATETOPICREQUESTHEADER_HPP_
