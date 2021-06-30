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
#ifndef ROCKETMQ_PROTOCOL_HEADER_UPDATECONSUMEROFFSETREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_UPDATECONSUMEROFFSETREQUESTHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <string>
#include <utility>  // std::move

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct UpdateConsumerOffsetRequestHeader : public CommandCustomHeader {
  std::string consumer_group;
  std::string topic;
  int32_t queue_id{0};
  int64_t commit_offset{0};

  UpdateConsumerOffsetRequestHeader() = default;

  UpdateConsumerOffsetRequestHeader(std::string consumer_group,
                                    std::string topic,
                                    int32_t queue_id,
                                    int64_t commit_offset)
      : consumer_group(std::move(consumer_group)),
        topic(std::move(topic)),
        queue_id(queue_id),
        commit_offset(commit_offset) {}

  void Encode(Json::Value& extend_fields) override {
    extend_fields["consumerGroup"] = consumer_group;
    extend_fields["topic"] = topic;
    extend_fields["queueId"] = UtilAll::to_string(queue_id);
    extend_fields["commitOffset"] = UtilAll::to_string(commit_offset);
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("consumerGroup", consumer_group);
    request_map.emplace("topic", topic);
    request_map.emplace("queueId", UtilAll::to_string(queue_id));
    request_map.emplace("commitOffset", UtilAll::to_string(commit_offset));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_UPDATECONSUMEROFFSETREQUESTHEADER_HPP_
