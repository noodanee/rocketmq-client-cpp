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
#ifndef ROCKETMQ_PROTOCOL_HEADER_SEARCHOFFSETREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_SEARCHOFFSETREQUESTHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <string>
#include <utility>  // std::move

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct SearchOffsetRequestHeader : public CommandCustomHeader {
  std::string topic;
  int32_t queue_id{0};
  int64_t timestamp{0};

  SearchOffsetRequestHeader() = default;

  SearchOffsetRequestHeader(std::string topic, int32_t queue_id, int64_t timestamp)
      : topic(std::move(topic)), queue_id(queue_id), timestamp(timestamp) {}

  void Encode(Json::Value& extFields) override {
    extFields["topic"] = topic;
    extFields["queueId"] = UtilAll::to_string(queue_id);
    extFields["timestamp"] = UtilAll::to_string(timestamp);
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& requestMap) override {
    requestMap.emplace("topic", topic);
    requestMap.emplace("queueId", UtilAll::to_string(queue_id));
    requestMap.emplace("timestamp", UtilAll::to_string(timestamp));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_SEARCHOFFSETREQUESTHEADER_HPP_
