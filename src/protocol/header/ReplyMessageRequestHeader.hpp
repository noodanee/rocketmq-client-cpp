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
#ifndef ROCKETMQ_PROTOCOL_HEADER_REPLYMESSAGEREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_REPLYMESSAGEREQUESTHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <memory>
#include <string>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct ReplyMessageRequestHeader : public CommandCustomHeader {
  std::string producer_group;
  std::string topic;
  std::string default_topic;
  int32_t default_topic_queue_count{0};
  int32_t queue_id{0};
  int32_t system_flag{0};
  int64_t born_timestamp{0};
  int32_t flag{0};
  std::string properties;      // nullable
  int32_t reconsume_times{0};  // nullable
  bool unit_mode{false};       // nullable

  std::string born_host;
  std::string store_host;
  int64_t store_timestamp{0};

  static std::unique_ptr<ReplyMessageRequestHeader> Decode(std::map<std::string, std::string>& extend_fields) {
    std::unique_ptr<ReplyMessageRequestHeader> header(new ReplyMessageRequestHeader());

    header->producer_group = extend_fields.at("producerGroup");
    header->topic = extend_fields.at("topic");
    header->default_topic = extend_fields.at("defaultTopic");
    header->default_topic_queue_count = std::stoi(extend_fields.at("defaultTopicQueueNums"));
    header->queue_id = std::stoi(extend_fields.at("queueId"));
    header->system_flag = std::stoi(extend_fields.at("sysFlag"));
    header->born_timestamp = std::stoll(extend_fields.at("bornTimestamp"));
    header->flag = std::stoi(extend_fields.at("flag"));

    auto it = extend_fields.find("properties");
    if (it != extend_fields.end()) {
      header->properties = it->second;
    }

    it = extend_fields.find("reconsumeTimes");
    if (it != extend_fields.end()) {
      header->reconsume_times = std::stoi(it->second);
    } else {
      header->reconsume_times = 0;
    }

    it = extend_fields.find("unitMode");
    if (it != extend_fields.end()) {
      header->unit_mode = UtilAll::stob(it->second);
    } else {
      header->unit_mode = false;
    }

    header->born_host = extend_fields.at("bornHost");
    header->store_host = extend_fields.at("storeHost");
    header->store_timestamp = std::stoll(extend_fields.at("storeTimestamp"));

    return header;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_REPLYMESSAGEREQUESTHEADER_HPP_
