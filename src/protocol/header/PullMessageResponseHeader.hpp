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
#ifndef ROCKETMQ_PROTOCOL_HEADER_PULLMESSAGERESPONSEHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_PULLMESSAGERESPONSEHEADER_HPP_

#include <cstdint>  // int64_t

#include <map>
#include <memory>
#include <string>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct PullMessageResponseHeader : public CommandCustomHeader {
  int64_t suggest_which_broker_id{0};
  int64_t next_begin_offset{0};
  int64_t min_offset{0};
  int64_t max_offset{0};

  static std::unique_ptr<PullMessageResponseHeader> Decode(std::map<std::string, std::string>& extend_fields) {
    std::unique_ptr<PullMessageResponseHeader> header(new PullMessageResponseHeader());
    header->suggest_which_broker_id = std::stoll(extend_fields.at("suggestWhichBrokerId"));
    header->next_begin_offset = std::stoll(extend_fields.at("nextBeginOffset"));
    header->min_offset = std::stoll(extend_fields.at("minOffset"));
    header->max_offset = std::stoll(extend_fields.at("maxOffset"));
    return header;
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    // NOTE: unnecessary
    request_map.emplace("suggestWhichBrokerId", UtilAll::to_string(suggest_which_broker_id));
    request_map.emplace("nextBeginOffset", UtilAll::to_string(next_begin_offset));
    request_map.emplace("minOffset", UtilAll::to_string(min_offset));
    request_map.emplace("maxOffset", UtilAll::to_string(max_offset));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_PULLMESSAGERESPONSEHEADER_HPP_
