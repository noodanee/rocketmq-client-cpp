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
#ifndef ROCKETMQ_PROTOCOL_HEADER_GETEARLIESTMSGSTORETIMERESPONSEHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_GETEARLIESTMSGSTORETIMERESPONSEHEADER_HPP_

#include <cstdint>  // int64_t

#include <map>
#include <memory>
#include <string>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct GetEarliestMsgStoretimeResponseHeader : public CommandCustomHeader {
  int64_t timestamp{0};

  static std::unique_ptr<GetEarliestMsgStoretimeResponseHeader> Decode(
      std::map<std::string, std::string>& extend_fields) {
    std::unique_ptr<GetEarliestMsgStoretimeResponseHeader> header(new GetEarliestMsgStoretimeResponseHeader());
    header->timestamp = std::stoll(extend_fields.at("timestamp"));
    return header;
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    // TODO: unnecessary
    request_map.emplace("timestamp", UtilAll::to_string(timestamp));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_GETEARLIESTMSGSTORETIMERESPONSEHEADER_HPP_
