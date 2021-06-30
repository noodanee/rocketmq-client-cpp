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
#ifndef ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGERESPONSEHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGERESPONSEHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <memory>
#include <string>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct SendMessageResponseHeader : public CommandCustomHeader {
  std::string message_id;
  int32_t queue_id{0};
  int64_t queue_offset{0};
  std::string transaction_id;  // nullable

  static std::unique_ptr<SendMessageResponseHeader> Decode(std::map<std::string, std::string>& extend_fields) {
    std::unique_ptr<SendMessageResponseHeader> header(new SendMessageResponseHeader());
    header->message_id = extend_fields.at("msgId");
    header->queue_id = std::stoi(extend_fields.at("queueId"));
    header->queue_offset = std::stoll(extend_fields.at("queueOffset"));

    const auto& it = extend_fields.find("transactionId");
    if (it != extend_fields.end()) {
      header->transaction_id = it->second;
    }

    return header;
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    // NOTE: unnecessary
    request_map.emplace("msgId", message_id);
    request_map.emplace("queueId", UtilAll::to_string(queue_id));
    request_map.emplace("queueOffset", UtilAll::to_string(queue_offset));
    request_map.emplace("transactionId", transaction_id);
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGERESPONSEHEADER_HPP_
