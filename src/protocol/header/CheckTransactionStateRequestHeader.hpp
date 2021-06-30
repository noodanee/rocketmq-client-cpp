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
#ifndef ROCKETMQ_PROTOCOL_HEADER_CHECKTRANSACTIONSTATEREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_CHECKTRANSACTIONSTATEREQUESTHEADER_HPP_

#include <cstdint>  // int64_t

#include <map>
#include <memory>
#include <sstream>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct CheckTransactionStateRequestHeader : public CommandCustomHeader {
  int64_t transaction_state_table_offset{0};
  int64_t commit_log_offset{0};
  std::string message_id;         // nullable
  std::string transaction_id;     // nullable
  std::string offset_message_id;  // nullable

  static std::unique_ptr<CheckTransactionStateRequestHeader> Decode(std::map<std::string, std::string>& extend_fields) {
    std::unique_ptr<CheckTransactionStateRequestHeader> header(new CheckTransactionStateRequestHeader());
    header->transaction_state_table_offset = std::stoll(extend_fields.at("tranStateTableOffset"));
    header->commit_log_offset = std::stoll(extend_fields.at("commitLogOffset"));

    auto it = extend_fields.find("msgId");
    if (it != extend_fields.end()) {
      header->message_id = it->second;
    }

    it = extend_fields.find("transactionId");
    if (it != extend_fields.end()) {
      header->transaction_id = it->second;
    }

    it = extend_fields.find("offsetMsgId");
    if (it != extend_fields.end()) {
      header->offset_message_id = it->second;
    }

    return header;
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    // TODO: unnecessary
    request_map.emplace("tranStateTableOffset", UtilAll::to_string(transaction_state_table_offset));
    request_map.emplace("commitLogOffset", UtilAll::to_string(commit_log_offset));
    request_map.emplace("msgId", message_id);
    request_map.emplace("transactionId", transaction_id);
    request_map.emplace("offsetMsgId", offset_message_id);
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "CheckTransactionStateRequestHeader:";
    ss << " tranStateTableOffset:" << transaction_state_table_offset;
    ss << " commitLogOffset:" << commit_log_offset;
    ss << " msgId:" << message_id;
    ss << " transactionId:" << transaction_id;
    ss << " offsetMsgId:" << offset_message_id;
    return ss.str();
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_CHECKTRANSACTIONSTATEREQUESTHEADER_HPP_
