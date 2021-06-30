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
#ifndef ROCKETMQ_PROTOCOL_HEADER_ENDTRANSACTIONREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_ENDTRANSACTIONREQUESTHEADER_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <sstream>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct EndTransactionRequestHeader : public CommandCustomHeader {
  std::string producer_group;
  int64_t transaction_state_table_offset{0};
  int64_t commit_log_offset{0};
  int32_t commit_or_rollback{0};
  bool from_transaction_check{false};  // nullable
  std::string message_id;
  std::string transaction_id;  // nullable

  void Encode(Json::Value& extend_fields) override {
    extend_fields["producerGroup"] = producer_group;
    extend_fields["tranStateTableOffset"] = UtilAll::to_string(transaction_state_table_offset);
    extend_fields["commitLogOffset"] = UtilAll::to_string(commit_log_offset);
    extend_fields["commitOrRollback"] = UtilAll::to_string(commit_or_rollback);
    extend_fields["fromTransactionCheck"] = UtilAll::to_string(from_transaction_check);
    extend_fields["msgId"] = message_id;
    if (!transaction_id.empty()) {
      extend_fields["transactionId"] = transaction_id;
    }
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("producerGroup", producer_group);
    request_map.emplace("tranStateTableOffset", UtilAll::to_string(transaction_state_table_offset));
    request_map.emplace("commitLogOffset", UtilAll::to_string(commit_log_offset));
    request_map.emplace("commitOrRollback", UtilAll::to_string(commit_or_rollback));
    request_map.emplace("fromTransactionCheck", UtilAll::to_string(from_transaction_check));
    request_map.emplace("msgId", message_id);
    if (!transaction_id.empty()) {
      request_map.emplace("transactionId", transaction_id);
    }
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "EndTransactionRequestHeader:";
    ss << " producerGroup:" << producer_group;
    ss << " tranStateTableOffset:" << transaction_state_table_offset;
    ss << " commitLogOffset:" << commit_log_offset;
    ss << " commitOrRollback:" << commit_or_rollback;
    ss << " fromTransactionCheck:" << from_transaction_check;
    ss << " msgId:" << message_id;
    ss << " transactionId:" << transaction_id;
    return ss.str();
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_ENDTRANSACTIONREQUESTHEADER_HPP_
