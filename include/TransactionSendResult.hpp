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
#ifndef ROCKETMQ_TRANSACTIONSENDRESULT_HPP_
#define ROCKETMQ_TRANSACTIONSENDRESULT_HPP_

#include "SendResult.hpp"

namespace rocketmq {

enum class LocalTransactionState { kCommitMessage, kRollbackMessage, kUnknown };

inline std::string toString(LocalTransactionState local_transaction_state) {
  switch (local_transaction_state) {
    case LocalTransactionState::kCommitMessage:
      return "COMMIT_MESSAGE";
    case LocalTransactionState::kRollbackMessage:
      return "ROLLBACK_MESSAGE";
    case LocalTransactionState::kUnknown:
      return "UNKNOWN";
  }
}

class ROCKETMQCLIENT_API TransactionSendResult : public SendResult {
 public:
  TransactionSendResult(const SendResult& send_result)
      : SendResult(send_result), local_transaction_state_(LocalTransactionState::kUnknown) {}

  LocalTransactionState local_transaction_state() const noexcept { return local_transaction_state_; }
  void set_local_transaction_state(LocalTransactionState local_transaction_state) noexcept {
    local_transaction_state_ = local_transaction_state;
  }

 private:
  LocalTransactionState local_transaction_state_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_TRANSACTIONSENDRESULT_HPP_
