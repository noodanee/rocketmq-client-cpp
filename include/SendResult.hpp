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
#ifndef ROCKETMQ_SENDRESULT_HPP_
#define ROCKETMQ_SENDRESULT_HPP_

#include <cstdint>  // int64_t

#include <sstream>  // std::stringstream
#include <string>   // std::string
#include <utility>  // std::move

#include "MessageQueue.hpp"

namespace rocketmq {

enum class SendStatus { kSendOk, kSendFlushDiskTimeout, kSendFlushSlaveTimeout, kSendSlaveNotAvailable };

inline std::string toString(SendStatus send_status) {
  switch (send_status) {
    case SendStatus::kSendOk:
      return "SEND_OK";
    case SendStatus::kSendFlushDiskTimeout:
      return "SEND_FLUSH_DISK_TIMEOUT";
    case SendStatus::kSendFlushSlaveTimeout:
      return "SEND_FLUSH_SLAVE_TIMEOUT";
    case SendStatus::kSendSlaveNotAvailable:
      return "SEND_SLAVE_NOT_AVAILABLE";
  }
}

class ROCKETMQCLIENT_API SendResult {
 public:
  SendResult(SendStatus send_status,
             std::string message_id,
             std::string offset_message_id,
             MessageQueue message_queue,
             int64_t queue_offset,
             std::string transaction_id)
      : send_status_(send_status),
        message_id_(std::move(message_id)),
        offset_message_id_(std::move(offset_message_id)),
        message_queue_(std::move(message_queue)),
        queue_offset_(queue_offset),
        transaction_id_(std::move(transaction_id)) {}

  std::string toString() const {
    std::stringstream ss;
    ss << "SendResult [";
    ss << " sendStatus=" << ::rocketmq::toString(send_status_);
    ss << ", msgId=" << message_id_;
    ss << ", offsetMsgId=" << offset_message_id_;
    ss << ", queueOffset=" << queue_offset_;
    ss << ", transactionId=" << transaction_id_;
    ss << ", messageQueue=" << message_queue_.toString();
    ss << " ]";
    return ss.str();
  }

  SendStatus send_status() const noexcept { return send_status_; }
  void set_send_status(SendStatus send_status) noexcept { send_status_ = send_status; }

  const std::string& message_id() const noexcept { return message_id_; }
  void set_message_id(std::string message_id) noexcept { message_id_ = std::move(message_id); }

  const std::string& offset_message_id() const { return offset_message_id_; }
  void set_offset_message_id(std::string offset_message_id) noexcept {
    offset_message_id_ = std::move(offset_message_id);
  }

  const MessageQueue& message_queue() const noexcept { return message_queue_; }
  void set_message_queue(MessageQueue message_queue) noexcept { message_queue_ = std::move(message_queue); }

  int64_t queue_offset() const noexcept { return queue_offset_; }
  void set_queue_offset(int64_t queue_offset) noexcept { queue_offset_ = queue_offset; }

  const std::string& transaction_id() const noexcept { return transaction_id_; }
  void set_transaction_id(std::string transaction_id) noexcept { transaction_id_ = std::move(transaction_id); }

 private:
  SendStatus send_status_{SendStatus::kSendOk};
  std::string message_id_;
  std::string offset_message_id_;
  MessageQueue message_queue_;
  int64_t queue_offset_{0};
  std::string transaction_id_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_SENDRESULT_HPP_
