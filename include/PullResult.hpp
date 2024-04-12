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
#ifndef ROCKETMQ_PULLRESULT_HPP_
#define ROCKETMQ_PULLRESULT_HPP_

#include <cstdint>  // int64_t

#include <sstream>  // std::stringstream
#include <string>   // std::string
#include <utility>  // std::move
#include <vector>   // std::vector

#include "MessageExt.h"

namespace rocketmq {

enum class PullStatus { kFound, kNoNewMessage, kNoMatchedMessage, kNoLatestMessage, kOffsetIllegal };

inline std::string toString(PullStatus send_status) {
  switch (send_status) {
    case PullStatus::kFound:
      return "FOUND";
    case PullStatus::kNoNewMessage:
      return "NO_NEW_MSG";
    case PullStatus::kNoMatchedMessage:
      return "NO_MATCHED_MSG";
    case PullStatus::kNoLatestMessage:
      return "NO_LATEST_MSG";
    case PullStatus::kOffsetIllegal:
      return "OFFSET_ILLEGAL";
  }
}

class PullResult {
 public:
  PullResult(PullStatus pull_status,
             int64_t next_begin_offset,
             int64_t min_offset,
             int64_t max_offset,
             std::vector<MessageExtPtr> found_message_list)
      : pull_status_(pull_status),
        next_begin_offset_(next_begin_offset),
        min_offset_(min_offset),
        max_offset_(max_offset),
        found_message_list_(std::move(found_message_list)) {}

  std::string toString() const {
    std::stringstream ss;
    ss << "PullResult [";
    ss << " pullStatus=" << ::rocketmq::toString(pull_status_);
    ss << ", nextBeginOffset=" << next_begin_offset_;
    ss << ", minOffset=" << min_offset_;
    ss << ", maxOffset=" << max_offset_;
    ss << ", msgFoundList=" << found_message_list_.size();
    ss << " ]";
    return ss.str();
  }

  PullStatus pull_status() const noexcept { return pull_status_; };
  int64_t next_begin_offset() const noexcept { return next_begin_offset_; };
  int64_t min_offset() const noexcept { return min_offset_; }
  int64_t max_offset() const noexcept { return max_offset_; }
  const std::vector<MessageExtPtr>& found_message_list() const noexcept { return found_message_list_; }
  std::vector<MessageExtPtr>& found_message_list() noexcept { return found_message_list_; }

 private:
  PullStatus pull_status_{PullStatus::kNoMatchedMessage};
  int64_t next_begin_offset_{0};
  int64_t min_offset_{0};
  int64_t max_offset_{0};
  std::vector<MessageExtPtr> found_message_list_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PULLRESULT_HPP_
