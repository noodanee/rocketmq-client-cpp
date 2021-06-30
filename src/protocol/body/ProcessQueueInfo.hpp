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
#ifndef ROCKETMQ_PROTOCOL_BODY_PROCESS_QUEUE_INFO_HPP_
#define ROCKETMQ_PROTOCOL_BODY_PROCESS_QUEUE_INFO_HPP_

#include <cstdint>  // int32_t, int64_t

#include <json/json.h>

#include "UtilAll.h"

namespace rocketmq {

struct ProcessQueueInfo {
  uint64_t commit_offset{0};
  uint64_t cached_message_min_offset{0};
  uint64_t cached_message_max_offset{0};
  int32_t cached_message_count{0};
  uint64_t transaction_message_min_offset{0};
  uint64_t transaction_message_max_offset{0};
  int32_t transaction_message_count{0};
  bool locked{false};
  int32_t try_unlock_times{0};
  uint64_t last_lock_timestamp{0};
  bool droped{false};
  uint64_t last_pull_timestamp{0};
  uint64_t last_consume_timestamp{0};

  Json::Value ToJson() const {
    Json::Value process_queue_info;
    process_queue_info["commitOffset"] = UtilAll::to_string(commit_offset);
    process_queue_info["cachedMsgMinOffset"] = UtilAll::to_string(cached_message_min_offset);
    process_queue_info["cachedMsgMaxOffset"] = UtilAll::to_string(cached_message_max_offset);
    process_queue_info["cachedMsgCount"] = cached_message_count;
    process_queue_info["transactionMsgMinOffset"] = UtilAll::to_string(transaction_message_min_offset);
    process_queue_info["transactionMsgMaxOffset"] = UtilAll::to_string(transaction_message_max_offset);
    process_queue_info["transactionMsgCount"] = transaction_message_count;
    process_queue_info["locked"] = locked;
    process_queue_info["tryUnlockTimes"] = try_unlock_times;
    process_queue_info["lastLockTimestamp"] = UtilAll::to_string(last_lock_timestamp);
    process_queue_info["droped"] = droped;
    process_queue_info["lastPullTimestamp"] = UtilAll::to_string(last_pull_timestamp);
    process_queue_info["lastConsumeTimestamp"] = UtilAll::to_string(last_consume_timestamp);
    return process_queue_info;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_BODY_PROCESS_QUEUE_INFO_HPP_
