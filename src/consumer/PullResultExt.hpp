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
#ifndef ROCKETMQ_CONSUMER_PULLRESULTEXT_HPP_
#define ROCKETMQ_CONSUMER_PULLRESULTEXT_HPP_

#include <cstdint>  // int64_t

#include <utility>  // std::move

#include "ByteArray.h"
#include "PullResult.hpp"

namespace rocketmq {

struct PullResultExt {
  PullStatus pull_status{PullStatus::kNoMatchedMessage};
  int64_t next_begin_offset{0};
  int64_t min_offset{0};
  int64_t max_offset{0};
  int64_t suggert_which_boker_id;
  ByteArrayRef message_binary;

  PullResultExt(PullStatus pull_status,
                int64_t next_begin_offset,
                int64_t min_offset,
                int64_t max_offset,
                int64_t suggest_which_broker_id,
                ByteArrayRef message_binary)
      : pull_status(pull_status),
        next_begin_offset(next_begin_offset),
        min_offset(min_offset),
        max_offset(max_offset),
        suggert_which_boker_id(suggest_which_broker_id),
        message_binary(std::move(message_binary)) {}
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_PULLRESULTEXT_HPP_
