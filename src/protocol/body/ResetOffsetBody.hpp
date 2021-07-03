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
#ifndef ROCKETMQ_PROTOCOL_BODY_RESETOFFSETBODY_HPP_
#define ROCKETMQ_PROTOCOL_BODY_RESETOFFSETBODY_HPP_

#include <cstdint>  // int64_t

#include <map>  // std::map

#include "MessageQueue.hpp"
#include "utility/JsonSerializer.h"

namespace rocketmq {

struct ResetOffsetBody {
  std::map<MessageQueue, int64_t> offset_table;

  static std::unique_ptr<ResetOffsetBody> Decode(const ByteArray& body_data) {
    std::unique_ptr<ResetOffsetBody> body(new ResetOffsetBody());
    Json::Value body_object = JsonSerializer::FromJson(body_data);
    const auto& offset_table = body_object["offsetTable"];
    // FIXME: object as key
    for (const auto& mq_data : offset_table.getMemberNames()) {
      Json::Value mq_object = JsonSerializer::FromJson(mq_data);
      MessageQueue mq(mq_object["topic"].asString(), mq_object["brokerName"].asString(), mq_object["queueId"].asInt());
      int64_t offset = offset_table[mq_data].asInt64();
      body->offset_table.emplace(std::move(mq), offset);
    }
    return body;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_BODY_RESETOFFSETBODY_HPP_
