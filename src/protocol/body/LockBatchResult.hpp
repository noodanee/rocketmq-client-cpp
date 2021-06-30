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
#ifndef ROCKETMQ_PROTOCOL_BODY_LOCKBATCHRESULT_HPP_
#define ROCKETMQ_PROTOCOL_BODY_LOCKBATCHRESULT_HPP_

#include <utility>  // std::move
#include <vector>   // std::vector

#include "Logging.h"
#include "MQMessageQueue.h"
#include "utility/JsonSerializer.h"

namespace rocketmq {

struct LockBatchResult {
  std::vector<MQMessageQueue> lock_ok_message_queue_set;

  static std::unique_ptr<LockBatchResult> Decode(const ByteArray& body_data) {
    std::unique_ptr<LockBatchResult> body(new LockBatchResult());
    Json::Value body_object = JsonSerializer::FromJson(body_data);
    const auto& message_queues = body_object["lockOKMQSet"];
    for (const auto& message_queue_object : message_queues) {
      MQMessageQueue message_queue(message_queue_object["topic"].asString(),
                                   message_queue_object["brokerName"].asString(),
                                   message_queue_object["queueId"].asInt());
      LOG_INFO_NEW("LockBatchResult MQ:{}", message_queue.toString());
      body->lock_ok_message_queue_set.push_back(std::move(message_queue));
    }
    return body;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_BODY_LOCKBATCHRESULT_HPP_
