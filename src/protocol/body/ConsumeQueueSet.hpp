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
#ifndef ROCKETMQ_PROTOCOL_BODY_CONSUMERQUEUESET_HPP_
#define ROCKETMQ_PROTOCOL_BODY_CONSUMERQUEUESET_HPP_

#include <utility>  // std::move
#include <vector>   // std::vector

#include "protocol/Serializer.hpp"
#include "utility/JsonSerializer.h"

namespace rocketmq {

struct ConsumeQueueSet {
  std::string consumer_group;
  std::string client_id;
  std::vector<MessageQueue> message_queue_set;

  std::string Encode() const {
    Json::Value body_object;
    body_object["consumerGroup"] = consumer_group;
    body_object["clientId"] = client_id;

    for (const auto& mq : message_queue_set) {
      body_object["mqSet"].append(ToJson(mq));
    }

    return JsonSerializer::ToJson(body_object);
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_BODY_CONSUMERQUEUESET_HPP_
