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
#ifndef ROCKETMQ_PROTOCOL_BODY_CONSUMERLIST_HPP_
#define ROCKETMQ_PROTOCOL_BODY_CONSUMERLIST_HPP_

#include <map>
#include <string>

#include <json/json.h>

#include "utility/JsonSerializer.h"

namespace rocketmq {

struct ConsumerList {
  std::vector<std::string> consumer_id_list;

  static std::unique_ptr<ConsumerList> Decode(const ByteArray& body_data) {
    Json::Value body_object = JsonSerializer::FromJson(body_data);
    const auto& consumer_id_list = body_object["consumerIdList"];
    std::unique_ptr<ConsumerList> body(new ConsumerList());
    for (const auto& consumer_id : consumer_id_list) {
      body->consumer_id_list.push_back(consumer_id.asString());
    }
    return body;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_BODY_CONSUMERLIST_HPP_
