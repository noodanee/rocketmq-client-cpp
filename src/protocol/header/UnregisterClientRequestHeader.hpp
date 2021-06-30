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
#ifndef ROCKETMQ_PROTOCOL_HEADER_UNREGISTERCLIENTREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_UNREGISTERCLIENTREQUESTHEADER_HPP_

#include <map>
#include <string>
#include <utility>  // std::move

#include <json/json.h>

#include "CommandCustomHeader.h"

namespace rocketmq {

struct UnregisterClientRequestHeader : public CommandCustomHeader {
  std::string client_id;
  std::string producer_group;  // nullable
  std::string consumer_group;  // nullable

  UnregisterClientRequestHeader() = default;

  UnregisterClientRequestHeader(std::string client_id, std::string producer_group, std::string consumer_group)
      : client_id(std::move(client_id)),
        producer_group(std::move(producer_group)),
        consumer_group(std::move(consumer_group)) {}

  void Encode(Json::Value& extend_fields) override {
    extend_fields["clientID"] = client_id;
    if (!producer_group.empty()) {
      extend_fields["producerGroup"] = producer_group;
    }
    if (!consumer_group.empty()) {
      extend_fields["consumerGroup"] = consumer_group;
    }
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("clientID", client_id);
    if (!producer_group.empty()) {
      request_map.emplace("producerGroup", producer_group);
    }
    if (!consumer_group.empty()) {
      request_map.emplace("consumerGroup", consumer_group);
    }
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_UNREGISTERCLIENTREQUESTHEADER_HPP_
