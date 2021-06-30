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
#ifndef ROCKETMQ_PROTOCOL_HEADER_GETCONSUMERRUNNINGINFOREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_GETCONSUMERRUNNINGINFOREQUESTHEADER_HPP_

#include <map>
#include <memory>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"
#include "log/Logging.h"

namespace rocketmq {

struct GetConsumerRunningInfoRequestHeader : public CommandCustomHeader {
  std::string consumer_group;
  std::string client_id;
  bool jstack_enable{false};  // nullable

  static std::unique_ptr<GetConsumerRunningInfoRequestHeader> Decode(
      std::map<std::string, std::string>& extend_fields) {
    std::unique_ptr<GetConsumerRunningInfoRequestHeader> header(new GetConsumerRunningInfoRequestHeader());
    header->consumer_group = extend_fields.at("consumerGroup");
    header->client_id = extend_fields.at("clientId");
    header->jstack_enable = UtilAll::stob(extend_fields.at("jstackEnable"));
    LOG_INFO_NEW("consumerGroup:{}, clientId:{},  jstackEnable:{}", header->consumer_group, header->client_id,
                 header->jstack_enable);
    return header;
  }

  void Encode(Json::Value& extend_fields) override {
    extend_fields["consumerGroup"] = consumer_group;
    extend_fields["clientId"] = client_id;
    extend_fields["jstackEnable"] = UtilAll::to_string(jstack_enable);
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("consumerGroup", consumer_group);
    request_map.emplace("clientId", client_id);
    request_map.emplace("jstackEnable", UtilAll::to_string(jstack_enable));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_GETCONSUMERRUNNINGINFOREQUESTHEADER_HPP_
