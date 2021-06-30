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
#ifndef ROCKETMQ_PROTOCOL_HEARTBEAT_HEARTBEATDATA_H_
#define ROCKETMQ_PROTOCOL_HEARTBEAT_HEARTBEATDATA_H_

#include <string>  // std::string
#include <vector>  // std::vector

#include "ConsumeType.h"
#include "SubscriptionData.hpp"
#include "utility/JsonSerializer.h"

namespace rocketmq {

struct ConsumerData {
  std::string group_name;
  ConsumeType consume_type;
  MessageModel message_model;
  ConsumeFromWhere consume_from_where;
  std::vector<SubscriptionData> subscription_data_set;

  ConsumerData(std::string group_name,
               ConsumeType consume_type,
               MessageModel message_model,
               ConsumeFromWhere consume_from_where,
               std::vector<SubscriptionData> subscription_data_set)
      : group_name(std::move(group_name)),
        consume_type(consume_type),
        message_model(message_model),
        consume_from_where(consume_from_where),
        subscription_data_set(std::move(subscription_data_set)) {}

  bool operator<(const ConsumerData& other) const { return group_name < other.group_name; }

  Json::Value ToJson() const {
    Json::Value root;
    root["groupName"] = group_name;
    root["consumeType"] = consume_type;
    root["messageModel"] = message_model;
    root["consumeFromWhere"] = consume_from_where;

    for (const auto& sd : subscription_data_set) {
      root["subscriptionDataSet"].append(sd.ToJson());
    }

    return root;
  }
};

struct ProducerData {
  std::string group_name;

  ProducerData(std::string group_name) : group_name(std::move(group_name)) {}

  bool operator<(const ProducerData& other) const { return group_name < other.group_name; }

  Json::Value ToJson() const {
    Json::Value root;
    root["groupName"] = group_name;
    return root;
  }
};

struct HeartbeatData {
  std::string client_id;
  std::vector<ConsumerData> consumer_data_set;
  std::vector<ProducerData> producer_data_set;

  std::string Encode() const {
    Json::Value heartbeat_object;

    // id
    heartbeat_object["clientID"] = client_id;

    // consumer
    for (const auto& consumer_data : consumer_data_set) {
      heartbeat_object["consumerDataSet"].append(consumer_data.ToJson());
    }

    // producer
    for (const auto& producer_data : producer_data_set) {
      heartbeat_object["producerDataSet"].append(producer_data.ToJson());
    }

    // output
    return JsonSerializer::ToJson(heartbeat_object);
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEARTBEAT_HEARTBEATDATA_H_
