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
#ifndef ROCKETMQ_PROTOCOL_CONSUMERRUNNINGINFO_HPP_
#define ROCKETMQ_PROTOCOL_CONSUMERRUNNINGINFO_HPP_

#include "MessageQueue.hpp"
#include "ProcessQueueInfo.hpp"
#include "protocol/Serializer.hpp"
#include "protocol/body/SubscriptionData.hpp"
#include "utility/JsonSerializer.h"

namespace rocketmq {

struct ConsumerRunningInfo {
  std::map<std::string, std::string> properties;
  std::vector<SubscriptionData> subscription_set;
  std::map<MessageQueue, ProcessQueueInfo> message_queue_table;
  // std::map<std::string, ConsumeStatus> statusTable;
  std::string jstack;

  std::string Encode() {
    Json::Value running_info_object;
    running_info_object["jstack"] = jstack;

    Json::Value properties_object;
    for (const auto& it : properties) {
      const auto& name = it.first;
      const auto& value = it.second;
      properties_object[name] = value;
    }
    running_info_object["properties"] = properties_object;

    for (const auto& subscription : subscription_set) {
      running_info_object["subscriptionSet"].append(subscription.ToJson());
    }

    std::string consumer_running_info = JsonSerializer::ToJson(running_info_object);

    std::string message_queue_table = "\"mqTable\":";
    message_queue_table.append("{");
    for (const auto& it : this->message_queue_table) {
      message_queue_table.append(ToJson(it.first).toStyledString());
      message_queue_table.erase(message_queue_table.end() - 1);
      message_queue_table.append(":");
      message_queue_table.append(it.second.ToJson().toStyledString());
      message_queue_table.append(",");
    }
    message_queue_table.erase(message_queue_table.end() - 1);
    message_queue_table.append("}");

    // insert mqTable to final string
    message_queue_table.append(",");
    consumer_running_info.insert(1, message_queue_table);

    return consumer_running_info;
  }

  static const std::string PROP_NAMESERVER_ADDR;
  static const std::string PROP_THREADPOOL_CORE_SIZE;
  static const std::string PROP_CONSUME_ORDERLY;
  static const std::string PROP_CONSUME_TYPE;
  static const std::string PROP_CLIENT_VERSION;
  static const std::string PROP_CONSUMER_START_TIMESTAMP;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_CONSUMERRUNNINGINFO_HPP_
