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
#ifndef ROCKETMQ_PROTOCOL_HEARTBEAT_SUBSCRIPTIONDATA_HPP_
#define ROCKETMQ_PROTOCOL_HEARTBEAT_SUBSCRIPTIONDATA_HPP_

#include <cstdint>  // int64_t

#include <set>      // std::set
#include <string>   // std::string
#include <utility>  // std::move

#include <json/json.h>

#include "ExpressionType.h"
#include "UtilAll.h"

namespace rocketmq {

struct SubscriptionData {
  std::string topic;
  std::string expression;
  int64_t version;
  std::set<std::string> tag_set;
  std::set<int32_t> code_set;
  std::string type;

  SubscriptionData() : version(UtilAll::currentTimeMillis()), type(ExpressionType::TAG) {}
  SubscriptionData(std::string topic, std::string expression)
      : topic(std::move(topic)),
        expression(std::move(expression)),
        version(UtilAll::currentTimeMillis()),
        type(ExpressionType::TAG) {}

  bool operator==(const SubscriptionData& other) const {
    return type == type && topic == other.topic && expression == other.expression && version == other.version &&
           tag_set == other.tag_set;
  }
  bool operator!=(const SubscriptionData& other) const { return !operator==(other); }

  bool operator<(const SubscriptionData& other) const {
    int ret = topic.compare(other.topic);
    if (ret == 0) {
      return expression.compare(other.expression) < 0;
    }
    return ret < 0;
  }

  bool ContainsTag(const std::string& tag) const { return tag_set.find(tag) != tag_set.end(); }

  Json::Value ToJson() const {
    Json::Value subscription_data;
    subscription_data["topic"] = topic;
    subscription_data["subString"] = expression;
    subscription_data["subVersion"] = UtilAll::to_string(version);

    for (const auto& tag : tag_set) {
      subscription_data["tagsSet"].append(tag);
    }

    for (const auto& code : code_set) {
      subscription_data["codeSet"].append(code);
    }

    return subscription_data;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEARTBEAT_SUBSCRIPTIONDATA_HPP_
