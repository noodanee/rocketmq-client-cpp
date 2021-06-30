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
#ifndef ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGEREQUESTHEADERV2_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGEREQUESTHEADERV2_HPP_

#include <cstdint>  // int32_t, int64_t

#include <map>
#include <memory>
#include <string>
#include <utility>  // std::move

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"
#include "protocol/header/SendMessageRequestHeader.hpp"

namespace rocketmq {

struct SendMessageRequestHeaderV2 : public CommandCustomHeader {
  std::string a;  // producerGroup
  std::string b;  // topic
  std::string c;  // defaultTopic
  int32_t d{0};   // defaultTopicQueueNums
  int32_t e{0};   // queueId
  int32_t f{0};   // sysFlag
  int64_t g{0};   // bornTimestamp
  int32_t h{0};   // flag
  std::string i;  // nullable, properties
  int32_t j{-1};  // nullable, reconsumeTimes
  bool k{false};  // nullable, unitMode
  int32_t l{1};   // nullable, maxReconsumeTimes
  bool m{false};  // nullable, batch

  SendMessageRequestHeaderV2() = default;

  SendMessageRequestHeaderV2(std::string producer_group,
                             std::string topic,
                             std::string default_topic,
                             int32_t default_topic_queue_nums,
                             int32_t queue_id,
                             int32_t system_flag,
                             int64_t born_timestamp,
                             int32_t flag,
                             std::string properties,
                             int32_t reconsumer_times,
                             bool unit_mode,
                             int32_t max_reconsume_times,
                             bool batch)
      : a(std::move(producer_group)),
        b(std::move(topic)),
        c(std::move(default_topic)),
        d(default_topic_queue_nums),
        e(queue_id),
        f(system_flag),
        g(born_timestamp),
        h(flag),
        i(std::move(properties)),
        j(reconsumer_times),
        k(unit_mode),
        l(max_reconsume_times),
        m(batch) {}

  static std::unique_ptr<SendMessageRequestHeaderV2> CreateSendMessageRequestHeaderV2(
      std::unique_ptr<SendMessageRequestHeader> v1) {
    std::unique_ptr<SendMessageRequestHeaderV2> v2(new SendMessageRequestHeaderV2(
        std::move(v1->producer_group), std::move(v1->topic), std::move(v1->default_topic), v1->default_topic_queue_nums,
        v1->queue_id, v1->system_flag, v1->born_timestamp, v1->flag, std::move(v1->properties), v1->reconsume_times,
        v1->unit_mode, v1->max_reconsume_times, v1->batch));
    return v2;
  }

  void Encode(Json::Value& extend_fields) override {
    extend_fields["a"] = a;
    extend_fields["b"] = b;
    extend_fields["c"] = c;
    extend_fields["d"] = UtilAll::to_string(d);
    extend_fields["e"] = UtilAll::to_string(e);
    extend_fields["f"] = UtilAll::to_string(f);
    extend_fields["g"] = UtilAll::to_string(g);
    extend_fields["h"] = UtilAll::to_string(h);
    if (!i.empty()) {
      extend_fields["i"] = i;
    }
    if (j != -1) {
      extend_fields["j"] = UtilAll::to_string(j);
    }
    extend_fields["k"] = UtilAll::to_string(k);
    if (l != -1) {
      extend_fields["l"] = UtilAll::to_string(l);
    }
    extend_fields["m"] = UtilAll::to_string(m);
  }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("a", a);
    request_map.emplace("b", b);
    request_map.emplace("c", c);
    request_map.emplace("d", UtilAll::to_string(d));
    request_map.emplace("e", UtilAll::to_string(e));
    request_map.emplace("f", UtilAll::to_string(f));
    request_map.emplace("g", UtilAll::to_string(g));
    request_map.emplace("h", UtilAll::to_string(h));
    if (!i.empty()) {
      request_map.emplace("i", i);
    }
    if (j != -1) {
      request_map.emplace("j", UtilAll::to_string(j));
    }
    request_map.emplace("k", UtilAll::to_string(k));
    if (l != -1) {
      request_map.emplace("l", UtilAll::to_string(l));
    }
    request_map.emplace("m", UtilAll::to_string(m));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_SENDMESSAGEREQUESTHEADERV2_HPP_
