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
#ifndef ROCKETMQ_PROTOCOL_HEADER_VIEWMESSAGEREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_VIEWMESSAGEREQUESTHEADER_HPP_

#include <cstdint>  // int64_t

#include <map>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"

namespace rocketmq {

struct ViewMessageRequestHeader : public CommandCustomHeader {
  int64_t offset{0};

  ViewMessageRequestHeader() = default;

  ViewMessageRequestHeader(int64_t offset) : offset(offset) {}

  void Encode(Json::Value& extend_fields) override { extend_fields["offset"] = UtilAll::to_string(offset); }

  void SetDeclaredFieldOfCommandHeader(std::map<std::string, std::string>& request_map) override {
    request_map.emplace("offset", UtilAll::to_string(offset));
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_VIEWMESSAGEREQUESTHEADER_HPP_
