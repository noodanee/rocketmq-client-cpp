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
#ifndef ROCKETMQ_PROTOCOL_HEADER_RESETOFFSETREQUESTHEADER_HPP_
#define ROCKETMQ_PROTOCOL_HEADER_RESETOFFSETREQUESTHEADER_HPP_

#include <cstdint>  // int64_t

#include <map>
#include <memory>
#include <string>

#include <json/json.h>

#include "CommandCustomHeader.h"
#include "common/UtilAll.h"
#include "log/Logging.h"

namespace rocketmq {

struct ResetOffsetRequestHeader : public CommandCustomHeader {
  std::string topic;
  std::string group;
  int64_t timestamp{0};
  bool force{false};

  static std::unique_ptr<ResetOffsetRequestHeader> Decode(std::map<std::string, std::string>& extend_fields) {
    std::unique_ptr<ResetOffsetRequestHeader> header(new ResetOffsetRequestHeader());
    header->topic = extend_fields.at("topic");
    header->group = extend_fields.at("group");
    header->timestamp = std::stoll(extend_fields.at("timestamp"));
    header->force = UtilAll::stob(extend_fields.at("isForce"));
    LOG_INFO_NEW("topic:{}, group:{}, timestamp:{}, isForce:{}", header->topic, header->group, header->timestamp,
                 header->force);
    return header;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_HEADER_RESETOFFSETREQUESTHEADER_HPP_
