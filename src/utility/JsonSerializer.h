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
#ifndef ROCKETMQ_UTILITY_JSONSERIALIZER_H_
#define ROCKETMQ_UTILITY_JSONSERIALIZER_H_

#include <json/json.h>

#include "ByteArray.h"

namespace rocketmq {
namespace JsonSerializer {

std::string ToJson(const Json::Value& object);
std::string ToJson(const Json::Value& object, bool pretty_format);
void ToJson(const Json::Value& object, std::ostream& sout, bool pretty_format);

Json::Value FromJson(std::istream& sin);
Json::Value FromJson(const std::string& json);
Json::Value FromJson(const ByteArray& bytes);
Json::Value FromJson(const char* begin, const char* end);

}  // namespace JsonSerializer
}  // namespace rocketmq

#endif  // ROCKETMQ_UTILITY_JSONSERIALIZER_H_
