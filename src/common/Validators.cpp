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
#include "Validators.h"

#include <regex>

#include "MQProtos.h"

namespace {

const char* kValidPattern = "^[a-zA-Z0-9_-]+$";
const int kCharacterMaxLength = 255;

}  // namespace

namespace rocketmq {

bool Validators::regularExpressionMatcher(const std::string& origin, const std::string& pattern) {
  if (UtilAll::isBlank(origin)) {
    return false;
  }

  if (UtilAll::isBlank(pattern)) {
    return true;
  }

#if defined(__GNUC__) && ((__GNUC__ < 4) || (__GNUC__ == 4 && __GNUC_MINOR__ <= 8))
  return true;
#else
  const std::regex regex(pattern, std::regex::extended);
  return std::regex_match(origin, regex);
#endif
}

std::string Validators::getGroupWithRegularExpression(const std::string& origin, const std::string& pattern) {
  if (!UtilAll::isBlank(pattern)) {
#if !defined(__GNUC__) || __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ > 8)
    const std::regex regex(pattern, std::regex::extended);
    std::smatch match;

    if (std::regex_match(origin, match, regex)) {
      // The first sub_match is the whole string; the next
      // sub_match is the first parenthesized expression.
      if (match.size() == 2) {
        std::ssub_match base_sub_match = match[1];
        return base_sub_match.str();
      }
    }
#endif
  }
  return "";
}

void Validators::checkTopic(const std::string& topic) {
  if (UtilAll::isBlank(topic)) {
    THROW_MQEXCEPTION(MQClientException, "the specified topic is blank", -1);
  }

  if ((int)topic.length() > kCharacterMaxLength) {
    THROW_MQEXCEPTION(MQClientException, "the specified topic is longer than topic max length 255.", -1);
  }

  if (topic == AUTO_CREATE_TOPIC_KEY_TOPIC) {
    THROW_MQEXCEPTION(MQClientException, "the topic[" + topic + "] is conflict with default topic.", -1);
  }

  if (!regularExpressionMatcher(topic, kValidPattern)) {
    std::string str = "the specified topic[" + topic + "] contains illegal characters, allowing only" + kValidPattern;
    THROW_MQEXCEPTION(MQClientException, str, -1);
  }
}

void Validators::checkGroup(const std::string& group) {
  if (UtilAll::isBlank(group)) {
    THROW_MQEXCEPTION(MQClientException, "the specified group is blank", -1);
  }

  if (!regularExpressionMatcher(group, kValidPattern)) {
    std::string str = "the specified group[" + group + "] contains illegal characters, allowing only" + kValidPattern;
    THROW_MQEXCEPTION(MQClientException, str, -1);
  }
  if ((int)group.length() > kCharacterMaxLength) {
    THROW_MQEXCEPTION(MQClientException, "the specified group is longer than group max length 255.", -1);
  }
}

void Validators::checkMessage(const Message& message, int maxMessageSize) {
  checkTopic(message.topic());

  const auto& body = message.body();
  if (body.empty()) {
    THROW_MQEXCEPTION(MQClientException, "the message body is empty", MESSAGE_ILLEGAL);
  }

  if (body.size() > (size_t)maxMessageSize) {
    std::string info = "the message body size over max value, MAX: " + UtilAll::to_string(maxMessageSize);
    THROW_MQEXCEPTION(MQClientException, info, MESSAGE_ILLEGAL);
  }
}

}  // namespace rocketmq
