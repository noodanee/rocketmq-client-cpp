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
#include "ConsumerRunningInfo.h"

#include "RemotingSerializable.h"
#include "UtilAll.h"

namespace rocketmq {

const std::string ConsumerRunningInfo::PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
const std::string ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
const std::string ConsumerRunningInfo::PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY";
const std::string ConsumerRunningInfo::PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE";
const std::string ConsumerRunningInfo::PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION";
const std::string ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP";

const std::map<std::string, std::string> ConsumerRunningInfo::getProperties() const {
  return properties;
}

void ConsumerRunningInfo::setProperties(const std::map<std::string, std::string>& properties) {
  this->properties = properties;
}

void ConsumerRunningInfo::setProperty(const std::string& key, const std::string& value) {
  properties[key] = value;
}

const std::map<MQMessageQueue, ProcessQueueInfo> ConsumerRunningInfo::getMqTable() const {
  return mqTable;
}

void ConsumerRunningInfo::setMqTable(const MQMessageQueue& queue, ProcessQueueInfo queueInfo) {
  mqTable[queue] = queueInfo;
}

/* const std::map<std::string, ConsumeStatus> ConsumerRunningInfo::getStatusTable() const {
  return statusTable;
}

void ConsumerRunningInfo::setStatusTable(const std::map<std::string, ConsumeStatus>& statusTable) {
  this->statusTable = statusTable;
} */

const std::vector<SubscriptionData> ConsumerRunningInfo::getSubscriptionSet() const {
  return subscriptionSet;
}

void ConsumerRunningInfo::setSubscriptionSet(const std::vector<SubscriptionData>& subscriptionSet) {
  this->subscriptionSet = subscriptionSet;
}

const std::string ConsumerRunningInfo::getJstack() const {
  return jstack;
}

void ConsumerRunningInfo::setJstack(const std::string& jstack) {
  this->jstack = jstack;
}

std::string ConsumerRunningInfo::encode() {
  Json::Value outData;

  outData[PROP_NAMESERVER_ADDR] = properties[PROP_NAMESERVER_ADDR];
  outData[PROP_CONSUME_TYPE] = properties[PROP_CONSUME_TYPE];
  outData[PROP_CLIENT_VERSION] = properties[PROP_CLIENT_VERSION];
  outData[PROP_CONSUMER_START_TIMESTAMP] = properties[PROP_CONSUMER_START_TIMESTAMP];
  outData[PROP_CONSUME_ORDERLY] = properties[PROP_CONSUME_ORDERLY];
  outData[PROP_THREADPOOL_CORE_SIZE] = properties[PROP_THREADPOOL_CORE_SIZE];

  Json::Value root;
  root["jstack"] = jstack;
  root["properties"] = outData;

  for (const auto& subscription : subscriptionSet) {
    root["subscriptionSet"].append(subscription.toJson());
  }

  std::string finals = RemotingSerializable::toJson(root);

  Json::Value mq;
  std::string key = "\"mqTable\":";
  key.append("{");
  for (const auto& it : mqTable) {
    key.append(toJson(it.first).toStyledString());
    key.erase(key.end() - 1);
    key.append(":");
    key.append(it.second.toJson().toStyledString());
    key.append(",");
  }
  key.erase(key.end() - 1);
  key.append("}");

  // insert mqTable to final string
  key.append(",");
  finals.insert(1, key);

  return finals;
}

}  // namespace rocketmq
