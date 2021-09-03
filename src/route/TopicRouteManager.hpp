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
#ifndef ROCKETMQ_ROUTE_TOPICROUTEMANAGER_HPP_
#define ROCKETMQ_ROUTE_TOPICROUTEMANAGER_HPP_

#include <map>     // std::map
#include <memory>  // std::shared_ptr
#include <mutex>   // std::mutex
#include <set>     // std::set
#include <string>  // std::string

#include "utility/MapAccessor.hpp"

namespace rocketmq {

struct TopicRouteData;
class TopicPublishInfo;
class TopicSubscribeInfo;

class TopicRouteManager {
 public:
  std::shared_ptr<TopicRouteData> GetTopicRouteData(const std::string& topic) {
    return MapAccessor::GetOrDefault(topic_route_table_, topic, nullptr, topic_route_table_mutex_);
  }

  void PutTopicRouteData(const std::string& topic, std::shared_ptr<TopicRouteData> topic_route) {
    MapAccessor::InsertOrAssign(topic_route_table_, topic, topic_route, topic_route_table_mutex_);
  }

  bool ContainsBrokerAddress(const std::string& address);

  std::shared_ptr<TopicPublishInfo> GetTopicPublishInfo(const std::string& topic) {
    return MapAccessor::GetOrDefault(topic_publish_info_table_, topic, nullptr, topic_publish_info_table_mutex_);
  }

  void PutTopicPublishInfo(const std::string& topic, std::shared_ptr<TopicPublishInfo> publish_info) {
    MapAccessor::InsertOrAssign(topic_publish_info_table_, topic, std::move(publish_info),
                                topic_publish_info_table_mutex_);
  }

  std::set<std::string> TopicInPublish() {
    return MapAccessor::KeySet(topic_publish_info_table_, topic_publish_info_table_mutex_);
  }

  std::shared_ptr<TopicSubscribeInfo> GetTopicSubscribeInfo(const std::string& topic) {
    return MapAccessor::GetOrDefault(topic_subscribe_info_table_, topic, nullptr, topic_subescribe_info_table_mutex_);
  }

  void PutTopicSubscribeInfo(const std::string& topic, std::shared_ptr<TopicSubscribeInfo> subscribe_info) {
    MapAccessor::InsertOrAssign(topic_subscribe_info_table_, topic, std::move(subscribe_info),
                                topic_subescribe_info_table_mutex_);
  }

 private:
  // topic -> TopicRouteData
  std::map<std::string, std::shared_ptr<TopicRouteData>> topic_route_table_;
  std::mutex topic_route_table_mutex_;

  // topic -> TopicPublishInfo
  std::map<std::string, std::shared_ptr<TopicPublishInfo>> topic_publish_info_table_;
  std::mutex topic_publish_info_table_mutex_;

  // topic -> TopicSubscribeInfo
  std::map<std::string, std::shared_ptr<TopicSubscribeInfo>> topic_subscribe_info_table_;
  std::mutex topic_subescribe_info_table_mutex_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_ROUTE_TOPICROUTEMANAGER_HPP_
