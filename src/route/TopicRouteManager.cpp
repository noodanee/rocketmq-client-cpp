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
#include "route/TopicRouteManager.hpp"

#include <mutex>   // std::lock_guard, std::mutex
#include <string>  // std::string

#include "protocol/body/TopicRouteData.hpp"

namespace rocketmq {

bool TopicRouteManager::ContainsBrokerAddress(const std::string& address) {
  std::lock_guard<std::mutex> lock(topic_route_table_mutex_);
  for (const auto& it : topic_route_table_) {
    const auto& topic_route_data = it.second;
    const auto& broker_datas = topic_route_data->broker_datas;
    for (const auto& broker_data : broker_datas) {
      for (const auto& it2 : broker_data.broker_addrs) {
        if (it2.second == address) {
          return true;
        }
      }
    }
  }
  return false;
}

}  // namespace rocketmq
