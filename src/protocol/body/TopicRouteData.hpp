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
#ifndef ROCKETMQ_PROTOCOL_TOPICROUTEDATA_HPP_
#define ROCKETMQ_PROTOCOL_TOPICROUTEDATA_HPP_

#include <algorithm>  // std::sort
#include <cstdlib>
#include <memory>
#include <utility>  // std::move

#include <json/json.h>

#include "ByteArray.h"
#include "Logging.h"
#include "UtilAll.h"
#include "utility/JsonSerializer.h"

namespace rocketmq {

struct QueueData {
  std::string broker_name;
  int read_queue_nums{16};
  int write_queue_nums{16};
  int perm{6};

  QueueData(std::string broker_name, int read_queue_nums, int write_queue_nums, int perm)
      : broker_name(std::move(broker_name)),
        read_queue_nums(read_queue_nums),
        write_queue_nums(write_queue_nums),
        perm(perm) {}

  bool operator<(const QueueData& other) const { return broker_name < other.broker_name; }

  bool operator==(const QueueData& other) const {
    return broker_name == other.broker_name && read_queue_nums == other.read_queue_nums &&
           write_queue_nums == other.write_queue_nums && perm == other.perm;
  }
  bool operator!=(const QueueData& other) const { return !operator==(other); }
};

struct BrokerData {
  std::string broker_name;
  std::map<int, std::string> broker_addrs;  // master:0; slave:1,2,3,etc.

  BrokerData(std::string broker_name) : broker_name(std::move(broker_name)) {}
  BrokerData(std::string broker_name, std::map<int, std::string> broker_addrs)
      : broker_name(std::move(broker_name)), broker_addrs(std::move(broker_addrs)) {}

  bool operator<(const BrokerData& other) const { return broker_name < other.broker_name; }

  bool operator==(const BrokerData& other) const {
    return broker_name == other.broker_name && broker_addrs == other.broker_addrs;
  }
  bool operator!=(const BrokerData& other) const { return !operator==(other); }
};

struct TopicRouteData;
using TopicRouteDataPtr = std::shared_ptr<TopicRouteData>;

struct TopicRouteData {
  std::string order_topic_conf;
  std::vector<QueueData> queue_datas;
  std::vector<BrokerData> broker_datas;

  static std::unique_ptr<TopicRouteData> Decode(const ByteArray& bodyData) {
    std::unique_ptr<TopicRouteData> topic_route_data(new TopicRouteData());

    Json::Value topic_route_object = JsonSerializer::FromJson(bodyData);

    topic_route_data->order_topic_conf = topic_route_object["orderTopicConf"].asString();

    auto& queue_datas = topic_route_object["queueDatas"];
    for (auto queue_data : queue_datas) {
      topic_route_data->queue_datas.emplace_back(queue_data["brokerName"].asString(),
                                                 queue_data["readQueueNums"].asInt(),
                                                 queue_data["writeQueueNums"].asInt(), queue_data["perm"].asInt());
    }
    sort(topic_route_data->queue_datas.begin(), topic_route_data->queue_datas.end());

    auto& broker_datas = topic_route_object["brokerDatas"];
    for (auto broker_data : broker_datas) {
      std::string broker_name = broker_data["brokerName"].asString();
      LOG_DEBUG_NEW("brokerName:{}", broker_name);
      auto& bas = broker_data["brokerAddrs"];
      Json::Value::Members members = bas.getMemberNames();
      std::map<int, std::string> broker_addrs;
      for (const auto& member : members) {
        int id = std::stoi(member);
        std::string addr = bas[member].asString();
        broker_addrs.emplace(id, std::move(addr));
        LOG_DEBUG_NEW("brokerId:{}, brokerAddr:{}", id, addr);
      }
      topic_route_data->broker_datas.emplace_back(std::move(broker_name), std::move(broker_addrs));
    }
    sort(topic_route_data->broker_datas.begin(), topic_route_data->broker_datas.end());

    return topic_route_data;
  }

  /**
   * Selects a (preferably master) broker address from the registered list.
   * If the master's address cannot be found, a slave broker address is selected in a random manner.
   *
   * @return Broker address.
   */
  std::string SelectBrokerAddr() {
    auto broker_data_size = broker_datas.size();
    if (broker_data_size > 0) {
      auto broker_data_index = std::rand() % broker_data_size;
      const auto& broker_data = broker_datas[broker_data_index];
      const auto& broker_addrs = broker_data.broker_addrs;
      auto it = broker_addrs.find(MASTER_ID);
      if (it == broker_addrs.end()) {
        auto broker_addr_size = broker_addrs.size();
        auto broker_addr_index = std::rand() % broker_addr_size;
        for (it = broker_addrs.begin(); broker_addr_index > 0; --broker_addr_index) {
          it++;
        }
      }
      return it->second;
    }
    return null;
  }

  bool operator==(const TopicRouteData& other) const {
    return broker_datas == other.broker_datas && order_topic_conf == other.order_topic_conf &&
           queue_datas == other.queue_datas;
  }
  bool operator!=(const TopicRouteData& other) const { return !operator==(other); }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROTOCOL_TOPICROUTEDATA_HPP_
