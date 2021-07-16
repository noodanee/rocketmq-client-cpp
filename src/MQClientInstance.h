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
#ifndef ROCKETMQ_MQCLIENTINSTANCE_H_
#define ROCKETMQ_MQCLIENTINSTANCE_H_

#include <cstddef>  // size_t
#include <cstdint>  // int64_t

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>

#include "ServiceState.h"
#include "concurrent/executor.hpp"

namespace rocketmq {

class RPCHook;
using RPCHookPtr = std::shared_ptr<RPCHook>;

class MQClientConfig;
class MQClientAPIImpl;
class MQAdminImpl;
class MQConsumerInner;
class MQProducerInner;
class ClientRemotingProcessor;
class RebalanceService;
class PullMessageService;
class MessageQueue;

struct ConsumerRunningInfo;
struct FindBrokerResult;
struct HeartbeatData;

struct TopicRouteData;
using TopicRouteDataPtr = std::shared_ptr<TopicRouteData>;

class TopicPublishInfo;
using TopicPublishInfoPtr = std::shared_ptr<const TopicPublishInfo>;

class MQClientInstance;
using MQClientInstancePtr = std::shared_ptr<MQClientInstance>;

class MQClientInstance {
 public:
  MQClientInstance(const MQClientConfig& client_config, std::string client_id);
  MQClientInstance(const MQClientConfig& client_config, std::string client_id, RPCHookPtr rpc_hook);
  ~MQClientInstance();

  // disable copy
  MQClientInstance(MQClientInstance&) = delete;
  MQClientInstance& operator=(MQClientInstance&) = delete;

  // disable move
  MQClientInstance(MQClientInstance&&) = delete;
  MQClientInstance& operator=(MQClientInstance&&) = delete;

  const std::string& GetClientId() const { return client_id_; }
  bool Running() const { return service_state_ == ServiceState::kRunning; }

  std::string GetNamesrvAddress() const;

  void Start();
  void Shutdown();

  bool RegisterProducer(const std::string& group, MQProducerInner* producer);
  bool RegisterConsumer(const std::string& group, MQConsumerInner* consumer);

  void UnregisterProducer(const std::string& group);
  void UnregisterConsumer(const std::string& group);

  MQProducerInner* SelectProducer(const std::string& producer_group);
  MQConsumerInner* SelectConsumer(const std::string& consumer_group);

  void UpdateTopicRouteInfoFromNameServer();
  bool UpdateTopicRouteInfoFromNameServer(const std::string& topic, bool is_default = false);

  TopicRouteDataPtr GetTopicRouteData(const std::string& topic);

  std::string FindBrokerAddressInPublish(const std::string& broker_name);
  FindBrokerResult FindBrokerAddressInSubscribe(const std::string& broker_name, int broker_id, bool only_this_broker);
  FindBrokerResult FindBrokerAddressInAdmin(const std::string& broker_name);

  std::string FindBrokerAddressInPublish(const MessageQueue& message_queue);
  FindBrokerResult FindBrokerAddressInSubscribe(const MessageQueue& message_queue,
                                                int broker_id,
                                                bool only_this_broker);
  FindBrokerResult FindBrokerAddressInAdmin(const MessageQueue& message_queue);

  std::string FindBrokerAddrByTopic(const std::string& topic);

  TopicPublishInfoPtr FindTopicPublishInfo(const std::string& topic);

  void SendHeartbeatToAllBrokerWithLock();

  void RebalanceImmediately();
  void DoRebalance();

  std::vector<std::string> FindConsumerIds(const std::string& topic, const std::string& group);

  void ResetOffset(const std::string& group,
                   const std::string& topic,
                   const std::map<MessageQueue, int64_t>& offset_table);

  std::unique_ptr<ConsumerRunningInfo> ConsumerRunningInfo(const std::string& consumer_group);

 public:
  MQClientAPIImpl* GetMQClientAPIImpl() const { return mq_client_api_impl_.get(); }
  MQAdminImpl* GetMQAdminImpl() const { return mq_admin_impl_.get(); }
  PullMessageService* GetPullMessageService() const { return pull_message_service_.get(); }

 private:
  void UnregisterClientWithLock(const std::string& producer_group, const std::string& consumer_group);
  void UnregisterClient(const std::string& producer_group, const std::string& consumer_group);

  // scheduled task
  void StartScheduledTask();
  void UpdateTopicRouteInfoPeriodically();
  void SendHeartbeatToAllBrokerPeriodically();
  void PersistAllConsumerOffsetPeriodically();

  // route
  void GetTopicListFromConsumerSubscription(std::set<std::string>& topic_list);

  // heartbeat
  void CleanOfflineBroker();
  bool IsBrokerAddrExist(const std::string& addr);
  void SendHeartbeatToAllBroker();
  HeartbeatData PrepareHeartbeatData();

  // offset
  void PersistAllConsumerOffset();

  // rebalance
  void DoRebalanceByConsumerGroup(const std::string& consumer_group);

 private:
  std::string client_id_;
  volatile ServiceState service_state_;

  // group -> MQProducer
  std::map<std::string, MQProducerInner*> producer_table_;
  std::mutex producer_table_mutex_;

  // group -> MQConsumer
  std::map<std::string, MQConsumerInner*> consumer_table_;
  std::mutex consumer_table_mutex_;

  // Topic -> TopicRouteData
  std::map<std::string, TopicRouteDataPtr> topic_route_table_;
  std::mutex topic_route_table_mutex_;

  // broker_name -> { broker_id : address }
  std::map<std::string, std::map<int, std::string>> broker_address_table_;
  std::mutex broker_address_table_mutex_;

  // topic -> TopicPublishInfo
  std::map<std::string, TopicPublishInfoPtr> topic_publish_info_table_;
  std::mutex topic_publish_info_table_mutex_;

  // topic -> <broker, time>
  std::map<std::string, std::pair<std::string, uint64_t>> topic_broker_addr_table_;
  std::mutex topic_broker_addr_table_mutex_;

  std::timed_mutex lock_namesrv_;
  std::timed_mutex lock_heartbeat_;

  std::unique_ptr<MQClientAPIImpl> mq_client_api_impl_;
  std::unique_ptr<MQAdminImpl> mq_admin_impl_;
  std::unique_ptr<ClientRemotingProcessor> client_remoting_processor_;

  std::unique_ptr<RebalanceService> rebalance_service_;
  std::unique_ptr<PullMessageService> pull_message_service_;
  scheduled_thread_pool_executor scheduled_executor_service_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_MQCLIENTINSTANCE_H_
