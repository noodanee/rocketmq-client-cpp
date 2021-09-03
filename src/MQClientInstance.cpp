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
#include "MQClientInstance.h"

#include <cstddef>

#include <memory>
#include <string>
#include <utility>  // std::move

#include "ClientRemotingProcessor.h"
#include "MQAdminImpl.h"
#include "MQClientAPIImpl.h"
#include "MQClientManager.h"
#include "common/FindBrokerResult.hpp"
#include "common/MQVersion.h"
#include "common/PermName.h"
#include "common/UtilAll.h"
#include "consumer/PullMessageService.hpp"
#include "consumer/RebalancePushImpl.h"
#include "consumer/RebalanceService.h"
#include "logging/Logging.hpp"
#include "protocol/body/ConsumerRunningInfo.hpp"
#include "protocol/body/HeartbeatData.hpp"
#include "route/TopicPublishInfo.hpp"
#include "route/TopicRouteManager.hpp"
#include "route/TopicSubscribeInfo.hpp"
#include "transport/TcpRemotingClient.h"
#include "utility/MakeUnique.hpp"
#include "utility/MapAccessor.hpp"
#include "utility/SetAccessor.hpp"

namespace {

constexpr long kLockTimeoutMillis = 3000L;

}

namespace rocketmq {

MQClientInstance::MQClientInstance(const MQClientConfig& client_config, std::string client_id)
    : MQClientInstance(client_config, std::move(client_id), nullptr) {}

MQClientInstance::MQClientInstance(const MQClientConfig& client_config, std::string client_id, RPCHookPtr rpc_hook)
    : client_id_(std::move(client_id)),
      rebalance_service_(MakeUnique<RebalanceService>(this)),
      pull_message_service_(MakeUnique<PullMessageService>(this)),
      scheduled_executor_service_("MQClient", false) {
  // default Topic register
  topic_route_manager_->PutTopicPublishInfo(AUTO_CREATE_TOPIC_KEY_TOPIC, std::make_shared<TopicPublishInfo>());

  client_remoting_processor_ = MakeUnique<ClientRemotingProcessor>(this);
  mq_client_api_impl_ =
      MakeUnique<MQClientAPIImpl>(client_remoting_processor_.get(), std::move(rpc_hook), client_config);

  std::string namesrv_addresses = client_config.namesrv_addr();
  if (!namesrv_addresses.empty()) {
    mq_client_api_impl_->UpdateNameServerAddressList(namesrv_addresses);
    LOG_INFO_NEW("user specified name server address: {}", namesrv_addresses);
  }

  mq_admin_impl_ = MakeUnique<MQAdminImpl>(this);

  service_state_ = ServiceState::kCreateJust;
  LOG_DEBUG_NEW("MQClientInstance construct");
}

MQClientInstance::~MQClientInstance() {
  LOG_INFO_NEW("MQClientInstance:{} destruct", client_id_);
}

std::string MQClientInstance::GetNamesrvAddress() const {
  auto namesrv_addresses = mq_client_api_impl_->GetNameServerAddressList();
  std::ostringstream oss;
  for (const auto& address : namesrv_addresses) {
    oss << address << ";";
  }
  return oss.str();
}

void MQClientInstance::Start() {
  switch (service_state_) {
    case ServiceState::kCreateJust:
      LOG_INFO_NEW("the client instance [{}] is starting", client_id_);
      service_state_ = ServiceState::kStartFailed;

      mq_client_api_impl_->Start();

      // start various schedule tasks
      StartScheduledTask();

      // start pull service
      pull_message_service_->start();

      // start rebalance service
      rebalance_service_->start();

      LOG_INFO_NEW("the client instance [{}] start OK", client_id_);
      service_state_ = ServiceState::kRunning;
      break;
    case ServiceState::kRunning:
      LOG_INFO_NEW("the client instance [{}] already running.", client_id_, service_state_);
      break;
    case ServiceState::kShutdownAlready:
    case ServiceState::kStartFailed:
      LOG_INFO_NEW("the client instance [{}] start failed with fault state:{}", client_id_, service_state_);
      break;
    default:
      break;
  }
}

void MQClientInstance::Shutdown() {
  if (MapAccessor::Size(consumer_table_, consumer_table_mutex_) > 0) {
    return;
  }

  if (MapAccessor::Size(producer_table_, producer_table_mutex_) > 0) {
    return;
  }

  switch (service_state_) {
    case ServiceState::kCreateJust:
      break;
    case ServiceState::kRunning: {
      service_state_ = ServiceState::kShutdownAlready;
      pull_message_service_->shutdown();
      scheduled_executor_service_.shutdown();
      mq_client_api_impl_->Shutdown();
      rebalance_service_->shutdown();

      MQClientManager::getInstance()->removeMQClientInstance(client_id_);
      LOG_INFO_NEW("the client instance [{}] shutdown OK", client_id_);
    } break;
    case ServiceState::kShutdownAlready:
    default:
      break;
  }
}

void MQClientInstance::StartScheduledTask() {
  LOG_INFO_NEW("start scheduled task:{}", client_id_);
  scheduled_executor_service_.startup();

  // updateTopicRouteInfoFromNameServer
  scheduled_executor_service_.schedule([this]() { UpdateTopicRouteInfoPeriodically(); }, 10, time_unit::milliseconds);

  // sendHeartbeatToAllBroker
  scheduled_executor_service_.schedule([this]() { SendHeartbeatToAllBrokerPeriodically(); }, 1000,
                                       time_unit::milliseconds);

  // persistAllConsumerOffset
  scheduled_executor_service_.schedule([this]() { PersistAllConsumerOffsetPeriodically(); }, 1000 * 10,
                                       time_unit::milliseconds);
}

bool MQClientInstance::RegisterConsumer(const std::string& group, MQConsumerInner* consumer) {
  if (group.empty()) {
    return false;
  }

  if (!MapAccessor::Insert(consumer_table_, group, consumer, consumer_table_mutex_)) {
    LOG_WARN_NEW("the consumer group[{}] exist already.", group);
    return false;
  }

  LOG_DEBUG_NEW("registerConsumer success:{}", group);
  return true;
}

bool MQClientInstance::RegisterProducer(const std::string& group, MQProducerInner* producer) {
  if (group.empty()) {
    return false;
  }

  if (!MapAccessor::Insert(producer_table_, group, producer, producer_table_mutex_)) {
    LOG_WARN_NEW("the consumer group[{}] exist already.", group);
    return false;
  }

  LOG_DEBUG_NEW("registerProducer success:{}", group);
  return true;
}

void MQClientInstance::UnregisterConsumer(const std::string& group) {
  MapAccessor::Erase(consumer_table_, group, consumer_table_mutex_);
  UnregisterClientWithLock(null, group);
}

void MQClientInstance::UnregisterProducer(const std::string& group) {
  MapAccessor::Erase(producer_table_, group, producer_table_mutex_);
  UnregisterClientWithLock(group, null);
}

void MQClientInstance::UnregisterClientWithLock(const std::string& producer_group, const std::string& consumer_group) {
  if (UtilAll::try_lock_for(lock_heartbeat_, kLockTimeoutMillis)) {
    std::lock_guard<std::timed_mutex> lock(lock_heartbeat_, std::adopt_lock);

    try {
      UnregisterClient(producer_group, consumer_group);
    } catch (const std::exception& e) {
      LOG_ERROR_NEW("unregisterClient exception: {}", e.what());
    }
  } else {
    LOG_WARN_NEW("lock heartBeat, but failed.");
  }
}

void MQClientInstance::UnregisterClient(const std::string& producer_group, const std::string& consumer_group) {
  auto broker_address_table = MapAccessor::Clone(broker_address_table_, broker_address_table_mutex_);
  for (const auto& it : broker_address_table) {
    const auto& broker_name = it.first;
    const auto& address_table = it.second;
    for (const auto& it2 : address_table) {
      const auto& broker_id = it2.first;
      const auto& address = it2.second;
      try {
        mq_client_api_impl_->UnregisterClient(address, client_id_, producer_group, consumer_group);
        LOG_INFO_NEW("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producer_group,
                     consumer_group, broker_name, broker_id, address);
      } catch (const std::exception& e) {
        LOG_ERROR_NEW("unregister client exception from broker: {}. EXCEPTION: {}", address, e.what());
      }
    }
  }
}

MQConsumerInner* MQClientInstance::SelectConsumer(const std::string& consumer_group) {
  return MapAccessor::GetOrDefault(consumer_table_, consumer_group, nullptr, consumer_table_mutex_);
}

MQProducerInner* MQClientInstance::SelectProducer(const std::string& producer_group) {
  return MapAccessor::GetOrDefault(producer_table_, producer_group, nullptr, producer_table_mutex_);
}

void MQClientInstance::UpdateTopicRouteInfoPeriodically() {
  UpdateTopicRouteInfoFromNameServer();

  // next round
  scheduled_executor_service_.schedule([this]() { UpdateTopicRouteInfoPeriodically(); }, 1000 * 30,
                                       time_unit::milliseconds);
}

void MQClientInstance::UpdateTopicRouteInfoFromNameServer() {
  std::set<std::string> topicList;

  // Consumer
  GetTopicListFromConsumerSubscription(topicList);

  // Producer
  SetAccessor::Merge(topicList, topic_route_manager_->TopicInPublish());

  // update
  if (!topicList.empty()) {
    for (const auto& topic : topicList) {
      UpdateTopicRouteInfoFromNameServer(topic, false);
    }
  }
}

void MQClientInstance::GetTopicListFromConsumerSubscription(std::set<std::string>& topic_list) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  for (const auto& it : consumer_table_) {
    std::vector<SubscriptionData> result = it.second->subscriptions();
    for (const auto& sd : result) {
      topic_list.insert(sd.topic);
    }
  }
}

namespace {

bool IsTopicRouteDataChanged(TopicRouteData* old_data, TopicRouteData* now_data) {
  return old_data == nullptr || now_data == nullptr || !(*old_data == *now_data);
}

}  // namespace

bool MQClientInstance::UpdateTopicRouteInfoFromNameServer(const std::string& topic, bool is_default) {
  if (!UtilAll::try_lock_for(lock_namesrv_, kLockTimeoutMillis)) {
    LOG_WARN_NEW("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", kLockTimeoutMillis);
    return false;
  }

  std::lock_guard<std::timed_mutex> lock(lock_namesrv_, std::adopt_lock);
  LOG_DEBUG_NEW("updateTopicRouteInfoFromNameServer start:{}", topic);

  try {
    auto topic_route_data = [this, &topic, is_default]() -> TopicRouteDataPtr {
      if (!is_default) {
        return mq_client_api_impl_->GetTopicRouteInfoFromNameServer(topic, 1000 * 3);
      }

      auto topic_route_data =
          mq_client_api_impl_->GetTopicRouteInfoFromNameServer(AUTO_CREATE_TOPIC_KEY_TOPIC, 1000 * 3);
      if (topic_route_data != nullptr) {
        auto& queue_datas = topic_route_data->queue_datas;
        for (auto& queue_data : queue_datas) {
          int queue_nums = std::min(4, queue_data.read_queue_nums);
          queue_data.read_queue_nums = queue_nums;
          queue_data.write_queue_nums = queue_nums;
        }
      } else {
        LOG_DEBUG_NEW("getTopicRouteInfoFromNameServer is null for topic: {}", topic);
      }
      return topic_route_data;
    }();

    if (!topic_route_data) {
      LOG_WARN_NEW("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
      return false;
    }

    LOG_INFO_NEW("updateTopicRouteInfoFromNameServer has data");
    auto old = topic_route_manager_->GetTopicRouteData(topic);
    if (IsTopicRouteDataChanged(old.get(), topic_route_data.get())) {
      LOG_INFO_NEW("updateTopicRouteInfoFromNameServer changed:{}", topic);

      // update broker addr
      const auto& broker_datas = topic_route_data->broker_datas;
      for (const auto& broker_data : broker_datas) {
        LOG_INFO_NEW("updateTopicRouteInfoFromNameServer changed with broker name:{}", broker_data.broker_name);
        MapAccessor::InsertOrAssign(broker_address_table_, broker_data.broker_name, broker_data.broker_addrs,
                                    broker_address_table_mutex_);
      }

      // update publish info
      topic_route_manager_->PutTopicPublishInfo(topic, std::make_shared<TopicPublishInfo>(topic, topic_route_data));

      // update subscribe info
      auto subscribe_info = std::make_shared<TopicSubscribeInfo>(topic, topic_route_data);
      const auto& message_queues = subscribe_info->message_queues();
      {
        std::lock_guard<std::mutex> lock(consumer_table_mutex_);
        if (!consumer_table_.empty()) {
          for (auto& it : consumer_table_) {
            it.second->updateTopicSubscribeInfo(topic, message_queues);
          }
        }
      }

      topic_route_manager_->PutTopicRouteData(topic, std::move(topic_route_data));
    }

    LOG_DEBUG_NEW("updateTopicRouteInfoFromNameServer end:{}", topic);
    return true;
  } catch (const std::exception& e) {
    if (!UtilAll::isRetryTopic(topic) && topic != AUTO_CREATE_TOPIC_KEY_TOPIC) {
      LOG_WARN_NEW("updateTopicRouteInfoFromNameServer Exception, {}", e.what());
    }
  }

  return false;
}

TopicRouteDataPtr MQClientInstance::GetTopicRouteData(const std::string& topic) {
  return topic_route_manager_->GetTopicRouteData(topic);
}

std::string MQClientInstance::FindBrokerAddressInPublish(const std::string& broker_name) {
  std::lock_guard<std::mutex> lock(broker_address_table_mutex_);
  const auto& it = broker_address_table_.find(broker_name);
  if (it != broker_address_table_.end()) {
    const auto& broker_map = it->second;
    const auto& it1 = broker_map.find(MASTER_ID);
    if (it1 != broker_map.end()) {
      return it1->second;
    }
  }
  return std::string();
}

FindBrokerResult MQClientInstance::FindBrokerAddressInSubscribe(const std::string& broker_name,
                                                                int broker_id,
                                                                bool only_this_broker) {
  std::lock_guard<std::mutex> lock(broker_address_table_mutex_);
  const auto& it = broker_address_table_.find(broker_name);
  if (it != broker_address_table_.end()) {
    const auto& broker_map = it->second;
    const auto& it1 = broker_map.find(broker_id);
    if (it1 != broker_map.end()) {
      return {it1->second, it1->first != MASTER_ID};
    }
    if (!only_this_broker) {  // not only from master
      const auto& it2 = broker_map.begin();
      if (it2 != broker_map.end()) {
        return {it2->second, it2->first != MASTER_ID};
      }
    }
  }
  return {};
}

FindBrokerResult MQClientInstance::FindBrokerAddressInAdmin(const std::string& broker_name) {
  std::lock_guard<std::mutex> lock(broker_address_table_mutex_);
  const auto& it = broker_address_table_.find(broker_name);
  if (it != broker_address_table_.end()) {
    const auto& broker_map = it->second;
    const auto& it1 = broker_map.begin();
    if (it1 != broker_map.end()) {
      return {it1->second, it1->first != MASTER_ID};
    }
  }
  return {};
}

std::string MQClientInstance::FindBrokerAddressInPublish(const MessageQueue& message_queue) {
  auto broker_address = FindBrokerAddressInPublish(message_queue.broker_name());
  if (broker_address.empty()) {
    UpdateTopicRouteInfoFromNameServer(message_queue.topic());
    broker_address = FindBrokerAddressInPublish(message_queue.broker_name());
  }
  return broker_address;
}

FindBrokerResult MQClientInstance::FindBrokerAddressInSubscribe(const MessageQueue& message_queue,
                                                                int broker_id,
                                                                bool only_this_broker) {
  auto find_broker_result = FindBrokerAddressInSubscribe(message_queue.broker_name(), broker_id, only_this_broker);
  if (!find_broker_result) {
    UpdateTopicRouteInfoFromNameServer(message_queue.topic());
    find_broker_result = FindBrokerAddressInSubscribe(message_queue.broker_name(), broker_id, only_this_broker);
  }
  return find_broker_result;
}

FindBrokerResult MQClientInstance::FindBrokerAddressInAdmin(const MessageQueue& message_queue) {
  auto find_broker_result = FindBrokerAddressInAdmin(message_queue.broker_name());
  if (!find_broker_result) {
    UpdateTopicRouteInfoFromNameServer(message_queue.topic());
    find_broker_result = FindBrokerAddressInAdmin(message_queue.broker_name());
  }
  return find_broker_result;
}

namespace {

/**
 * @brief Selects a (preferably master) broker address from the registered list.
 *
 * @note If the master's address cannot be found, a slave broker address is selected in a random manner.
 *
 * @return Broker address.
 */
std::string SelectBrokerAddr(const TopicRouteData& topic_route_data) {
  auto broker_data_size = topic_route_data.broker_datas.size();
  if (broker_data_size > 0) {
    auto broker_data_index = std::rand() % broker_data_size;
    const auto& broker_data = topic_route_data.broker_datas[broker_data_index];
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
  return std::string();
}

}  // namespace

std::string MQClientInstance::FindBrokerAddrByTopic(const std::string& topic) {
  auto topic_route_data = topic_route_manager_->GetTopicRouteData(topic);
  if (topic_route_data != nullptr) {
    return SelectBrokerAddr(*topic_route_data);
  }
  return std::string();
}

TopicPublishInfoPtr MQClientInstance::FindTopicPublishInfo(const std::string& topic) {
  auto topic_publish_info = topic_route_manager_->GetTopicPublishInfo(topic);
  if (!topic_publish_info) {
    UpdateTopicRouteInfoFromNameServer(topic);
    topic_publish_info = topic_route_manager_->GetTopicPublishInfo(topic);
  }

  if (topic_publish_info && topic_publish_info->ok()) {
    return topic_publish_info;
  }

  LOG_INFO_NEW("updateTopicRouteInfoFromNameServer with default");
  UpdateTopicRouteInfoFromNameServer(topic, true);
  return topic_route_manager_->GetTopicPublishInfo(topic);
}

void MQClientInstance::SendHeartbeatToAllBrokerPeriodically() {
  CleanOfflineBroker();
  SendHeartbeatToAllBrokerWithLock();

  // next round
  scheduled_executor_service_.schedule([this]() { SendHeartbeatToAllBrokerPeriodically(); }, 1000 * 30,
                                       time_unit::milliseconds);
}

void MQClientInstance::CleanOfflineBroker() {
  if (!UtilAll::try_lock_for(lock_namesrv_, kLockTimeoutMillis)) {
    LOG_WARN_NEW("lock namesrv, but failed.");
    return;
  }

  std::lock_guard<std::timed_mutex> lock(lock_namesrv_, std::adopt_lock);

  std::set<std::string> offline_brokers;

  auto updated_table = MapAccessor::Clone(broker_address_table_, broker_address_table_mutex_);
  for (auto it = updated_table.begin(); it != updated_table.end();) {
    const auto& broker_name = it->first;
    auto& clone_address_table = it->second;

    for (auto it2 = clone_address_table.begin(); it2 != clone_address_table.end();) {
      const auto& address = it2->second;
      if (!topic_route_manager_->ContainsBrokerAddress(address)) {
        offline_brokers.insert(address);
        it2 = clone_address_table.erase(it2);
        LOG_INFO_NEW("the broker addr[{} {}] is offline, remove it", broker_name, address);
      } else {
        it2++;
      }
    }

    if (clone_address_table.empty()) {
      it = updated_table.erase(it);
      LOG_INFO_NEW("the broker[{}] name's host is offline, remove it", broker_name);
    } else {
      it++;
    }
  }

  if (!offline_brokers.empty()) {
    MapAccessor::Assign(broker_address_table_, std::move(updated_table), broker_address_table_mutex_);

    std::lock_guard<std::mutex> lock(topic_broker_addr_table_mutex_);
    for (auto it = topic_broker_addr_table_.begin(); it != topic_broker_addr_table_.end();) {
      if (offline_brokers.find(it->second.first) != offline_brokers.end()) {
        it = topic_broker_addr_table_.erase(it);
      } else {
        it++;
      }
    }
  }
}

void MQClientInstance::SendHeartbeatToAllBrokerWithLock() {
  if (lock_heartbeat_.try_lock()) {
    std::lock_guard<std::timed_mutex> lock(lock_heartbeat_, std::adopt_lock);
    SendHeartbeatToAllBroker();
  } else {
    LOG_WARN_NEW("lock heartBeat, but failed.");
  }
}

void MQClientInstance::SendHeartbeatToAllBroker() {
  auto heartbeat_data = PrepareHeartbeatData();

  bool producer_empty = heartbeat_data.producer_data_set.empty();
  bool consumer_empty = heartbeat_data.consumer_data_set.empty();
  if (producer_empty && consumer_empty) {
    LOG_WARN_NEW("sending heartbeat, but no consumer and no producer");
    return;
  }

  auto broker_address_table = MapAccessor::Clone(broker_address_table_, broker_address_table_mutex_);
  if (broker_address_table.empty()) {
    LOG_WARN_NEW("sendheartbeat brokerAddrTable is empty");
    return;
  }

  for (const auto& it : broker_address_table) {
    // const auto& broker_name = it.first;
    const auto& address_table = it.second;
    for (const auto& it2 : address_table) {
      const auto broker_id = it2.first;
      const auto& address = it2.second;
      if (consumer_empty && broker_id != MASTER_ID) {
        continue;
      }

      try {
        mq_client_api_impl_->SendHearbeat(address, heartbeat_data, 3000);
      } catch (const MQException& e) {
        LOG_ERROR_NEW("{}", e.what());
      }
    }
  }
}

HeartbeatData MQClientInstance::PrepareHeartbeatData() {
  HeartbeatData heartbeat_data;

  // clientID
  heartbeat_data.client_id = client_id_;

  // Consumer
  {
    std::lock_guard<std::mutex> lock(consumer_table_mutex_);
    for (const auto& it : consumer_table_) {
      const auto* consumer = it.second;
      // TODO: unitMode
      heartbeat_data.consumer_data_set.emplace_back(consumer->groupName(), consumer->consumeType(),
                                                    consumer->messageModel(), consumer->consumeFromWhere(),
                                                    consumer->subscriptions());
    }
  }

  // Producer
  {
    std::lock_guard<std::mutex> lock(producer_table_mutex_);
    for (const auto& it : producer_table_) {
      heartbeat_data.producer_data_set.emplace_back(it.first);
    }
  }

  return heartbeat_data;
}

void MQClientInstance::PersistAllConsumerOffsetPeriodically() {
  PersistAllConsumerOffset();

  // next round
  scheduled_executor_service_.schedule([this]() { PersistAllConsumerOffsetPeriodically(); }, 1000 * 5,
                                       time_unit::milliseconds);
}

void MQClientInstance::PersistAllConsumerOffset() {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  for (const auto& it : consumer_table_) {
    LOG_DEBUG_NEW("the client instance [{}] start persistAllConsumerOffset", client_id_);
    it.second->persistConsumerOffset();
  }
}

void MQClientInstance::RebalanceImmediately() {
  rebalance_service_->wakeup();
}

void MQClientInstance::DoRebalance() {
  LOG_INFO_NEW("the client instance:{} start doRebalance", client_id_);
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  if (!consumer_table_.empty()) {
    for (auto& it : consumer_table_) {
      it.second->doRebalance();
    }
  }
  LOG_INFO_NEW("the client instance [{}] finish doRebalance", client_id_);
}

void MQClientInstance::DoRebalanceByConsumerGroup(const std::string& consumer_group) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  auto it = consumer_table_.find(consumer_group);
  if (it != consumer_table_.end()) {
    try {
      LOG_INFO_NEW("the client instance [{}] start doRebalance for consumer [{}]", client_id_, consumer_group);
      auto* consumer = it->second;
      consumer->doRebalance();
    } catch (const std::exception& e) {
      LOG_ERROR_NEW("{}", e.what());
    }
  }
}

std::vector<std::string> MQClientInstance::FindConsumerIds(const std::string& topic, const std::string& group) {
  std::string broker_addr = [this, &topic]() {
    // find consumerIds from same broker every 40s
    std::lock_guard<std::mutex> lock(topic_broker_addr_table_mutex_);
    const auto& it = topic_broker_addr_table_.find(topic);
    if (it != topic_broker_addr_table_.end()) {
      if (UtilAll::currentTimeMillis() < it->second.second + 120 * 1000) {
        return it->second.first;
      }
    }
    return std::string();
  }();

  if (broker_addr.empty()) {
    // select new one
    broker_addr = FindBrokerAddrByTopic(topic);
    if (broker_addr.empty()) {
      UpdateTopicRouteInfoFromNameServer(topic);
      broker_addr = FindBrokerAddrByTopic(topic);
    }

    if (!broker_addr.empty()) {
      MapAccessor::InsertOrAssign(topic_broker_addr_table_, topic,
                                  std::make_pair(broker_addr, UtilAll::currentTimeMillis()),
                                  topic_broker_addr_table_mutex_);
    }
  }

  if (!broker_addr.empty()) {
    try {
      LOG_INFO_NEW("getConsumerIdList from broker:{}", broker_addr);
      return mq_client_api_impl_->GetConsumerIdListByGroup(broker_addr, group, 5000);
    } catch (const MQException& e) {
      LOG_ERROR_NEW("encounter exception when getConsumerIdList: {}", e.what());
      MapAccessor::Erase(topic_broker_addr_table_, topic, topic_broker_addr_table_mutex_);
    }
  }

  return {};
}

void MQClientInstance::ResetOffset(const std::string& group,
                                   const std::string& topic,
                                   const std::map<MessageQueue, int64_t>& offset_table) {
  DefaultMQPushConsumerImpl* consumer = nullptr;
  try {
    consumer = dynamic_cast<DefaultMQPushConsumerImpl*>(SelectConsumer(group));
    if (consumer == nullptr) {
      LOG_INFO_NEW("[reset-offset] consumer dose not exist. group={}", group);
      return;
    }

    consumer->Suspend();

    auto process_queue_table = consumer->rebalance_impl()->getProcessQueueTable();
    for (const auto& it : process_queue_table) {
      const auto& mq = it.first;
      if (topic == mq.topic() && offset_table.find(mq) != offset_table.end()) {
        auto pq = it.second;
        pq->set_dropped(true);
        pq->ClearAllMessages();
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));

    for (const auto& it : process_queue_table) {
      const auto& message_queue = it.first;
      const auto& it2 = offset_table.find(message_queue);
      if (it2 != offset_table.end()) {
        auto offset = it2->second;
        consumer->UpdateConsumeOffset(message_queue, offset);
        consumer->rebalance_impl()->removeUnnecessaryMessageQueue(message_queue, it.second);
        consumer->rebalance_impl()->removeProcessQueueDirectly(message_queue);
      }
    }
  } catch (...) {
    if (consumer != nullptr) {
      consumer->Resume();
    }
    throw;
  }
  if (consumer != nullptr) {
    consumer->Resume();
  }
}

std::unique_ptr<ConsumerRunningInfo> MQClientInstance::ConsumerRunningInfo(const std::string& consumer_group) {
  auto* consumer = SelectConsumer(consumer_group);
  if (consumer != nullptr) {
    auto running_info = consumer->consumerRunningInfo();
    if (running_info != nullptr) {
      std::string namesrv_address = GetNamesrvAddress();
      running_info->properties.emplace(ConsumerRunningInfo::PROP_NAMESERVER_ADDR, namesrv_address);

      if (consumer->consumeType() == CONSUME_PASSIVELY) {
        running_info->properties.emplace(ConsumerRunningInfo::PROP_CONSUME_TYPE, "CONSUME_PASSIVELY");
      } else {
        running_info->properties.emplace(ConsumerRunningInfo::PROP_CONSUME_TYPE, "CONSUME_ACTIVELY");
      }

      running_info->properties.emplace(ConsumerRunningInfo::PROP_CLIENT_VERSION,
                                       MQVersion::GetVersionDesc(MQVersion::CURRENT_VERSION));

      return running_info;
    }
  }

  LOG_ERROR_NEW("no corresponding consumer found for group:{}", consumer_group);
  return nullptr;
}

}  // namespace rocketmq
