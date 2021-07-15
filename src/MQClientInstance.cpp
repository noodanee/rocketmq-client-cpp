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

#include <typeindex>
#include <utility>  // std::move

#include "ClientRemotingProcessor.h"
#include "Logging.h"
#include "MQAdminImpl.h"
#include "MQClientAPIImpl.h"
#include "MQClientManager.h"
#include "MQVersion.h"
#include "PermName.h"
#include "PullMessageService.hpp"
#include "PullRequest.h"
#include "RebalancePushImpl.h"
#include "RebalanceService.h"
#include "TcpRemotingClient.h"
#include "UtilAll.h"
#include "producer/TopicPublishInfo.hpp"
#include "protocol/body/ConsumerRunningInfo.hpp"
#include "utility/MakeUnique.hpp"

namespace rocketmq {

static const long LOCK_TIMEOUT_MILLIS = 3000L;

MQClientInstance::MQClientInstance(const MQClientConfig& clientConfig, std::string clientId)
    : MQClientInstance(clientConfig, std::move(clientId), nullptr) {}

MQClientInstance::MQClientInstance(const MQClientConfig& clientConfig, std::string clientId, RPCHookPtr rpcHook)
    : client_id_(std::move(clientId)),
      rebalance_service_(MakeUnique<RebalanceService>(this)),
      pull_message_service_(MakeUnique<PullMessageService>(this)),
      scheduled_executor_service_("MQClient", false) {
  // default Topic register
  topic_publish_info_table_[AUTO_CREATE_TOPIC_KEY_TOPIC] = std::make_shared<TopicPublishInfo>();

  client_remoting_processor_ = MakeUnique<ClientRemotingProcessor>(this);
  mq_client_api_impl_ = MakeUnique<MQClientAPIImpl>(client_remoting_processor_.get(), std::move(rpcHook), clientConfig);

  std::string namesrvAddr = clientConfig.namesrv_addr();
  if (!namesrvAddr.empty()) {
    mq_client_api_impl_->UpdateNameServerAddressList(namesrvAddr);
    LOG_INFO_NEW("user specified name server address: {}", namesrvAddr);
  }

  mq_admin_impl_ = MakeUnique<MQAdminImpl>(this);

  service_state_ = ServiceState::kCreateJust;
  LOG_DEBUG_NEW("MQClientInstance construct");
}

MQClientInstance::~MQClientInstance() {
  LOG_INFO_NEW("MQClientInstance:{} destruct", client_id_);

  // UNNECESSARY:
  producer_table_.clear();
  consumer_table_.clear();
  topic_publish_info_table_.clear();
  topic_route_table_.clear();
  broker_addr_table_.clear();

  mq_client_api_impl_ = nullptr;
}

std::string MQClientInstance::getNamesrvAddr() const {
  auto namesrvAddrs = mq_client_api_impl_->GetNameServerAddressList();
  std::ostringstream oss;
  for (const auto& addr : namesrvAddrs) {
    oss << addr << ";";
  }
  return oss.str();
}

std::vector<MessageQueue> MQClientInstance::topicRouteData2TopicSubscribeInfo(const std::string& topic,
                                                                              TopicRouteDataPtr route) {
  std::vector<MessageQueue> mqList;
  const auto& queueDatas = route->queue_datas;
  for (const auto& qd : queueDatas) {
    if (PermName::isReadable(qd.perm)) {
      for (int i = 0; i < qd.read_queue_nums; i++) {
        MessageQueue mq(topic, qd.broker_name, i);
        mqList.push_back(mq);
      }
    }
  }
  return mqList;
}

void MQClientInstance::start() {
  switch (service_state_) {
    case ServiceState::kCreateJust:
      LOG_INFO_NEW("the client instance [{}] is starting", client_id_);
      service_state_ = ServiceState::kStartFailed;

      mq_client_api_impl_->Start();

      // start various schedule tasks
      startScheduledTask();

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

void MQClientInstance::shutdown() {
  if (getConsumerTableSize() != 0) {
    return;
  }

  if (getProducerTableSize() != 0) {
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
      break;
    default:
      break;
  }
}

bool MQClientInstance::isRunning() {
  return service_state_ == ServiceState::kRunning;
}

void MQClientInstance::startScheduledTask() {
  LOG_INFO_NEW("start scheduled task:{}", client_id_);
  scheduled_executor_service_.startup();

  // updateTopicRouteInfoFromNameServer
  scheduled_executor_service_.schedule([this]() { updateTopicRouteInfoPeriodically(); }, 10, time_unit::milliseconds);

  // sendHeartbeatToAllBroker
  scheduled_executor_service_.schedule([this]() { sendHeartbeatToAllBrokerPeriodically(); }, 1000,
                                       time_unit::milliseconds);

  // persistAllConsumerOffset
  scheduled_executor_service_.schedule([this]() { persistAllConsumerOffsetPeriodically(); }, 1000 * 10,
                                       time_unit::milliseconds);
}

void MQClientInstance::updateTopicRouteInfoPeriodically() {
  updateTopicRouteInfoFromNameServer();

  // next round
  scheduled_executor_service_.schedule([this]() { updateTopicRouteInfoPeriodically(); }, 1000 * 30,
                                       time_unit::milliseconds);
}

void MQClientInstance::sendHeartbeatToAllBrokerPeriodically() {
  cleanOfflineBroker();
  sendHeartbeatToAllBrokerWithLock();

  // next round
  scheduled_executor_service_.schedule([this]() { sendHeartbeatToAllBrokerPeriodically(); }, 1000 * 30,
                                       time_unit::milliseconds);
}

void MQClientInstance::persistAllConsumerOffsetPeriodically() {
  persistAllConsumerOffset();

  // next round
  scheduled_executor_service_.schedule([this]() { persistAllConsumerOffsetPeriodically(); }, 1000 * 5,
                                       time_unit::milliseconds);
}

const std::string& MQClientInstance::getClientId() const {
  return client_id_;
}

void MQClientInstance::updateTopicRouteInfoFromNameServer() {
  std::set<std::string> topicList;

  // Consumer
  getTopicListFromConsumerSubscription(topicList);

  // Producer
  getTopicListFromTopicPublishInfo(topicList);

  // update
  if (!topicList.empty()) {
    for (const auto& topic : topicList) {
      updateTopicRouteInfoFromNameServer(topic);
    }
  }
}

void MQClientInstance::cleanOfflineBroker() {
  if (UtilAll::try_lock_for(lock_namesrv_, LOCK_TIMEOUT_MILLIS)) {
    std::lock_guard<std::timed_mutex> lock(lock_namesrv_, std::adopt_lock);

    std::set<std::string> offlineBrokers;
    BrokerAddrMAP updatedTable(getBrokerAddrTable());
    for (auto itBrokerTable = updatedTable.begin(); itBrokerTable != updatedTable.end();) {
      const auto& brokerName = itBrokerTable->first;
      auto& cloneAddrTable = itBrokerTable->second;

      for (auto it = cloneAddrTable.begin(); it != cloneAddrTable.end();) {
        const auto& addr = it->second;
        if (!isBrokerAddrExistInTopicRouteTable(addr)) {
          offlineBrokers.insert(addr);
          it = cloneAddrTable.erase(it);
          LOG_INFO_NEW("the broker addr[{} {}] is offline, remove it", brokerName, addr);
        } else {
          it++;
        }
      }

      if (cloneAddrTable.empty()) {
        itBrokerTable = updatedTable.erase(itBrokerTable);
        LOG_INFO_NEW("the broker[{}] name's host is offline, remove it", brokerName);
      } else {
        itBrokerTable++;
      }
    }

    if (!offlineBrokers.empty()) {
      resetBrokerAddrTable(std::move(updatedTable));

      std::lock_guard<std::mutex> lock(topic_broker_addr_table_mutex_);
      for (auto it = topic_broker_addr_table_.begin(); it != topic_broker_addr_table_.end();) {
        if (offlineBrokers.find(it->second.first) != offlineBrokers.end()) {
          it = topic_broker_addr_table_.erase(it);
        } else {
          it++;
        }
      }
    }
  } else {
    LOG_WARN_NEW("lock namesrv, but failed.");
  }
}

bool MQClientInstance::isBrokerAddrExistInTopicRouteTable(const std::string& addr) {
  std::lock_guard<std::mutex> lock(topic_route_table_mutex_);
  for (const auto& it : topic_route_table_) {
    const auto topicRouteData = it.second;
    const auto& bds = topicRouteData->broker_datas;
    for (const auto& bd : bds) {
      for (const auto& itAddr : bd.broker_addrs) {
        if (itAddr.second == addr) {
          return true;
        }
      }
    }
  }
  return false;
}

void MQClientInstance::sendHeartbeatToAllBrokerWithLock() {
  if (lock_heartbeat_.try_lock()) {
    std::lock_guard<std::timed_mutex> lock(lock_heartbeat_, std::adopt_lock);
    sendHeartbeatToAllBroker();
  } else {
    LOG_WARN_NEW("lock heartBeat, but failed.");
  }
}

void MQClientInstance::persistAllConsumerOffset() {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  for (const auto& it : consumer_table_) {
    LOG_DEBUG_NEW("the client instance [{}] start persistAllConsumerOffset", client_id_);
    it.second->persistConsumerOffset();
  }
}

void MQClientInstance::sendHeartbeatToAllBroker() {
  std::unique_ptr<HeartbeatData> heartbeatData(prepareHeartbeatData());
  bool producerEmpty = heartbeatData->producer_data_set.empty();
  bool consumerEmpty = heartbeatData->consumer_data_set.empty();
  if (producerEmpty && consumerEmpty) {
    LOG_WARN_NEW("sending heartbeat, but no consumer and no producer");
    return;
  }

  auto brokerAddrTable = getBrokerAddrTable();
  if (!brokerAddrTable.empty()) {
    for (const auto& it : brokerAddrTable) {
      // const auto& brokerName = it.first;
      const auto& oneTable = it.second;
      for (const auto& it2 : oneTable) {
        const auto id = it2.first;
        const auto& addr = it2.second;
        if (consumerEmpty && id != MASTER_ID) {
          continue;
        }

        try {
          mq_client_api_impl_->SendHearbeat(addr, *heartbeatData, 3000);
        } catch (const MQException& e) {
          LOG_ERROR_NEW("{}", e.what());
        }
      }
    }
    brokerAddrTable.clear();
  } else {
    LOG_WARN_NEW("sendheartbeat brokerAddrTable is empty");
  }
}

bool MQClientInstance::updateTopicRouteInfoFromNameServer(const std::string& topic, bool isDefault) {
  if (UtilAll::try_lock_for(lock_namesrv_, LOCK_TIMEOUT_MILLIS)) {
    std::lock_guard<std::timed_mutex> lock(lock_namesrv_, std::adopt_lock);
    LOG_DEBUG_NEW("updateTopicRouteInfoFromNameServer start:{}", topic);

    try {
      TopicRouteDataPtr topicRouteData;
      if (isDefault) {
        topicRouteData = mq_client_api_impl_->GetTopicRouteInfoFromNameServer(AUTO_CREATE_TOPIC_KEY_TOPIC, 1000 * 3);
        if (topicRouteData != nullptr) {
          auto& queueDatas = topicRouteData->queue_datas;
          for (auto& qd : queueDatas) {
            int queueNums = std::min(4, qd.read_queue_nums);
            qd.read_queue_nums = queueNums;
            qd.write_queue_nums = queueNums;
          }
        }
        LOG_DEBUG_NEW("getTopicRouteInfoFromNameServer is null for topic: {}", topic);
      } else {
        topicRouteData = mq_client_api_impl_->GetTopicRouteInfoFromNameServer(topic, 1000 * 3);
      }
      if (topicRouteData != nullptr) {
        LOG_INFO_NEW("updateTopicRouteInfoFromNameServer has data");
        auto old = getTopicRouteData(topic);
        bool changed = topicRouteDataIsChange(old.get(), topicRouteData.get());

        if (changed) {
          LOG_INFO_NEW("updateTopicRouteInfoFromNameServer changed:{}", topic);

          // update broker addr
          const auto& brokerDatas = topicRouteData->broker_datas;
          for (const auto& bd : brokerDatas) {
            LOG_INFO_NEW("updateTopicRouteInfoFromNameServer changed with broker name:{}", bd.broker_name);
            addBrokerToAddrTable(bd.broker_name, bd.broker_addrs);
          }

          // update publish info
          {
            auto publishInfo = std::make_shared<TopicPublishInfo>(topic, topicRouteData);
            updateProducerTopicPublishInfo(topic, std::move(publishInfo));
          }

          // update subscribe info
          if (getConsumerTableSize() > 0) {
            std::vector<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
            updateConsumerTopicSubscribeInfo(topic, subscribeInfo);
          }

          addTopicRouteData(topic, topicRouteData);
        }

        LOG_DEBUG_NEW("updateTopicRouteInfoFromNameServer end:{}", topic);
        return true;
      }

      LOG_WARN_NEW("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
    } catch (const std::exception& e) {
      if (!UtilAll::isRetryTopic(topic) && topic != AUTO_CREATE_TOPIC_KEY_TOPIC) {
        LOG_WARN_NEW("updateTopicRouteInfoFromNameServer Exception, {}", e.what());
      }
    }
  } else {
    LOG_WARN_NEW("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
  }

  return false;
}

std::unique_ptr<HeartbeatData> MQClientInstance::prepareHeartbeatData() {
  std::unique_ptr<HeartbeatData> heartbeat_data(new HeartbeatData());

  // clientID
  heartbeat_data->client_id = client_id_;

  // Consumer
  insertConsumerInfoToHeartBeatData(heartbeat_data.get());

  // Producer
  insertProducerInfoToHeartBeatData(heartbeat_data.get());

  return heartbeat_data;
}

void MQClientInstance::insertConsumerInfoToHeartBeatData(HeartbeatData* heartbeatData) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  for (const auto& it : consumer_table_) {
    const auto* consumer = it.second;
    // TODO: unitMode
    heartbeatData->consumer_data_set.emplace_back(consumer->groupName(), consumer->consumeType(),
                                                  consumer->messageModel(), consumer->consumeFromWhere(),
                                                  consumer->subscriptions());
  }
}

void MQClientInstance::insertProducerInfoToHeartBeatData(HeartbeatData* heartbeatData) {
  std::lock_guard<std::mutex> lock(producer_table_mutex_);
  for (const auto& it : producer_table_) {
    heartbeatData->producer_data_set.emplace_back(it.first);
  }
}

bool MQClientInstance::topicRouteDataIsChange(TopicRouteData* olddata, TopicRouteData* nowdata) {
  if (olddata == nullptr || nowdata == nullptr) {
    return true;
  }
  return !(*olddata == *nowdata);
}

TopicRouteDataPtr MQClientInstance::getTopicRouteData(const std::string& topic) {
  std::lock_guard<std::mutex> lock(topic_route_table_mutex_);
  const auto& it = topic_route_table_.find(topic);
  if (it != topic_route_table_.end()) {
    return it->second;
  }
  return nullptr;
}

void MQClientInstance::addTopicRouteData(const std::string& topic, TopicRouteDataPtr topicRouteData) {
  std::lock_guard<std::mutex> lock(topic_route_table_mutex_);
  topic_route_table_[topic] = std::move(topicRouteData);
}

bool MQClientInstance::registerConsumer(const std::string& group, MQConsumerInner* consumer) {
  if (group.empty()) {
    return false;
  }

  if (!addConsumerToTable(group, consumer)) {
    LOG_WARN_NEW("the consumer group[{}] exist already.", group);
    return false;
  }

  LOG_DEBUG_NEW("registerConsumer success:{}", group);
  return true;
}

void MQClientInstance::unregisterConsumer(const std::string& group) {
  eraseConsumerFromTable(group);
  unregisterClientWithLock(null, group);
}

void MQClientInstance::unregisterClientWithLock(const std::string& producerGroup, const std::string& consumerGroup) {
  if (UtilAll::try_lock_for(lock_heartbeat_, LOCK_TIMEOUT_MILLIS)) {
    std::lock_guard<std::timed_mutex> lock(lock_heartbeat_, std::adopt_lock);

    try {
      unregisterClient(producerGroup, consumerGroup);
    } catch (const std::exception& e) {
      LOG_ERROR_NEW("unregisterClient exception: {}", e.what());
    }
  } else {
    LOG_WARN_NEW("lock heartBeat, but failed.");
  }
}

void MQClientInstance::unregisterClient(const std::string& producerGroup, const std::string& consumerGroup) {
  BrokerAddrMAP brokerAddrTable(getBrokerAddrTable());
  for (const auto& it : brokerAddrTable) {
    const auto& brokerName = it.first;
    const auto& oneTable = it.second;
    for (const auto& it2 : oneTable) {
      const auto& index = it2.first;
      const auto& addr = it2.second;
      try {
        mq_client_api_impl_->UnregisterClient(addr, client_id_, producerGroup, consumerGroup);
        LOG_INFO_NEW("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup,
                     consumerGroup, brokerName, index, addr);
      } catch (const std::exception& e) {
        LOG_ERROR_NEW("unregister client exception from broker: {}. EXCEPTION: {}", addr, e.what());
      }
    }
  }
}

bool MQClientInstance::registerProducer(const std::string& group, MQProducerInner* producer) {
  if (group.empty()) {
    return false;
  }

  if (!addProducerToTable(group, producer)) {
    LOG_WARN_NEW("the consumer group[{}] exist already.", group);
    return false;
  }

  LOG_DEBUG_NEW("registerProducer success:{}", group);
  return true;
}

void MQClientInstance::unregisterProducer(const std::string& group) {
  eraseProducerFromTable(group);
  unregisterClientWithLock(group, null);
}

void MQClientInstance::rebalanceImmediately() {
  rebalance_service_->wakeup();
}

void MQClientInstance::doRebalance() {
  LOG_INFO_NEW("the client instance:{} start doRebalance", client_id_);
  if (getConsumerTableSize() > 0) {
    std::lock_guard<std::mutex> lock(consumer_table_mutex_);
    for (auto& it : consumer_table_) {
      it.second->doRebalance();
    }
  }
  LOG_INFO_NEW("the client instance [{}] finish doRebalance", client_id_);
}

void MQClientInstance::doRebalanceByConsumerGroup(const std::string& consumerGroup) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  const auto& it = consumer_table_.find(consumerGroup);
  if (it != consumer_table_.end()) {
    try {
      LOG_INFO_NEW("the client instance [{}] start doRebalance for consumer [{}]", client_id_, consumerGroup);
      auto* consumer = it->second;
      consumer->doRebalance();
    } catch (const std::exception& e) {
      LOG_ERROR_NEW("{}", e.what());
    }
  }
}

MQProducerInner* MQClientInstance::selectProducer(const std::string& producerName) {
  std::lock_guard<std::mutex> lock(producer_table_mutex_);
  const auto& it = producer_table_.find(producerName);
  if (it != producer_table_.end()) {
    return it->second;
  }
  return nullptr;
}

bool MQClientInstance::addProducerToTable(const std::string& producerName, MQProducerInner* producer) {
  std::lock_guard<std::mutex> lock(producer_table_mutex_);
  if (producer_table_.find(producerName) != producer_table_.end()) {
    return false;
  } else {
    producer_table_[producerName] = producer;
    return true;
  }
}

void MQClientInstance::eraseProducerFromTable(const std::string& producerName) {
  std::lock_guard<std::mutex> lock(producer_table_mutex_);
  const auto& it = producer_table_.find(producerName);
  if (it != producer_table_.end()) {
    producer_table_.erase(it);
  }
}

int MQClientInstance::getProducerTableSize() {
  std::lock_guard<std::mutex> lock(producer_table_mutex_);
  return producer_table_.size();
}

void MQClientInstance::getTopicListFromTopicPublishInfo(std::set<std::string>& topicList) {
  std::lock_guard<std::mutex> lock(topic_publish_info_table_mutex_);
  for (const auto& it : topic_publish_info_table_) {
    topicList.insert(it.first);
  }
}

void MQClientInstance::updateProducerTopicPublishInfo(const std::string& topic, TopicPublishInfoPtr publishInfo) {
  addTopicInfoToTable(topic, std::move(publishInfo));
}

MQConsumerInner* MQClientInstance::selectConsumer(const std::string& group) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  const auto& it = consumer_table_.find(group);
  if (it != consumer_table_.end()) {
    return it->second;
  }
  return nullptr;
}

bool MQClientInstance::addConsumerToTable(const std::string& consumerName, MQConsumerInner* consumer) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  if (consumer_table_.find(consumerName) != consumer_table_.end()) {
    return false;
  }
  consumer_table_[consumerName] = consumer;
  return true;
}

void MQClientInstance::eraseConsumerFromTable(const std::string& consumerName) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  const auto& it = consumer_table_.find(consumerName);
  if (it != consumer_table_.end()) {
    consumer_table_.erase(it);  // do not need free consumer, as it was allocated by user
  } else {
    LOG_WARN_NEW("could not find consumer:{} from table", consumerName);
  }
}

int MQClientInstance::getConsumerTableSize() {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  return consumer_table_.size();
}

void MQClientInstance::getTopicListFromConsumerSubscription(std::set<std::string>& topicList) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  for (const auto& it : consumer_table_) {
    std::vector<SubscriptionData> result = it.second->subscriptions();
    for (const auto& sd : result) {
      topicList.insert(sd.topic);
    }
  }
}

void MQClientInstance::updateConsumerTopicSubscribeInfo(const std::string& topic,
                                                        std::vector<MessageQueue> subscribeInfo) {
  std::lock_guard<std::mutex> lock(consumer_table_mutex_);
  for (auto& it : consumer_table_) {
    it.second->updateTopicSubscribeInfo(topic, subscribeInfo);
  }
}

void MQClientInstance::addTopicInfoToTable(const std::string& topic, TopicPublishInfoPtr topicPublishInfo) {
  std::lock_guard<std::mutex> lock(topic_publish_info_table_mutex_);
  topic_publish_info_table_[topic] = std::move(topicPublishInfo);
}

void MQClientInstance::eraseTopicInfoFromTable(const std::string& topic) {
  std::lock_guard<std::mutex> lock(topic_publish_info_table_mutex_);
  const auto& it = topic_publish_info_table_.find(topic);
  if (it != topic_publish_info_table_.end()) {
    topic_publish_info_table_.erase(it);
  }
}

TopicPublishInfoPtr MQClientInstance::getTopicPublishInfoFromTable(const std::string& topic) {
  std::lock_guard<std::mutex> lock(topic_publish_info_table_mutex_);
  const auto& it = topic_publish_info_table_.find(topic);
  if (it != topic_publish_info_table_.end()) {
    return it->second;
  }
  return nullptr;
}

bool MQClientInstance::isTopicInfoValidInTable(const std::string& topic) {
  std::lock_guard<std::mutex> lock(topic_publish_info_table_mutex_);
  return topic_publish_info_table_.find(topic) != topic_publish_info_table_.end();
}

TopicPublishInfoPtr MQClientInstance::tryToFindTopicPublishInfo(const std::string& topic) {
  auto topicPublishInfo = getTopicPublishInfoFromTable(topic);
  if (nullptr == topicPublishInfo) {
    updateTopicRouteInfoFromNameServer(topic);
    topicPublishInfo = getTopicPublishInfoFromTable(topic);
  }

  if (nullptr != topicPublishInfo && topicPublishInfo->ok()) {
    return topicPublishInfo;
  }

  LOG_INFO_NEW("updateTopicRouteInfoFromNameServer with default");
  updateTopicRouteInfoFromNameServer(topic, true);
  return getTopicPublishInfoFromTable(topic);
}

std::unique_ptr<FindBrokerResult> MQClientInstance::findBrokerAddressInAdmin(const std::string& brokerName) {
  BrokerAddrMAP brokerTable(getBrokerAddrTable());
  bool found = false;
  bool slave = false;
  std::string brokerAddr;

  const auto& it = brokerTable.find(brokerName);
  if (it != brokerTable.end()) {
    const auto& brokerMap = it->second;
    const auto& it1 = brokerMap.begin();
    if (it1 != brokerMap.end()) {
      slave = (it1->first != MASTER_ID);
      found = true;
      brokerAddr = it1->second;
    }
  }

  brokerTable.clear();
  if (found) {
    return MakeUnique<FindBrokerResult>(brokerAddr, slave);
  }

  return nullptr;
}

std::string MQClientInstance::findBrokerAddressInPublish(const std::string& brokerName) {
  BrokerAddrMAP brokerTable(getBrokerAddrTable());
  std::string brokerAddr;
  bool found = false;

  const auto& it = brokerTable.find(brokerName);
  if (it != brokerTable.end()) {
    const auto& brokerMap = it->second;
    const auto& it1 = brokerMap.find(MASTER_ID);
    if (it1 != brokerMap.end()) {
      brokerAddr = it1->second;
      found = true;
    }
  }

  brokerTable.clear();
  if (found) {
    return brokerAddr;
  }

  return null;
}

std::unique_ptr<FindBrokerResult> MQClientInstance::findBrokerAddressInSubscribe(const std::string& brokerName,
                                                                                 int brokerId,
                                                                                 bool onlyThisBroker) {
  std::string brokerAddr;
  bool slave = false;
  bool found = false;
  BrokerAddrMAP brokerTable(getBrokerAddrTable());

  const auto& it = brokerTable.find(brokerName);
  if (it != brokerTable.end()) {
    const auto& brokerMap = it->second;
    if (!brokerMap.empty()) {
      const auto& it1 = brokerMap.find(brokerId);
      if (it1 != brokerMap.end()) {
        brokerAddr = it1->second;
        slave = it1->first != MASTER_ID;
        found = true;
      } else if (!onlyThisBroker) {  // not only from master
        const auto& it2 = brokerMap.begin();
        brokerAddr = it2->second;
        slave = it2->first != MASTER_ID;
        found = true;
      }
    }
  }

  brokerTable.clear();

  if (found) {
    return std::unique_ptr<FindBrokerResult>(new FindBrokerResult(brokerAddr, slave));
  }

  return nullptr;
}

void MQClientInstance::findConsumerIds(const std::string& topic,
                                       const std::string& group,
                                       std::vector<std::string>& cids) {
  std::string brokerAddr;

  // find consumerIds from same broker every 40s
  {
    std::lock_guard<std::mutex> lock(topic_broker_addr_table_mutex_);
    const auto& it = topic_broker_addr_table_.find(topic);
    if (it != topic_broker_addr_table_.end()) {
      if (UtilAll::currentTimeMillis() < it->second.second + 120000) {
        brokerAddr = it->second.first;
      }
    }
  }

  if (brokerAddr.empty()) {
    // select new one
    brokerAddr = findBrokerAddrByTopic(topic);
    if (brokerAddr.empty()) {
      updateTopicRouteInfoFromNameServer(topic);
      brokerAddr = findBrokerAddrByTopic(topic);
    }

    if (!brokerAddr.empty()) {
      std::lock_guard<std::mutex> lock(topic_broker_addr_table_mutex_);
      topic_broker_addr_table_[topic] = std::make_pair(brokerAddr, UtilAll::currentTimeMillis());
    }
  }

  if (!brokerAddr.empty()) {
    try {
      LOG_INFO_NEW("getConsumerIdList from broker:{}", brokerAddr);
      cids = mq_client_api_impl_->GetConsumerIdListByGroup(brokerAddr, group, 5000);
    } catch (const MQException& e) {
      LOG_ERROR_NEW("encounter exception when getConsumerIdList: {}", e.what());

      std::lock_guard<std::mutex> lock(topic_broker_addr_table_mutex_);
      topic_broker_addr_table_.erase(topic);
    }
  }
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

std::string MQClientInstance::findBrokerAddrByTopic(const std::string& topic) {
  auto topicRouteData = getTopicRouteData(topic);
  if (topicRouteData != nullptr) {
    return SelectBrokerAddr(*topicRouteData);
  }
  return std::string();
}

void MQClientInstance::resetOffset(const std::string& group,
                                   const std::string& topic,
                                   const std::map<MessageQueue, int64_t>& offsetTable) {
  DefaultMQPushConsumerImpl* consumer = nullptr;
  try {
    auto* impl = selectConsumer(group);
    if (impl != nullptr && std::type_index(typeid(*impl)) == std::type_index(typeid(DefaultMQPushConsumerImpl))) {
      consumer = static_cast<DefaultMQPushConsumerImpl*>(impl);
    } else {
      LOG_INFO_NEW("[reset-offset] consumer dose not exist. group={}", group);
      return;
    }
    consumer->Suspend();

    auto processQueueTable = consumer->rebalance_impl()->getProcessQueueTable();
    for (const auto& it : processQueueTable) {
      const auto& mq = it.first;
      if (topic == mq.topic() && offsetTable.find(mq) != offsetTable.end()) {
        auto pq = it.second;
        pq->set_dropped(true);
        pq->ClearAllMessages();
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));

    for (const auto& it : processQueueTable) {
      const auto& mq = it.first;
      const auto& it2 = offsetTable.find(mq);
      if (it2 != offsetTable.end()) {
        auto offset = it2->second;
        consumer->UpdateConsumeOffset(mq, offset);
        consumer->rebalance_impl()->removeUnnecessaryMessageQueue(mq, it.second);
        consumer->rebalance_impl()->removeProcessQueueDirectly(mq);
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

std::unique_ptr<ConsumerRunningInfo> MQClientInstance::consumerRunningInfo(const std::string& consumerGroup) {
  auto* consumer = selectConsumer(consumerGroup);
  if (consumer != nullptr) {
    std::unique_ptr<ConsumerRunningInfo> runningInfo(consumer->consumerRunningInfo());
    if (runningInfo != nullptr) {
      std::string nsAddr = getNamesrvAddr();
      runningInfo->properties.emplace(ConsumerRunningInfo::PROP_NAMESERVER_ADDR, nsAddr);

      if (consumer->consumeType() == CONSUME_PASSIVELY) {
        runningInfo->properties.emplace(ConsumerRunningInfo::PROP_CONSUME_TYPE, "CONSUME_PASSIVELY");
      } else {
        runningInfo->properties.emplace(ConsumerRunningInfo::PROP_CONSUME_TYPE, "CONSUME_ACTIVELY");
      }

      runningInfo->properties.emplace(ConsumerRunningInfo::PROP_CLIENT_VERSION,
                                      MQVersion::GetVersionDesc(MQVersion::CURRENT_VERSION));

      return runningInfo;
    }
  }

  LOG_ERROR_NEW("no corresponding consumer found for group:{}", consumerGroup);
  return nullptr;
}

void MQClientInstance::addBrokerToAddrTable(const std::string& brokerName,
                                            const std::map<int, std::string>& brokerAddrs) {
  std::lock_guard<std::mutex> lock(broker_addr_table_mutex_);
  broker_addr_table_[brokerName] = brokerAddrs;
}

void MQClientInstance::resetBrokerAddrTable(BrokerAddrMAP&& table) {
  std::lock_guard<std::mutex> lock(broker_addr_table_mutex_);
  broker_addr_table_ = std::forward<BrokerAddrMAP>(table);
}

void MQClientInstance::clearBrokerAddrTable() {
  std::lock_guard<std::mutex> lock(broker_addr_table_mutex_);
  broker_addr_table_.clear();
}

MQClientInstance::BrokerAddrMAP MQClientInstance::getBrokerAddrTable() {
  std::lock_guard<std::mutex> lock(broker_addr_table_mutex_);
  return broker_addr_table_;
}

}  // namespace rocketmq
