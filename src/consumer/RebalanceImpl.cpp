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
#include "RebalanceImpl.h"

#include "ConsumeType.h"
#include "MQClientInstance.h"
#include "OffsetStore.h"
#include "logging/Logging.hpp"

namespace rocketmq {

RebalanceImpl::RebalanceImpl(const std::string& consumerGroup,
                             MessageModel messageModel,
                             const AllocateMQStrategy& allocateMqStrategy,
                             MQClientInstance* instance)
    : consumer_group_(consumerGroup),
      message_model_(messageModel),
      allocate_mq_strategy_(allocateMqStrategy),
      client_instance_(instance) {}

RebalanceImpl::~RebalanceImpl() = default;

void RebalanceImpl::doRebalance(bool orderly) {
  LOG_DEBUG_NEW("start doRebalance");
  for (const auto& it : subscription_inner_) {
    const std::string& topic = it.first;
    LOG_INFO_NEW("current topic is:{}", topic);
    try {
      rebalanceByTopic(topic, orderly);
    } catch (MQException& e) {
      LOG_ERROR_NEW("{}", e.what());
    }
  }

  truncateMessageQueueNotMyTopic();
}

void RebalanceImpl::rebalanceByTopic(const std::string& topic, bool orderly) {
  // msg model
  switch (message_model_) {
    case BROADCASTING: {
      std::vector<MessageQueue> mqSet;
      if (!getTopicSubscribeInfo(topic, mqSet)) {
        LOG_WARN_NEW("doRebalance, {}, but the topic[{}] not exist.", consumer_group_, topic);
        return;
      }
      bool changed = updateMessageQueueInRebalance(topic, mqSet, orderly);
      if (changed) {
        messageQueueChanged(topic, mqSet, mqSet);
      }
    } break;
    case CLUSTERING: {
      std::vector<MessageQueue> mqAll;
      if (!getTopicSubscribeInfo(topic, mqAll)) {
        if (!UtilAll::isRetryTopic(topic)) {
          LOG_WARN_NEW("doRebalance, {}, but the topic[{}] not exist.", consumer_group_, topic);
        }
        return;
      }

      auto cidAll = client_instance_->FindConsumerIds(topic, consumer_group_);

      if (cidAll.empty()) {
        LOG_WARN_NEW("doRebalance, {} {}, get consumer id list failed", consumer_group_, topic);
        return;
      }

      // log
      for (auto& cid : cidAll) {
        LOG_INFO_NEW("client id:{} of topic:{}", cid, topic);
      }

      // allocate mqs
      std::vector<MessageQueue> allocateResult;
      try {
        allocateResult = allocate_mq_strategy_(client_instance_->GetClientId(), mqAll, cidAll);
      } catch (MQException& e) {
        LOG_ERROR_NEW("encounter exception when invoke AllocateMQStrategy: {}", e.what());
        return;
      }

      // update local
      bool changed = updateMessageQueueInRebalance(topic, allocateResult, orderly);
      if (changed) {
        LOG_INFO_NEW(
            "rebalanced result changed. group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, "
            "rebalanceResultSize={}, rebalanceResultSet:",
            consumer_group_, topic, client_instance_->GetClientId(), mqAll.size(), cidAll.size(),
            allocateResult.size());
        for (auto& mq : allocateResult) {
          LOG_INFO_NEW("allocate mq:{}", mq.ToString());
        }
        messageQueueChanged(topic, mqAll, allocateResult);
      }
    } break;
    default:
      break;
  }
}

int64_t RebalanceImpl::computePullFromWhereImpl(const MessageQueue& mq,
                                                ConsumeFromWhere consume_from_where,
                                                const std::string& consume_timestamp,
                                                OffsetStore& offset_store,
                                                MQAdmin& admin) {
  int64_t result = -1;
  switch (consume_from_where) {
    default:
    case CONSUME_FROM_LAST_OFFSET: {
      long lastOffset = offset_store.readOffset(mq, ReadOffsetType::MEMORY_FIRST_THEN_STORE);
      if (lastOffset >= 0) {
        result = lastOffset;
      } else if (-1 == lastOffset) {
        if (UtilAll::isRetryTopic(mq.topic())) {  // First start, no offset
          result = 0;
        } else {
          try {
            result = admin.maxOffset(mq);
          } catch (MQClientException& e) {
            result = -1;
          }
        }
      } else {
        result = -1;
      }
    } break;
    case CONSUME_FROM_FIRST_OFFSET: {
      long lastOffset = offset_store.readOffset(mq, ReadOffsetType::MEMORY_FIRST_THEN_STORE);
      if (lastOffset >= 0) {
        result = lastOffset;
      } else if (-1 == lastOffset) {
        result = 0L;
      } else {
        result = -1;
      }
    } break;
    case CONSUME_FROM_TIMESTAMP: {
      long lastOffset = offset_store.readOffset(mq, ReadOffsetType::MEMORY_FIRST_THEN_STORE);
      if (lastOffset >= 0) {
        result = lastOffset;
      } else if (-1 == lastOffset) {
        if (UtilAll::isRetryTopic(mq.topic())) {
          try {
            result = admin.maxOffset(mq);
          } catch (MQClientException& e) {
            result = -1;
          }
        } else {
          try {
            // FIXME: parseDate by YYYYMMDDHHMMSS
            auto timestamp = std::stoull(consume_timestamp);
            result = admin.searchOffset(mq, timestamp);
          } catch (MQClientException& e) {
            result = -1;
          }
        }
      } else {
        result = -1;
      }
    } break;
  }
  return result;
}

TOPIC2SD& RebalanceImpl::getSubscriptionInner() {
  return subscription_inner_;
}

SubscriptionData* RebalanceImpl::getSubscriptionData(const std::string& topic) {
  const auto& it = subscription_inner_.find(topic);
  if (it != subscription_inner_.end()) {
    return it->second.get();
  }
  return nullptr;
}

void RebalanceImpl::setSubscriptionData(const std::string& topic,
                                        std::unique_ptr<SubscriptionData> subscription_data) noexcept {
  if (subscription_data != nullptr) {
    subscription_inner_[topic] = std::move(subscription_data);
  }
}

bool RebalanceImpl::getTopicSubscribeInfo(const std::string& topic, std::vector<MessageQueue>& mqs) {
  std::lock_guard<std::mutex> lock(topic_subscribe_info_table_mutex_);
  const auto& it = topic_subscribe_info_table_.find(topic);
  if (it != topic_subscribe_info_table_.end()) {
    mqs = it->second;  // mqs will out
    return true;
  }
  return false;
}

void RebalanceImpl::setTopicSubscribeInfo(const std::string& topic, std::vector<MessageQueue>& mqs) {
  if (subscription_inner_.find(topic) == subscription_inner_.end()) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(topic_subscribe_info_table_mutex_);
    topic_subscribe_info_table_[topic] = mqs;
  }

  // log
  for (const auto& mq : mqs) {
    LOG_DEBUG_NEW("topic [{}] has :{}", topic, mq.ToString());
  }
}

}  // namespace rocketmq
