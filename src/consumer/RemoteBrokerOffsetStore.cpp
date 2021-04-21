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
#include "RemoteBrokerOffsetStore.h"

#include <utility>  // std::move

#include "Logging.h"
#include "MQClientAPIImpl.h"
#include "MQClientInstance.h"
#include "UtilAll.h"

namespace rocketmq {

RemoteBrokerOffsetStore::RemoteBrokerOffsetStore(MQClientInstance* instance, std::string groupName)
    : client_instance_(instance), group_name_(std::move(groupName)) {}

RemoteBrokerOffsetStore::~RemoteBrokerOffsetStore() {
  client_instance_ = nullptr;
  offset_table_.clear();
}

void RemoteBrokerOffsetStore::load() {}

void RemoteBrokerOffsetStore::updateOffset(const MessageQueue& mq, int64_t offset, bool increaseOnly) {
  std::lock_guard<std::mutex> lock(lock_);
  const auto& it = offset_table_.find(mq);
  if (it == offset_table_.end() || !increaseOnly || offset > it->second) {
    offset_table_[mq] = offset;
  }
}

int64_t RemoteBrokerOffsetStore::readOffset(const MessageQueue& mq, ReadOffsetType type) {
  switch (type) {
    case MEMORY_FIRST_THEN_STORE:
    case READ_FROM_MEMORY: {
      std::lock_guard<std::mutex> lock(lock_);

      const auto& it = offset_table_.find(mq);
      if (it != offset_table_.end()) {
        return it->second;
      } else if (READ_FROM_MEMORY == type) {
        return -1;
      }
    }
    case READ_FROM_STORE: {
      try {
        int64_t brokerOffset = fetchConsumeOffsetFromBroker(mq);
        // update
        updateOffset(mq, brokerOffset, false);
        return brokerOffset;
      } catch (MQBrokerException& e) {
        LOG_ERROR(e.what());
        return -1;
      } catch (MQException& e) {
        LOG_ERROR(e.what());
        return -2;
      }
    }
    default:
      break;
  }
  return -1;
}

void RemoteBrokerOffsetStore::persist(const MessageQueue& mq) {
  int64_t offset = -1;
  {
    std::lock_guard<std::mutex> lock(lock_);
    const auto& it = offset_table_.find(mq);
    if (it != offset_table_.end()) {
      offset = it->second;
    }
  }

  if (offset >= 0) {
    try {
      updateConsumeOffsetToBroker(mq, offset);
      LOG_INFO_NEW("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}", group_name_,
                   client_instance_->getClientId(), mq.ToString(), offset);
    } catch (MQException& e) {
      LOG_ERROR("updateConsumeOffsetToBroker error");
    }
  }
}

void RemoteBrokerOffsetStore::persistAll(std::vector<MessageQueue>& mqs) {
  if (mqs.empty()) {
    return;
  }

  std::sort(mqs.begin(), mqs.end());

  std::vector<MessageQueue> unused_mqs;

  std::map<MessageQueue, int64_t> offset_table;
  {
    std::lock_guard<std::mutex> lock(lock_);
    offset_table = offset_table_;
  }

  for (const auto& it : offset_table) {
    const auto& mq = it.first;
    auto offset = it.second;
    if (offset >= 0) {
      if (std::binary_search(mqs.begin(), mqs.end(), mq)) {
        try {
          updateConsumeOffsetToBroker(mq, offset);
          LOG_INFO_NEW("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}", group_name_,
                       client_instance_->getClientId(), mq.ToString(), offset);
        } catch (std::exception& e) {
          LOG_ERROR_NEW("updateConsumeOffsetToBroker exception, {} {}", mq.ToString(), e.what());
        }
      } else {
        unused_mqs.push_back(mq);
      }
    }
  }

  if (!unused_mqs.empty()) {
    std::lock_guard<std::mutex> lock(lock_);
    for (const auto& mq : unused_mqs) {
      offset_table_.erase(mq);
      LOG_INFO_NEW("remove unused mq, {}, {}", mq.ToString(), group_name_);
    }
  }
}

void RemoteBrokerOffsetStore::removeOffset(const MessageQueue& mq) {
  std::lock_guard<std::mutex> lock(lock_);
  const auto& it = offset_table_.find(mq);
  if (it != offset_table_.end()) {
    offset_table_.erase(it);
  }
}

void RemoteBrokerOffsetStore::updateConsumeOffsetToBroker(const MessageQueue& mq, int64_t offset) {
  auto findBrokerResult = client_instance_->FindBrokerAddressInAdmin(mq.broker_name());

  if (!findBrokerResult) {
    client_instance_->updateTopicRouteInfoFromNameServer(mq.topic());
    findBrokerResult = client_instance_->FindBrokerAddressInAdmin(mq.broker_name());
  }

  if (findBrokerResult) {
    try {
      return client_instance_->getMQClientAPIImpl()->UpdateConsumerOffsetOneway(
          findBrokerResult.broker_addr, group_name_, mq.topic(), mq.queue_id(), offset);
    } catch (MQException& e) {
      LOG_ERROR(e.what());
    }
  } else {
    LOG_WARN("The broker not exist");
  }
}

int64_t RemoteBrokerOffsetStore::fetchConsumeOffsetFromBroker(const MessageQueue& mq) {
  auto findBrokerResult = client_instance_->FindBrokerAddressInAdmin(mq.broker_name());

  if (!findBrokerResult) {
    client_instance_->updateTopicRouteInfoFromNameServer(mq.topic());
    findBrokerResult = client_instance_->FindBrokerAddressInAdmin(mq.broker_name());
  }

  if (findBrokerResult) {
    return client_instance_->getMQClientAPIImpl()->QueryConsumerOffset(findBrokerResult.broker_addr, group_name_,
                                                                       mq.topic(), mq.queue_id(), 1000 * 5);
  }

  LOG_ERROR("The broker not exist when fetchConsumeOffsetFromBroker");
  THROW_MQEXCEPTION(MQClientException, "The broker not exist", -1);
}

}  // namespace rocketmq
