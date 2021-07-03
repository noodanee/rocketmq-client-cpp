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
#include "PullAPIWrapper.h"

#include <memory>

#include "ByteBuffer.hpp"
#include "MQClientAPIImpl.h"
#include "MQClientInstance.h"
#include "MessageAccessor.hpp"
#include "MessageDecoder.h"
#include "PullResultExt.hpp"
#include "PullSysFlag.h"

namespace rocketmq {

PullAPIWrapper::PullAPIWrapper(MQClientInstance* client_instance, const std::string& consumer_group)
    : client_instance_(client_instance), consumer_group_(consumer_group) {}

std::unique_ptr<PullResult> PullAPIWrapper::PullKernelImpl(const MessageQueue& message_queue,
                                                           const std::string& expression,
                                                           const std::string& expression_type,
                                                           int64_t version,
                                                           int64_t offset,
                                                           int max_nums,
                                                           int system_flag,
                                                           int64_t commit_offset,
                                                           int broker_suspend_max_time_millis,
                                                           int timeout_millis,
                                                           CommunicationMode communication_mode,
                                                           PullCallback pull_callback) {
  std::unique_ptr<FindBrokerResult> find_broker_result(client_instance_->findBrokerAddressInSubscribe(
      message_queue.broker_name(), RecalculatePullFromWhichNode(message_queue), false));
  if (find_broker_result == nullptr) {
    client_instance_->updateTopicRouteInfoFromNameServer(message_queue.topic());
    find_broker_result = client_instance_->findBrokerAddressInSubscribe(
        message_queue.broker_name(), RecalculatePullFromWhichNode(message_queue), false);
  }

  if (find_broker_result != nullptr) {
    if (find_broker_result->slave()) {
      system_flag = PullSysFlag::clearCommitOffsetFlag(system_flag);
    }

    std::unique_ptr<PullMessageRequestHeader> request_header{new PullMessageRequestHeader()};
    request_header->consumer_group = consumer_group_;
    request_header->topic = message_queue.topic();
    request_header->queue_id = message_queue.queue_id();
    request_header->queue_offset = offset;
    request_header->max_message_nums = max_nums;
    request_header->system_flag = system_flag;
    request_header->commit_offset = commit_offset;
    request_header->suspend_timeout_millis = broker_suspend_max_time_millis;
    request_header->subscription = expression;
    request_header->subscription_version = version;

    return client_instance_->getMQClientAPIImpl()->pullMessage(find_broker_result->broker_addr(),
                                                               std::move(request_header), timeout_millis,
                                                               communication_mode, std::move(pull_callback));
  }

  THROW_MQEXCEPTION(MQClientException, "The broker [" + message_queue.broker_name() + "] not exist", -1);
}

int PullAPIWrapper::RecalculatePullFromWhichNode(const MessageQueue& message_queue) {
  std::lock_guard<std::mutex> lock(lock_);
  const auto& it = pull_from_which_node_table_.find(message_queue);
  if (it != pull_from_which_node_table_.end()) {
    return it->second;
  }
  return MASTER_ID;
}

std::unique_ptr<PullResult> PullAPIWrapper::ProcessPullResult(const MessageQueue& message_queue,
                                                              std::unique_ptr<PullResult> pull_result,
                                                              SubscriptionData* subscription_data) {
  auto* pull_result_ext = dynamic_cast<PullResultExt*>(pull_result.get());
  if (pull_result_ext == nullptr) {
    return pull_result;
  }

  // update node
  UpdatePullFromWhichNode(message_queue, pull_result_ext->suggert_which_boker_id());

  std::vector<MessageExtPtr> filtered_message_list;
  if (PullStatus::FOUND == pull_result_ext->pull_status()) {
    // decode all msg list
    std::unique_ptr<ByteBuffer> byte_buffer(ByteBuffer::wrap(pull_result_ext->message_binary()));
    auto message_list = MessageDecoder::decodes(*byte_buffer);

    // filter msg list again
    if (subscription_data != nullptr && !subscription_data->tag_set.empty()) {
      filtered_message_list.reserve(message_list.size());
      for (const auto& msg : message_list) {
        const auto& msgTag = msg->tags();
        if (subscription_data->ContainsTag(msgTag)) {
          filtered_message_list.push_back(msg);
        }
      }
    } else {
      filtered_message_list.swap(message_list);
    }

    if (!filtered_message_list.empty()) {
      std::string min_offset = UtilAll::to_string(pull_result_ext->min_offset());
      std::string max_offset = UtilAll::to_string(pull_result_ext->max_offset());
      for (auto& message : filtered_message_list) {
        const auto& transaction_flag = message->getProperty(MQMessageConst::PROPERTY_TRANSACTION_PREPARED);
        if (UtilAll::stob(transaction_flag)) {
          message->set_transaction_id(message->getProperty(MQMessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        }
        MessageAccessor::putProperty(*message, MQMessageConst::PROPERTY_MIN_OFFSET, min_offset);
        MessageAccessor::putProperty(*message, MQMessageConst::PROPERTY_MAX_OFFSET, max_offset);
      }
    }
  }

  return std::unique_ptr<PullResult>(new PullResult(pull_result_ext->pull_status(),
                                                    pull_result_ext->next_begin_offset(), pull_result_ext->min_offset(),
                                                    pull_result_ext->max_offset(), std::move(filtered_message_list)));
}

void PullAPIWrapper::UpdatePullFromWhichNode(const MessageQueue& message_queue, int broker_id) {
  std::lock_guard<std::mutex> lock(lock_);
  pull_from_which_node_table_[message_queue] = broker_id;
}

}  // namespace rocketmq
