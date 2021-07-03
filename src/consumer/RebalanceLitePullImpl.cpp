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
#include "RebalanceLitePullImpl.h"

#include "AllocateMQAveragely.h"
#include "OffsetStore.h"
#include "UtilAll.h"

namespace rocketmq {

RebalanceLitePullImpl::RebalanceLitePullImpl(DefaultLitePullConsumerImpl* pull_consumer_impl)
    : RebalanceImpl(null, CLUSTERING, AllocateMQAveragely, nullptr), lite_pull_consumer_impl_(pull_consumer_impl) {}

void RebalanceLitePullImpl::shutdown() {}

bool RebalanceLitePullImpl::updateMessageQueueInRebalance(const std::string& topic,
                                                          std::vector<MessageQueue>& allocated_message_queues,
                                                          bool orderly) {
  return true;
}

bool RebalanceLitePullImpl::removeUnnecessaryMessageQueue(const MessageQueue& message_queue,
                                                          ProcessQueuePtr process_queue) {
  lite_pull_consumer_impl_->offset_store()->persist(message_queue);
  lite_pull_consumer_impl_->offset_store()->removeOffset(message_queue);
  return true;
}

void RebalanceLitePullImpl::removeDirtyOffset(const MessageQueue& message_queue) {
  lite_pull_consumer_impl_->offset_store()->removeOffset(message_queue);
}

int64_t RebalanceLitePullImpl::computePullFromWhere(const MessageQueue& message_queue) {
  return RebalanceImpl::computePullFromWhereImpl(message_queue, lite_pull_consumer_impl_->config().consume_from_where(),
                                                 lite_pull_consumer_impl_->config().consume_timestamp(),
                                                 *lite_pull_consumer_impl_->offset_store(), *lite_pull_consumer_impl_);
}

void RebalanceLitePullImpl::messageQueueChanged(const std::string& topic,
                                                std::vector<MessageQueue>& all_message_queues,
                                                std::vector<MessageQueue>& divided_message_queues) {
  auto* message_queue_listener = lite_pull_consumer_impl_->message_queue_listener();
  if (message_queue_listener != nullptr) {
    try {
      message_queue_listener->messageQueueChanged(topic, all_message_queues, divided_message_queues);
    } catch (std::exception& e) {
      LOG_ERROR_NEW("messageQueueChanged exception {}", e.what());
    }
  }
}

void RebalanceLitePullImpl::truncateMessageQueueNotMyTopic() {}

}  // namespace rocketmq
