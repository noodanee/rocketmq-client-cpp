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
#ifndef ROCKETMQ_CONSUMER_REBALANCELITEPULLIMPL_H_
#define ROCKETMQ_CONSUMER_REBALANCELITEPULLIMPL_H_

#include "DefaultLitePullConsumerImpl.h"
#include "PullRequest.h"
#include "RebalanceImpl.h"

namespace rocketmq {

using MQ2PQ = std::map<MQMessageQueue, ProcessQueuePtr>;

class RebalanceLitePullImpl : public RebalanceImpl {
 public:
  RebalanceLitePullImpl(DefaultLitePullConsumerImpl* pull_consumer_impl);

 public:
  void shutdown() override;
  ConsumeType consumeType() final { return CONSUME_ACTIVELY; }

 protected:
  bool updateMessageQueueInRebalance(const std::string& topic,
                                     std::vector<MQMessageQueue>& allocated_message_queues,
                                     bool orderly) override;
  void messageQueueChanged(const std::string& topic,
                           std::vector<MQMessageQueue>& all_message_queues,
                           std::vector<MQMessageQueue>& allocated_message_queues) override;
  void truncateMessageQueueNotMyTopic() override;

 public:
  bool removeUnnecessaryMessageQueue(const MQMessageQueue& message_queue, ProcessQueuePtr process_queue);
  void removeDirtyOffset(const MQMessageQueue& message_queue);
  int64_t computePullFromWhere(const MQMessageQueue& message_queue);

 private:
  DefaultLitePullConsumerImpl* lite_pull_consumer_impl_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_REBALANCELITEPULLIMPL_H_
