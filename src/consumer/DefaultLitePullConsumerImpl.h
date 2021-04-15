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
#ifndef ROCKETMQ_DEFAULTLITEPULLCONSUMERIMPL_H_
#define ROCKETMQ_DEFAULTLITEPULLCONSUMERIMPL_H_

#include <functional>  // std::function
#include <memory>      // std::shared_ptr
#include <mutex>       // std::mutex
#include <string>      // std::string
#include <utility>

#include "DefaultLitePullConsumerConfigImpl.hpp"
#include "MQClientImpl.h"
#include "MQConsumerInner.h"
#include "MessageQueueListener.h"
#include "MessageQueueLock.hpp"
#include "MessageSelector.h"
#include "PollingMessageCache.hpp"
#include "concurrent/executor.hpp"

namespace rocketmq {

class AssignedMessageQueue;
class OffsetStore;
class PullAPIWrapper;
class PullResult;
class RebalanceLitePullImpl;

class DefaultLitePullConsumerImpl;
using DefaultLitePullConsumerImplPtr = std::shared_ptr<DefaultLitePullConsumerImpl>;

class DefaultLitePullConsumerImpl final : public std::enable_shared_from_this<DefaultLitePullConsumerImpl>,
                                          public MQClientImpl,
                                          public MQConsumerInner {
 private:
  enum class SubscriptionType { kNone, kSubscribe, kAssign };
  class MessageQueueListenerImpl;
  class AsyncPullCallback;

 public:
  using TopicMessageQueuesChangedListener =
      std::function<void(const std::string&, const std::vector<MQMessageQueue>&) /* noexcept */>;

 public:
  /**
   * create() - Factory method for DefaultLitePullConsumerImpl, used to ensure that all objects of
   * DefaultLitePullConsumerImpl are managed by std::share_ptr
   */
  static DefaultLitePullConsumerImplPtr Create(const std::shared_ptr<DefaultLitePullConsumerConfigImpl>& config,
                                               RPCHookPtr rpc_hook = nullptr) {
    return DefaultLitePullConsumerImplPtr{new DefaultLitePullConsumerImpl(config, std::move(rpc_hook))};
  }

  ~DefaultLitePullConsumerImpl();

  // disable copy
  DefaultLitePullConsumerImpl(const DefaultLitePullConsumerImpl&) = delete;
  DefaultLitePullConsumerImpl& operator=(const DefaultLitePullConsumerImpl&) = delete;

  // disable move
  DefaultLitePullConsumerImpl(DefaultLitePullConsumerImpl&&) = delete;
  DefaultLitePullConsumerImpl& operator=(DefaultLitePullConsumerImpl&&) = delete;

 private:
  DefaultLitePullConsumerImpl(const std::shared_ptr<DefaultLitePullConsumerConfigImpl>& config, RPCHookPtr rpc_hook);

 public:  // LitePullConsumer
  void start() override;
  void shutdown() override;

  bool isAutoCommit() const;
  void setAutoCommit(bool auto_commit);

  std::vector<MQMessageExt> Poll(int64_t timeout);

  void Subscribe(const std::string& topic, const std::string& expression);
  void Subscribe(const std::string& topic, const MessageSelector& selector);
  void Unsubscribe(const std::string& topic);

  std::vector<MQMessageQueue> FetchMessageQueues(const std::string& topic);
  void Assign(std::vector<MQMessageQueue>& message_queues);

  void Seek(const MQMessageQueue& message_queue, int64_t offset);
  void SeekToBegin(const MQMessageQueue& message_queue);
  void SeekToEnd(const MQMessageQueue& message_queue);

  int64_t OffsetForTimestamp(const MQMessageQueue& message_queue, int64_t timestamp);

  void Pause(const std::vector<MQMessageQueue>& message_queues);
  void Resume(const std::vector<MQMessageQueue>& message_queues);

  void CommitSync();

  int64_t Committed(const MQMessageQueue& message_queue);

  void RegisterTopicMessageQueuesChangedListener(
      const std::string& topic,
      TopicMessageQueuesChangedListener topic_message_queues_changed_listener);

 public:  // MQConsumerInner
  const std::string& groupName() const override;
  MessageModel messageModel() const override;
  ConsumeType consumeType() const override;
  ConsumeFromWhere consumeFromWhere() const override;

  std::vector<SubscriptionData> subscriptions() const override;

  // service discovery
  void updateTopicSubscribeInfo(const std::string& topic, std::vector<MQMessageQueue>& info) override;

  // load balancing
  void doRebalance() override;

  // offset persistence
  void persistConsumerOffset() override;

  void pullMessage(PullRequestPtr pull_request) override;

  std::unique_ptr<ConsumerRunningInfo> consumerRunningInfo() override;

 public:
  void ExecutePullRequestLater(PullRequestPtr pull_request, long delay);
  void ExecutePullRequestImmediately(PullRequestPtr pull_request);

 private:
  void CheckConfig();
  void StartScheduleTask();
  void OperateAfterRunning();

  void FetchTopicMessageQueuesAndComparePeriodically();
  void FetchTopicMessageQueuesAndCompare();

  void UpdateTopicSubscribeInfoWhenSubscriptionChanged();

  void ResetTopic(std::vector<MessageExtPtr>& messages);

  void UpdateAssignedMessageQueue(const std::string& topic, std::vector<MQMessageQueue>& assigned_message_queues);
  void UpdateAssignedMessageQueue(std::vector<MQMessageQueue>& assigned_message_queues);
  void DispatchAssigndPullRequest(std::vector<PullRequestPtr>& pull_request_list);

  int64_t NextPullOffset(const ProcessQueuePtr& process_queue);
  int64_t FetchConsumeOffset(const MQMessageQueue& message_queue);

  void MaybeAutoCommit();
  void CommitAll();

  void UpdateConsumeOffset(const MQMessageQueue& mq, int64_t offset);

  void ParseMessageQueues(std::vector<MQMessageQueue>& queueSet);

 public:
  PollingMessageCache& message_cache() { return message_cache_; }

  MessageQueueListener* message_queue_listener() const { return message_queue_listener_.get(); }

  OffsetStore* offset_store() const { return offset_store_.get(); }

  DefaultLitePullConsumerConfigImpl& config() const {
    return static_cast<DefaultLitePullConsumerConfigImpl&>(*client_config_);
  }

 private:
  void set_subscription_type(SubscriptionType subscription_type);

 private:
  std::mutex mutex_;

  uint64_t start_time_{0};

  SubscriptionType subscription_type_{SubscriptionType::kNone};

  // long consume_request_flow_control_times_{0};
  long queue_flow_control_times_{0};

  int64_t next_auto_commit_deadline_{-1};

  bool auto_commit_{true};

  std::unique_ptr<MessageQueueListener> message_queue_listener_;

  std::map<std::string, TopicMessageQueuesChangedListener> topic_message_queues_changed_listener_map_;
  std::map<std::string, std::vector<MQMessageQueue>> message_queues_for_topic_;

  std::unique_ptr<AssignedMessageQueue> assigned_message_queue_;

  PollingMessageCache message_cache_;

  scheduled_thread_pool_executor scheduled_executor_service_;

  std::unique_ptr<RebalanceLitePullImpl> rebalance_impl_;
  std::unique_ptr<PullAPIWrapper> pull_api_wrapper_;
  std::unique_ptr<OffsetStore> offset_store_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_DEFAULTLITEPULLCONSUMERIMPL_H_
