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

#include <memory>  // std::shared_ptr
#include <mutex>   // std::mutex
#include <string>  // std::string

#include "DefaultLitePullConsumer.h"
#include "MQClientImpl.h"
#include "MQConsumerInner.h"
#include "MessageQueueListener.h"
#include "MessageQueueLock.hpp"
#include "PollingMessageCache.hpp"
#include "TopicMessageQueueChangeListener.h"
#include "concurrent/executor.hpp"

namespace rocketmq {

class AssignedMessageQueue;
class OffsetStore;
class PullAPIWrapper;
class PullResult;
class RebalanceLitePullImpl;

class DefaultLitePullConsumerImpl;
typedef std::shared_ptr<DefaultLitePullConsumerImpl> DefaultLitePullConsumerImplPtr;

enum SubscriptionType { NONE, SUBSCRIBE, ASSIGN };

class DefaultLitePullConsumerImpl : public std::enable_shared_from_this<DefaultLitePullConsumerImpl>,
                                    public LitePullConsumer,
                                    public MQClientImpl,
                                    public MQConsumerInner {
 private:
  class MessageQueueListenerImpl;
  class AsyncPullCallback;

 public:
  /**
   * create() - Factory method for DefaultLitePullConsumerImpl, used to ensure that all objects of
   * DefaultLitePullConsumerImpl are managed by std::share_ptr
   */
  static DefaultLitePullConsumerImplPtr create(DefaultLitePullConsumerConfigPtr config, RPCHookPtr rpcHook = nullptr) {
    if (nullptr == rpcHook) {
      return DefaultLitePullConsumerImplPtr(new DefaultLitePullConsumerImpl(config));
    } else {
      return DefaultLitePullConsumerImplPtr(new DefaultLitePullConsumerImpl(config, rpcHook));
    }
  }

 private:
  DefaultLitePullConsumerImpl(DefaultLitePullConsumerConfigPtr config);
  DefaultLitePullConsumerImpl(DefaultLitePullConsumerConfigPtr config, RPCHookPtr rpcHook);

 public:
  virtual ~DefaultLitePullConsumerImpl();

 public:  // LitePullConsumer
  void start() override;
  void shutdown() override;

  bool isAutoCommit() const override;
  void setAutoCommit(bool auto_commit) override;

  void subscribe(const std::string& topic, const std::string& subExpression) override;
  void subscribe(const std::string& topic, const MessageSelector& selector) override;

  void unsubscribe(const std::string& topic) override;

  std::vector<MQMessageExt> poll() override;
  std::vector<MQMessageExt> poll(long timeout) override;

  std::vector<MQMessageQueue> fetchMessageQueues(const std::string& topic) override;
  void assign(std::vector<MQMessageQueue>& messageQueues) override;

  void seek(const MQMessageQueue& messageQueue, int64_t offset) override;
  void seekToBegin(const MQMessageQueue& messageQueue) override;
  void seekToEnd(const MQMessageQueue& messageQueue) override;

  int64_t offsetForTimestamp(const MQMessageQueue& messageQueue, int64_t timestamp) override;

  void pause(const std::vector<MQMessageQueue>& messageQueues) override;
  void resume(const std::vector<MQMessageQueue>& messageQueues) override;

  void commitSync() override;

  int64_t committed(const MQMessageQueue& messageQueue) override;

  void registerTopicMessageQueueChangeListener(
      const std::string& topic,
      TopicMessageQueueChangeListener* topicMessageQueueChangeListener) override;

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
  void executePullRequestLater(PullRequestPtr pull_request, long delay);
  void executePullRequestImmediately(PullRequestPtr pull_request);

 private:
  void checkConfig();
  void startScheduleTask();
  void operateAfterRunning();

  void fetchTopicMessageQueuesAndComparePeriodically();
  void fetchTopicMessageQueuesAndCompare();

  bool isSetEqual(std::vector<MQMessageQueue>& newMessageQueues, std::vector<MQMessageQueue>& oldMessageQueues);

  void updateTopicSubscribeInfoWhenSubscriptionChanged();


  void updateAssignedMessageQueue(const std::string& topic, std::vector<MQMessageQueue>& assigned_message_queues);
  void updateAssignedMessageQueue(std::vector<MQMessageQueue>& assigned_message_queues);
  void dispatchAssigndPullRequest(std::vector<PullRequestPtr>& pull_request_list);

  int64_t nextPullOffset(const ProcessQueuePtr& process_queue);
  int64_t fetchConsumeOffset(const MQMessageQueue& messageQueue);

  void maybeAutoCommit();

  void resetTopic(std::vector<MessageExtPtr>& msg_list);

  void commitAll();

  void updateConsumeOffset(const MQMessageQueue& mq, int64_t offset);

  void parseMessageQueues(std::vector<MQMessageQueue>& queueSet);

 public:
  PollingMessageCache& message_cache() { return message_cache_; }

  inline MessageQueueListener* getMessageQueueListener() const { return message_queue_listener_.get(); }

  inline OffsetStore* getOffsetStore() const { return offset_store_.get(); }

  inline DefaultLitePullConsumerConfig* getDefaultLitePullConsumerConfig() const {
    return dynamic_cast<DefaultLitePullConsumerConfig*>(client_config_.get());
  }

 private:
  void set_subscription_type(SubscriptionType subscription_type);

 private:
  std::mutex mutex_;

  uint64_t start_time_;

  SubscriptionType subscription_type_;

  long consume_request_flow_control_times_;
  long queue_flow_control_times_;

  int64_t next_auto_commit_deadline_;

  bool auto_commit_;

  std::unique_ptr<MessageQueueListener> message_queue_listener_;

  std::map<std::string, TopicMessageQueueChangeListener*> topic_message_queue_change_listener_map_;
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
