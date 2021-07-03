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
#include "DefaultLitePullConsumer.h"

#include <memory>

#include "DefaultLitePullConsumerConfigImpl.hpp"
#include "DefaultLitePullConsumerImpl.h"
#include "MQClientConfigProxy.h"
#include "UtilAll.h"

namespace rocketmq {

namespace {

class TopicMessageQueuesChangedListenerImpl {
 public:
  TopicMessageQueuesChangedListenerImpl(TopicMessageQueueChangeListener* listener) : listener_(listener) {}

  void operator()(const std::string& topic, const std::vector<MessageQueue>& message_queues) noexcept {
    listener_->onChanged(topic, message_queues);
  }

 private:
  TopicMessageQueueChangeListener* listener_;
};

}  // namespace

DefaultLitePullConsumer::DefaultLitePullConsumer(const std::string& groupname)
    : DefaultLitePullConsumer(groupname, nullptr) {}

DefaultLitePullConsumer::DefaultLitePullConsumer(const std::string& groupname, RPCHookPtr rpc_hook)
    : DefaultLitePullConsumer(groupname, rpc_hook, std::make_shared<DefaultLitePullConsumerConfigImpl>()) {}

DefaultLitePullConsumer::DefaultLitePullConsumer(const std::string& groupname,
                                                 RPCHookPtr rpc_hook,
                                                 std::shared_ptr<DefaultLitePullConsumerConfigImpl> config)
    : DefaultLitePullConsumerConfigProxy(*config), MQClientConfigProxy(config), pull_consumer_config_impl_(config) {
  // set default group name
  if (groupname.empty()) {
    set_group_name(DEFAULT_CONSUMER_GROUP);
  } else {
    set_group_name(groupname);
  }

  // create DefaultLitePullConsumerImpl
  pull_consumer_impl_ = DefaultLitePullConsumerImpl::Create(pull_consumer_config_impl_, rpc_hook);
}

void DefaultLitePullConsumer::start() {
  pull_consumer_impl_->start();
}

void DefaultLitePullConsumer::shutdown() {
  pull_consumer_impl_->shutdown();
}

bool DefaultLitePullConsumer::isAutoCommit() const {
  return pull_consumer_impl_->isAutoCommit();
}

void DefaultLitePullConsumer::setAutoCommit(bool auto_commit) {
  pull_consumer_impl_->setAutoCommit(auto_commit);
}

std::vector<MQMessageExt> DefaultLitePullConsumer::poll() {
  return poll(pull_consumer_config_impl_->poll_timeout_millis());
}

std::vector<MQMessageExt> DefaultLitePullConsumer::poll(long timeout) {
  return pull_consumer_impl_->Poll(timeout);
}

void DefaultLitePullConsumer::subscribe(const std::string& topic, const std::string& expression) {
  pull_consumer_impl_->Subscribe(topic, expression);
}

void DefaultLitePullConsumer::subscribe(const std::string& topic, const MessageSelector& selector) {
  pull_consumer_impl_->Subscribe(topic, selector);
}

void DefaultLitePullConsumer::unsubscribe(const std::string& topic) {
  pull_consumer_impl_->Unsubscribe(topic);
}

std::vector<MessageQueue> DefaultLitePullConsumer::fetchMessageQueues(const std::string& topic) {
  return pull_consumer_impl_->FetchMessageQueues(topic);
}

void DefaultLitePullConsumer::assign(std::vector<MessageQueue>& message_queues) {
  pull_consumer_impl_->Assign(message_queues);
}

void DefaultLitePullConsumer::seek(const MessageQueue& message_queue, int64_t offset) {
  pull_consumer_impl_->Seek(message_queue, offset);
}

void DefaultLitePullConsumer::seekToBegin(const MessageQueue& message_queue) {
  pull_consumer_impl_->SeekToBegin(message_queue);
}

void DefaultLitePullConsumer::seekToEnd(const MessageQueue& message_queue) {
  pull_consumer_impl_->SeekToEnd(message_queue);
}

int64_t DefaultLitePullConsumer::offsetForTimestamp(const MessageQueue& message_queue, int64_t timestamp) {
  return pull_consumer_impl_->OffsetForTimestamp(message_queue, timestamp);
}

void DefaultLitePullConsumer::pause(const std::vector<MessageQueue>& message_queues) {
  pull_consumer_impl_->Pause(message_queues);
}

void DefaultLitePullConsumer::resume(const std::vector<MessageQueue>& message_queues) {
  pull_consumer_impl_->Resume(message_queues);
}

void DefaultLitePullConsumer::commitSync() {
  pull_consumer_impl_->CommitSync();
}

int64_t DefaultLitePullConsumer::committed(const MessageQueue& message_queue) {
  return pull_consumer_impl_->Committed(message_queue);
}

void DefaultLitePullConsumer::registerTopicMessageQueueChangeListener(
    const std::string& topic,
    TopicMessageQueueChangeListener* topic_message_queue_change_listener) {
  pull_consumer_impl_->RegisterTopicMessageQueuesChangedListener(
      topic, TopicMessageQueuesChangedListenerImpl{topic_message_queue_change_listener});
}

void DefaultLitePullConsumer::setRPCHook(RPCHookPtr rpc_hook) {
  pull_consumer_impl_->setRPCHook(rpc_hook);
}

}  // namespace rocketmq
