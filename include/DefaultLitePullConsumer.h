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
#ifndef ROCKETMQ_DEFAULTLITEPULLCONSUMER_H_
#define ROCKETMQ_DEFAULTLITEPULLCONSUMER_H_

#include <memory>

#include "DefaultLitePullConsumerConfigProxy.h"
#include "MQClientConfigProxy.h"
#include "MQMessageExt.h"
#include "MessageSelector.h"
#include "RPCHook.h"
#include "TopicMessageQueueChangeListener.h"

namespace rocketmq {

class DefaultLitePullConsumerConfigImpl;
class DefaultLitePullConsumerImpl;

class ROCKETMQCLIENT_API DefaultLitePullConsumer : public DefaultLitePullConsumerConfigProxy,
                                                   public MQClientConfigProxy {
 public:
  DefaultLitePullConsumer(const std::string& groupname);
  DefaultLitePullConsumer(const std::string& groupname, RPCHookPtr rpc_hook);

 private:
  DefaultLitePullConsumer(const std::string& groupname,
                          RPCHookPtr rpc_hook,
                          std::shared_ptr<DefaultLitePullConsumerConfigImpl> config);

 public:  // LitePullConsumer
  void start();
  void shutdown();

  bool isAutoCommit() const;
  void setAutoCommit(bool auto_commit);

  std::vector<MQMessageExt> poll();
  std::vector<MQMessageExt> poll(long timeout);

  void subscribe(const std::string& topic, const std::string& expression);
  void subscribe(const std::string& topic, const MessageSelector& selector);
  void unsubscribe(const std::string& topic);

  std::vector<MQMessageQueue> fetchMessageQueues(const std::string& topic);
  void assign(std::vector<MQMessageQueue>& message_queues);

  void seek(const MQMessageQueue& message_queue, int64_t offset);
  void seekToBegin(const MQMessageQueue& message_queue);
  void seekToEnd(const MQMessageQueue& message_queue);

  int64_t offsetForTimestamp(const MQMessageQueue& message_queue, int64_t timestamp);

  void pause(const std::vector<MQMessageQueue>& message_queues);
  void resume(const std::vector<MQMessageQueue>& message_queues);

  void commitSync();

  int64_t committed(const MQMessageQueue& message_queue);

  void registerTopicMessageQueueChangeListener(const std::string& topic,
                                               TopicMessageQueueChangeListener* topic_message_queue_change_listener);

 public:
  void setRPCHook(RPCHookPtr rpc_hook);

 protected:
  std::shared_ptr<DefaultLitePullConsumerConfigImpl> pull_consumer_config_impl_;
  std::shared_ptr<DefaultLitePullConsumerImpl> pull_consumer_impl_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_DEFAULTLITEPULLCONSUMER_H_
