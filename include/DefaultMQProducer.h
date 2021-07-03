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
#ifndef ROCKETMQ_DEFAULTMQPRODUCER_H_
#define ROCKETMQ_DEFAULTMQPRODUCER_H_

#include <memory>

#include "DefaultMQProducerConfigProxy.h"
#include "MQClientConfigProxy.h"
#include "MQMessage.h"
#include "MQSelector.h"
#include "MessageQueue.hpp"
#include "RPCHook.h"
#include "RequestCallback.h"
#include "SendCallback.h"
#include "SendResult.hpp"

namespace rocketmq {

class DefaultMQProducerConfigImpl;
class DefaultMQProducerImpl;

class ROCKETMQCLIENT_API DefaultMQProducer : public DefaultMQProducerConfigProxy, public MQClientConfigProxy {
 public:
  DefaultMQProducer(const std::string& groupname);
  DefaultMQProducer(const std::string& groupname, RPCHookPtr rpc_hook);

 private:
  DefaultMQProducer(const std::string& groupname,
                    RPCHookPtr rpc_hook,
                    std::shared_ptr<DefaultMQProducerConfigImpl> config);

 public:
  void start();
  void shutdown();

  std::vector<MessageQueue> fetchPublishMessageQueues(const std::string& topic);

  // Sync: caller will be responsible for the lifecycle of messages.
  SendResult send(MQMessage& message);
  SendResult send(MQMessage& message, long timeout);
  SendResult send(MQMessage& message, const MessageQueue& message_queue);
  SendResult send(MQMessage& message, const MessageQueue& message_queue, long timeout);

  // Async: don't delete msg object, until callback occur.
  void send(MQMessage& message, SendCallback* send_callback) noexcept;
  void send(MQMessage& message, SendCallback* send_callback, long timeout) noexcept;
  void send(MQMessage& message, const MessageQueue& message_queue, SendCallback* send_callback) noexcept;
  void send(MQMessage& message, const MessageQueue& message_queue, SendCallback* send_callback, long timeout) noexcept;

  // Oneyway: same as sync send, but don't care its result.
  void sendOneway(MQMessage& message);
  void sendOneway(MQMessage& message, const MessageQueue& message_queue);

  // Select
  SendResult send(MQMessage& message, MessageQueueSelector* selector, void* arg);
  SendResult send(MQMessage& message, MessageQueueSelector* selector, void* arg, long timeout);
  void send(MQMessage& message, MessageQueueSelector* selector, void* arg, SendCallback* send_callback) noexcept;
  void send(MQMessage& message,
            MessageQueueSelector* selector,
            void* arg,
            SendCallback* send_callback,
            long timeout) noexcept;
  void sendOneway(MQMessage& message, MessageQueueSelector* selector, void* arg);

  // Batch: power by sync send, caller will be responsible for the lifecycle of messages.
  SendResult send(std::vector<MQMessage>& messages);
  SendResult send(std::vector<MQMessage>& messages, long timeout);
  SendResult send(std::vector<MQMessage>& messages, const MessageQueue& message_queue);
  SendResult send(std::vector<MQMessage>& messages, const MessageQueue& message_queue, long timeout);

  void send(std::vector<MQMessage>& messages, SendCallback* send_callback);
  void send(std::vector<MQMessage>& messages, SendCallback* send_callback, long timeout);
  void send(std::vector<MQMessage>& messages, const MessageQueue& message_queue, SendCallback* send_callback);
  void send(std::vector<MQMessage>& messages,
            const MessageQueue& message_queue,
            SendCallback* send_callback,
            long timeout);

  // RPC
  MQMessage request(MQMessage& message, long timeout);
  void request(MQMessage& message, RequestCallback* request_callback, long timeout);
  MQMessage request(MQMessage& msg, const MessageQueue& message_queue, long timeout);
  void request(MQMessage& message, const MessageQueue& message_queue, RequestCallback* request_callback, long timeout);
  MQMessage request(MQMessage& message, MessageQueueSelector* selector, void* arg, long timeout);
  void request(MQMessage& message,
               MessageQueueSelector* selector,
               void* arg,
               RequestCallback* request_callback,
               long timeout);

 public:
  void setRPCHook(RPCHookPtr rpc_hook);

 protected:
  std::shared_ptr<DefaultMQProducerConfigImpl> producer_config_impl_;
  std::shared_ptr<DefaultMQProducerImpl> producer_impl_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_DEFAULTMQPRODUCER_H_
