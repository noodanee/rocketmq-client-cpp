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
#ifndef ROCKETMQ_DEFAULTMQPUSHCONSUMER_H_
#define ROCKETMQ_DEFAULTMQPUSHCONSUMER_H_

#include <memory>

#include "DefaultMQPushConsumerConfigProxy.h"
#include "MQClientConfigProxy.h"
#include "MQMessageListener.h"
#include "RPCHook.h"

namespace rocketmq {

class DefaultMQPushConsumerConfigImpl;
class DefaultMQPushConsumerImpl;

class ROCKETMQCLIENT_API DefaultMQPushConsumer : public DefaultMQPushConsumerConfigProxy, public MQClientConfigProxy {
 public:
  DefaultMQPushConsumer(const std::string& groupname);
  DefaultMQPushConsumer(const std::string& groupname, RPCHookPtr rpc_hook);

 private:
  DefaultMQPushConsumer(const std::string& groupname,
                        RPCHookPtr rpc_hook,
                        std::shared_ptr<DefaultMQPushConsumerConfigImpl> config);

 public:  // MQPushConsumer
  void start();
  void shutdown();

  void suspend();
  void resume();

  MQMessageListener* getMessageListener() const;

  void registerMessageListener(MessageListenerConcurrently* message_listener);
  void registerMessageListener(MessageListenerOrderly* message_listener);

  void subscribe(const std::string& topic, const std::string& expression);

 public:
  void setRPCHook(RPCHookPtr rpc_hook);

 protected:
  std::shared_ptr<DefaultMQPushConsumerConfigImpl> push_consumer_config_impl_;
  std::shared_ptr<DefaultMQPushConsumerImpl> push_consumer_impl_;
  MQMessageListener* message_listener_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_DEFAULTMQPUSHCONSUMER_H_
