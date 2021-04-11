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
#include "DefaultMQPushConsumer.h"

#include <vector>

#include "DefaultMQPushConsumerConfigImpl.hpp"
#include "DefaultMQPushConsumerImpl.h"
#include "MQClientConfigProxy.h"
#include "UtilAll.h"

namespace rocketmq {

namespace {

class MessageListenerImpl {
 public:
  MessageListenerImpl(MQMessageListener* listener) : listener_(listener) {}

  ConsumeStatus operator()(std::vector<MessageExtPtr>& messages) {
    auto message_list = MQMessageExt::Wrap(messages);
    return listener_->consumeMessage(message_list);
  }

 private:
  MQMessageListener* listener_;
};

}  // namespace

DefaultMQPushConsumer::DefaultMQPushConsumer(const std::string& groupname)
    : DefaultMQPushConsumer(groupname, nullptr) {}

DefaultMQPushConsumer::DefaultMQPushConsumer(const std::string& groupname, RPCHookPtr rpc_hook)
    : DefaultMQPushConsumer(groupname, rpc_hook, std::make_shared<DefaultMQPushConsumerConfigImpl>()) {}

DefaultMQPushConsumer::DefaultMQPushConsumer(const std::string& groupname,
                                             RPCHookPtr rpc_hook,
                                             std::shared_ptr<DefaultMQPushConsumerConfigImpl> config)
    : DefaultMQPushConsumerConfigProxy(*config), MQClientConfigProxy(config), push_consumer_config_impl_(config) {
  // set default group name
  if (groupname.empty()) {
    set_group_name(DEFAULT_CONSUMER_GROUP);
  } else {
    set_group_name(groupname);
  }

  // create DefaultMQPushConsumerImpl
  push_consumer_impl_ = DefaultMQPushConsumerImpl::Create(push_consumer_config_impl_, rpc_hook);
}

void DefaultMQPushConsumer::start() {
  push_consumer_impl_->start();
}

void DefaultMQPushConsumer::shutdown() {
  push_consumer_impl_->shutdown();
}

void DefaultMQPushConsumer::suspend() {
  push_consumer_impl_->Suspend();
}

void DefaultMQPushConsumer::resume() {
  push_consumer_impl_->Resume();
}

MQMessageListener* DefaultMQPushConsumer::getMessageListener() const {
  return message_listener_;
}

void DefaultMQPushConsumer::registerMessageListener(MessageListenerConcurrently* message_listener) {
  message_listener_ = message_listener;
  push_consumer_impl_->RegisterMessageListener(MessageListenerImpl{message_listener},
                                               DefaultMQPushConsumerImpl::MessageListenerType::kConcurrently);
}

void DefaultMQPushConsumer::registerMessageListener(MessageListenerOrderly* message_listener) {
  message_listener_ = message_listener;
  push_consumer_impl_->RegisterMessageListener(MessageListenerImpl{message_listener},
                                               DefaultMQPushConsumerImpl::MessageListenerType::kOrderly);
}

void DefaultMQPushConsumer::subscribe(const std::string& topic, const std::string& expression) {
  push_consumer_impl_->Subscribe(topic, expression);
}

void DefaultMQPushConsumer::setRPCHook(RPCHookPtr rpc_hook) {
  push_consumer_impl_->setRPCHook(rpc_hook);
}

}  // namespace rocketmq
