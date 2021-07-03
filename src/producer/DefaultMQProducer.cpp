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
#include "DefaultMQProducer.h"

#include "DefaultMQProducerConfigImpl.hpp"
#include "DefaultMQProducerImpl.h"
#include "MQClientConfigProxy.h"
#include "MQException.h"
#include "UtilAll.h"

namespace rocketmq {

namespace {

template <typename Callback, typename Result, typename Converter>
class CallbackImpl {
 public:
  CallbackImpl(Callback* callback) : callback_(callback) {}
  ~CallbackImpl() = default;

  // enable copy
  CallbackImpl(const CallbackImpl&) = default;
  CallbackImpl& operator=(const CallbackImpl&) = default;

  // enable move
  CallbackImpl(CallbackImpl&&) noexcept = default;
  CallbackImpl& operator=(CallbackImpl&&) noexcept = default;

  void operator()(ResultState<Result> state) noexcept {
    try {
      callback_->invokeOnSuccess(Converter()(state.GetResult()));
    } catch (MQException& e) {
      LOG_ERROR_NEW("send failed, exception:{}", e.what());
      callback_->invokeOnException(e);
    } catch (std::exception& e) {
      LOG_FATAL_NEW("[BUG] encounter unexcepted exception: {}", e.what());
      exit(-1);
    }
  }

 private:
  Callback* callback_;
};

struct SendResultConverter {
  SendResult& operator()(std::unique_ptr<SendResult>& send_result) { return *send_result; }
};

using SendCallbackImpl = CallbackImpl<SendCallback, std::unique_ptr<SendResult>, SendResultConverter>;

struct MessageConverter {
  MQMessage operator()(MessagePtr& message) { return MQMessage{message}; }
};

using RequestCallbackImpl = CallbackImpl<RequestCallback, MessagePtr, MessageConverter>;

class MessageQueueSelectorImpl {
 public:
  MessageQueueSelectorImpl(MessageQueueSelector* selector, void* arg) : selector_(selector), arg_(arg) {}

  MessageQueue operator()(const std::vector<MessageQueue>& message_queues, const MessagePtr& message) noexcept {
    return selector_->select(message_queues, MQMessage{message}, arg_);
  }

 private:
  MessageQueueSelector* selector_;
  void* arg_;
};

}  // namespace

DefaultMQProducer::DefaultMQProducer(const std::string& groupname) : DefaultMQProducer(groupname, nullptr) {}

DefaultMQProducer::DefaultMQProducer(const std::string& groupname, RPCHookPtr rpc_hook)
    : DefaultMQProducer(groupname, rpc_hook, std::make_shared<DefaultMQProducerConfigImpl>()) {}

DefaultMQProducer::DefaultMQProducer(const std::string& groupname,
                                     RPCHookPtr rpc_hook,
                                     std::shared_ptr<DefaultMQProducerConfigImpl> config)
    : DefaultMQProducerConfigProxy(*config), MQClientConfigProxy(config), producer_config_impl_(config) {
  // set default group name
  if (groupname.empty()) {
    set_group_name(DEFAULT_PRODUCER_GROUP);
  } else {
    set_group_name(groupname);
  }

  // create DefaultMQProducerImpl
  producer_impl_ = DefaultMQProducerImpl::Create(producer_config_impl_, rpc_hook);
}

void DefaultMQProducer::start() {
  producer_impl_->start();
}

void DefaultMQProducer::shutdown() {
  producer_impl_->shutdown();
}

std::vector<MessageQueue> DefaultMQProducer::fetchPublishMessageQueues(const std::string& topic) {
  return producer_impl_->FetchPublishMessageQueues(topic);
}

SendResult DefaultMQProducer::send(MQMessage& message) {
  return send(message, send_msg_timeout());
}

SendResult DefaultMQProducer::send(MQMessage& message, long timeout) {
  return producer_impl_->Send(message.getMessageImpl(), timeout);
}

SendResult DefaultMQProducer::send(MQMessage& message, const MessageQueue& message_queue) {
  return send(message, message_queue, send_msg_timeout());
}

SendResult DefaultMQProducer::send(MQMessage& message, const MessageQueue& message_queue, long timeout) {
  return producer_impl_->Send(message.getMessageImpl(), message_queue, timeout);
}

void DefaultMQProducer::send(MQMessage& message, SendCallback* send_callback) noexcept {
  send(message, send_callback, send_msg_timeout());
}

void DefaultMQProducer::send(MQMessage& message, SendCallback* send_callback, long timeout) noexcept {
  producer_impl_->Send(message.getMessageImpl(), SendCallbackImpl{send_callback}, timeout);
}

void DefaultMQProducer::send(MQMessage& message,
                             const MessageQueue& message_queue,
                             SendCallback* send_callback) noexcept {
  send(message, message_queue, send_callback, send_msg_timeout());
}

void DefaultMQProducer::send(MQMessage& message,
                             const MessageQueue& message_queue,
                             SendCallback* send_callback,
                             long timeout) noexcept {
  producer_impl_->Send(message.getMessageImpl(), message_queue, SendCallbackImpl{send_callback}, timeout);
}

void DefaultMQProducer::sendOneway(MQMessage& message) {
  producer_impl_->SendOneway(message.getMessageImpl());
}

void DefaultMQProducer::sendOneway(MQMessage& message, const MessageQueue& message_queue) {
  producer_impl_->SendOneway(message.getMessageImpl(), message_queue);
}

SendResult DefaultMQProducer::send(MQMessage& message, MessageQueueSelector* selector, void* arg) {
  return send(message, selector, arg, send_msg_timeout());
}

SendResult DefaultMQProducer::send(MQMessage& message, MessageQueueSelector* selector, void* arg, long timeout) {
  return producer_impl_->Send(message.getMessageImpl(), MessageQueueSelectorImpl{selector, arg}, timeout);
}

void DefaultMQProducer::send(MQMessage& message,
                             MessageQueueSelector* selector,
                             void* arg,
                             SendCallback* send_callback) noexcept {
  send(message, selector, arg, send_callback, send_msg_timeout());
}

void DefaultMQProducer::send(MQMessage& message,
                             MessageQueueSelector* selector,
                             void* arg,
                             SendCallback* send_callback,
                             long timeout) noexcept {
  producer_impl_->Send(message.getMessageImpl(), MessageQueueSelectorImpl{selector, arg},
                       SendCallbackImpl{send_callback}, timeout);
}

void DefaultMQProducer::sendOneway(MQMessage& message, MessageQueueSelector* selector, void* arg) {
  producer_impl_->SendOneway(message.getMessageImpl(), MessageQueueSelectorImpl{selector, arg});
}

namespace {

MessagePtr batch(std::vector<MQMessage>& messages) {
  if (messages.empty()) {
    THROW_MQEXCEPTION(MQClientException, "msgs need one message at least", -1);
  }
  std::vector<MessagePtr> message_list;
  std::transform(messages.begin(), messages.end(), std::back_inserter(message_list),
                 [](MQMessage& message) { return message.getMessageImpl(); });
  return MessageBatch::Wrap(message_list);
}

}  // namespace

SendResult DefaultMQProducer::send(std::vector<MQMessage>& messages) {
  return send(messages, send_msg_timeout());
}

SendResult DefaultMQProducer::send(std::vector<MQMessage>& messages, long timeout) {
  return producer_impl_->Send(batch(messages), timeout);
}

SendResult DefaultMQProducer::send(std::vector<MQMessage>& messages, const MessageQueue& message_queue) {
  return send(messages, message_queue, send_msg_timeout());
}

SendResult DefaultMQProducer::send(std::vector<MQMessage>& messages, const MessageQueue& message_queue, long timeout) {
  return producer_impl_->Send(batch(messages), message_queue, timeout);
}

void DefaultMQProducer::send(std::vector<MQMessage>& messages, SendCallback* send_callback) {
  send(messages, send_callback, send_msg_timeout());
}

void DefaultMQProducer::send(std::vector<MQMessage>& messages, SendCallback* send_callback, long timeout) {
  producer_impl_->Send(batch(messages), SendCallbackImpl{send_callback}, timeout);
}

void DefaultMQProducer::send(std::vector<MQMessage>& messages,
                             const MessageQueue& message_queue,
                             SendCallback* send_callback) {
  send(messages, message_queue, send_callback, send_msg_timeout());
}

void DefaultMQProducer::send(std::vector<MQMessage>& messages,
                             const MessageQueue& message_queue,
                             SendCallback* send_callback,
                             long timeout) {
  producer_impl_->Send(batch(messages), message_queue, SendCallbackImpl{send_callback}, timeout);
}

MQMessage DefaultMQProducer::request(MQMessage& message, long timeout) {
  return MQMessage{producer_impl_->Request(message.getMessageImpl(), timeout)};
}

void DefaultMQProducer::request(MQMessage& message, RequestCallback* request_callback, long timeout) {
  producer_impl_->Request(message.getMessageImpl(), RequestCallbackImpl{request_callback}, timeout);
}

MQMessage DefaultMQProducer::request(MQMessage& message, const MessageQueue& message_queue, long timeout) {
  return MQMessage{producer_impl_->Request(message.getMessageImpl(), message_queue, timeout)};
}

void DefaultMQProducer::request(MQMessage& message,
                                const MessageQueue& message_queue,
                                RequestCallback* request_callback,
                                long timeout) {
  producer_impl_->Request(message.getMessageImpl(), message_queue, RequestCallbackImpl{request_callback}, timeout);
}

MQMessage DefaultMQProducer::request(MQMessage& message, MessageQueueSelector* selector, void* arg, long timeout) {
  return MQMessage{producer_impl_->Request(message.getMessageImpl(), MessageQueueSelectorImpl{selector, arg}, timeout)};
}

void DefaultMQProducer::request(MQMessage& message,
                                MessageQueueSelector* selector,
                                void* arg,
                                RequestCallback* request_callback,
                                long timeout) {
  producer_impl_->Request(message.getMessageImpl(), MessageQueueSelectorImpl{selector, arg},
                          RequestCallbackImpl{request_callback}, timeout);
}

void DefaultMQProducer::setRPCHook(RPCHookPtr rpc_hook) {
  producer_impl_->setRPCHook(rpc_hook);
}

}  // namespace rocketmq
