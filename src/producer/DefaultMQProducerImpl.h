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
#ifndef ROCKETMQ_PRODUCER_DEFAULTMQPRODUCERIMPL_H_
#define ROCKETMQ_PRODUCER_DEFAULTMQPRODUCERIMPL_H_

#include <memory>
#include <utility>

#include "CommunicationMode.h"
#include "DefaultMQProducerConfigImpl.hpp"
#include "MQClientImpl.h"
#include "MQProducerInner.h"
#include "MessageBatch.h"
#include "common/ResultState.hpp"
#include "concurrent/executor.hpp"

namespace rocketmq {

class MQFaultStrategy;
class TopicPublishInfo;

class DefaultMQProducerImpl;
using DefaultMQProducerImplPtr = std::shared_ptr<DefaultMQProducerImpl>;

/**
 * DefaultMQProducerImpl - implement of DefaultMQProducer
 */
class DefaultMQProducerImpl final : public std::enable_shared_from_this<DefaultMQProducerImpl>,
                                    public MQClientImpl,
                                    public MQProducerInner {
 public:
  using MessageQueueSelector =
      std::function<MQMessageQueue(const std::vector<MQMessageQueue>&, const MessagePtr&) /* noexcept */>;
  using SendCallback = std::function<void(ResultState<std::unique_ptr<SendResult>>) /* noexcept */>;
  using RequestCallback = std::function<void(ResultState<MessagePtr>) /* noexcept */>;

 public:
  /**
   * Create() - Factory method for DefaultMQProducerImpl, used to ensure that all objects of DefaultMQProducerImpl are
   * managed by std::share_ptr
   */
  static std::shared_ptr<DefaultMQProducerImpl> Create(const std::shared_ptr<DefaultMQProducerConfigImpl>& config,
                                                       RPCHookPtr rpc_hook = nullptr) {
    return DefaultMQProducerImplPtr{new DefaultMQProducerImpl(config, std::move(rpc_hook))};
  }

  ~DefaultMQProducerImpl();

  // disable copy
  DefaultMQProducerImpl(const DefaultMQProducerImpl&) = delete;
  DefaultMQProducerImpl& operator=(const DefaultMQProducerImpl&) = delete;

  // disable move
  DefaultMQProducerImpl(DefaultMQProducerImpl&&) = delete;
  DefaultMQProducerImpl& operator=(DefaultMQProducerImpl&&) = delete;

 private:
  DefaultMQProducerImpl(const std::shared_ptr<DefaultMQProducerConfigImpl>& config, RPCHookPtr rpc_hook);

 public:  // MQProducer
  void start() override;
  void shutdown() override;

  std::vector<MQMessageQueue> FetchPublishMessageQueues(const std::string& topic);

  // Sync
  SendResult Send(const MessagePtr& message, int64_t timeout);
  SendResult Send(const MessagePtr& message, const MQMessageQueue& message_queue, int64_t timeout);

  // Async
  void Send(const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept;
  void Send(const MessagePtr& message,
            const MQMessageQueue& message_queue,
            SendCallback send_callback,
            int64_t timeout) noexcept;

  // Oneyway: same as sync send, but don't care its result.
  void SendOneway(const MessagePtr& message);
  void SendOneway(const MessagePtr& message, const MQMessageQueue& message_queue);

  // Select
  SendResult Send(const MessagePtr& message, MessageQueueSelector selector, int64_t timeout);
  void Send(const MessagePtr& message,
            MessageQueueSelector selector,
            SendCallback send_callback,
            int64_t timeout) noexcept;
  void SendOneway(const MessagePtr& message, MessageQueueSelector selector);

  // Transaction
  TransactionSendResult SendMessageInTransaction(const MessagePtr& message, void* arg);

  // RPC
  MessagePtr Request(const MessagePtr& message, int64_t timeout);
  void Request(const MessagePtr& message, RequestCallback request_callback, int64_t timeout) noexcept;
  MessagePtr Request(const MessagePtr& message, const MQMessageQueue& message_queue, int64_t timeout);
  void Request(const MessagePtr&,
               const MQMessageQueue& message_queue,
               RequestCallback request_callback,
               int64_t timeout) noexcept;
  MessagePtr Request(const MessagePtr& message, MessageQueueSelector selector, int64_t timeout);
  void Request(const MessagePtr& message,
               MessageQueueSelector selector,
               RequestCallback request_callback,
               int64_t timeout) noexcept;

 public:  // MQProducerInner
  void checkTransactionState(const std::string& addr,
                             MessageExtPtr message,
                             CheckTransactionStateRequestHeader* check_request_header) override;

 private:
  using AsyncSendFunction = std::function<void(const MessagePtr&, SendCallback, long) /* noexcept */>;

  class UnifiedSendDefaultImpl;
  friend UnifiedSendDefaultImpl;
  std::unique_ptr<SendResult> SendDefaultImpl(const MessagePtr& message,
                                              CommunicationMode communication_mode,
                                              SendCallback send_callback,
                                              int64_t timeout);

  std::unique_ptr<SendResult> SendToQueueImpl(const MessagePtr& message,
                                              const MQMessageQueue& message_queue,
                                              CommunicationMode communication_mode,
                                              SendCallback send_callback,
                                              int64_t timeout);

  std::unique_ptr<SendResult> SendSelectQueueImpl(const MessagePtr& message,
                                                  const MessageQueueSelector& selector,
                                                  CommunicationMode communication_mode,
                                                  SendCallback send_callback,
                                                  int64_t timeout);

  std::unique_ptr<SendResult> SendKernelImpl(const MessagePtr& message,
                                             const MQMessageQueue& message_queue,
                                             CommunicationMode communication_mode,
                                             SendCallback send_callback,
                                             int64_t timeout);

  std::unique_ptr<TransactionSendResult> SendInTransactionImpl(const MessagePtr& message, void* arg, int64_t timeout);

  void CheckTransactionStateImpl(const std::string& address,
                                 const MessageExtPtr& message,
                                 long transaction_state_table_offset,
                                 long commit_log_offset,
                                 const std::string& message_id,
                                 const std::string& transaction_id,
                                 const std::string& offset_message_id);

  void EndTransaction(SendResult& send_result,
                      LocalTransactionState local_transaction_state,
                      const std::exception_ptr& local_exception);

  MessagePtr SyncRequestImpl(const MessagePtr& message, int64_t timeout, const AsyncSendFunction& send_delegate);
  void AsyncRequestImpl(const MessagePtr& message,
                        int64_t timeout,
                        RequestCallback request_callback,
                        const AsyncSendFunction& send_delegate) noexcept;

 public:
  void InitialTransactionEnv();
  void DestroyTransactionEnv();

 public:
  DefaultMQProducerConfigImpl& config() const { return static_cast<DefaultMQProducerConfigImpl&>(*client_config_); }

 private:
  std::unique_ptr<MQFaultStrategy> mq_fault_strategy_;
  std::unique_ptr<thread_pool_executor> async_send_executor_;
  std::unique_ptr<thread_pool_executor> check_transaction_executor_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PRODUCER_DEFAULTMQPRODUCERIMPL_H_
