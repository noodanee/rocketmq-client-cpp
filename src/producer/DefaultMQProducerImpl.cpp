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
#include "DefaultMQProducerImpl.h"

#ifndef WIN32
#include <signal.h>
#endif

#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include "ClientErrorCode.h"
#include "CommunicationMode.h"
#include "CorrelationIdUtil.hpp"
#include "Logging.h"
#include "MQClientAPIImpl.h"
#include "MQClientInstance.h"
#include "MQClientManager.h"
#include "MQException.h"
#include "MQFaultStrategy.h"
#include "MQMessageQueue.h"
#include "MQProtos.h"
#include "MessageBatch.h"
#include "MessageClientIDSetter.h"
#include "MessageDecoder.h"
#include "MessageSysFlag.h"
#include "RequestFutureTable.h"
#include "TopicPublishInfo.hpp"
#include "TransactionMQProducerConfig.h"
#include "UtilAll.h"
#include "Validators.h"
#include "protocol/header/CommandHeader.h"

namespace rocketmq {

DefaultMQProducerImpl::DefaultMQProducerImpl(const std::shared_ptr<DefaultMQProducerConfigImpl>& config,
                                             RPCHookPtr rpc_hook)
    : MQClientImpl(std::dynamic_pointer_cast<MQClientConfig>(config), std::move(rpc_hook)),
      mq_fault_strategy_(new MQFaultStrategy()) {}

DefaultMQProducerImpl::~DefaultMQProducerImpl() = default;

void DefaultMQProducerImpl::start() {
#ifndef WIN32
  /* Ignore the SIGPIPE */
  struct sigaction sa;
  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_handler = SIG_IGN;
  sa.sa_flags = 0;
  ::sigaction(SIGPIPE, &sa, 0);
#endif

  switch (service_state_) {
    case ServiceState::kCreateJust: {
      LOG_INFO_NEW("DefaultMQProducerImpl: {} start", config().group_name());

      service_state_ = ServiceState::kStartFailed;

      config().changeInstanceNameToPID();
      mq_fault_strategy_->set_enable(config().send_latency_fault_enable());

      MQClientImpl::start();

      bool registerOK = client_instance_->registerProducer(config().group_name(), this);
      if (!registerOK) {
        service_state_ = ServiceState::kCreateJust;
        THROW_MQEXCEPTION(
            MQClientException,
            "The producer group[" + config().group_name() + "] has been created before, specify another name please.",
            -1);
      }

      if (nullptr == async_send_executor_) {
#if __cplusplus >= 201402L
        async_send_executor_ =
            std::make_unique<thread_pool_executor>("AsyncSendThread", config().async_send_thread_nums(), false);
#else
        async_send_executor_ = std::unique_ptr<thread_pool_executor>{
            new thread_pool_executor("AsyncSendThread", config().async_send_thread_nums(), false)};
#endif
      }
      async_send_executor_->startup();

      client_instance_->start();

      LOG_INFO_NEW("the producer [{}] start OK.", config().group_name());
      service_state_ = ServiceState::kRunning;
      break;
    }
    case ServiceState::kRunning:
    case ServiceState::kStartFailed:
    case ServiceState::kShutdownAlready:
      THROW_MQEXCEPTION(MQClientException, "The producer service state not OK, maybe started once", -1);
      break;
    default:
      break;
  }

  client_instance_->sendHeartbeatToAllBrokerWithLock();
}

void DefaultMQProducerImpl::shutdown() {
  switch (service_state_) {
    case ServiceState::kRunning: {
      LOG_INFO("DefaultMQProducerImpl shutdown");

      async_send_executor_->shutdown();

      client_instance_->unregisterProducer(config().group_name());
      client_instance_->shutdown();

      service_state_ = ServiceState::kShutdownAlready;
      break;
    }
    case ServiceState::kShutdownAlready:
    case ServiceState::kCreateJust:
    default:
      break;
  }
}

std::vector<MQMessageQueue> DefaultMQProducerImpl::FetchPublishMessageQueues(const std::string& topic) {
  auto topicPublishInfo = client_instance_->tryToFindTopicPublishInfo(topic);
  if (topicPublishInfo != nullptr) {
    return topicPublishInfo->getMessageQueueList();
  }
  return std::vector<MQMessageQueue>{};
}

SendResult DefaultMQProducerImpl::Send(const MessagePtr& message, int64_t timeout) {
  try {
    return *SendDefaultImpl(message, CommunicationMode::SYNC, nullptr, timeout);
  } catch (MQException& e) {
    LOG_ERROR_NEW("send failed, exception:{}", e.what());
    throw;
  }
}

SendResult DefaultMQProducerImpl::Send(const MessagePtr& message,
                                       const MQMessageQueue& message_queue,
                                       int64_t timeout) {
  try {
    return *SendToQueueImpl(message, message_queue, CommunicationMode::SYNC, nullptr, timeout);
  } catch (MQException& e) {
    LOG_ERROR_NEW("send failed, exception:{}", e.what());
    throw;
  }
}

void DefaultMQProducerImpl::Send(const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
  async_send_executor_->submit(
#if __cplusplus >= 201402L
      [this, message, send_callback = std::move(send_callback), timeout]
#else
      [this, message, send_callback, timeout]
#endif
      () {
        try {
          (void)SendDefaultImpl(message, CommunicationMode::ASYNC, send_callback, timeout);
        } catch (...) {
          send_callback({std::current_exception()});
        }
      });
}

void DefaultMQProducerImpl::Send(const MessagePtr& message,
                                 const MQMessageQueue& message_queue,
                                 SendCallback send_callback,
                                 int64_t timeout) noexcept {
  async_send_executor_->submit(
#if __cplusplus >= 201402L
      [this, message, message_queue, send_callback = std::move(send_callback), timeout]
#else
      [this, message, message_queue, send_callback, timeout]
#endif
      () {
        try {
          try {
            (void)SendToQueueImpl(message, message_queue, CommunicationMode::ASYNC, send_callback, timeout);
          } catch (MQBrokerException& e) {
            std::string error_message = std::string("unknown exception, ") + e.what();
            THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
          }
        } catch (...) {
          send_callback({std::current_exception()});
        }
      });
}

void DefaultMQProducerImpl::SendOneway(const MessagePtr& message) {
  try {
    (void)SendDefaultImpl(message, CommunicationMode::ONEWAY, nullptr, config().send_msg_timeout());
  } catch (MQBrokerException& e) {
    std::string error_message = std::string("unknown exception, ") + e.what();
    THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
  }
}

void DefaultMQProducerImpl::SendOneway(const MessagePtr& message, const MQMessageQueue& message_queue) {
  try {
    (void)SendToQueueImpl(message, message_queue, CommunicationMode::ASYNC, nullptr, config().send_msg_timeout());
  } catch (MQBrokerException& e) {
    std::string error_message = std::string("unknown exception, ") + e.what();
    THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
  }
}

SendResult DefaultMQProducerImpl::Send(const MessagePtr& message, MessageQueueSelector selector, int64_t timeout) {
  try {
    return *SendSelectQueueImpl(message, selector, CommunicationMode::SYNC, nullptr, timeout);
  } catch (MQException& e) {
    LOG_ERROR_NEW("send failed, exception:{}", e.what());
    throw;
  }
}

void DefaultMQProducerImpl::Send(const MessagePtr& message,
                                 MessageQueueSelector selector,
                                 SendCallback send_callback,
                                 int64_t timeout) noexcept {
  async_send_executor_->submit(
#if __cplusplus >= 201402L
      [this, message, selector = std::move(selector), send_callback = std::move(send_callback), timeout]
#else
      [this, message, selector, send_callback, timeout]
#endif
      () mutable {
        try {
          try {
            (void)SendSelectQueueImpl(message, selector, CommunicationMode::ASYNC, send_callback, timeout);
          } catch (MQBrokerException& e) {
            std::string error_message = std::string("unknown exception, ") + e.what();
            THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
          }
        } catch (...) {
          send_callback({std::current_exception()});
        }
      });
}

void DefaultMQProducerImpl::SendOneway(const MessagePtr& message, MessageQueueSelector selector) {
  try {
    (void)SendSelectQueueImpl(message, selector, CommunicationMode::ONEWAY, nullptr, config().send_msg_timeout());
  } catch (MQBrokerException& e) {
    std::string error_message = std::string("unknown exception, ") + e.what();
    THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
  }
}

namespace {

void TryToCompressMessage(Message& message, DefaultMQProducerConfig& config) {
  const auto& body = message.body();
  if (body.size() >= config.compress_msg_body_over_howmuch()) {
    std::string compressed_body;
    if (UtilAll::deflate(body, compressed_body, config.compress_level())) {
      message.set_body(std::move(compressed_body));
      message.putProperty(MQMessageConst::PROPERTY_ALREADY_COMPRESSED_FLAG, "true");
    }
  }
}

void Preprocess(Message& message, DefaultMQProducerConfig& config) {
  if (message.isBatch()) {
    try {
      auto& message_batch = dynamic_cast<MessageBatch&>(message);
      for (const auto& message : message_batch.messages()) {
        Validators::checkMessage(*message, config.max_message_size());
        MessageClientIDSetter::setUniqID(*message);
      }
      message_batch.set_body(message_batch.Encode());
    } catch (std::exception& e) {
      THROW_MQEXCEPTION(MQClientException, "Failed to initiate the MessageBatch", -1);
    }
  }

  Validators::checkMessage(message, config.max_message_size());

  if (!message.isBatch()) {
    MessageClientIDSetter::setUniqID(message);
    TryToCompressMessage(message, config);
  }
}

inline void Preprocess(Message& message, const std::string& topic, DefaultMQProducerConfig& config) {
  Preprocess(message, config);

  if (message.topic() != topic) {
    THROW_MQEXCEPTION(MQClientException, "message's topic not equal mq's topic", -1);
  }
}

inline TopicPublishInfoPtr TryToFindTopicPublishInfo(const std::string& topic, MQClientInstance& client_instance) {
  auto topic_publish_info = client_instance.tryToFindTopicPublishInfo(topic);
  if (topic_publish_info == nullptr || !topic_publish_info->ok()) {
    THROW_MQEXCEPTION(MQClientException, "No route info of this topic: " + topic, -1);
  }
  return topic_publish_info;
}

}  // namespace

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendDefaultImpl(const MessagePtr& message,
                                                                   CommunicationMode communication_mode,
                                                                   SendCallback send_callback,
                                                                   int64_t timeout) {
  Preprocess(*message, config());

  int64_t begin_time_first = UtilAll::currentTimeMillis();
  int64_t begin_time_prev = begin_time_first;
  int64_t end_time = begin_time_first;

  auto topic_publish_info = TryToFindTopicPublishInfo(message->topic(), *client_instance_);

  std::unique_ptr<SendResult> send_result;
  int timesTotal = communication_mode == CommunicationMode::SYNC ? 1 + config().retry_times() : 1;
  int times = 0;
  std::string last_broker_name;
  for (; times < timesTotal; times++) {
    const auto& message_queue = SelectOneMessageQueue(topic_publish_info.get(), last_broker_name);
    last_broker_name = message_queue.broker_name();

    try {
      LOG_DEBUG_NEW("send to mq: {}", message_queue.toString());

      begin_time_prev = UtilAll::currentTimeMillis();
      if (times > 0) {
        // TODO: Reset topic with namespace during resend.
      }
      long cost_time = begin_time_prev - begin_time_first;
      if (timeout < cost_time) {
        break;
      }

      send_result = SendKernelImpl(message, message_queue, communication_mode, send_callback, topic_publish_info,
                                   timeout - cost_time);
      end_time = UtilAll::currentTimeMillis();
      UpdateFaultItem(message_queue.broker_name(), end_time - begin_time_prev, false);
      switch (communication_mode) {
        case CommunicationMode::SYNC:
          if (send_result->send_status() != SEND_OK) {
            if (config().retry_another_broker_when_not_store_ok()) {
              continue;
            }
          }
          return send_result;
        case CommunicationMode::ASYNC:
        case CommunicationMode::ONEWAY:
          return nullptr;
        default:
          break;
      }
    } catch (const std::exception& e) {
      // TODO: 区分异常类型
      end_time = UtilAll::currentTimeMillis();
      UpdateFaultItem(message_queue.broker_name(), end_time - begin_time_prev, true);
      LOG_WARN_NEW("send failed of times:{}, brokerName:{}. exception:{}", times, message_queue.broker_name(),
                   e.what());
      continue;
    }
  }  // end of for

  if (send_result != nullptr) {
    return send_result;
  }

  std::string error_message = "Send [" + UtilAll::to_string(times) + "] times, still failed, cost [" +
                              UtilAll::to_string(end_time - begin_time_first) + "]ms, Topic: " + message->topic();
  THROW_MQEXCEPTION(MQClientException, error_message, -1);
}

const MQMessageQueue& DefaultMQProducerImpl::SelectOneMessageQueue(const TopicPublishInfo* topic_publish_info,
                                                                   const std::string& last_broker_name) {
  return mq_fault_strategy_->SelectOneMessageQueue(topic_publish_info, last_broker_name);
}

void DefaultMQProducerImpl::UpdateFaultItem(const std::string& broker_name, long current_latency, bool isolation) {
  mq_fault_strategy_->UpdateFaultItem(broker_name, current_latency, isolation);
}

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendToQueueImpl(const MessagePtr& message,
                                                                   const MQMessageQueue& message_queue,
                                                                   CommunicationMode communication_mode,
                                                                   SendCallback send_callback,
                                                                   int64_t timeout) {
  Preprocess(*message, message_queue.topic(), config());

  return SendKernelImpl(message, message_queue, communication_mode, std::move(send_callback), nullptr, timeout);
}

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendSelectQueueImpl(const MessagePtr& message,
                                                                       const MessageQueueSelector& selector,
                                                                       CommunicationMode communication_mode,
                                                                       SendCallback send_callback,
                                                                       int64_t timeout) {
  Preprocess(*message, config());

  int64_t begin_time = UtilAll::currentTimeMillis();

  TopicPublishInfoPtr topic_publish_info = client_instance_->tryToFindTopicPublishInfo(message->topic());
  if (topic_publish_info != nullptr && topic_publish_info->ok()) {
    auto message_queue = selector(topic_publish_info->getMessageQueueList(), message);

    int64_t cost_time = UtilAll::currentTimeMillis() - begin_time;
    if (timeout < cost_time) {
      THROW_MQEXCEPTION(RemotingTooMuchRequestException, "sendSelectImpl call timeout", -1);
    }

    return SendKernelImpl(message, message_queue, communication_mode, std::move(send_callback), nullptr,
                          timeout - cost_time);
  }

  THROW_MQEXCEPTION(MQClientException, "No route info of this topic: " + message->topic(), -1);
}

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendKernelImpl(const MessagePtr& message,
                                                                  const MQMessageQueue& message_queue,
                                                                  CommunicationMode communication_mode,
                                                                  SendCallback send_callback,
                                                                  const TopicPublishInfoPtr& topic_publish_info,
                                                                  int64_t timeout) {
  int64_t begin_time = UtilAll::currentTimeMillis();

  std::string broker_addr = client_instance_->findBrokerAddressInPublish(message_queue.broker_name());
  if (broker_addr.empty()) {
    client_instance_->tryToFindTopicPublishInfo(message_queue.topic());
    broker_addr = client_instance_->findBrokerAddressInPublish(message_queue.broker_name());
  }

  if (!broker_addr.empty()) {
    try {
      int system_flag = 0;
      if (UtilAll::stob(message->getProperty(MQMessageConst::PROPERTY_ALREADY_COMPRESSED_FLAG))) {
        system_flag |= MessageSysFlag::COMPRESSED_FLAG;
      }
      if (UtilAll::stob(message->getProperty(MQMessageConst::PROPERTY_TRANSACTION_PREPARED))) {
        system_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
      }

      // TOOD: send message hook

#if __cplusplus >= 201402L
      auto request_header = std::make_unique<SendMessageRequestHeader>();
#else
      auto request_header = std::unique_ptr<SendMessageRequestHeader>(new SendMessageRequestHeader());
#endif
      request_header->producerGroup = config().group_name();
      request_header->topic = message->topic();
      request_header->defaultTopic = AUTO_CREATE_TOPIC_KEY_TOPIC;
      request_header->defaultTopicQueueNums = 4;
      request_header->queueId = message_queue.queue_id();
      request_header->sysFlag = system_flag;
      request_header->bornTimestamp = UtilAll::currentTimeMillis();
      request_header->flag = message->flag();
      request_header->properties = MessageDecoder::messageProperties2String(message->properties());
      request_header->reconsumeTimes = 0;
      request_header->unitMode = false;
      request_header->batch = message->isBatch();

      if (UtilAll::isRetryTopic(message_queue.topic())) {
        const auto& reconsume_time = MessageAccessor::getReconsumeTime(*message);
        if (!reconsume_time.empty()) {
          request_header->reconsumeTimes = std::stoi(reconsume_time);
          MessageAccessor::clearProperty(*message, MQMessageConst::PROPERTY_RECONSUME_TIME);
        }

        const auto& max_reconsume_times = MessageAccessor::getMaxReconsumeTimes(*message);
        if (!max_reconsume_times.empty()) {
          request_header->maxReconsumeTimes = std::stoi(max_reconsume_times);
          MessageAccessor::clearProperty(*message, MQMessageConst::PROPERTY_MAX_RECONSUME_TIMES);
        }
      }

      switch (communication_mode) {
        case CommunicationMode::ASYNC: {
          long costTimeAsync = UtilAll::currentTimeMillis() - begin_time;
          if (timeout < costTimeAsync) {
            THROW_MQEXCEPTION(RemotingTooMuchRequestException, "sendKernelImpl call timeout", -1);
          }
          return client_instance_->getMQClientAPIImpl()->sendMessage(
              broker_addr, message_queue.broker_name(), message, std::move(request_header), timeout, communication_mode,
              send_callback, topic_publish_info, client_instance_, config().retry_times_for_async(),
              shared_from_this());
        } break;
        case CommunicationMode::ONEWAY:
        case CommunicationMode::SYNC: {
          long costTimeSync = UtilAll::currentTimeMillis() - begin_time;
          if (timeout < costTimeSync) {
            THROW_MQEXCEPTION(RemotingTooMuchRequestException, "sendKernelImpl call timeout", -1);
          }
          return client_instance_->getMQClientAPIImpl()->sendMessage(broker_addr, message_queue.broker_name(), message,
                                                                     std::move(request_header), timeout,
                                                                     communication_mode, shared_from_this());
        } break;
        default:
          assert(false);
          break;
      }
    } catch (MQException& e) {
      throw;
    }
  }

  THROW_MQEXCEPTION(MQClientException, "The broker[" + message_queue.broker_name() + "] not exist", -1);
}

//
// Transaction

void DefaultMQProducerImpl::InitialTransactionEnv() {
  if (nullptr == check_transaction_executor_) {
#if __cplusplus >= 201402L
    check_transaction_executor_ = std::make_unique<thread_pool_executor>(1, false);
#else
    check_transaction_executor_ = std::unique_ptr<thread_pool_executor>{new thread_pool_executor(1, false)};
#endif
  }
  check_transaction_executor_->startup();
}

void DefaultMQProducerImpl::DestroyTransactionEnv() {
  check_transaction_executor_->shutdown();
}

TransactionSendResult DefaultMQProducerImpl::SendMessageInTransaction(const MessagePtr& message, void* arg) {
  try {
    return *SendInTransactionImpl(message, arg, config().send_msg_timeout());
  } catch (MQException& e) {
    LOG_ERROR_NEW("sendMessageInTransaction failed, exception:{}", e.what());
    throw;
  }
}

std::unique_ptr<TransactionSendResult> DefaultMQProducerImpl::SendInTransactionImpl(const MessagePtr& message,
                                                                                    void* arg,
                                                                                    int64_t timeout) {
  auto* transaction_listener = config().transaction_listener();
  if (nullptr == transaction_listener) {
    THROW_MQEXCEPTION(MQClientException, "transactionListener is null", -1);
  }

  MessageAccessor::putProperty(*message, MQMessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
  MessageAccessor::putProperty(*message, MQMessageConst::PROPERTY_PRODUCER_GROUP, config().group_name());

  std::unique_ptr<SendResult> send_result;
  try {
    send_result = SendDefaultImpl(message, CommunicationMode::SYNC, nullptr, timeout);
  } catch (MQException& e) {
    THROW_MQEXCEPTION(MQClientException, "send message Exception", -1);
  }

  LocalTransactionState local_transaction_state = LocalTransactionState::UNKNOWN;
  std::exception_ptr local_exception;
  switch (send_result->send_status()) {
    case SendStatus::SEND_OK:
      try {
        if (!send_result->transaction_id().empty()) {
          message->putProperty("__transactionId__", send_result->transaction_id());
        }
        const auto& transaction_id = message->getProperty(MQMessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (!transaction_id.empty()) {
          message->set_transaction_id(transaction_id);
        }
        local_transaction_state = transaction_listener->executeLocalTransaction(MQMessage(message), arg);
        if (local_transaction_state != LocalTransactionState::COMMIT_MESSAGE) {
          LOG_INFO_NEW("executeLocalTransaction return not COMMIT_MESSAGE, msg:{}", message->toString());
        }
      } catch (MQException& e) {
        LOG_INFO_NEW("executeLocalTransaction exception, msg:{}", message->toString());
        local_exception = std::current_exception();
      }
      break;
    case SendStatus::SEND_FLUSH_DISK_TIMEOUT:
    case SendStatus::SEND_FLUSH_SLAVE_TIMEOUT:
    case SendStatus::SEND_SLAVE_NOT_AVAILABLE:
      local_transaction_state = LocalTransactionState::ROLLBACK_MESSAGE;
      LOG_WARN_NEW("sendMessageInTransaction, send not ok, rollback, result:{}", send_result->toString());
      break;
    default:
      break;
  }

  try {
    EndTransaction(*send_result, local_transaction_state, local_exception);
  } catch (MQException& e) {
    LOG_WARN_NEW("local transaction execute {}, but end broker transaction failed: {}", local_transaction_state,
                 e.what());
  }

  // FIXME: setTransactionId will cause OOM?
  std::unique_ptr<TransactionSendResult> transaction_send_result(new TransactionSendResult(*send_result));
  transaction_send_result->set_transaction_id(message->transaction_id());
  transaction_send_result->set_local_transaction_state(local_transaction_state);
  return transaction_send_result;
}

void DefaultMQProducerImpl::checkTransactionState(const std::string& addr,
                                                  MessageExtPtr message,
                                                  CheckTransactionStateRequestHeader* check_request_header) {
  long transaction_state_table_offset = check_request_header->tranStateTableOffset;
  long commit_log_offset = check_request_header->commitLogOffset;
  const auto& message_id = check_request_header->msgId;
  const auto& transaction_id = check_request_header->transactionId;
  const auto& offset_message_id = check_request_header->offsetMsgId;

  check_transaction_executor_->submit([this, addr, message, transaction_state_table_offset, commit_log_offset,
                                       message_id, transaction_id, offset_message_id] {
    CheckTransactionStateImpl(addr, message, transaction_state_table_offset, commit_log_offset, message_id,
                              transaction_id, offset_message_id);
  });
}

void DefaultMQProducerImpl::CheckTransactionStateImpl(const std::string& address,
                                                      const MessageExtPtr& message,
                                                      long transaction_state_table_offset,
                                                      long commit_log_offset,
                                                      const std::string& message_id,
                                                      const std::string& transaction_id,
                                                      const std::string& offset_message_id) {
  auto* transaction_check_listener = config().transaction_listener();
  if (nullptr == transaction_check_listener) {
    LOG_WARN_NEW("CheckTransactionState, pick transactionCheckListener by group[{}] failed", config().group_name());
    return;
  }

  LocalTransactionState local_transaction_state = LocalTransactionState::UNKNOWN;
  std::exception_ptr exception = nullptr;
  try {
    local_transaction_state = transaction_check_listener->checkLocalTransaction(MQMessageExt(message));
  } catch (MQException& e) {
    LOG_ERROR_NEW("Broker call checkTransactionState, but checkLocalTransactionState exception, {}", e.what());
    exception = std::current_exception();
  }

  EndTransactionRequestHeader* request_header = new EndTransactionRequestHeader();
  request_header->commitLogOffset = commit_log_offset;
  request_header->producerGroup = config().group_name();
  request_header->tranStateTableOffset = transaction_state_table_offset;
  request_header->fromTransactionCheck = true;

  std::string unique_key = message->getProperty(MQMessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
  if (unique_key.empty()) {
    unique_key = message->msg_id();
  }

  request_header->msgId = unique_key;
  request_header->transactionId = transaction_id;
  switch (local_transaction_state) {
    case LocalTransactionState::COMMIT_MESSAGE:
      request_header->commitOrRollback = MessageSysFlag::TRANSACTION_COMMIT_TYPE;
      break;
    case LocalTransactionState::ROLLBACK_MESSAGE:
      request_header->commitOrRollback = MessageSysFlag::TRANSACTION_ROLLBACK_TYPE;
      LOG_WARN_NEW("when broker check, client rollback this transaction, {}", request_header->toString());
      break;
    case LocalTransactionState::UNKNOWN:
      request_header->commitOrRollback = MessageSysFlag::TRANSACTION_NOT_TYPE;
      LOG_WARN_NEW("when broker check, client does not know this transaction state, {}", request_header->toString());
      break;
    default:
      break;
  }

  std::string remark;
  if (exception != nullptr) {
    remark = "checkLocalTransactionState Exception: " + UtilAll::to_string(exception);
  }

  try {
    client_instance_->getMQClientAPIImpl()->endTransactionOneway(address, request_header, remark);
  } catch (std::exception& e) {
    LOG_ERROR_NEW("endTransactionOneway exception: {}", e.what());
  }
}

void DefaultMQProducerImpl::EndTransaction(SendResult& send_result,
                                           LocalTransactionState local_transaction_state,
                                           const std::exception_ptr& local_exception) {
  const auto& message_id = !send_result.offset_msg_id().empty() ? send_result.offset_msg_id() : send_result.msg_id();
  auto id = MessageDecoder::decodeMessageId(message_id);
  const auto& transaction_id = send_result.transaction_id();
  std::string broker_address = client_instance_->findBrokerAddressInPublish(send_result.message_queue().broker_name());
  EndTransactionRequestHeader* request_header = new EndTransactionRequestHeader();
  request_header->transactionId = transaction_id;
  request_header->commitLogOffset = id.getOffset();
  switch (local_transaction_state) {
    case LocalTransactionState::COMMIT_MESSAGE:
      request_header->commitOrRollback = MessageSysFlag::TRANSACTION_COMMIT_TYPE;
      break;
    case LocalTransactionState::ROLLBACK_MESSAGE:
      request_header->commitOrRollback = MessageSysFlag::TRANSACTION_ROLLBACK_TYPE;
      break;
    case LocalTransactionState::UNKNOWN:
      request_header->commitOrRollback = MessageSysFlag::TRANSACTION_NOT_TYPE;
      break;
    default:
      break;
  }
  request_header->producerGroup = config().group_name();
  request_header->tranStateTableOffset = send_result.queue_offset();
  request_header->msgId = send_result.msg_id();

  std::string remark =
      local_exception ? ("executeLocalTransactionBranch exception: " + UtilAll::to_string(local_exception)) : null;

  client_instance_->getMQClientAPIImpl()->endTransactionOneway(broker_address, request_header, remark);
}

//
// RPC

MessagePtr DefaultMQProducerImpl::Request(const MessagePtr& message, int64_t timeout) {
  return SyncRequestImpl(message, timeout,
                         [this](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
                           Send(message, std::move(send_callback), timeout);
                         });
}

void DefaultMQProducerImpl::Request(const MessagePtr& message,
                                    RequestCallback request_callback,
                                    int64_t timeout) noexcept {
  AsyncRequestImpl(message, timeout, std::move(request_callback),
                   [this](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
                     Send(message, std::move(send_callback), timeout);
                   });
}

MessagePtr DefaultMQProducerImpl::Request(const MessagePtr& message,
                                          const MQMessageQueue& message_queue,
                                          int64_t timeout) {
  return SyncRequestImpl(
      message, timeout,
      [this, &message_queue](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
        Send(message, message_queue, std::move(send_callback), timeout);
      });
}

void DefaultMQProducerImpl::Request(const MessagePtr& message,
                                    const MQMessageQueue& message_queue,
                                    RequestCallback request_callback,
                                    int64_t timeout) noexcept {
  AsyncRequestImpl(
      message, timeout, std::move(request_callback),
      [this, &message_queue](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
        Send(message, message_queue, std::move(send_callback), timeout);
      });
}

MessagePtr DefaultMQProducerImpl::Request(const MessagePtr& message, MessageQueueSelector selector, int64_t timeout) {
  return SyncRequestImpl(
      message, timeout,
      [this, &selector](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
        Send(message, std::move(selector), std::move(send_callback), timeout);
      });
}

void DefaultMQProducerImpl::Request(const MessagePtr& message,
                                    MessageQueueSelector selector,
                                    RequestCallback request_callback,
                                    int64_t timeout) noexcept {
  AsyncRequestImpl(
      message, timeout, std::move(request_callback),
      [this, &selector](const MessagePtr& message, SendCallback send_callback, int64_t timeout) mutable noexcept {
        Send(message, std::move(selector), std::move(send_callback), timeout);
      });
}

namespace {

const std::string& PrepareSendRequest(Message& message, int64_t timeout, MQClientInstance& client_instance) {
  MessageAccessor::putProperty(message, MQMessageConst::PROPERTY_CORRELATION_ID,
                               CorrelationIdUtil::createCorrelationId());
  MessageAccessor::putProperty(message, MQMessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
                               client_instance.getClientId());
  MessageAccessor::putProperty(message, MQMessageConst::PROPERTY_MESSAGE_TTL, UtilAll::to_string(timeout));

  bool has_route_data = client_instance.getTopicRouteData(message.topic()) != nullptr;
  if (!has_route_data) {
    int64_t begin_time = UtilAll::currentTimeMillis();
    client_instance.tryToFindTopicPublishInfo(message.topic());
    client_instance.sendHeartbeatToAllBrokerWithLock();
    int64_t cost_time = UtilAll::currentTimeMillis() - begin_time;
    if (cost_time > 500) {
      LOG_WARN_NEW("prepare send request for <{}> cost {} ms", message.topic(), cost_time);
    }
  }
  return message.getProperty(MQMessageConst::PROPERTY_CORRELATION_ID);
}

}  // namespace

MessagePtr DefaultMQProducerImpl::SyncRequestImpl(const MessagePtr& message,
                                                  int64_t timeout,
                                                  const AsyncSendFunction& send_delegate) {
  int64_t begin_time = UtilAll::currentTimeMillis();

  const auto& correlation_id = PrepareSendRequest(*message, timeout, *client_instance_);
  auto response_future = std::make_shared<RequestResponseFuture>(correlation_id, timeout, nullptr);
  RequestFutureTable::putRequestFuture(correlation_id, response_future);

  int64_t cost_timeout = UtilAll::currentTimeMillis() - begin_time;

  send_delegate(
      message,
      [response_future](ResultState<std::unique_ptr<SendResult>> state) noexcept {
        try {
          (void)state.GetResult();
          response_future->set_send_request_ok(true);
        } catch (...) {
          response_future->set_send_request_ok(false);
          response_future->putResponseMessage(nullptr);
          response_future->set_cause(std::current_exception());
        }
      },
      timeout - cost_timeout);

  auto response_message = response_future->waitResponseMessage(timeout - cost_timeout);
  if (response_message != nullptr) {
    return response_message;
  }

  (void)RequestFutureTable::removeRequestFuture(correlation_id);
  if (response_future->send_request_ok()) {
    std::string error_message = "send request message to <" + message->topic() +
                                "> OK, but wait reply message timeout, " + UtilAll::to_string(timeout) + " ms.";
    THROW_MQEXCEPTION(RequestTimeoutException, error_message, ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION);
  }

  std::string error_message = "send request message to <" + message->topic() + "> fail";
  THROW_MQEXCEPTION2(MQClientException, error_message, -1, response_future->cause());
}

void DefaultMQProducerImpl::AsyncRequestImpl(const MessagePtr& message,
                                             int64_t timeout,
                                             RequestCallback request_callback,
                                             const AsyncSendFunction& send_delegate) noexcept {
  int64_t begin_time = UtilAll::currentTimeMillis();

  const auto& correlation_id = PrepareSendRequest(*message, timeout, *client_instance_);
  auto response_future = std::make_shared<RequestResponseFuture>(correlation_id, timeout, std::move(request_callback));
  RequestFutureTable::putRequestFuture(correlation_id, response_future);

  int64_t cost_time = UtilAll::currentTimeMillis() - begin_time;

  // async
  send_delegate(
      message,
      [response_future](ResultState<std::unique_ptr<SendResult>> state) noexcept {
        try {
          (void)state.GetResult();
          response_future->set_send_request_ok(true);
        } catch (...) {
          response_future->set_cause(std::current_exception());
          if (RequestFutureTable::removeRequestFuture(response_future->correlation_id()) != nullptr) {
            response_future->set_send_request_ok(false);
            response_future->putResponseMessage(nullptr);
            try {
              response_future->executeRequestCallback();
            } catch (std::exception& e) {
              LOG_WARN_NEW("execute request_callback in request fail, and callback throw {}", e.what());
            }
          }
        }
      },
      timeout - cost_time);
}

}  // namespace rocketmq
