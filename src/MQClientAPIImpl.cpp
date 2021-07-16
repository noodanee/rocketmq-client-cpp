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
#include "MQClientAPIImpl.h"

#include <cassert>
#include <cstring>

#include <exception>
#include <memory>
#include <typeindex>
#include <utility>  // std::move

#include "ByteBuffer.hpp"
#include "ClientRemotingProcessor.h"
#include "MQClientConfig.h"
#include "MQClientInstance.h"
#include "MessageBatch.h"
#include "MessageClientIDSetter.h"
#include "MessageDecoder.h"
#include "PullResultExt.hpp"
#include "RemotingCommand.h"
#include "ResponseCode.h"
#include "ResultState.hpp"
#include "TcpRemotingClient.h"
#include "protocol/body/ConsumeQueueSet.hpp"
#include "protocol/body/ConsumerList.hpp"
#include "protocol/body/HeartbeatData.hpp"
#include "protocol/body/LockBatchResult.hpp"
#include "protocol/header/ConsumerSendMsgBackRequestHeader.hpp"
#include "protocol/header/CreateTopicRequestHeader.hpp"
#include "protocol/header/EndTransactionRequestHeader.hpp"
#include "protocol/header/GetConsumerListByGroupRequestHeader.hpp"
#include "protocol/header/GetEarliestMsgStoretimeRequestHeader.hpp"
#include "protocol/header/GetEarliestMsgStoretimeResponseHeader.hpp"
#include "protocol/header/GetMaxOffsetRequestHeader.hpp"
#include "protocol/header/GetMaxOffsetResponseHeader.hpp"
#include "protocol/header/GetMinOffsetRequestHeader.hpp"
#include "protocol/header/GetMinOffsetResponseHeader.hpp"
#include "protocol/header/GetRouteInfoRequestHeader.hpp"
#include "protocol/header/PullMessageRequestHeader.hpp"
#include "protocol/header/PullMessageResponseHeader.hpp"
#include "protocol/header/QueryConsumerOffsetRequestHeader.hpp"
#include "protocol/header/QueryConsumerOffsetResponseHeader.hpp"
#include "protocol/header/SearchOffsetRequestHeader.hpp"
#include "protocol/header/SearchOffsetResponseHeader.hpp"
#include "protocol/header/SendMessageRequestHeaderV2.hpp"
#include "protocol/header/SendMessageResponseHeader.hpp"
#include "protocol/header/UnregisterClientRequestHeader.hpp"
#include "protocol/header/UpdateConsumerOffsetRequestHeader.hpp"
#include "protocol/header/ViewMessageRequestHeader.hpp"
#include "utility/MakeUnique.hpp"

namespace {

constexpr int64_t kRpcTimeoutMillis = 3000;

}

namespace rocketmq {

MQClientAPIImpl::MQClientAPIImpl(ClientRemotingProcessor* client_remoting_processor,
                                 RPCHookPtr rpc_hook,
                                 const MQClientConfig& client_config)
    : remoting_client_(new TcpRemotingClient(client_config.tcp_transport_worker_thread_nums(),
                                             client_config.tcp_transport_connect_timeout(),
                                             client_config.tcp_transport_try_lock_timeout())) {
  remoting_client_->RegisterRPCHook(std::move(rpc_hook));
  remoting_client_->RegisterProcessor(CHECK_TRANSACTION_STATE, client_remoting_processor);
  remoting_client_->RegisterProcessor(NOTIFY_CONSUMER_IDS_CHANGED, client_remoting_processor);
  remoting_client_->RegisterProcessor(RESET_CONSUMER_CLIENT_OFFSET, client_remoting_processor);
  remoting_client_->RegisterProcessor(GET_CONSUMER_STATUS_FROM_CLIENT, client_remoting_processor);
  remoting_client_->RegisterProcessor(GET_CONSUMER_RUNNING_INFO, client_remoting_processor);
  remoting_client_->RegisterProcessor(CONSUME_MESSAGE_DIRECTLY, client_remoting_processor);
  remoting_client_->RegisterProcessor(PUSH_REPLY_MESSAGE_TO_CLIENT, client_remoting_processor);
}

std::vector<std::string> MQClientAPIImpl::GetNameServerAddressList() const {
  return remoting_client_->GetNameServerAddressList();
}

void MQClientAPIImpl::UpdateNameServerAddressList(const std::string& address_list) {
  remoting_client_->UpdateNameServerAddressList(address_list);
}

void MQClientAPIImpl::Start() {
  remoting_client_->Start();
}

void MQClientAPIImpl::Shutdown() {
  remoting_client_->Shutdown();
}

std::unique_ptr<TopicRouteData> MQClientAPIImpl::GetTopicRouteInfoFromNameServer(std::string topic,
                                                                                 int64_t timeout_millis) {
  auto request_header = MakeUnique<GetRouteInfoRequestHeader>(std::move(topic));

  RemotingCommand request(GET_ROUTEINFO_BY_TOPIC, std::move(request_header));

  auto response = remoting_client_->InvokeSync(null, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto response_body = response->body();
    if (response_body != nullptr && response_body->size() > 0) {
      return TopicRouteData::Decode(*response_body);
    }
  }

  THROW_MQEXCEPTION(MQClientException, response->remark(), response->code());
}

namespace {

RemotingCommand MakeSendMessageRequest(const MessagePtr& message,
                                       std::unique_ptr<SendMessageRequestHeader> request_header) {
  int command_code = SEND_MESSAGE;
  if (message->getProperty(MQMessageConst::PROPERTY_MESSAGE_TYPE) == REPLY_MESSAGE_FLAG) {
    command_code = SEND_REPLY_MESSAGE_V2;
  } else if (message->isBatch()) {
    command_code = SEND_BATCH_MESSAGE;
  } else {
    command_code = SEND_MESSAGE_V2;
  }

  auto header = [command_code, &request_header]() -> std::unique_ptr<CommandCustomHeader> {
    if (command_code != SEND_MESSAGE && command_code != SEND_REPLY_MESSAGE) {
      return SendMessageRequestHeaderV2::CreateSendMessageRequestHeaderV2(std::move(request_header));
    }
    return std::move(request_header);
  }();

  RemotingCommand request(command_code, std::move(header));
  request.set_body(message->body());

  return request;
}

std::unique_ptr<SendResult> ProcessSendMessageResponse(const std::string& broker_name,
                                                       const Message& message,
                                                       RemotingCommand& response) {
  auto send_status = [&response]() -> SendStatus {
    switch (response.code()) {
      case SUCCESS:
        return SendStatus::kSendOk;
      case FLUSH_DISK_TIMEOUT:
        return SendStatus::kSendFlushDiskTimeout;
      case FLUSH_SLAVE_TIMEOUT:
        return SendStatus::kSendFlushSlaveTimeout;
      case SLAVE_NOT_AVAILABLE:
        return SendStatus::kSendSlaveNotAvailable;
      default:
        THROW_MQEXCEPTION(MQBrokerException, response.remark(), response.code());
    }
  }();

  auto* response_header = response.DecodeHeader<SendMessageResponseHeader>();
  assert(response_header != nullptr);

  MessageQueue message_queue(message.topic(), broker_name, response_header->queue_id);

  std::string unique_message_id = MessageClientIDSetter::getUniqID(message);

  // MessageBatch
  if (message.isBatch()) {
    const auto& batched_messages = dynamic_cast<const MessageBatch&>(message).messages();
    unique_message_id.clear();
    unique_message_id.reserve(33 * batched_messages.size() + 1);
    for (const auto& batched_message : batched_messages) {
      unique_message_id.append(MessageClientIDSetter::getUniqID(*batched_message));
      unique_message_id.append(",");
    }
    if (!unique_message_id.empty()) {
      unique_message_id.resize(unique_message_id.length() - 1);
    }
  }

  return MakeUnique<SendResult>(send_status, unique_message_id, response_header->message_id, message_queue,
                                response_header->queue_offset, response_header->transaction_id);
}

}  // namespace

std::unique_ptr<SendResult> MQClientAPIImpl::SendMessageSync(const std::string& broker_address,
                                                             const std::string& broker_name,
                                                             const MessagePtr& message,
                                                             std::unique_ptr<SendMessageRequestHeader> request_header,
                                                             int64_t timeout_millis) {
  RemotingCommand request = MakeSendMessageRequest(message, std::move(request_header));
  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  return ProcessSendMessageResponse(broker_name, *message, *response);
}

void MQClientAPIImpl::SendMessageAsync(const std::string& broker_address,
                                       const std::string& broker_name,
                                       const MessagePtr& message,
                                       std::unique_ptr<SendMessageRequestHeader> request_header,
                                       int64_t timeout_millis,
                                       SendCallback send_callback) {
  assert(send_callback != nullptr);
  RemotingCommand request = MakeSendMessageRequest(message, std::move(request_header));
  remoting_client_->InvokeAsync(
      broker_address, std::move(request),
#if __cplusplus >= 201402L
      [broker_name, message, send_callback = std::move(send_callback)]
#else
      [broker_name, message, send_callback]
#endif
      (ResultState<std::unique_ptr<RemotingCommand>> state) {
        try {
          auto response = std::move(state.GetResult());
          auto send_result = ProcessSendMessageResponse(broker_name, *message, *response);
          assert(send_result != nullptr);
          send_callback({std::move(send_result)});
        } catch (...) {
          send_callback({std::current_exception()});
        }
      },
      timeout_millis);
}

void MQClientAPIImpl::SendMessageOneway(const std::string& broker_address,
                                        const MessagePtr& message,
                                        std::unique_ptr<SendMessageRequestHeader> request_header) {
  RemotingCommand request = MakeSendMessageRequest(message, std::move(request_header));
  remoting_client_->InvokeOneway(broker_address, std::move(request));
}

std::unique_ptr<SendResult> MQClientAPIImpl::SendMessage(const std::string& broker_address,
                                                         const std::string& broker_name,
                                                         const MessagePtr& message,
                                                         std::unique_ptr<SendMessageRequestHeader> request_header,
                                                         int64_t timeout_millis,
                                                         CommunicationMode communication_mode,
                                                         SendCallback send_callback) {
  switch (communication_mode) {
    case CommunicationMode::kSync:
      return SendMessageSync(broker_address, broker_name, message, std::move(request_header), timeout_millis);
    case CommunicationMode::kAsync:
      SendMessageAsync(broker_address, broker_name, message, std::move(request_header), timeout_millis,
                       std::move(send_callback));
      return nullptr;
    case CommunicationMode::kOneway:
      SendMessageOneway(broker_address, message, std::move(request_header));
      return nullptr;
  }
}

void MQClientAPIImpl::EndTransactionOneway(const std::string& broker_address,
                                           std::unique_ptr<EndTransactionRequestHeader> request_header,
                                           std::string remark) {
  RemotingCommand request(END_TRANSACTION, std::move(request_header));
  request.set_remark(std::move(remark));
  remoting_client_->InvokeOneway(broker_address, std::move(request));
}

void MQClientAPIImpl::ConsumerSendMessageBack(const std::string& broker_address,
                                              const MessageExtPtr& message,
                                              std::string consumer_group,
                                              int32_t delay_level,
                                              int32_t max_consume_retry_times,
                                              int64_t timeout_millis) {
  auto request_header =
      MakeUnique<ConsumerSendMsgBackRequestHeader>(message->commit_log_offset(), std::move(consumer_group), delay_level,
                                                   message->msg_id(), message->topic(), max_consume_retry_times);

  RemotingCommand request(CONSUMER_SEND_MSG_BACK, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    return;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

namespace {

RemotingCommand MakePullMessageRequest(std::unique_ptr<PullMessageRequestHeader> request_header) {
  return RemotingCommand(PULL_MESSAGE, std::move(request_header));
}

std::unique_ptr<PullResultExt> ProcessPullResponse(RemotingCommand& response) {
  auto pull_status = [&response]() -> PullStatus {
    switch (response.code()) {
      case SUCCESS:
        return PullStatus::kFound;
      case PULL_NOT_FOUND:
        return PullStatus::kNoNewMessage;
      case PULL_RETRY_IMMEDIATELY:
        if ("OFFSET_OVERFLOW_BADLY" == response.remark()) {
          return PullStatus::kNoLatestMessage;
        } else {
          return PullStatus::kNoMatchedMessage;
        }
      case PULL_OFFSET_MOVED:
        return PullStatus::kOffsetIllegal;
      default:
        THROW_MQEXCEPTION(MQBrokerException, response.remark(), response.code());
    }
  }();

  auto* response_header = response.DecodeHeader<PullMessageResponseHeader>();
  assert(response_header != nullptr);

  return MakeUnique<PullResultExt>(pull_status, response_header->next_begin_offset, response_header->min_offset,
                                   response_header->max_offset, response_header->suggest_which_broker_id,
                                   response.body());
}

}  // namespace

std::unique_ptr<PullResultExt> MQClientAPIImpl::PullMessageSync(
    const std::string& broker_address,
    std::unique_ptr<PullMessageRequestHeader> request_header,
    int64_t timeout_millis) {
  RemotingCommand request = MakePullMessageRequest(std::move(request_header));
  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  return ProcessPullResponse(*response);
}

void MQClientAPIImpl::PullMessageAsync(const std::string& broker_address,
                                       std::unique_ptr<PullMessageRequestHeader> request_header,
                                       int64_t timeout_millis,
                                       PullCallback pull_callback) {
  assert(pull_callback != nullptr);
  RemotingCommand request = MakePullMessageRequest(std::move(request_header));
  remoting_client_->InvokeAsync(
      broker_address, std::move(request),
#if __cplusplus >= 201402L
      [pull_callback = std::move(pull_callback)]
#else
      [pull_callback]
#endif
      (ResultState<std::unique_ptr<RemotingCommand>> state) noexcept {
        try {
          auto response = std::move(state.GetResult());
          auto pull_result = ProcessPullResponse(*response);
          assert(pull_result != nullptr);
          pull_callback({std::move(pull_result)});
        } catch (...) {
          pull_callback({std::current_exception()});
        }
      },
      timeout_millis);
}

std::unique_ptr<PullResultExt> MQClientAPIImpl::PullMessage(const std::string& broker_address,
                                                            std::unique_ptr<PullMessageRequestHeader> request_header,
                                                            int64_t timeoutMillis,
                                                            CommunicationMode communication_mode,
                                                            PullCallback pull_callback) {
  switch (communication_mode) {
    case CommunicationMode::kAsync:
      PullMessageAsync(broker_address, std::move(request_header), timeoutMillis, std::move(pull_callback));
      return nullptr;
    case CommunicationMode::kSync:
      return PullMessageSync(broker_address, std::move(request_header), timeoutMillis);
    default:
      assert(false);
      return nullptr;
  }
}

std::vector<std::string> MQClientAPIImpl::GetConsumerIdListByGroup(const std::string& brocker_address,
                                                                   std::string consumer_group,
                                                                   int64_t timeout_millis) {
  auto request_header = MakeUnique<GetConsumerListByGroupRequestHeader>(std::move(consumer_group));

  RemotingCommand request(GET_CONSUMER_LIST_BY_GROUP, std::move(request_header));

  auto response = remoting_client_->InvokeSync(brocker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto response_body = response->body();
    if (response_body != nullptr && response_body->size() > 0) {
      auto consumer_list = ConsumerList::Decode(*response_body);
      return std::move(consumer_list->consumer_id_list);
    }
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

namespace {

RemotingCommand MakeUpdateConsumerOffsetRequest(std::string consumer_group,
                                                std::string topic,
                                                int32_t queue_id,
                                                int64_t commit_offset) {
  auto request_header = MakeUnique<UpdateConsumerOffsetRequestHeader>(std::move(consumer_group), std::move(topic),
                                                                      queue_id, commit_offset);
  return RemotingCommand(UPDATE_CONSUMER_OFFSET, std::move(request_header));
}

}  // namespace

void MQClientAPIImpl::UpdateConsumerOffset(const std::string& broker_address,
                                           std::string consumer_group,
                                           std::string topic,
                                           int32_t queue_id,
                                           int64_t commit_offset,
                                           int64_t timeout_millis) {
  RemotingCommand request =
      MakeUpdateConsumerOffsetRequest(std::move(consumer_group), std::move(topic), queue_id, commit_offset);

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    return;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::UpdateConsumerOffsetOneway(const std::string& broker_address,
                                                 std::string consumer_group,
                                                 std::string topic,
                                                 int32_t queue_id,
                                                 int64_t commit_offset) {
  RemotingCommand request =
      MakeUpdateConsumerOffsetRequest(std::move(consumer_group), std::move(topic), queue_id, commit_offset);
  remoting_client_->InvokeOneway(broker_address, std::move(request));
}

int64_t MQClientAPIImpl::QueryConsumerOffset(const std::string& broker_address,
                                             std::string consumer_group,
                                             std::string topic,
                                             int32_t queue_id,
                                             int64_t timeout_millis) {
  auto request_header =
      MakeUnique<QueryConsumerOffsetRequestHeader>(std::move(consumer_group), std::move(topic), queue_id);

  RemotingCommand request(QUERY_CONSUMER_OFFSET, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto* response_header = response->DecodeHeader<QueryConsumerOffsetResponseHeader>();
    assert(response_header != nullptr);
    return response_header->offset;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::GetMaxOffset(const std::string& broker_address,
                                      std::string topic,
                                      int32_t queue_id,
                                      int64_t timeout_millis) {
  auto request_header = MakeUnique<GetMaxOffsetRequestHeader>(std::move(topic), queue_id);

  RemotingCommand request(GET_MAX_OFFSET, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto* response_header = response->DecodeHeader<GetMaxOffsetResponseHeader>();
    assert(response_header != nullptr);
    return response_header->offset;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::GetMinOffset(const std::string& broker_address,
                                      std::string topic,
                                      int32_t queue_id,
                                      int64_t timeout_millis) {
  auto request_header = MakeUnique<GetMinOffsetRequestHeader>(std::move(topic), queue_id);

  RemotingCommand request(GET_MIN_OFFSET, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto* response_header = response->DecodeHeader<GetMinOffsetResponseHeader>();
    assert(response_header != nullptr);
    return response_header->offset;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::SearchOffset(const std::string& broker_address,
                                      std::string topic,
                                      int32_t queue_id,
                                      int64_t timestamp,
                                      int64_t timeout_millis) {
  auto request_header = MakeUnique<SearchOffsetRequestHeader>(std::move(topic), queue_id, timestamp);

  RemotingCommand request(SEARCH_OFFSET_BY_TIMESTAMP, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto* response_header = response->DecodeHeader<SearchOffsetResponseHeader>();
    assert(response_header != nullptr);
    return response_header->offset;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

std::vector<MessageQueue> MQClientAPIImpl::LockBatchMQ(const std::string& broker_address,
                                                       std::string consumer_group,
                                                       std::string client_id,
                                                       std::vector<MessageQueue> message_queue_set,
                                                       int64_t timeout_millis) {
  ConsumeQueueSet consume_queue_set{std::move(consumer_group), std::move(client_id), std::move(message_queue_set)};

  RemotingCommand request(LOCK_BATCH_MQ, nullptr);
  request.set_body(consume_queue_set.Encode());

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto request_body = response->body();
    if (request_body != nullptr && request_body->size() > 0) {
      std::unique_ptr<LockBatchResult> result = LockBatchResult::Decode(*request_body);
      return std::move(result->lock_ok_message_queue_set);
    }
    return {};
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::UnlockBatchMQSync(const std::string& broker_address,
                                        std::string consumer_group,
                                        std::string client_id,
                                        std::vector<MessageQueue> message_queue_set,
                                        int64_t timeout_millis) {
  ConsumeQueueSet consume_queue_set{std::move(consumer_group), std::move(client_id), std::move(message_queue_set)};

  RemotingCommand request(UNLOCK_BATCH_MQ, nullptr);
  request.set_body(consume_queue_set.Encode());

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    return;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::UnlockBatchMQOneway(const std::string& broker_address,
                                          std::string consumer_group,
                                          std::string client_id,
                                          std::vector<MessageQueue> message_queue_set) {
  ConsumeQueueSet consume_queue_set{std::move(consumer_group), std::move(client_id), std::move(message_queue_set)};

  RemotingCommand request(UNLOCK_BATCH_MQ, nullptr);
  request.set_body(consume_queue_set.Encode());

  remoting_client_->InvokeOneway(broker_address, std::move(request));
}

void MQClientAPIImpl::UnlockBatchMQ(const std::string& broker_address,
                                    std::string consumer_group,
                                    std::string client_id,
                                    std::vector<MessageQueue> message_queue_set,
                                    int64_t timeout_millis,
                                    bool oneway) {
  if (oneway) {
    UnlockBatchMQOneway(broker_address, std::move(consumer_group), std::move(client_id), std::move(message_queue_set));
  } else {
    UnlockBatchMQSync(broker_address, std::move(consumer_group), std::move(client_id), std::move(message_queue_set),
                      timeout_millis);
  }
}

void MQClientAPIImpl::SendHearbeat(const std::string& broker_address,
                                   const HeartbeatData& heartbeat_data,
                                   int64_t timeout_millis) {
  RemotingCommand request(HEART_BEAT, nullptr);
  request.set_body(heartbeat_data.Encode());

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    LOG_DEBUG_NEW("sendHeartbeat to broker:{} success", broker_address);
    return;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::UnregisterClient(const std::string& broker_address,
                                       std::string client_id,
                                       std::string producer_group,
                                       std::string consumer_group) {
  LOG_INFO_NEW("unregisterClient to broker: {}", broker_address);

  auto request_header = MakeUnique<UnregisterClientRequestHeader>(std::move(client_id), std::move(producer_group),
                                                                  std::move(consumer_group));

  RemotingCommand request(UNREGISTER_CLIENT, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), kRpcTimeoutMillis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    LOG_INFO_NEW("unregisterClient to: {} success", broker_address);
    return;
  }

  LOG_WARN_NEW("unregisterClient fail: {}, {}", response->remark(), response->code());
  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::CreateTopic(const std::string& broker_address,
                                  std::string default_topic,
                                  const TopicConfig& topic_config) {
  auto request_header = MakeUnique<CreateTopicRequestHeader>(
      topic_config.topic_name(), std::move(default_topic), topic_config.read_queue_nums(),
      topic_config.write_queue_nums(), topic_config.perm(),
      topic_config.topic_filter_type() == SINGLE_TAG ? "SINGLE_TAG" : "MULTI_TAG");

  RemotingCommand request(UPDATE_AND_CREATE_TOPIC, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), kRpcTimeoutMillis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    return;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::GetEarliestMsgStoretime(const std::string& broker_address,
                                                 std::string topic,
                                                 int32_t queue_id,
                                                 int64_t timeout_millis) {
  auto request_header = MakeUnique<GetEarliestMsgStoretimeRequestHeader>(std::move(topic), queue_id);

  RemotingCommand request(GET_EARLIEST_MSG_STORETIME, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto* response_header = response->DecodeHeader<GetEarliestMsgStoretimeResponseHeader>();
    assert(response_header != nullptr);
    return response_header->timestamp;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

MessageExtPtr MQClientAPIImpl::ViewMessage(const std::string& broker_address,
                                           int64_t physical_offset,
                                           int64_t timeout_millis) {
  auto request_header = MakeUnique<ViewMessageRequestHeader>(physical_offset);

  RemotingCommand request(VIEW_MESSAGE_BY_ID, std::move(request_header));

  auto response = remoting_client_->InvokeSync(broker_address, std::move(request), timeout_millis);
  assert(response != nullptr);
  if (response->code() == SUCCESS) {
    auto response_body = response->body();
    if (response_body != nullptr && response_body->size() > 0) {
      auto message_ext = MessageDecoder::clientDecode(*ByteBuffer::wrap(response_body), true);
      // TODO: check namespace
      return message_ext;
    }
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

}  // namespace rocketmq
