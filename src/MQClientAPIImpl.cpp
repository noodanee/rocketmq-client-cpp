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

#include "ClientRemotingProcessor.h"
#include "MQClientInstance.h"
#include "MessageBatch.h"
#include "MessageClientIDSetter.h"
#include "PullResultExt.hpp"
#include "ResultState.hpp"
#include "TcpRemotingClient.h"
#include "protocol/body/ConsumeQueueSet.hpp"
#include "protocol/body/ConsumerList.hpp"
#include "protocol/body/LockBatchResult.hpp"
#include "protocol/header/ConsumerSendMsgBackRequestHeader.hpp"
#include "protocol/header/CreateTopicRequestHeader.hpp"
#include "protocol/header/GetConsumerListByGroupRequestHeader.hpp"
#include "protocol/header/GetEarliestMsgStoretimeRequestHeader.hpp"
#include "protocol/header/GetEarliestMsgStoretimeResponseHeader.hpp"
#include "protocol/header/GetMaxOffsetRequestHeader.hpp"
#include "protocol/header/GetMaxOffsetResponseHeader.hpp"
#include "protocol/header/GetMinOffsetRequestHeader.hpp"
#include "protocol/header/GetMinOffsetResponseHeader.hpp"
#include "protocol/header/GetRouteInfoRequestHeader.hpp"
#include "protocol/header/PullMessageResponseHeader.hpp"
#include "protocol/header/QueryConsumerOffsetResponseHeader.hpp"
#include "protocol/header/SearchOffsetRequestHeader.hpp"
#include "protocol/header/SearchOffsetResponseHeader.hpp"
#include "protocol/header/SendMessageRequestHeaderV2.hpp"
#include "protocol/header/SendMessageResponseHeader.hpp"
#include "protocol/header/UnregisterClientRequestHeader.hpp"
#include "protocol/header/UpdateConsumerOffsetRequestHeader.hpp"
#include "protocol/header/ViewMessageRequestHeader.hpp"

namespace {

constexpr int64_t kRpcTimeoutMillis = 3000;

}

namespace rocketmq {

MQClientAPIImpl::MQClientAPIImpl(ClientRemotingProcessor* clientRemotingProcessor,
                                 RPCHookPtr rpcHook,
                                 const MQClientConfig& clientConfig)
    : remoting_client_(new TcpRemotingClient(clientConfig.tcp_transport_worker_thread_nums(),
                                             clientConfig.tcp_transport_connect_timeout(),
                                             clientConfig.tcp_transport_try_lock_timeout())) {
  remoting_client_->RegisterRPCHook(std::move(rpcHook));
  remoting_client_->RegisterProcessor(CHECK_TRANSACTION_STATE, clientRemotingProcessor);
  remoting_client_->RegisterProcessor(NOTIFY_CONSUMER_IDS_CHANGED, clientRemotingProcessor);
  remoting_client_->RegisterProcessor(RESET_CONSUMER_CLIENT_OFFSET, clientRemotingProcessor);
  remoting_client_->RegisterProcessor(GET_CONSUMER_STATUS_FROM_CLIENT, clientRemotingProcessor);
  remoting_client_->RegisterProcessor(GET_CONSUMER_RUNNING_INFO, clientRemotingProcessor);
  remoting_client_->RegisterProcessor(CONSUME_MESSAGE_DIRECTLY, clientRemotingProcessor);
  remoting_client_->RegisterProcessor(PUSH_REPLY_MESSAGE_TO_CLIENT, clientRemotingProcessor);
}

MQClientAPIImpl::~MQClientAPIImpl() = default;

void MQClientAPIImpl::start() {
  remoting_client_->Start();
}

void MQClientAPIImpl::shutdown() {
  remoting_client_->Shutdown();
}

void MQClientAPIImpl::updateNameServerAddressList(const std::string& addrs) {
  // TODO: split addrs
  remoting_client_->UpdateNameServerAddressList(addrs);
}

void MQClientAPIImpl::createTopic(const std::string& addr, const std::string& defaultTopic, TopicConfig topicConfig) {
  auto* requestHeader = new CreateTopicRequestHeader();
  requestHeader->topic = topicConfig.topic_name();
  requestHeader->default_topic = defaultTopic;
  requestHeader->read_queue_nums = topicConfig.read_queue_nums();
  requestHeader->write_queue_nums = topicConfig.write_queue_nums();
  requestHeader->perm = topicConfig.perm();
  requestHeader->topic_filter_type = topicConfig.topic_filter_type();

  RemotingCommand request(UPDATE_AND_CREATE_TOPIC, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), kRpcTimeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      return;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

std::unique_ptr<SendResult> MQClientAPIImpl::sendMessage(const std::string& addr,
                                                         const std::string& brokerName,
                                                         const MessagePtr& msg,
                                                         std::unique_ptr<SendMessageRequestHeader> requestHeader,
                                                         int timeoutMillis,
                                                         CommunicationMode communicationMode,
                                                         SendCallback sendCallback) {
  int code = SEND_MESSAGE;
  if (msg->getProperty(MQMessageConst::PROPERTY_MESSAGE_TYPE) == REPLY_MESSAGE_FLAG) {
    code = SEND_REPLY_MESSAGE_V2;
  } else if (msg->isBatch()) {
    code = SEND_BATCH_MESSAGE;
  } else {
    code = SEND_MESSAGE_V2;
  }

  std::unique_ptr<CommandCustomHeader> header;
  if (code != SEND_MESSAGE && code != SEND_REPLY_MESSAGE) {
    header = SendMessageRequestHeaderV2::CreateSendMessageRequestHeaderV2(std::move(requestHeader));
  } else {
    header = std::move(requestHeader);
  }

  RemotingCommand request(code, header.release());
  request.set_body(msg->body());

  switch (communicationMode) {
    case CommunicationMode::kOneway:
      remoting_client_->InvokeOneway(addr, std::move(request));
      return nullptr;
    case CommunicationMode::kAsync:
      sendMessageAsync(addr, brokerName, msg, std::move(request), std::move(sendCallback), timeoutMillis);
      return nullptr;
    case CommunicationMode::kSync:
      return sendMessageSync(addr, brokerName, msg, std::move(request), timeoutMillis);
    default:
      assert(false);
      break;
  }

  return nullptr;
}

void MQClientAPIImpl::sendMessageAsync(const std::string& addr,
                                       const std::string& brokerName,
                                       const MessagePtr& msg,
                                       RemotingCommand request,
                                       SendCallback sendCallback,
                                       int64_t timeoutMillis) {
  assert(sendCallback != nullptr);
  remoting_client_->InvokeAsync(
      addr, std::move(request),
#if __cplusplus >= 201402L
      [this, brokerName, msg, sendCallback = std::move(sendCallback)]
#else
      [this, brokerName, msg, sendCallback]
#endif
      (ResultState<std::unique_ptr<RemotingCommand>> state) {
        try {
          auto response = std::move(state.GetResult());
          auto send_result = processSendResponse(brokerName, *msg, *response);
          assert(send_result != nullptr);
          sendCallback({std::move(send_result)});
        } catch (...) {
          sendCallback({std::current_exception()});
        }
      },
      timeoutMillis);
}

std::unique_ptr<SendResult> MQClientAPIImpl::sendMessageSync(const std::string& addr,
                                                             const std::string& brokerName,
                                                             const MessagePtr& msg,
                                                             RemotingCommand request,
                                                             int timeoutMillis) {
  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  return processSendResponse(brokerName, *msg, *response);
}

std::unique_ptr<SendResult> MQClientAPIImpl::processSendResponse(const std::string& brokerName,
                                                                 Message& msg,
                                                                 RemotingCommand& response) {
  SendStatus sendStatus = SEND_OK;
  switch (response.code()) {
    case FLUSH_DISK_TIMEOUT:
      sendStatus = SEND_FLUSH_DISK_TIMEOUT;
      break;
    case FLUSH_SLAVE_TIMEOUT:
      sendStatus = SEND_FLUSH_SLAVE_TIMEOUT;
      break;
    case SLAVE_NOT_AVAILABLE:
      sendStatus = SEND_SLAVE_NOT_AVAILABLE;
      break;
    case SUCCESS:
      sendStatus = SEND_OK;
      break;
    default:
      THROW_MQEXCEPTION(MQBrokerException, response.remark(), response.code());
      return nullptr;
  }

  auto* responseHeader = response.decodeCommandCustomHeader<SendMessageResponseHeader>();
  assert(responseHeader != nullptr);

  MQMessageQueue messageQueue(msg.topic(), brokerName, responseHeader->queue_id);

  std::string uniqMsgId = MessageClientIDSetter::getUniqID(msg);

  // MessageBatch
  if (msg.isBatch()) {
    const auto& messages = dynamic_cast<MessageBatch&>(msg).messages();
    uniqMsgId.clear();
    uniqMsgId.reserve(33 * messages.size() + 1);
    for (const auto& message : messages) {
      uniqMsgId.append(MessageClientIDSetter::getUniqID(*message));
      uniqMsgId.append(",");
    }
    if (!uniqMsgId.empty()) {
      uniqMsgId.resize(uniqMsgId.length() - 1);
    }
  }

  std::unique_ptr<SendResult> sendResult(
      new SendResult(sendStatus, uniqMsgId, responseHeader->message_id, messageQueue, responseHeader->queue_offset));
  sendResult->set_transaction_id(responseHeader->transaction_id);

  return sendResult;
}

std::unique_ptr<PullResult> MQClientAPIImpl::pullMessage(const std::string& addr,
                                                         std::unique_ptr<PullMessageRequestHeader> requestHeader,
                                                         int timeoutMillis,
                                                         CommunicationMode communicationMode,
                                                         PullCallback pullCallback) {
  RemotingCommand request(PULL_MESSAGE, requestHeader.release());

  switch (communicationMode) {
    case CommunicationMode::kAsync:
      pullMessageAsync(addr, std::move(request), timeoutMillis, std::move(pullCallback));
      return nullptr;
    case CommunicationMode::kSync:
      return pullMessageSync(addr, std::move(request), timeoutMillis);
    default:
      assert(false);
      return nullptr;
  }
}

void MQClientAPIImpl::pullMessageAsync(const std::string& addr,
                                       RemotingCommand request,
                                       int timeoutMillis,
                                       PullCallback pullCallback) {
  assert(pullCallback != nullptr);
  remoting_client_->InvokeAsync(
      addr, std::move(request),
#if __cplusplus >= 201402L
      [this, pullCallback = std::move(pullCallback)]
#else
      [this, pullCallback]
#endif
      (ResultState<std::unique_ptr<RemotingCommand>> state) noexcept {
        try {
          auto response = std::move(state.GetResult());
          auto pull_result = processPullResponse(*response);
          assert(pull_result != nullptr);
          pullCallback({std::move(pull_result)});
        } catch (...) {
          pullCallback({std::current_exception()});
        }
      },
      timeoutMillis);
}

std::unique_ptr<PullResult> MQClientAPIImpl::pullMessageSync(const std::string& addr,
                                                             RemotingCommand request,
                                                             int timeoutMillis) {
  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  return processPullResponse(*response);
}

std::unique_ptr<PullResult> MQClientAPIImpl::processPullResponse(RemotingCommand& response) {
  PullStatus pullStatus = NO_NEW_MSG;
  switch (response.code()) {
    case SUCCESS:
      pullStatus = FOUND;
      break;
    case PULL_NOT_FOUND:
      pullStatus = NO_NEW_MSG;
      break;
    case PULL_RETRY_IMMEDIATELY:
      if ("OFFSET_OVERFLOW_BADLY" == response.remark()) {
        pullStatus = NO_LATEST_MSG;
      } else {
        pullStatus = NO_MATCHED_MSG;
      }
      break;
    case PULL_OFFSET_MOVED:
      pullStatus = OFFSET_ILLEGAL;
      break;
    default:
      THROW_MQEXCEPTION(MQBrokerException, response.remark(), response.code());
  }

  auto* responseHeader = response.decodeCommandCustomHeader<PullMessageResponseHeader>();
  assert(responseHeader != nullptr);

  return std::unique_ptr<PullResult>(new PullResultExt(pullStatus, responseHeader->next_begin_offset,
                                                       responseHeader->min_offset, responseHeader->max_offset,
                                                       (int)responseHeader->suggest_which_broker_id, response.body()));
}

MQMessageExt MQClientAPIImpl::viewMessage(const std::string& addr, int64_t phyoffset, int timeoutMillis) {
  auto* requestHeader = new ViewMessageRequestHeader();
  requestHeader->offset = phyoffset;

  RemotingCommand request(VIEW_MESSAGE_BY_ID, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      // TODO: ...
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::searchOffset(const std::string& addr,
                                      const std::string& topic,
                                      int queueId,
                                      int64_t timestamp,
                                      int timeoutMillis) {
  auto* requestHeader = new SearchOffsetRequestHeader();
  requestHeader->topic = topic;
  requestHeader->queue_id = queueId;
  requestHeader->timestamp = timestamp;

  RemotingCommand request(SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto* responseHeader = response->decodeCommandCustomHeader<SearchOffsetResponseHeader>();
      assert(responseHeader != nullptr);
      return responseHeader->offset;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::getMaxOffset(const std::string& addr,
                                      const std::string& topic,
                                      int queueId,
                                      int timeoutMillis) {
  auto* requestHeader = new GetMaxOffsetRequestHeader();
  requestHeader->topic = topic;
  requestHeader->queue_id = queueId;

  RemotingCommand request(GET_MAX_OFFSET, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto* responseHeader = response->decodeCommandCustomHeader<GetMaxOffsetResponseHeader>();
      return responseHeader->offset;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::getMinOffset(const std::string& addr,
                                      const std::string& topic,
                                      int queueId,
                                      int timeoutMillis) {
  auto* requestHeader = new GetMinOffsetRequestHeader();
  requestHeader->topic = topic;
  requestHeader->queue_id = queueId;

  RemotingCommand request(GET_MIN_OFFSET, requestHeader);

  std::unique_ptr<RemotingCommand> response(remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis));
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto* responseHeader = response->decodeCommandCustomHeader<GetMinOffsetResponseHeader>();
      assert(responseHeader != nullptr);
      return responseHeader->offset;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::getEarliestMsgStoretime(const std::string& addr,
                                                 const std::string& topic,
                                                 int queueId,
                                                 int timeoutMillis) {
  auto* requestHeader = new GetEarliestMsgStoretimeRequestHeader();
  requestHeader->topic = topic;
  requestHeader->queue_id = queueId;

  RemotingCommand request(GET_EARLIEST_MSG_STORETIME, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto* responseHeader = response->decodeCommandCustomHeader<GetEarliestMsgStoretimeResponseHeader>();
      assert(responseHeader != nullptr);
      return responseHeader->timestamp;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::getConsumerIdListByGroup(const std::string& addr,
                                               const std::string& consumerGroup,
                                               std::vector<std::string>& cids,
                                               int timeoutMillis) {
  auto* requestHeader = new GetConsumerListByGroupRequestHeader();
  requestHeader->consumer_group = consumerGroup;

  RemotingCommand request(GET_CONSUMER_LIST_BY_GROUP, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto responseBody = response->body();
      if (responseBody != nullptr && responseBody->size() > 0) {
        std::unique_ptr<ConsumerList> body(ConsumerList::Decode(*responseBody));
        cids = std::move(body->consumer_id_list);
        return;
      }
    }
    case SYSTEM_ERROR:
    // no consumer for this group
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

int64_t MQClientAPIImpl::queryConsumerOffset(const std::string& addr,
                                             QueryConsumerOffsetRequestHeader* requestHeader,
                                             int timeoutMillis) {
  RemotingCommand request(QUERY_CONSUMER_OFFSET, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto* responseHeader = response->decodeCommandCustomHeader<QueryConsumerOffsetResponseHeader>();
      assert(responseHeader != nullptr);
      return responseHeader->offset;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::updateConsumerOffset(const std::string& addr,
                                           UpdateConsumerOffsetRequestHeader* requestHeader,
                                           int timeoutMillis) {
  RemotingCommand request(UPDATE_CONSUMER_OFFSET, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      return;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::updateConsumerOffsetOneway(const std::string& addr,
                                                 UpdateConsumerOffsetRequestHeader* requestHeader,
                                                 int timeoutMillis) {
  RemotingCommand request(UPDATE_CONSUMER_OFFSET, requestHeader);

  remoting_client_->InvokeOneway(addr, std::move(request));
}

void MQClientAPIImpl::sendHearbeat(const std::string& addr, HeartbeatData* heartbeatData, long timeoutMillis) {
  RemotingCommand request(HEART_BEAT, nullptr);
  request.set_body(heartbeatData->Encode());

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      LOG_DEBUG_NEW("sendHeartbeat to broker:{} success", addr);
      return;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::unregisterClient(const std::string& addr,
                                       const std::string& clientID,
                                       const std::string& producerGroup,
                                       const std::string& consumerGroup) {
  LOG_INFO("unregisterClient to broker:%s", addr.c_str());
  RemotingCommand request(UNREGISTER_CLIENT, new UnregisterClientRequestHeader(clientID, producerGroup, consumerGroup));

  auto response = remoting_client_->InvokeSync(addr, std::move(request), kRpcTimeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS:
      LOG_INFO("unregisterClient to:%s success", addr.c_str());
      return;
    default:
      break;
  }

  LOG_WARN("unregisterClient fail:%s, %d", response->remark().c_str(), response->code());
  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::endTransactionOneway(const std::string& addr,
                                           EndTransactionRequestHeader* requestHeader,
                                           const std::string& remark) {
  RemotingCommand request(END_TRANSACTION, requestHeader);
  request.set_remark(remark);

  remoting_client_->InvokeOneway(addr, std::move(request));
}

void MQClientAPIImpl::consumerSendMessageBack(const std::string& addr,
                                              MessageExtPtr msg,
                                              const std::string& consumerGroup,
                                              int delayLevel,
                                              int timeoutMillis,
                                              int maxConsumeRetryTimes) {
  auto* requestHeader = new ConsumerSendMsgBackRequestHeader();
  requestHeader->group = consumerGroup;
  requestHeader->origin_topic = msg->topic();
  requestHeader->offset = msg->commit_log_offset();
  requestHeader->delay_level = delayLevel;
  requestHeader->origin_message_id = msg->msg_id();
  requestHeader->max_reconsume_times = maxConsumeRetryTimes;

  RemotingCommand request(CONSUMER_SEND_MSG_BACK, requestHeader);

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      return;
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::lockBatchMQ(const std::string& addr,
                                  ConsumeQueueSet* requestBody,
                                  std::vector<MQMessageQueue>& mqs,
                                  int timeoutMillis) {
  RemotingCommand request(LOCK_BATCH_MQ, nullptr);
  request.set_body(requestBody->Encode());

  auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto requestBody = response->body();
      if (requestBody != nullptr && requestBody->size() > 0) {
        std::unique_ptr<LockBatchResult> body(LockBatchResult::Decode(*requestBody));
        mqs = std::move(body->lock_ok_message_queue_set);
      } else {
        mqs.clear();
      }
      return;
    } break;
    default:
      break;
  }

  THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
}

void MQClientAPIImpl::unlockBatchMQ(const std::string& addr,
                                    ConsumeQueueSet* requestBody,
                                    int timeoutMillis,
                                    bool oneway) {
  RemotingCommand request(UNLOCK_BATCH_MQ, nullptr);
  request.set_body(requestBody->Encode());

  if (oneway) {
    remoting_client_->InvokeOneway(addr, std::move(request));
  } else {
    auto response = remoting_client_->InvokeSync(addr, std::move(request), timeoutMillis);
    assert(response != nullptr);
    switch (response->code()) {
      case SUCCESS: {
        return;
      } break;
      default:
        break;
    }

    THROW_MQEXCEPTION(MQBrokerException, response->remark(), response->code());
  }
}

std::unique_ptr<TopicRouteData> MQClientAPIImpl::getTopicRouteInfoFromNameServer(const std::string& topic,
                                                                                 int timeoutMillis) {
  RemotingCommand request(GET_ROUTEINFO_BY_TOPIC, new GetRouteInfoRequestHeader(topic));

  auto response = remoting_client_->InvokeSync(null, std::move(request), timeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto responseBody = response->body();
      if (responseBody != nullptr && responseBody->size() > 0) {
        return TopicRouteData::Decode(*responseBody);
      }
    }
    case TOPIC_NOT_EXIST:
    default:
      break;
  }

  THROW_MQEXCEPTION(MQClientException, response->remark(), response->code());
}

std::unique_ptr<TopicList> MQClientAPIImpl::getTopicListFromNameServer() {
  RemotingCommand request(GET_ALL_TOPIC_LIST_FROM_NAMESERVER, nullptr);

  auto response = remoting_client_->InvokeSync(null, std::move(request), kRpcTimeoutMillis);
  assert(response != nullptr);
  switch (response->code()) {
    case SUCCESS: {
      auto responseBody = response->body();
      if (responseBody != nullptr && responseBody->size() > 0) {
        return TopicList::Decode(*responseBody);
      }
    }
    default:
      break;
  }

  THROW_MQEXCEPTION(MQClientException, response->remark(), response->code());
}

int MQClientAPIImpl::wipeWritePermOfBroker(const std::string& namesrvAddr,
                                           const std::string& brokerName,
                                           int timeoutMillis) {
  return 0;
}

void MQClientAPIImpl::deleteTopicInBroker(const std::string& addr, const std::string& topic, int timeoutMillis) {}

void MQClientAPIImpl::deleteTopicInNameServer(const std::string& addr, const std::string& topic, int timeoutMillis) {}

void MQClientAPIImpl::deleteSubscriptionGroup(const std::string& addr,
                                              const std::string& groupName,
                                              int timeoutMillis) {}

std::string MQClientAPIImpl::getKVConfigByValue(const std::string& projectNamespace,
                                                const std::string& projectGroup,
                                                int timeoutMillis) {
  return "";
}

void MQClientAPIImpl::deleteKVConfigByValue(const std::string& projectNamespace,
                                            const std::string& projectGroup,
                                            int timeoutMillis) {}

KVTable MQClientAPIImpl::getKVListByNamespace(const std::string& projectNamespace, int timeoutMillis) {
  return KVTable();
}

}  // namespace rocketmq
