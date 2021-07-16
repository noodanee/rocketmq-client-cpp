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
#include "ClientRemotingProcessor.h"

#include <cassert>
#include <memory>

#include "MQProtos.h"
#include "MessageAccessor.hpp"
#include "MessageDecoder.h"
#include "MessageSysFlag.h"
#include "RequestFutureTable.h"
#include "SocketUtil.h"
#include "protocol/body/ConsumerRunningInfo.hpp"
#include "protocol/body/ResetOffsetBody.hpp"
#include "protocol/header/CheckTransactionStateRequestHeader.hpp"
#include "protocol/header/GetConsumerRunningInfoRequestHeader.hpp"
#include "protocol/header/NotifyConsumerIdsChangedRequestHeader.hpp"
#include "protocol/header/ReplyMessageRequestHeader.hpp"
#include "protocol/header/ResetOffsetRequestHeader.hpp"
#include "utility/MakeUnique.hpp"

namespace rocketmq {

ClientRemotingProcessor::ClientRemotingProcessor(MQClientInstance* clientInstance) : client_instance_(clientInstance) {}

ClientRemotingProcessor::~ClientRemotingProcessor() = default;

std::unique_ptr<RemotingCommand> ClientRemotingProcessor::processRequest(TcpTransportPtr channel,
                                                                         RemotingCommand* request) {
  const auto& addr = channel->peer_address();
  LOG_DEBUG_NEW("processRequest, code:{}, addr:{}", request->code(), addr);
  switch (request->code()) {
    case CHECK_TRANSACTION_STATE:
      return checkTransactionState(addr, request);
    case NOTIFY_CONSUMER_IDS_CHANGED:
      return notifyConsumerIdsChanged(request);
    case RESET_CONSUMER_CLIENT_OFFSET:  // oneWayRPC
      return resetOffset(request);
    case GET_CONSUMER_STATUS_FROM_CLIENT:
      // return getConsumeStatus( request);
      break;
    case GET_CONSUMER_RUNNING_INFO:
      return getConsumerRunningInfo(addr, request);
    case CONSUME_MESSAGE_DIRECTLY:
      // return consumeMessageDirectly( request);
      break;
    case PUSH_REPLY_MESSAGE_TO_CLIENT:
      return receiveReplyMessage(request);
    default:
      break;
  }
  return nullptr;
}

std::unique_ptr<RemotingCommand> ClientRemotingProcessor::checkTransactionState(const std::string& addr,
                                                                                RemotingCommand* request) {
  auto* requestHeader = request->DecodeHeader<CheckTransactionStateRequestHeader>();
  assert(requestHeader != nullptr);

  auto requestBody = request->body();
  if (requestBody != nullptr && requestBody->size() > 0) {
    std::unique_ptr<ByteBuffer> byteBuffer(ByteBuffer::wrap(requestBody));
    MessageExtPtr messageExt = MessageDecoder::decode(*byteBuffer);
    if (messageExt != nullptr) {
      const auto& transactionId = messageExt->getProperty(MQMessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
      if (!transactionId.empty()) {
        messageExt->set_transaction_id(transactionId);
      }
      const auto& group = messageExt->getProperty(MQMessageConst::PROPERTY_PRODUCER_GROUP);
      if (!group.empty()) {
        auto* producer = client_instance_->SelectProducer(group);
        if (producer != nullptr) {
          producer->checkTransactionState(addr, messageExt, requestHeader);
        } else {
          LOG_DEBUG_NEW("checkTransactionState, pick producer by group[{}] failed", group);
        }
      } else {
        LOG_WARN_NEW("checkTransactionState, pick producer group failed");
      }
    } else {
      LOG_WARN_NEW("checkTransactionState, decode message failed");
    }
  } else {
    LOG_ERROR_NEW("checkTransactionState, request body is empty, request header: {}", requestHeader->ToString());
  }

  return nullptr;
}

std::unique_ptr<RemotingCommand> ClientRemotingProcessor::notifyConsumerIdsChanged(RemotingCommand* request) {
  auto* requestHeader = request->DecodeHeader<NotifyConsumerIdsChangedRequestHeader>();
  LOG_INFO_NEW("notifyConsumerIdsChanged, group:{}", requestHeader->consumer_group);
  client_instance_->rebalanceImmediately();
  return nullptr;
}

std::unique_ptr<RemotingCommand> ClientRemotingProcessor::resetOffset(RemotingCommand* request) {
  auto* responseHeader = request->DecodeHeader<ResetOffsetRequestHeader>();
  auto requestBody = request->body();
  if (requestBody != nullptr && requestBody->size() > 0) {
    std::unique_ptr<ResetOffsetBody> body(ResetOffsetBody::Decode(*requestBody));
    if (body != nullptr) {
      client_instance_->resetOffset(responseHeader->group, responseHeader->topic, body->offset_table);
    } else {
      LOG_ERROR("resetOffset failed as received data could not be unserialized");
    }
  }
  return nullptr;  // as resetOffset is oneWayRPC, do not need return any response
}

std::unique_ptr<RemotingCommand> ClientRemotingProcessor::getConsumerRunningInfo(const std::string& addr,
                                                                                 RemotingCommand* request) {
  auto* requestHeader = request->DecodeHeader<GetConsumerRunningInfoRequestHeader>();
  LOG_INFO_NEW("getConsumerRunningInfo, group:{}", requestHeader->consumer_group);

  auto response = MakeUnique<RemotingCommand>(MQResponseCode::SYSTEM_ERROR, "not set any response code", nullptr);

  std::unique_ptr<ConsumerRunningInfo> runningInfo(
      client_instance_->consumerRunningInfo(requestHeader->consumer_group));
  if (runningInfo != nullptr) {
    if (requestHeader->jstack_enable) {
      /*string jstack = UtilAll::jstack();
       consumerRunningInfo->setJstack(jstack);*/
    }
    response->set_code(SUCCESS);
    response->set_body(runningInfo->Encode());
  } else {
    response->set_code(SYSTEM_ERROR);
    response->set_remark("The Consumer Group not exist in this consumer");
  }

  return response;
}

std::unique_ptr<RemotingCommand> ClientRemotingProcessor::receiveReplyMessage(RemotingCommand* request) {
  auto response = MakeUnique<RemotingCommand>(MQResponseCode::SYSTEM_ERROR, "not set any response code", nullptr);

  auto receiveTime = UtilAll::currentTimeMillis();
  auto* requestHeader = request->DecodeHeader<ReplyMessageRequestHeader>();

  try {
    std::unique_ptr<MQMessageExt> msg(new MQMessageExt);

    msg->set_topic(requestHeader->topic);
    msg->set_queue_id(requestHeader->queue_id);
    msg->set_store_timestamp(requestHeader->store_timestamp);

    if (!requestHeader->born_host.empty()) {
      msg->set_born_host(StringToSockaddr(requestHeader->born_host));
    }

    if (!requestHeader->store_host.empty()) {
      msg->set_store_host(StringToSockaddr(requestHeader->store_host));
    }

    auto body = request->body();
    if ((requestHeader->system_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG) {
      std::string origin_body;
      if (UtilAll::inflate(*body, origin_body)) {
        msg->set_body(std::move(origin_body));
      } else {
        LOG_WARN_NEW("err when uncompress constant");
      }
    } else {
      msg->set_body(std::string(body->array(), body->size()));
    }

    msg->set_flag(requestHeader->flag);
    MessageAccessor::setProperties(*msg, MessageDecoder::string2messageProperties(requestHeader->properties));
    MessageAccessor::putProperty(*msg, MQMessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
                                 UtilAll::to_string(receiveTime));
    msg->set_born_timestamp(requestHeader->born_timestamp);
    msg->set_reconsume_times(requestHeader->reconsume_times);
    LOG_DEBUG_NEW("receive reply message:{}", msg->toString());

    processReplyMessage(std::move(msg));

    response->set_code(MQResponseCode::SUCCESS);
    response->set_remark(null);
  } catch (const std::exception& e) {
    LOG_WARN_NEW("unknown err when receiveReplyMsg, {}", e.what());
    response->set_code(MQResponseCode::SYSTEM_ERROR);
    response->set_remark("process reply message fail");
  }

  return response;
}

void ClientRemotingProcessor::processReplyMessage(std::unique_ptr<MQMessageExt> replyMsg) {
  const auto& correlationId = replyMsg->getProperty(MQMessageConst::PROPERTY_CORRELATION_ID);
  auto requestResponseFuture = RequestFutureTable::removeRequestFuture(correlationId);
  if (requestResponseFuture != nullptr) {
    requestResponseFuture->putResponseMessage(std::move(replyMsg));
    requestResponseFuture->executeRequestCallback();
  } else {
    auto bornHost = replyMsg->born_host_string();
    LOG_WARN_NEW("receive reply message, but not matched any request, CorrelationId: {} , reply from host: {}",
                 correlationId, bornHost);
  }
}

}  // namespace rocketmq
