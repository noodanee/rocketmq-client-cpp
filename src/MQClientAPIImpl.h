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
#ifndef ROCKETMQ_MQCLIENTAPIIMPL_H_
#define ROCKETMQ_MQCLIENTAPIIMPL_H_

#include <functional>
#include <memory>

#include "CommunicationMode.h"
#include "KVTable.h"
#include "MQClientInstance.h"
#include "MQException.h"
#include "MQMessageExt.h"
#include "PullResult.h"
#include "RemotingCommand.h"
#include "SendResult.hpp"
#include "TopicConfig.h"
#include "TopicList.h"
#include "TopicPublishInfo.hpp"
#include "common/ResultState.hpp"
#include "protocol/body/ConsumeQueueSet.hpp"
#include "protocol/body/HeartbeatData.hpp"
#include "protocol/body/TopicRouteData.hpp"
#include "protocol/header/EndTransactionRequestHeader.hpp"
#include "protocol/header/PullMessageRequestHeader.hpp"
#include "protocol/header/QueryConsumerOffsetRequestHeader.hpp"
#include "protocol/header/SendMessageRequestHeader.hpp"
#include "protocol/header/UpdateConsumerOffsetRequestHeader.hpp"

namespace rocketmq {

class TcpRemotingClient;
class ClientRemotingProcessor;
class RPCHook;

/**
 * wrap all RPC API
 */
class MQClientAPIImpl {
 public:
  using SendCallback = std::function<void(ResultState<std::unique_ptr<SendResult>>) /* noexcept */>;
  using PullCallback = std::function<void(ResultState<std::unique_ptr<PullResult>>) /* noexcept */>;

 public:
  MQClientAPIImpl(ClientRemotingProcessor* clientRemotingProcessor,
                  RPCHookPtr rpcHook,
                  const MQClientConfig& clientConfig);
  virtual ~MQClientAPIImpl();

  void start();
  void shutdown();

  void updateNameServerAddressList(const std::string& addrs);

  void createTopic(const std::string& addr, const std::string& defaultTopic, TopicConfig topicConfig);

  std::unique_ptr<SendResult> sendMessage(const std::string& addr,
                                          const std::string& brokerName,
                                          const MessagePtr& msg,
                                          std::unique_ptr<SendMessageRequestHeader> requestHeader,
                                          int timeoutMillis,
                                          CommunicationMode communicationMode,
                                          SendCallback sendCallback);

  std::unique_ptr<PullResult> pullMessage(const std::string& addr,
                                          std::unique_ptr<PullMessageRequestHeader> requestHeader,
                                          int timeoutMillis,
                                          CommunicationMode communicationMode,
                                          PullCallback pullCallback);

  MQMessageExt viewMessage(const std::string& addr, int64_t phyoffset, int timeoutMillis);

  int64_t searchOffset(const std::string& addr,
                       const std::string& topic,
                       int queueId,
                       int64_t timestamp,
                       int timeoutMillis);

  int64_t getMaxOffset(const std::string& addr, const std::string& topic, int queueId, int timeoutMillis);
  int64_t getMinOffset(const std::string& addr, const std::string& topic, int queueId, int timeoutMillis);

  int64_t getEarliestMsgStoretime(const std::string& addr, const std::string& topic, int queueId, int timeoutMillis);

  void getConsumerIdListByGroup(const std::string& addr,
                                const std::string& consumerGroup,
                                std::vector<std::string>& cids,
                                int timeoutMillis);

  int64_t queryConsumerOffset(const std::string& addr,
                              QueryConsumerOffsetRequestHeader* requestHeader,
                              int timeoutMillis);

  void updateConsumerOffset(const std::string& addr,
                            UpdateConsumerOffsetRequestHeader* requestHeader,
                            int timeoutMillis);
  void updateConsumerOffsetOneway(const std::string& addr,
                                  UpdateConsumerOffsetRequestHeader* requestHeader,
                                  int timeoutMillis);

  void sendHearbeat(const std::string& addr, HeartbeatData* heartbeatData, long timeoutMillis);
  void unregisterClient(const std::string& addr,
                        const std::string& clientID,
                        const std::string& producerGroup,
                        const std::string& consumerGroup);

  void endTransactionOneway(const std::string& addr,
                            EndTransactionRequestHeader* requestHeader,
                            const std::string& remark);

  void consumerSendMessageBack(const std::string& addr,
                               MessageExtPtr msg,
                               const std::string& consumerGroup,
                               int delayLevel,
                               int timeoutMillis,
                               int maxConsumeRetryTimes);

  void lockBatchMQ(const std::string& addr,
                   ConsumeQueueSet* requestBody,
                   std::vector<MessageQueue>& mqs,
                   int timeoutMillis);
  void unlockBatchMQ(const std::string& addr, ConsumeQueueSet* requestBody, int timeoutMillis, bool oneway = false);

  std::unique_ptr<TopicRouteData> getTopicRouteInfoFromNameServer(const std::string& topic, int timeoutMillis);

  std::unique_ptr<TopicList> getTopicListFromNameServer();

  int wipeWritePermOfBroker(const std::string& namesrvAddr, const std::string& brokerName, int timeoutMillis);

  void deleteTopicInBroker(const std::string& addr, const std::string& topic, int timeoutMillis);
  void deleteTopicInNameServer(const std::string& addr, const std::string& topic, int timeoutMillis);

  void deleteSubscriptionGroup(const std::string& addr, const std::string& groupName, int timeoutMillis);

  std::string getKVConfigByValue(const std::string& projectNamespace,
                                 const std::string& projectGroup,
                                 int timeoutMillis);
  void deleteKVConfigByValue(const std::string& projectNamespace, const std::string& projectGroup, int timeoutMillis);

  KVTable getKVListByNamespace(const std::string& projectNamespace, int timeoutMillis);

 public:
  TcpRemotingClient* getRemotingClient() { return remoting_client_.get(); }

 private:
  std::unique_ptr<SendResult> sendMessageSync(const std::string& addr,
                                              const std::string& brokerName,
                                              const MessagePtr& msg,
                                              RemotingCommand request,
                                              int timeoutMillis);

  void sendMessageAsync(const std::string& addr,
                        const std::string& brokerName,
                        const MessagePtr& msg,
                        RemotingCommand request,
                        SendCallback sendCallback,
                        int64_t timeoutMilliseconds);

  std::unique_ptr<SendResult> processSendResponse(const std::string& brokerName,
                                                  Message& message,
                                                  RemotingCommand& response);

  std::unique_ptr<PullResult> pullMessageSync(const std::string& addr, RemotingCommand request, int timeoutMillis);

  void pullMessageAsync(const std::string& addr, RemotingCommand request, int timeoutMillis, PullCallback pullCallback);

  std::unique_ptr<PullResult> processPullResponse(RemotingCommand& response);

 private:
  std::unique_ptr<TcpRemotingClient> remoting_client_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_MQCLIENTAPIIMPL_H_
