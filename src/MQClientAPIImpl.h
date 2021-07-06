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

#include <cstdint>  // int64_t

#include <functional>  // std::function
#include <memory>      // std::unique_ptr

#include "CommunicationMode.h"
#include "MQClientInstance.h"
#include "MQException.h"
#include "MQMessageExt.h"
#include "PullResultExt.hpp"
#include "RemotingCommand.h"
#include "SendResult.hpp"
#include "TopicConfig.h"
#include "TopicPublishInfo.hpp"
#include "common/ResultState.hpp"

namespace rocketmq {

class TcpRemotingClient;
class ClientRemotingProcessor;
class RPCHook;

struct TopicRouteData;
struct SendMessageRequestHeader;
struct EndTransactionRequestHeader;
struct PullMessageRequestHeader;
struct HeartbeatData;

/**
 * wrap all RPC API
 */
class MQClientAPIImpl {
 public:
  using SendCallback = std::function<void(ResultState<std::unique_ptr<SendResult>>) /* noexcept */>;
  using PullCallback = std::function<void(ResultState<std::unique_ptr<PullResultExt>>) /* noexcept */>;

 public:
  MQClientAPIImpl(ClientRemotingProcessor* client_remoting_processor,
                  RPCHookPtr rpc_hook,
                  const MQClientConfig& client_config);
  ~MQClientAPIImpl() = default;

  // disable copy
  MQClientAPIImpl(const MQClientAPIImpl&) = delete;
  MQClientAPIImpl& operator=(const MQClientAPIImpl&) = delete;

  // disable move
  MQClientAPIImpl(MQClientAPIImpl&&) = delete;
  MQClientAPIImpl& operator=(MQClientAPIImpl&&) = delete;

  std::vector<std::string> GetNameServerAddressList() const;
  void UpdateNameServerAddressList(const std::string& address_list);

  void Start();
  void Shutdown();

  std::unique_ptr<TopicRouteData> GetTopicRouteInfoFromNameServer(std::string topic, int64_t timeout_millis);

  std::unique_ptr<SendResult> SendMessageSync(const std::string& broker_address,
                                              const std::string& broker_name,
                                              const MessagePtr& message,
                                              std::unique_ptr<SendMessageRequestHeader> request_header,
                                              int64_t timeout_millis);

  void SendMessageAsync(const std::string& broker_address,
                        const std::string& broker_name,
                        const MessagePtr& message,
                        std::unique_ptr<SendMessageRequestHeader> request_header,
                        int64_t timeout_millis,
                        SendCallback send_callback);

  void SendMessageOneway(const std::string& broker_address,
                         const MessagePtr& message,
                         std::unique_ptr<SendMessageRequestHeader> request_header);

  std::unique_ptr<SendResult> SendMessage(const std::string& broker_address,
                                          const std::string& broker_name,
                                          const MessagePtr& message,
                                          std::unique_ptr<SendMessageRequestHeader> request_header,
                                          int64_t timeout_millis,
                                          CommunicationMode communication_mode,
                                          SendCallback send_callback);

  void EndTransactionOneway(const std::string& broker_address,
                            std::unique_ptr<EndTransactionRequestHeader> request_header,
                            std::string remark);

  void ConsumerSendMessageBack(const std::string& broker_address,
                               const MessageExtPtr& message,
                               std::string consumer_group,
                               int32_t delay_level,
                               int32_t max_consume_retry_times,
                               int64_t timeout_millis);

  std::unique_ptr<PullResultExt> PullMessageSync(const std::string& broker_address,
                                                 std::unique_ptr<PullMessageRequestHeader> request_header,
                                                 int64_t timeout_millis);

  void PullMessageAsync(const std::string& broker_address,
                        std::unique_ptr<PullMessageRequestHeader> request_header,
                        int64_t timeout_millis,
                        PullCallback pull_callback);

  std::unique_ptr<PullResultExt> PullMessage(const std::string& broker_address,
                                             std::unique_ptr<PullMessageRequestHeader> request_header,
                                             int64_t timeoutMillis,
                                             CommunicationMode communication_mode,
                                             PullCallback pull_callback);

  std::vector<std::string> GetConsumerIdListByGroup(const std::string& brocker_address,
                                                    std::string consumer_group,
                                                    int64_t timeout_millis);

  void UpdateConsumerOffset(const std::string& broker_address,
                            std::string consumer_group,
                            std::string topic,
                            int32_t queue_id,
                            int64_t commit_offset,
                            int64_t timeout_millis);

  void UpdateConsumerOffsetOneway(const std::string& broker_address,
                                  std::string consumer_group,
                                  std::string topic,
                                  int32_t queue_id,
                                  int64_t commit_offset);

  int64_t QueryConsumerOffset(const std::string& broker_address,
                              std::string consumer_group,
                              std::string topic,
                              int32_t queue_id,
                              int64_t timeout_millis);

  int64_t GetMaxOffset(const std::string& broker_address, std::string topic, int32_t queue_id, int64_t timeout_millis);

  int64_t GetMinOffset(const std::string& broker_address, std::string topic, int32_t queue_id, int64_t timeout_millis);

  int64_t SearchOffset(const std::string& broker_address,
                       std::string topic,
                       int32_t queue_id,
                       int64_t timestamp,
                       int64_t timeout_millis);

  std::vector<MessageQueue> LockBatchMQ(const std::string& broker_address,
                                        std::string consumer_group,
                                        std::string client_id,
                                        std::vector<MessageQueue> message_queue_set,
                                        int64_t timeout_millis);

  void UnlockBatchMQSync(const std::string& broker_address,
                         std::string consumer_group,
                         std::string client_id,
                         std::vector<MessageQueue> message_queue_set,
                         int64_t timeout_millis);

  void UnlockBatchMQOneway(const std::string& broker_address,
                           std::string consumer_group,
                           std::string client_id,
                           std::vector<MessageQueue> message_queue_set);

  void UnlockBatchMQ(const std::string& broker_address,
                     std::string consumer_group,
                     std::string client_id,
                     std::vector<MessageQueue> message_queue_set,
                     int64_t timeout_millis,
                     bool oneway);

  void SendHearbeat(const std::string& broker_address, const HeartbeatData& heartbeat_data, int64_t timeout_millis);

  void UnregisterClient(const std::string& broker_address,
                        std::string client_id,
                        std::string producer_group,
                        std::string consumer_group);

  void CreateTopic(const std::string& broker_address, std::string default_topic, const TopicConfig& topic_config);

  int64_t GetEarliestMsgStoretime(const std::string& broker_address,
                                  std::string topic,
                                  int32_t queue_id,
                                  int64_t timeout_millis);

  MessageExtPtr ViewMessage(const std::string& broker_address, int64_t physical_offset, int64_t timeout_millis);

 private:
  std::unique_ptr<TcpRemotingClient> remoting_client_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_MQCLIENTAPIIMPL_H_
