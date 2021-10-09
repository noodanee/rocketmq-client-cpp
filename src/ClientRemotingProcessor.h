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
#ifndef ROCKETMQ_CLIENTREMOTINGPROCESSOR_H_
#define ROCKETMQ_CLIENTREMOTINGPROCESSOR_H_

#include "MQClientInstance.h"
#include "MessageQueue.hpp"
#include "RequestProcessor.h"

namespace rocketmq {

class MQMessageExt;

class ClientRemotingProcessor : public RequestProcessor {
 public:
  ClientRemotingProcessor(MQClientInstance* clientInstance);
  virtual ~ClientRemotingProcessor();

  std::unique_ptr<RemotingCommand> processRequest(TcpTransportPtr channel, RemotingCommand* request) override;

  std::unique_ptr<RemotingCommand> checkTransactionState(const std::string& addr, RemotingCommand* request);
  std::unique_ptr<RemotingCommand> notifyConsumerIdsChanged(RemotingCommand* request);
  std::unique_ptr<RemotingCommand> resetOffset(RemotingCommand* request);
  std::unique_ptr<RemotingCommand> getConsumerRunningInfo(const std::string& addr, RemotingCommand* request);
  std::unique_ptr<RemotingCommand> receiveReplyMessage(RemotingCommand* request);

 private:
  void processReplyMessage(std::unique_ptr<MQMessageExt> replyMsg);

 private:
  MQClientInstance* client_instance_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENTREMOTINGPROCESSOR_H_
