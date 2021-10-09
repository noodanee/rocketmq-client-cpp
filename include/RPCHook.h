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
#ifndef ROCKETMQ_RPCHOOK_H_
#define ROCKETMQ_RPCHOOK_H_

#include <memory>  // std::shared_ptr
#include <string>  // std::string

#include "RemotingCommand.h"

namespace rocketmq {

class RPCHook;
using RPCHookPtr = std::shared_ptr<RPCHook>;

class ROCKETMQCLIENT_API RPCHook {
 public:
  RPCHook() = default;
  virtual ~RPCHook() = default;

  virtual void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request, bool toSent) = 0;
  virtual void doAfterResponse(const std::string& remoteAddr,
                               RemotingCommand& request,
                               RemotingCommand* response,
                               bool toSent) = 0;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_RPCHOOK_H_
