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
#ifndef ROCKETMQ_TRANSPORT_RESPONSEFUTURE_H_
#define ROCKETMQ_TRANSPORT_RESPONSEFUTURE_H_

#include <cstdint>
#include <memory>

#include "RemotingCommand.h"
#include "common/ResultState.hpp"
#include "concurrent/executor.hpp"
#include "concurrent/latch.hpp"

namespace rocketmq {

class ResponseFuture;
using ResponseFuturePtr = std::shared_ptr<ResponseFuture>;

class ResponseFuture final {
 public:
  using RequestCallback = std::function<void(ResultState<std::unique_ptr<RemotingCommand>>) /* noexcept */>;
  using Executor = std::function<void(std::function<void()>)>;

 public:
  ResponseFuture(int request_code,
                 int opaque,
                 int64_t timeout_millis,
                 RequestCallback request_callback = nullptr,
                 Executor executor = nullptr);
  ~ResponseFuture();

  // disable copy
  ResponseFuture(const ResponseFuture&) = delete;
  ResponseFuture& operator=(const ResponseFuture&) = delete;

  // disable move
  ResponseFuture(ResponseFuture&&) = delete;
  ResponseFuture& operator=(ResponseFuture&&) = delete;

  std::unique_ptr<RemotingCommand> WaitResponseCommand(int64_t timeout_millis);
  void PutResponseCommand(std::unique_ptr<RemotingCommand> response_command);

  bool Timeout(int64_t now = -1) const;

 private:
  void ExecuteRequestCallback() noexcept;

 public:
  int request_code() const { return request_code_; }
  int opaque() const { return opaque_; }

  bool send_request_ok() const { return send_request_ok_; }
  void set_send_request_ok(bool send_request_ok = true) { send_request_ok_ = send_request_ok; };

 private:
  int request_code_;
  int opaque_;
  int64_t deadline_;
  RequestCallback request_callback_;
  Executor executor_;

  bool send_request_ok_{false};

  std::unique_ptr<RemotingCommand> response_command_;

  std::unique_ptr<latch> count_down_latch_;  // use for synchronization rpc
};

}  // namespace rocketmq

#endif  // ROCKETMQ_TRANSPORT_RESPONSEFUTURE_H_
