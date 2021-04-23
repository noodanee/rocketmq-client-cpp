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
#include "ResponseFuture.h"

#include <exception>
#include <memory>
#include <utility>

#include "RemotingCommand.h"
#include "UtilAll.h"
#include "concurrent/executor.hpp"

namespace rocketmq {

ResponseFuture::ResponseFuture(int request_code,
                               int opaque,
                               int64_t timeout_millis,
                               RequestCallback request_callback,
                               Executor executor)
    : request_code_(request_code),
      opaque_(opaque),
      deadline_(UtilAll::currentTimeMillis() + timeout_millis),
      request_callback_(std::move(request_callback)),
      executor_(std::move(executor)) {
  if (nullptr == request_callback) {
#if __cplusplus >= 201402L
    count_down_latch_ = std::make_unique<latch>(1);
#else
    count_down_latch_ = std::unique_ptr<latch>(new latch(1));
#endif
  }
}

ResponseFuture::~ResponseFuture() = default;

void ResponseFuture::ExecuteRequestCallback() noexcept {
  if (request_callback_ == nullptr) {
    return;
  }

  if (response_command_ != nullptr) {
    if (executor_ != nullptr) {
      auto request_callback = std::move(request_callback_);
      auto response_command = std::make_shared<std::unique_ptr<RemotingCommand>>(std::move(response_command_));
      executor_(
#if __cplusplus >= 201402L
          [request_callback = std::move(request_callback), response_command = std::move(response_command)]
#else
          [request_callback, response_command]
#endif
          () { request_callback({std::move(*response_command)}); });
    } else {
      request_callback_({std::move(response_command_)});
    }
  } else {
    std::string error_message;
    if (!send_request_ok()) {
      error_message = "send request failed";
    } else if (Timeout()) {
      error_message = "wait response timeout";
    } else {
      error_message = "unknown reason";
    }
    MQException exception(error_message, -1, __FILE__, __LINE__);
    auto exception_ptr = std::make_exception_ptr(exception);
    if (executor_ != nullptr) {
      auto request_callback = std::move(request_callback_);
      executor_(
#if __cplusplus >= 201402L
          [request_callback = std::move(request_callback), exception_ptr]
#else
          [request_callback, exception_ptr]
#endif
          () { request_callback({exception_ptr}); });
    } else {
      request_callback_({exception_ptr});
    }
  }
}

std::unique_ptr<RemotingCommand> ResponseFuture::WaitResponseCommand(int64_t timeout_millis) {
  if (count_down_latch_ != nullptr) {
    if (timeout_millis < 0) {
      timeout_millis = 0;
    }
    count_down_latch_->wait(timeout_millis, time_unit::milliseconds);
  }
  return std::move(response_command_);
}

void ResponseFuture::PutResponseCommand(std::unique_ptr<RemotingCommand> response_command) {
  response_command_ = std::move(response_command);
  if (request_callback_ != nullptr) {
    // async
    ExecuteRequestCallback();
  } else {
    // sync
    count_down_latch_->count_down();
  }
}

bool ResponseFuture::Timeout(int64_t now) const {
  if (now <= 0) {
    now = UtilAll::currentTimeMillis();
  }
  return now > deadline_;
}

}  // namespace rocketmq
