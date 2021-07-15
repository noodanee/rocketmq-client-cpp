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
#include "RequestResponseFuture.h"

#include <utility>  // std::move

#include "Logging.h"
#include "UtilAll.h"

namespace rocketmq {

RequestResponseFuture::RequestResponseFuture(const std::string& correlationId,
                                             long timeoutMillis,
                                             RequestCallback requestCallback)
    : correlation_id_(correlationId),
      request_callback_(std::move(requestCallback)),
      begin_timestamp_(UtilAll::currentTimeMillis()),
      timeout_millis_(timeoutMillis) {
  if (nullptr == requestCallback) {
    count_down_latch_.reset(new latch(1));
  }
}

void RequestResponseFuture::executeRequestCallback() noexcept {
  if (request_callback_ != nullptr) {
    if (send_request_ok_ && cause_ == nullptr) {
      request_callback_(std::move(response_msg_));
    } else {
      request_callback_(cause_);
    }
  }
}

bool RequestResponseFuture::isTimeout() {
  auto diff = UtilAll::currentTimeMillis() - begin_timestamp_;
  return diff > timeout_millis_;
}

MessagePtr RequestResponseFuture::waitResponseMessage(int64_t timeout) {
  if (count_down_latch_ != nullptr) {
    if (timeout < 0) {
      timeout = 0;
    }
    count_down_latch_->wait(timeout, time_unit::milliseconds);
  }
  return response_msg_;
}

void RequestResponseFuture::putResponseMessage(MessagePtr responseMsg) {
  response_msg_ = std::move(responseMsg);
  if (count_down_latch_ != nullptr) {
    count_down_latch_->count_down();
  }
}

}  // namespace rocketmq
