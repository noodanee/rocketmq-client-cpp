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
#include "PullCallback.h"

#include <algorithm>
#include <exception>
#include <memory>

#include "log/Logging.h"

namespace rocketmq {

void PullCallback::invokeOnSuccess(std::unique_ptr<PullResult> pull_result) noexcept {
  auto type = getPullCallbackType();
  try {
    onSuccess(std::move(pull_result));
  } catch (const std::exception& e) {
    LOG_WARN_NEW("encounter exception when invoke PullCallback::onSuccess(), {}", e.what());
  }
  if (type == PullCallbackType::kAutoDelete) {
    delete this;
  }
}

void PullCallback::invokeOnException(MQException& exception) noexcept {
  auto type = getPullCallbackType();
  try {
    onException(exception);
  } catch (const std::exception& e) {
    LOG_WARN_NEW("encounter exception when invoke PullCallback::onException(), {}", e.what());
  }
  if (type == PullCallbackType::kAutoDelete) {
    delete this;
  }
}

}  // namespace rocketmq
