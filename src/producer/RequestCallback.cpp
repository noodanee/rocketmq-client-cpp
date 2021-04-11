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
#include "RequestCallback.h"

#include <algorithm>
#include <exception>

#include "Logging.h"

namespace rocketmq {

void RequestCallback::invokeOnSuccess(MQMessage message) noexcept {
  auto type = getRequestCallbackType();
  try {
    onSuccess(std::move(message));
  } catch (const std::exception& e) {
    LOG_WARN_NEW("encounter exception when invoke RequestCallback::onSuccess(), {}", e.what());
  }
  if (type == RequestCallbackType::kAutoDelete) {
    delete this;
  }
}

void RequestCallback::invokeOnException(MQException& exception) noexcept {
  auto type = getRequestCallbackType();
  try {
    onException(exception);
  } catch (const std::exception& e) {
    LOG_WARN_NEW("encounter exception when invoke RequestCallback::onException(), {}", e.what());
  }
  if (type == RequestCallbackType::kAutoDelete) {
    delete this;
  }
}

}  // namespace rocketmq
