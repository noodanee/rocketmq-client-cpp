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
#ifndef ROCKETMQ_CONSUMER_PULLREQUEST_H_
#define ROCKETMQ_CONSUMER_PULLREQUEST_H_

#include <cassert>
#include <sstream>  // std::stringstream
#include <string>

#include "MQMessageQueue.h"
#include "ProcessQueue.h"

namespace rocketmq {

class PullRequest;
typedef std::shared_ptr<PullRequest> PullRequestPtr;

class ROCKETMQCLIENT_API PullRequest {
 public:
  PullRequest(const std::string& consumer_group, ProcessQueuePtr process_queue)
      : consumer_group_(consumer_group), process_queue_(std::move(process_queue)) {
    assert(process_queue_ != nullptr);
  }

  const std::string& consumer_group() const { return consumer_group_; }
  const ProcessQueuePtr& process_queue() const { return process_queue_; }
  const MQMessageQueue& message_queue() const { return process_queue_->message_queue(); }

  bool locked_first() const { return locked_first_; }
  void set_locked_first(bool locked_first) { locked_first_ = locked_first; }

  int64_t next_offset() const { return process_queue_->pull_offset(); }
  void set_next_offset(int64_t next_offset) { process_queue_->set_pull_offset(next_offset); }

  std::string toString() const;
  operator std::string() const { return toString(); }

 private:
  std::string consumer_group_;
  ProcessQueuePtr process_queue_;
  bool locked_first_{false};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_PULLREQUEST_H_
