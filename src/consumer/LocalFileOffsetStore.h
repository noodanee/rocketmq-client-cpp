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
#ifndef ROCKETMQ_CONSUMER_LOCALFILEOFFSETSTORE_H_
#define ROCKETMQ_CONSUMER_LOCALFILEOFFSETSTORE_H_

#include <map>    // std::map
#include <mutex>  // std::mutex

#include "MQClientInstance.h"
#include "OffsetStore.h"

namespace rocketmq {

class LocalFileOffsetStore : public OffsetStore {
 public:
  LocalFileOffsetStore(MQClientInstance* instance, const std::string& groupName);
  virtual ~LocalFileOffsetStore();

  void load() override;
  void updateOffset(const MessageQueue& mq, int64_t offset, bool increaseOnly) override;
  int64_t readOffset(const MessageQueue& mq, ReadOffsetType type) override;
  void persist(const MessageQueue& mq) override;
  void persistAll(std::vector<MessageQueue>& mqs) override;
  void removeOffset(const MessageQueue& mq) override;

 private:
  std::map<MessageQueue, int64_t> readLocalOffset();
  std::map<MessageQueue, int64_t> readLocalOffsetBak();

 private:
  MQClientInstance* client_instance_;
  std::string group_name_;

  std::map<MessageQueue, int64_t> offset_table_;
  std::mutex lock_;

  std::string store_path_;
  std::mutex file_mutex_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_LOCALFILEOFFSETSTORE_H_
