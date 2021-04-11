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
#ifndef ROCKETMQ_DEFAULTMQPUSHCONSUMERCONFIGPROXY_H_
#define ROCKETMQ_DEFAULTMQPUSHCONSUMERCONFIGPROXY_H_

#include "DefaultMQPushConsumerConfig.h"
#include "MQClientConfigProxy.h"

namespace rocketmq {

class ROCKETMQCLIENT_API DefaultMQPushConsumerConfigProxy : public DefaultMQPushConsumerConfig {
 public:
  DefaultMQPushConsumerConfigProxy(DefaultMQPushConsumerConfig& config) : config_(config) {}

  MessageModel message_model() const override { return config_.message_model(); }
  void set_message_model(MessageModel messageModel) override { config_.set_message_model(messageModel); }

  ConsumeFromWhere consume_from_where() const override { return config_.consume_from_where(); }
  void set_consume_from_where(ConsumeFromWhere consumeFromWhere) override {
    config_.set_consume_from_where(consumeFromWhere);
  }

  const std::string& consume_timestamp() const override { return config_.consume_timestamp(); }
  void set_consume_timestamp(const std::string& consumeTimestamp) override {
    config_.set_consume_timestamp(consumeTimestamp);
  }

  int consume_thread_nums() const override { return config_.consume_thread_nums(); }
  void set_consume_thread_nums(int threadNum) override { config_.set_consume_thread_nums(threadNum); }

  int pull_threshold_for_queue() const override { return config_.pull_threshold_for_queue(); }
  void set_pull_threshold_for_queue(int maxCacheSize) override { config_.set_pull_threshold_for_queue(maxCacheSize); }

  int consume_message_batch_max_size() const override { return config_.consume_message_batch_max_size(); }
  void set_consume_message_batch_max_size(int consumeMessageBatchMaxSize) override {
    config_.set_consume_message_batch_max_size(consumeMessageBatchMaxSize);
  }

  int pull_batch_size() const override { return config_.pull_batch_size(); }
  void set_pull_batch_size(int pull_batch_size) override { config_.set_pull_batch_size(pull_batch_size); }

  int max_reconsume_times() const override { return config_.max_reconsume_times(); }
  void set_max_reconsume_times(int maxReconsumeTimes) override { config_.set_max_reconsume_times(maxReconsumeTimes); }

  long pull_time_delay_millis_when_exception() const override {
    return config_.pull_time_delay_millis_when_exception();
  }
  void set_pull_time_delay_millis_when_exception(long pull_time_delay_millis_when_exception) override {
    config_.set_pull_time_delay_millis_when_exception(pull_time_delay_millis_when_exception);
  }

  const AllocateMQStrategy& allocate_mq_strategy() const override { return config_.allocate_mq_strategy(); }
  void set_allocate_mq_strategy(const AllocateMQStrategy& strategy) override {
    config_.set_allocate_mq_strategy(strategy);
  }

 private:
  DefaultMQPushConsumerConfig& config_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_DEFAULTMQPUSHCONSUMERCONFIGPROXY_H_
