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
#ifndef ROCKETMQ_DEFAULTLITEPULLCONSUMERCONFIGPROXY_H__
#define ROCKETMQ_DEFAULTLITEPULLCONSUMERCONFIGPROXY_H__

#include "DefaultLitePullConsumerConfig.h"
#include "MQClientConfigProxy.h"

namespace rocketmq {

class ROCKETMQCLIENT_API DefaultLitePullConsumerConfigProxy : public DefaultLitePullConsumerConfig {
 public:
  DefaultLitePullConsumerConfigProxy(DefaultLitePullConsumerConfig& config) : config_(config) {}

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

  long auto_commit_interval_millis() const override { return config_.auto_commit_interval_millis(); }
  void set_auto_commit_interval_millis(long auto_commit_interval_millis) override {
    config_.set_auto_commit_interval_millis(auto_commit_interval_millis);
  }

  int pull_batch_size() const override { return config_.pull_batch_size(); }
  void set_pull_batch_size(int pull_batch_size) override { config_.set_pull_batch_size(pull_batch_size); }

  long consumer_pull_timeout_millis() const override { return config_.consumer_pull_timeout_millis(); }
  void set_consumer_pull_timeout_millis(long consumer_pull_timeout_millis) override {
    config_.set_consumer_pull_timeout_millis(consumer_pull_timeout_millis);
  }

  long consumer_timeout_millis_when_suspend() const override { return config_.consumer_timeout_millis_when_suspend(); }
  void set_consumer_timeout_millis_when_suspend(long consumer_timeout_millis_when_suspend) override {
    config_.set_consumer_timeout_millis_when_suspend(consumer_timeout_millis_when_suspend);
  }

  long broker_suspend_max_time_millis() const override { return config_.broker_suspend_max_time_millis(); }
  void set_broker_suspend_max_time_millis(long broker_suspend_max_time_millis) override {
    config_.set_broker_suspend_max_time_millis(broker_suspend_max_time_millis);
  }

  long pull_threshold_for_all() const override { return config_.pull_threshold_for_all(); }
  void set_pull_threshold_for_all(long pull_threshold_for_all) override {
    config_.set_pull_threshold_for_all(pull_threshold_for_all);
  }

  int pull_threshold_for_queue() const override { return config_.pull_threshold_for_queue(); }
  void set_pull_threshold_for_queue(int pull_threshold_for_queue) override {
    config_.set_pull_threshold_for_queue(pull_threshold_for_queue);
  }

  long pull_time_delay_millis_when_exception() const override {
    return config_.pull_time_delay_millis_when_exception();
  }
  void set_pull_time_delay_millis_when_exception(long pull_time_delay_millis_when_exception) override {
    config_.set_pull_time_delay_millis_when_exception(pull_time_delay_millis_when_exception);
  }

  long poll_timeout_millis() const override { return config_.poll_timeout_millis(); }
  void set_poll_timeout_millis(long poll_timeout_millis) override {
    config_.set_poll_timeout_millis(poll_timeout_millis);
  }

  long topic_metadata_check_interval_millis() const override { return config_.topic_metadata_check_interval_millis(); }
  void set_topic_metadata_check_interval_millis(long topic_metadata_check_interval_millis) override {
    config_.set_topic_metadata_check_interval_millis(topic_metadata_check_interval_millis);
  }

  const AllocateMQStrategy& allocate_mq_strategy() const override { return config_.allocate_mq_strategy(); }
  void set_allocate_mq_strategy(const AllocateMQStrategy& strategy) override {
    config_.set_allocate_mq_strategy(strategy);
  }

 private:
  DefaultLitePullConsumerConfig& config_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_DEFAULTLITEPULLCONSUMERCONFIGPROXY_H__
