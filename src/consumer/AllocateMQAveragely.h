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
#ifndef ROCKETMQ_CONSUMER_ALLOCATEMQAVERAGELY_HPP_
#define ROCKETMQ_CONSUMER_ALLOCATEMQAVERAGELY_HPP_

#include <string>  // std::string
#include <vector>  // std::vector

#include "MQMessageQueue.h"

namespace rocketmq {

std::vector<MQMessageQueue> AllocateMQAveragely(const std::string& current_cid,
                                                std::vector<MQMessageQueue>& all_mqs,
                                                std::vector<std::string>& all_cids);

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_ALLOCATEMQAVERAGELY_HPP_
