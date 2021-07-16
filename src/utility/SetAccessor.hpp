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
#ifndef ROCKETMQ_UTILITY_SETACCESSOR_HPP_
#define ROCKETMQ_UTILITY_SETACCESSOR_HPP_

#include <mutex>        // std::lock_guard
#include <set>          // std::set
#include <type_traits>  // std::is_convertible
#include <utility>      // std::forward

namespace rocketmq {
namespace SetAccessor {

template <typename Set, typename OtherSet>
void Merge(Set& set, OtherSet&& other) {
#if __cplusplus >= 201703L
  set.merge(std::forward<OtherSet>(other));
#else
  for (const auto& key : other) {
    set.insert(key);
  }
#endif
}

}  // namespace SetAccessor
}  // namespace rocketmq

#endif  // ROCKETMQ_UTILITY_SETACCESSOR_HPP_
