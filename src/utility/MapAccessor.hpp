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
#ifndef ROCKETMQ_UTILITY_MAPACCESSOR_HPP_
#define ROCKETMQ_UTILITY_MAPACCESSOR_HPP_

#include <cstddef>  // size_t

#include <mutex>        // std::lock_guard
#include <set>          // std::set
#include <type_traits>  // std::is_convertible
#include <utility>      // std::forward

namespace rocketmq {
namespace MapAccessor {

template <typename Map, typename Mutex>
size_t Size(const Map& map, Mutex& mutex) {
  std::lock_guard<Mutex> lock(mutex);
  return map.size();
}

template <typename Map, typename Mutex>
void Assign(Map& map, Map&& other, Mutex& mutex) {
  std::lock_guard<Mutex> lock(mutex);
  map = std::forward<Map>(other);
}

template <typename Map, typename Mutex>
void Clear(Map& map, Mutex& mutex) {
  std::lock_guard<Mutex> lock(mutex);
  map.clear();
}

template <typename Map, typename Mutex>
Map Clone(Map& map, Mutex& mutex) {
  std::lock_guard<Mutex> lock(mutex);
  return map;
}

template <typename Map, typename Key, typename Value, typename Mutex>
bool Insert(Map& map, Key&& key, Value&& value, Mutex& mutex) {
  static_assert(std::is_convertible<Key, typename Map::key_type>::value, "Key is not convertible into Map::key_type");
  static_assert(std::is_convertible<Value, typename Map::mapped_type>::value,
                "Value is not convertible into Map::mapped_type");

  std::lock_guard<Mutex> lock(mutex);
  return map.insert({std::forward<Key>(key), std::forward<Value>(value)}).second;
}

template <typename Map, typename Key, typename Value, typename Mutex>
void InsertOrAssign(Map& map, Key&& key, Value&& value, Mutex& mutex) {
  static_assert(std::is_convertible<Key, typename Map::key_type>::value, "Key is not convertible into Map::key_type");
  static_assert(std::is_convertible<Value, typename Map::mapped_type>::value,
                "Value is not convertible into Map::mapped_type");

  std::lock_guard<Mutex> lock(mutex);
  map[std::forward<Key>(key)] = std::forward<Value>(value);
}

template <typename Map, typename Mutex>
void Erase(Map& map, const typename Map::key_type& key, Mutex& mutex) {
  std::lock_guard<Mutex> lock(mutex);
  map.erase(key);
}

template <typename Map, typename Key, typename DefaultValue, typename Mutex>
typename Map::mapped_type GetOrDefault(const Map& map, const Key& key, DefaultValue&& value, Mutex& mutex) {
  static_assert(std::is_convertible<Key, typename Map::key_type>::value, "Key is not convertible into Map::key_type");
  static_assert(std::is_convertible<DefaultValue, typename Map::mapped_type>::value,
                "DefaultValue is not convertible into Map::mapped_type");

  std::lock_guard<std::mutex> lock(mutex);
  const auto& it = map.find(key);
  if (it != map.end()) {
    return it->second;
  }
  return std::forward<DefaultValue>(value);
}

template <typename Map, typename Mutex>
std::set<typename Map::key_type> KeySet(const Map& map, Mutex& mutex) {
  using Key = typename Map::key_type;

  std::set<Key> key_set;
  std::lock_guard<std::mutex> lock(mutex);
  for (const auto& it : map) {
    key_set.insert(it.first);
  }
  return key_set;
}

}  // namespace MapAccessor
}  // namespace rocketmq

#endif  // ROCKETMQ_UTILITY_MAPACCESSOR_HPP_
