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
#ifndef ROCKETMQ_COMMON_UTILALL_H_
#define ROCKETMQ_COMMON_UTILALL_H_

#include <cctype>  // std::tolower

#include <exception>  // std::exception
#include <mutex>      // std::timed_mutex
#include <string>     // std::string
#include <utility>    // std::move
#include <vector>     // std::vector

#include "ByteArray.h"

namespace rocketmq {

const std::string WHITESPACE = " \t\r\n";

const int MASTER_ID = 0;

const std::string SUB_ALL = "*";
const std::string AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102";
const std::string BENCHMARK_TOPIC = "BenchmarkTest";
const std::string DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
const std::string DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
const std::string TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
const std::string CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
const std::string SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
const std::string RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
const std::string DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
const std::string REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";
const std::string REPLY_MESSAGE_FLAG = "reply";

const std::string ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
const std::string MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";

const std::string ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
const std::string ROCKETMQ_NAMESRV_ADDR_ENV = "NAMESRV_ADDR";

const int POLL_NAMESERVER_INTEVAL = 1000 * 30;
const int HEARTBEAT_BROKER_INTERVAL = 1000 * 30;
const int PERSIST_CONSUMER_OFFSET_INTERVAL = 1000 * 5;

const std::string WS_ADDR = "please set nameserver domain by setDomainName, there is no default nameserver domain";

const int LINE_SEPARATOR = 1;
const int WORD_SEPARATOR = 2;

const int HTTP_TIMEOUT = 3000;  // 3S
const int HTTP_CONFLICT = 409;
const int HTTP_OK = 200;
const int HTTP_NOTFOUND = 404;
const int CONNETERROR = -1;

const std::string null = "";

template <typename T>
inline void deleteAndZero(T& pointer) {
  delete pointer;
  pointer = nullptr;
}

#define EMPTY_STR_PTR(ptr) (ptr == nullptr || ptr[0] == '\0')

#ifdef WIN32
typedef pid_t DWORD;
#endif

namespace UtilAll {

bool try_lock_for(std::timed_mutex& mutex, uint64_t timeout);

inline bool stob(std::string const& s) {
  return s.size() == 4 && std::tolower(s[0]) == 't' && std::tolower(s[1]) == 'r' && std::tolower(s[2]) == 'u' &&
         std::tolower(s[3]) == 'e';
}

int32_t hash_code(const std::string& str);

std::string bytes2string(const char* bytes, size_t len);
void string2bytes(char* dest, const std::string& src);

bool isRetryTopic(const std::string& resource);
bool isDLQTopic(const std::string& resource);

std::string getRetryTopic(const std::string& consumerGroup);
std::string getDLQTopic(const std::string& consumerGroup);

std::string getReplyTopic(const std::string& clusterName);

void Trim(std::string& str);
bool isBlank(const std::string& str);

bool SplitURL(const std::string& serverURL, std::string& addr, short& nPort);
int Split(std::vector<std::string>& ret_, const std::string& strIn, const char sep);
int Split(std::vector<std::string>& ret_, const std::string& strIn, const std::string& sep);

std::string getHomeDirectory();
void createDirectory(std::string const& dir);
bool existDirectory(std::string const& dir);

pid_t getProcessId();
std::string getProcessName();

int64_t currentTimeMillis();
int64_t currentTimeSeconds();

bool deflate(const std::string& input, std::string& out, int level);
bool deflate(const ByteArray& in, std::string& out, int level);
bool inflate(const std::string& input, std::string& out);
bool inflate(const ByteArray& in, std::string& out);

// Renames file |from_path| to |to_path|. Both paths must be on the same
// volume, or the function will fail. Destination file will be created
// if it doesn't exist. Prefer this function over Move when dealing with
// temporary files. On Windows it preserves attributes of the target file.
// Returns true on success.
// Returns false on failure..
bool ReplaceFile(const std::string& from_path, const std::string& to_path);

template <typename T>
std::string to_string(T value);

template <typename T>
inline std::string to_string(T value) {
  return std::to_string(value);
}

template <>
inline std::string to_string<bool>(bool value) {
  return value ? "true" : "false";
}

template <>
inline std::string to_string<char*>(char* value) {
  return std::string(value);
}

template <>
inline std::string to_string<ByteArrayRef>(ByteArrayRef value) {
  return batos(std::move(value));
}

template <>
inline std::string to_string<std::exception_ptr>(std::exception_ptr eptr) {
  try {
    if (eptr) {
      std::rethrow_exception(eptr);
    }
  } catch (const std::exception& e) {
    return e.what();
  }
  return null;
}

}  // namespace UtilAll
}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_UTILALL_H_
