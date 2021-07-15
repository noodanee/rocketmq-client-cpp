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
#include "logging/Logging.hpp"

#include <cstdlib>  // std::getenv

#include <string>

#include "common/UtilAll.h"

namespace rocketmq {

namespace {

const char* const ROCKETMQ_CPP_LOG_DIR_ENV = "ROCKETMQ_CPP_LOG_DIR";

std::string GetDefaultLogDir() {
  auto log_dir = []() -> std::string {
    const char* dir = std::getenv(ROCKETMQ_CPP_LOG_DIR_ENV);
    if (dir != nullptr && dir[0] != '\0') {
      // FIXME: replace '~' by home directory.
      return dir;
    }
    return UtilAll::getHomeDirectory() + "/logs/rocketmq-cpp/";
  }();
  if (log_dir[log_dir.size() - 1] != FILE_SEPARATOR) {
    log_dir += FILE_SEPARATOR;
  }
  std::string log_file_name = UtilAll::to_string(UtilAll::getProcessId()) + "_" + "rocketmq-cpp.log";
  return log_dir + log_file_name;
}

}  // namespace

LoggerConfig& GetDefaultLoggerConfig() {
  static LoggerConfig default_logger_config("default", GetDefaultLogDir());
  return default_logger_config;
}

Logger<SpdlogLoggerImpl>& GetDefaultLogger() {
  static Logger<SpdlogLoggerImpl> default_logger{GetDefaultLoggerConfig()};
  return default_logger;
}

}  // namespace rocketmq
