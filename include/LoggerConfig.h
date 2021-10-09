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
#ifndef ROCKETMQ_LOGGERCONFIG_H_
#define ROCKETMQ_LOGGERCONFIG_H_

#include <string>
#include <utility>  // std::move

namespace rocketmq {

enum LogLevel {
  LOG_LEVEL_FATAL = 1,
  LOG_LEVEL_ERROR = 2,
  LOG_LEVEL_WARN = 3,
  LOG_LEVEL_INFO = 4,
  LOG_LEVEL_DEBUG = 5,
  LOG_LEVEL_TRACE = 6,
  LOG_LEVEL_LEVEL_NUM = 7
};

class LoggerConfig {
 public:
  LoggerConfig(std::string name, std::string path)
      : LoggerConfig(std::move(name), LOG_LEVEL_DEBUG, std::move(path), 1024 * 1024 * 100, 3) {}
  LoggerConfig(std::string name, LogLevel level, std::string path, int file_size, int file_count)
      : name_(std::move(name)), level_(level), path_(std::move(path)), file_size_(file_size), file_count_(file_count) {}

 public:
  const std::string& name() const { return name_; }
  void set_name(const std::string& name) { name_ = name; }

  LogLevel level() const { return level_; }
  void set_level(LogLevel level) { level_ = level; }

  const std::string& path() const { return path_; }
  void set_path(const std::string& path) { path_ = path; }

  int file_size() const { return file_size_; }
  void set_file_size(int file_size) { file_size_ = file_size; }

  int file_count() const { return file_count_; }
  void set_file_count(int file_count) { file_count_ = file_count; }

  bool config_spdlog() const { return config_spdlog_; }
  void set_config_spdlog(bool config_spdlog) { config_spdlog_ = config_spdlog; }

 private:
  std::string name_;
  LogLevel level_;
  std::string path_;
  int file_size_;
  int file_count_;
  bool config_spdlog_{true};
};

LoggerConfig& GetDefaultLoggerConfig();

}  // namespace rocketmq

#endif  // ROCKETMQ_LOGGERCONFIG_H_
