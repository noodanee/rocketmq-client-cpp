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
#ifndef ROCKETMQ_LOGGERCONFIG_HPP_
#define ROCKETMQ_LOGGERCONFIG_HPP_

#include <string>   // std::string
#include <utility>  // std::move

#include "LogLevel.hpp"

namespace rocketmq {

class LoggerConfig {
 public:
  LoggerConfig() = default;
  LoggerConfig(std::string name, std::string path) : name_(std::move(name)), path_(std::move(path)) {}
  LoggerConfig(std::string name, std::string path, LogLevel level, int file_size, int file_nums)
      : name_(std::move(name)), path_(std::move(path)), level_(level), file_size_(file_size), file_nums_(file_nums) {}

  const std::string& name() const { return name_; }
  void set_name(const std::string& name) { name_ = name; }

  LogLevel level() const { return level_; }
  void set_level(LogLevel level) { level_ = level; }

  const std::string& path() const { return path_; }
  void set_path(const std::string& path) { path_ = path; }

  int file_size() const { return file_size_; }
  void set_file_size(int file_size) { file_size_ = file_size; }

  int file_nums() const { return file_nums_; }
  void set_file_nums(int file_nums) { file_nums_ = file_nums; }

 private:
  std::string name_;
  std::string path_;
  LogLevel level_{LogLevel::kInfo};
  int file_size_{1024 * 1024 * 100};
  int file_nums_{3};
};

LoggerConfig& GetDefaultLoggerConfig();

void SkipConfigSpdlog();

}  // namespace rocketmq

#endif  // ROCKETMQ_LOGGERCONFIG_HPP_
