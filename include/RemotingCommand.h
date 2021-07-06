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
#ifndef ROCKETMQ_REMOTINGCOMMAND_H_
#define ROCKETMQ_REMOTINGCOMMAND_H_

#include <exception>  // std::exception
#include <map>        // std::map
#include <memory>     // std::unique_ptr, std::shared_ptr
#include <string>     // std::string
#include <typeindex>  // std::type_index

#include "ByteArray.h"
#include "CommandCustomHeader.h"
#include "MQException.h"

namespace rocketmq {

/**
 * RemotingCommand - rocketmq rpc protocol
 */
class ROCKETMQCLIENT_API RemotingCommand {
 public:
  RemotingCommand() = default;
  RemotingCommand(int32_t code, std::unique_ptr<CommandCustomHeader> header);
  RemotingCommand(int32_t code, std::string remark, std::unique_ptr<CommandCustomHeader> header);
  RemotingCommand(int32_t code,
                  std::string language,
                  int32_t version,
                  int32_t opaque,
                  int32_t flag,
                  std::string remark,
                  std::unique_ptr<CommandCustomHeader> header);

  ~RemotingCommand() = default;

  // disable copy
  RemotingCommand(const RemotingCommand&) = delete;
  RemotingCommand& operator=(const RemotingCommand&) = delete;

  // enable move
  RemotingCommand(RemotingCommand&&) = default;
  RemotingCommand& operator=(RemotingCommand&&) = default;

  bool IsResponse() const;
  void MarkResponse();

  bool IsOneway() const;
  void MarkOneway();

  CommandCustomHeader* GetHeader() const;

  ByteArrayRef Encode() const;

  static std::unique_ptr<RemotingCommand> Decode(ByteArrayRef array, bool has_package_length = false);

  template <class H>
  H* DecodeHeader(bool use_cache = true);

  std::string ToString() const;

 public:
  int32_t code() const { return code_; }
  void set_code(int32_t code) { code_ = code; }

  int32_t version() const { return version_; }

  int32_t opaque() const { return opaque_; }
  void set_opaque(int32_t opaque) { opaque_ = opaque; }

  int32_t flag() const { return flag_; }

  const std::string& remark() const { return remark_; }
  void set_remark(std::string remark) { remark_ = std::move(remark); }

  void set_extend_field(const std::string& name, std::string value) { extend_fields_[name] = std::move(value); }

  ByteArrayRef body() const { return body_; }
  void set_body(ByteArrayRef body) { body_ = std::move(body); }
  void set_body(const std::string& body) { body_ = stoba(body); }
  void set_body(std::string&& body) { body_ = stoba(std::move(body)); }

 private:
  int32_t code_{0};
  std::string language_;
  int32_t version_{0};
  int32_t opaque_{0};
  int32_t flag_{0};
  std::string remark_;
  std::map<std::string, std::string> extend_fields_;

  std::unique_ptr<CommandCustomHeader> header_;  // transient

  ByteArrayRef body_;  // transient
};

template <class H>
H* RemotingCommand::DecodeHeader(bool use_cache) {
  if (use_cache) {
    auto* cached = header_.get();
    if (cached != nullptr && std::type_index(typeid(*cached)) == std::type_index(typeid(H))) {
      return static_cast<H*>(cached);
    }
  }

  try {
    header_ = H::Decode(extend_fields_);
    return static_cast<H*>(header_.get());
  } catch (std::exception& e) {
    THROW_MQEXCEPTION(RemotingCommandException, e.what(), -1);
  }
}

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTINGCOMMAND_H_
