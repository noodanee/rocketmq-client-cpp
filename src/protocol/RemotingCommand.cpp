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
#include "RemotingCommand.h"

#include <atomic>   // std::atomic
#include <limits>   // std::numeric_limits
#include <utility>  // std::move

#include "ByteBuffer.hpp"
#include "ByteOrder.h"
#include "Logging.h"
#include "MQVersion.h"
#include "utility/JsonSerializer.h"

namespace rocketmq {

namespace {

constexpr int kRpcType = 0;    // 0 - REQUEST_COMMAND; 1 - RESPONSE_COMMAND;
constexpr int kRpcOneway = 1;  // 0 - RPC; 1 - Oneway;

int32_t GenerateRequestId() {
  static std::atomic<int32_t> sequence_generator;
  // mask sign bit
  return sequence_generator.fetch_add(1, std::memory_order_relaxed) & std::numeric_limits<int32_t>::max();
}

}  // namespace

RemotingCommand::RemotingCommand(int32_t code, std::unique_ptr<CommandCustomHeader> custom_header)
    : RemotingCommand(code, "", std::move(custom_header)) {}

RemotingCommand::RemotingCommand(int32_t code, std::string remark, std::unique_ptr<CommandCustomHeader> custom_header)
    : RemotingCommand(code,
                      MQVersion::CURRENT_LANGUAGE,
                      MQVersion::CURRENT_VERSION,
                      GenerateRequestId(),
                      0,
                      std::move(remark),
                      std::move(custom_header)) {}

RemotingCommand::RemotingCommand(int32_t code,
                                 std::string language,
                                 int32_t version,
                                 int32_t opaque,
                                 int32_t flag,
                                 std::string remark,
                                 std::unique_ptr<CommandCustomHeader> custom_header)
    : code_(code),
      language_(std::move(language)),
      version_(version),
      opaque_(opaque),
      flag_(flag),
      remark_(std::move(remark)),
      header_(std::move(custom_header)) {}

ByteArrayRef RemotingCommand::Encode() const {
  Json::Value header_object;
  header_object["code"] = code_;
  header_object["language"] = language_;
  header_object["version"] = version_;
  header_object["opaque"] = opaque_;
  header_object["flag"] = flag_;
  header_object["remark"] = remark_;

  Json::Value extend_fields;
  for (const auto& it : extend_fields_) {
    extend_fields[it.first] = it.second;
  }
  if (header_ != nullptr) {
    // write customHeader to extFields
    header_->Encode(extend_fields);
  }
  header_object["extFields"] = extend_fields;

  // serialize header
  std::string header = JsonSerializer::ToJson(header_object);

  uint32_t total_length = 4 + header.size() + (body_ != nullptr ? body_->size() : 0);

  std::unique_ptr<ByteBuffer> result(ByteBuffer::allocate(4 + total_length));

  // total length
  result->putInt(static_cast<int32_t>(total_length));
  // header length
  result->putInt(static_cast<int32_t>(header.size()));
  // header data
  result->put(ByteArray(const_cast<char*>(header.data()), header.size()));
  // body data;
  if (body_ != nullptr) {
    result->put(*body_);
  }

  // result->flip();

  return result->byte_array();
}

namespace {

inline int32_t RealHeaderLength(int32_t length) {
  return length & 0x00FFFFFF;
}

std::unique_ptr<RemotingCommand> Decode(ByteBuffer& byte_buffer, bool has_package_length) {
  // decode package: [4 bytes(packageLength) +] 4 bytes(headerLength) + header + body

  int32_t total_length = byte_buffer.limit();
  if (has_package_length) {
    // skip package length
    (void)byte_buffer.getInt();
    total_length -= 4;
  }

  // decode header

  int32_t raw_header_length = byte_buffer.getInt();
  int32_t header_length = RealHeaderLength(raw_header_length);

  // temporary ByteArray
  ByteArray header_data(byte_buffer.array() + byte_buffer.arrayOffset() + byte_buffer.position(), header_length);
  byte_buffer.position(byte_buffer.position() + header_length);

  Json::Value header_object = [&header_data]() {
    try {
      return JsonSerializer::FromJson(header_data);
    } catch (std::exception& e) {
      LOG_WARN_NEW("parse json failed. {}", e.what());
      THROW_MQEXCEPTION(MQClientException, "can not parse json", -1);
    }
  }();

  int32_t code = header_object["code"].asInt();
  std::string language = header_object["language"].asString();
  int32_t version = header_object["version"].asInt();
  int32_t opaque = header_object["opaque"].asInt();
  int32_t flag = header_object["flag"].asInt();
  std::string remark = [&header_object]() {
    if (!header_object["remark"].isNull()) {
      return header_object["remark"].asString();
    }
    return std::string();
  }();

  std::unique_ptr<RemotingCommand> command(new RemotingCommand(code, language, version, opaque, flag, remark, nullptr));

  if (!header_object["extFields"].isNull()) {
    auto extend_fields = header_object["extFields"];
    for (const auto& name : extend_fields.getMemberNames()) {
      const auto& value = extend_fields[name];
      if (value.isString()) {
        command->set_extend_field(name, value.asString());
      }
    }
  }

  // decode body

  int32_t body_length = total_length - 4 - header_length;
  if (body_length > 0) {
    // slice ByteArray of byteBuffer to avoid copy data.
    ByteArrayRef body_data =
        slice(byte_buffer.byte_array(), byte_buffer.arrayOffset() + byte_buffer.position(), body_length);
    byte_buffer.position(byte_buffer.position() + body_length);
    command->set_body(std::move(body_data));
  }

  LOG_DEBUG_NEW("code:{}, language:{}, version:{}, opaque:{}, flag:{}, remark:{}, headLen:{}, bodyLen:{}", code,
                language, version, opaque, flag, remark, header_length, body_length);

  return command;
}

}  // namespace

std::unique_ptr<RemotingCommand> RemotingCommand::Decode(ByteArrayRef array, bool has_package_length) {
  std::unique_ptr<ByteBuffer> byteBuffer(ByteBuffer::wrap(std::move(array)));
  return rocketmq::Decode(*byteBuffer, has_package_length);
}

bool RemotingCommand::IsResponse() const {
  int bits = 1 << kRpcType;
  return (flag_ & bits) == bits;
}

void RemotingCommand::MarkResponse() {
  int bits = 1 << kRpcType;
  flag_ |= bits;
}

bool RemotingCommand::IsOneway() const {
  int bits = 1 << kRpcOneway;
  return (flag_ & bits) == bits;
}

void RemotingCommand::MarkOneway() {
  int bits = 1 << kRpcOneway;
  flag_ |= bits;
}

CommandCustomHeader* RemotingCommand::GetHeader() const {
  return header_.get();
}

std::string RemotingCommand::ToString() const {
  std::stringstream ss;
  ss << "code:" << code_ << ", opaque:" << opaque_ << ", flag:" << flag_ << ", body.size:" << body_->size();
  return ss.str();
}

}  // namespace rocketmq
