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
#ifndef ROCKETMQ_TRANSPORT_TCPTRANSPORT_H_
#define ROCKETMQ_TRANSPORT_TCPTRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <utility>

#include "ByteArray.h"
#include "EventLoop.h"

namespace rocketmq {

enum class TcpConnectStatus { kCreated, kConnecting, kConnected, kFailed, kClosed };

inline std::string ToString(TcpConnectStatus tcp_connect_status) {
  switch (tcp_connect_status) {
    case TcpConnectStatus::kCreated:
      return "CREATED";
    case TcpConnectStatus::kConnecting:
      return "CONNECTING";
    case TcpConnectStatus::kConnected:
      return "CONNECTED";
    case TcpConnectStatus::kFailed:
      return "FAILED";
    case TcpConnectStatus::kClosed:
      return "CLOSED";
  }
}

class TcpTransport;
using TcpTransportPtr = std::shared_ptr<TcpTransport>;

class TcpTransportInfo {
 public:
  TcpTransportInfo() = default;
  virtual ~TcpTransportInfo() = default;

  // disable copy
  TcpTransportInfo(const TcpTransportInfo&) = delete;
  TcpTransportInfo& operator=(const TcpTransportInfo&) = delete;

  // disable move
  TcpTransportInfo(TcpTransportInfo&&) = delete;
  TcpTransportInfo& operator=(TcpTransportInfo&&) = delete;
};

class TcpTransport : public std::enable_shared_from_this<TcpTransport> {
 public:
  using ReadCallback = std::function<void(ByteArrayRef, TcpTransportPtr) /* noexcept */>;
  using CloseCallback = std::function<void(TcpTransportPtr) /* noexcept */>;

 public:
  static TcpTransportPtr CreateTransport(ReadCallback readCallback,
                                         CloseCallback closeCallback,
                                         std::unique_ptr<TcpTransportInfo> info) {
    // transport must be managed by smart pointer
    return TcpTransportPtr(new TcpTransport(std::move(readCallback), std::move(closeCallback), std::move(info)));
  }

  ~TcpTransport();

  // disable copy
  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;

  // disable move
  TcpTransport(TcpTransport&&) = delete;
  TcpTransport& operator=(TcpTransport&&) = delete;

  void Disconnect(const std::string& address);
  TcpConnectStatus Connect(const std::string& address, int64_t timeout_millis);
  TcpConnectStatus WaitConnecting(int64_t timeout_millis);
  TcpConnectStatus LoadStatus();

  bool SendPackage(const char* pData, size_t len);

 private:
  // don't instance object directly.
  TcpTransport(ReadCallback read_callback,
               CloseCallback close_callback,
               std::unique_ptr<TcpTransportInfo> extend_information);

  // BufferEvent callback
  void DataArrived(BufferEvent& event);
  void EventOccurred(BufferEvent& event, short what);

  void PackageReceived(ByteArrayRef msg);

  TcpConnectStatus CloseBufferEvent(bool deleted = false);

  TcpConnectStatus ExchangeStatus(TcpConnectStatus desired);
  bool CompareChangeStatus(TcpConnectStatus& expected, TcpConnectStatus desired);

 public:
  int64_t start_time() const { return start_time_; }

  const std::string& peer_address();

  TcpTransportInfo* extend_information() { return extend_information_.get(); }

 private:
  int64_t start_time_;

  std::unique_ptr<BufferEvent> event_;  // NOTE: use event_ in callback is unsafe.

  std::atomic<TcpConnectStatus> status_{TcpConnectStatus::kCreated};
  std::mutex status_mutex_;
  std::condition_variable status_event_;

  // callback
  ReadCallback read_callback_;
  CloseCallback close_callback_;

  // info
  std::unique_ptr<TcpTransportInfo> extend_information_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_TRANSPORT_TCPTRANSPORT_H_
