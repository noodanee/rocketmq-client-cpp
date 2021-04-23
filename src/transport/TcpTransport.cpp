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
#include "TcpTransport.h"

#ifndef WIN32
#include <arpa/inet.h>  // for sockaddr_in and inet_ntoa...
#include <netinet/tcp.h>
#include <sys/socket.h>  // for socket(), bind(), and connect()...
#endif

#include <array>
#include <chrono>
#include <utility>

#include "ByteBuffer.hpp"
#include "ByteOrder.h"
#include "Logging.h"
#include "TcpRemotingClient.h"
#include "UtilAll.h"

namespace rocketmq {

TcpTransport::TcpTransport(ReadCallback read_callback,
                           CloseCallback close_callback,
                           std::unique_ptr<TcpTransportInfo> extend_information)
    : start_time_(UtilAll::currentTimeMillis()),
      read_callback_(std::move(read_callback)),
      close_callback_(std::move(close_callback)),
      extend_information_(std::move(extend_information)) {}

TcpTransport::~TcpTransport() {
  CloseBufferEvent(true);
}

TcpConnectStatus TcpTransport::CloseBufferEvent(bool deleted) {
  // CloseBufferEvent is idempotent.
  if (ExchangeStatus(TcpConnectStatus::kClosed) != TcpConnectStatus::kClosed) {
    if (event_ != nullptr) {
      event_->Close();
    }
    if (!deleted && close_callback_ != nullptr) {
      close_callback_(shared_from_this());
    }
  }
  return TcpConnectStatus::kClosed;
}

TcpConnectStatus TcpTransport::WaitConnecting(int64_t timeout_millis) {
  if (status_ == TcpConnectStatus::kConnecting) {
    std::unique_lock<std::mutex> lock(status_mutex_);
    if (!status_event_.wait_for(lock, std::chrono::milliseconds(timeout_millis),
                                [&] { return status_ != TcpConnectStatus::kConnecting; })) {
      LOG_INFO("connect timeout");
    }
  }
  return status_;
}

TcpConnectStatus TcpTransport::LoadStatus() {
  return status_;
}

// internal method
TcpConnectStatus TcpTransport::ExchangeStatus(TcpConnectStatus desired) {
  TcpConnectStatus oldStatus = status_.exchange(desired, std::memory_order_relaxed);
  if (oldStatus == TcpConnectStatus::kConnecting) {
    // awake waiting thread
    status_event_.notify_all();
  }
  return oldStatus;
}

bool TcpTransport::CompareChangeStatus(TcpConnectStatus& expected, TcpConnectStatus desired) {
  bool successed = status_.compare_exchange_strong(expected, desired);
  if (expected == TcpConnectStatus::kConnecting) {
    // awake waiting thread
    status_event_.notify_all();
  }
  return successed;
}

void TcpTransport::Disconnect(const std::string& address) {
  // disconnect is idempotent.
  LOG_INFO_NEW("disconnect:{} start. event:{}", address, (void*)event_.get());
  CloseBufferEvent();
  LOG_INFO_NEW("disconnect:{} completely", address);
}

TcpConnectStatus TcpTransport::Connect(const std::string& address, int64_t timeout_millis) {
  LOG_INFO_NEW("connect to {}.", address);

  TcpConnectStatus status = TcpConnectStatus::kCreated;
  if (CompareChangeStatus(status, TcpConnectStatus::kConnecting)) {
    // create BufferEvent
    event_ = EventLoop::GetDefaultEventLoop()->CreateBufferEvent(-1, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
    if (nullptr == event_) {
      LOG_ERROR_NEW("create BufferEvent failed");
      return CloseBufferEvent();
    }

    // then, configure BufferEvent
    auto transport_ptr = std::weak_ptr<TcpTransport>(shared_from_this());
    auto read_callback = [=](BufferEvent& event) {
      auto channel = transport_ptr.lock();
      if (channel != nullptr) {
        channel->DataArrived(event);
      } else {
        LOG_WARN_NEW("[BUG] TcpTransport object is released.");
      }
    };
    auto event_callback = [=](BufferEvent& event, short what) {
      auto channel = transport_ptr.lock();
      if (channel != nullptr) {
        channel->EventOccurred(event, what);
      } else {
        LOG_WARN_NEW("[BUG] TcpTransport object is released.");
      }
    };
    event_->SetCallback(read_callback, nullptr, event_callback);
    event_->SetWatermark(EV_READ, 4, 0);
    event_->Enable(EV_READ | EV_WRITE);

    if (event_->Connect(address) < 0) {
      LOG_WARN_NEW("connect to fd:{} failed", event_->GetFD());
      return CloseBufferEvent();
    }
  } else {
    return status;
  }

  if (timeout_millis <= 0) {
    LOG_INFO_NEW("try to connect to fd:{}, addr:{}", event_->GetFD(), address);
    return TcpConnectStatus::kConnecting;
  }

  status = WaitConnecting(timeout_millis);
  if (status != TcpConnectStatus::kConnected) {
    LOG_WARN_NEW("can not connect to server:{}", address);
    return CloseBufferEvent();
  }

  return TcpConnectStatus::kConnected;
}

void TcpTransport::EventOccurred(BufferEvent& event, short what) {
  socket_t fd = event.GetFD();
  LOG_INFO_NEW("eventcb: received event:0x{:X} on fd:{}", what, fd);

  if ((what & BEV_EVENT_CONNECTED) != 0) {
    LOG_INFO_NEW("eventcb: connect to fd:{} successfully", fd);

    // disable Nagle
    int val = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void*)&val, sizeof(val)) < 0) {
      LOG_WARN_NEW("eventcb: disable Nagle failed. fd:{}", fd);
    }

    // disable Keep-Alive
    val = 0;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void*)&val, sizeof(val)) < 0) {
      LOG_WARN_NEW("eventcb: disable Keep-Alive failed. fd:{}", fd);
    }

    TcpConnectStatus curStatus = TcpConnectStatus::kConnecting;
    CompareChangeStatus(curStatus, TcpConnectStatus::kConnected);
  } else if ((what & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) != 0) {
    LOG_ERROR_NEW("eventcb: received error event:0x{:X} on fd:{}", what, fd);
    CloseBufferEvent();
  } else {
    LOG_ERROR_NEW("eventcb: received error event:0x{:X} on fd:{}", what, fd);
  }
}

void TcpTransport::DataArrived(BufferEvent& event) {
  /* This callback is invoked when there is data to read on bev. */

  // protocol:  <length> <header length> <header data> <body data>
  //               1            2               3           4
  // rocketmq protocol contains 4 parts as following:
  //     1, big endian 4 bytes int, its length is sum of 2,3 and 4
  //     2, big endian 4 bytes int, its length is 3
  //     3, use json to serialization data
  //     4, application could self-defined binary data

  struct evbuffer* input = event.GetInput();
  while (true) {
    // glance at first 4 byte to get package length
    std::array<evbuffer_iovec, 4> iovec{};
    std::array<char, 4> length_buffer{};

    std::size_t needed = 4;
    int n = evbuffer_peek(input, 4, nullptr, iovec.data(), iovec.size());
    for (int i = 0; i < n && needed > 0; i++) {
      const auto& elem = iovec.at(i);
      auto size = std::min(needed, elem.iov_len);
      std::memcpy(&length_buffer.at(4 - needed), elem.iov_base, size);
      needed -= size;
    }

    if (needed > 0) {
      LOG_DEBUG_NEW("too little data received with {} byte(s)", 4 - needed);
      return;
    }

    auto package_length = ByteOrderUtil::Read<uint32_t>(length_buffer.data(), true);
    size_t received_length = evbuffer_get_length(input);
    if (received_length >= package_length + 4) {
      LOG_DEBUG_NEW("had received all data. package length:{}, from:{}, received length:{}", package_length,
                    event.GetFD(), received_length);
    } else {
      LOG_DEBUG_NEW("didn't received whole. package length:{}, from:{}, received length:{}", package_length,
                    event.GetFD(), received_length);
      return;  // consider large data which was not received completely by now
    }

    if (package_length > 0) {
      auto package = std::make_shared<ByteArray>(package_length);

      event.Read(length_buffer.data(), 4);  // skip length field
      event.Read(package->array(), package_length);

      PackageReceived(package);
    }
  }
}

void TcpTransport::PackageReceived(ByteArrayRef msg) {
  if (read_callback_ != nullptr) {
    read_callback_(std::move(msg), shared_from_this());
  }
}

bool TcpTransport::SendPackage(const char* data, size_t len) {
  if (LoadStatus() != TcpConnectStatus::kConnected) {
    return false;
  }

  /* NOTE:
      do not need to consider large data which could not send by once, as
      bufferevent could handle this case;
   */
  return event_ != nullptr && event_->Write(data, len) == 0;
}

const std::string& TcpTransport::peer_address() {
  return event_ != nullptr ? event_->peer_address() : null;
}

}  // namespace rocketmq
