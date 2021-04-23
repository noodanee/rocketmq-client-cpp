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
#ifndef ROCKETMQ_TRANSPORT_EVENTLOOP_H_
#define ROCKETMQ_TRANSPORT_EVENTLOOP_H_

#include <functional>  // std::function

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>

#include "concurrent/thread.hpp"

using socket_t = evutil_socket_t;

namespace rocketmq {

class BufferEvent;

class EventLoop final {
 public:
  static EventLoop* GetDefaultEventLoop();

 public:
  explicit EventLoop(const event_config* config = nullptr, bool run_immediately = true);
  ~EventLoop();

  // disable copy
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // disable move
  EventLoop(EventLoop&&) = delete;
  EventLoop& operator=(EventLoop&&) = delete;

  void Start();
  void Stop();

  bool running() const { return running_; }

  std::unique_ptr<BufferEvent> CreateBufferEvent(socket_t fd, int options);

 private:
  void RunLoop();

 private:
  struct event_base* event_base_{nullptr};
  thread loop_thread_{"EventLoop"};

  bool running_{false};  // aotmic is unnecessary
};

class BufferEvent {
 public:
  using DataCallback = std::function<void(BufferEvent&)>;
  using EventCallback = std::function<void(BufferEvent&, short)>;

 private:
  BufferEvent(bufferevent* event, bool unlock_callbacks, EventLoop& loop);
  friend EventLoop;

 public:
  ~BufferEvent();

  // disable copy
  BufferEvent(const BufferEvent&) = delete;
  BufferEvent& operator=(const BufferEvent&) = delete;

  // disable move
  BufferEvent(BufferEvent&&) = delete;
  BufferEvent& operator=(BufferEvent&&) = delete;

  void SetCallback(DataCallback read_callback, DataCallback write_callback, EventCallback event_callback);

  void SetWatermark(short events, size_t lowmark, size_t highmark) {
    bufferevent_setwatermark(buffer_event_, events, lowmark, highmark);
  }

  int Enable(short event) { return bufferevent_enable(buffer_event_, event); }
  int Disable(short event) { return bufferevent_disable(buffer_event_, event); }

  int Connect(const std::string& address);
  void Close();

  int Write(const void* data, size_t size) { return bufferevent_write(buffer_event_, data, size); }
  size_t Read(void* data, size_t size) { return bufferevent_read(buffer_event_, data, size); }

  struct evbuffer* GetInput() {
    return bufferevent_get_input(buffer_event_);
  }

  socket_t GetFD() const { return bufferevent_getfd(buffer_event_); }

  const std::string& peer_address() const { return peer_address_; }

 private:
  static void ReadCallbackImpl(bufferevent* bev, void* ctx);
  static void WriteCallbackImpl(bufferevent* bev, void* ctx);
  static void EventCallbackImpl(bufferevent* bev, short what, void* ctx);

 private:
  EventLoop& event_loop_;
  bufferevent* buffer_event_;
  const bool unlock_callbacks_;

  DataCallback read_callback_;
  DataCallback write_callback_;
  EventCallback event_callback_;

  // cached properties
  std::string peer_address_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_TRANSPORT_EVENTLOOP_H_
