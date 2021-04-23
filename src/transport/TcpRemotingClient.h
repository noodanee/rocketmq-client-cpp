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
#ifndef ROCKETMQ_TRANSPORT_TCPREMOTINGCLIENT_H_
#define ROCKETMQ_TRANSPORT_TCPREMOTINGCLIENT_H_

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "MQException.h"
#include "MQProtos.h"
#include "RPCHook.h"
#include "RemotingCommand.h"
#include "RequestProcessor.h"
#include "ResponseFuture.h"
#include "ResultState.hpp"
#include "TcpTransport.h"
#include "concurrent/executor.hpp"

namespace rocketmq {

class TcpRemotingClient final {
 public:
  using InvokeCallback = std::function<void(ResultState<std::unique_ptr<RemotingCommand>>) /* noexcept */>;

 public:
  TcpRemotingClient(int worker_thread_nums, int64_t connect_timeout_millis, int64_t try_lock_transport_table_timeout);
  ~TcpRemotingClient();

  // disable copy
  TcpRemotingClient(const TcpRemotingClient&) = delete;
  TcpRemotingClient& operator=(const TcpRemotingClient&) = delete;

  // disable move
  TcpRemotingClient(TcpRemotingClient&&) = delete;
  TcpRemotingClient& operator=(TcpRemotingClient&&) = delete;

 public:
  void Start();
  void Shutdown();

  void RegisterRPCHook(RPCHookPtr rpc_hook);

  void UpdateNameServerAddressList(const std::string& addresses);
  std::vector<std::string> GetNameServerAddressList() const { return nameserver_address_list_; }

  void RegisterProcessor(MQRequestCode request_code, RequestProcessor* request_processor);

  std::unique_ptr<RemotingCommand> InvokeSync(const std::string& address,
                                              RemotingCommand request,
                                              int64_t timeout_millis);
  void InvokeAsync(const std::string& address,
                   RemotingCommand request,
                   InvokeCallback invoke_callback,
                   int64_t timeout_millis);
  void InvokeOneway(const std::string& address, RemotingCommand request);

 private:
  void ProcessPackageReceived(ByteArrayRef package, TcpTransportPtr channel);

  // timeout daemon
  void ScanTimeoutFuturePeriodically();
  void ScanTimeoutFuture();

  TcpTransportPtr GetTransport(const std::string& address);
  TcpTransportPtr CreateTransport(const std::string& address);
  TcpTransportPtr CreateNameServerTransport();

  bool CloseTransport(const std::string& address, const TcpTransportPtr& channel);
  bool CloseNameServerTransport(const TcpTransportPtr& channel);

 private:
  using ProcessorMap = std::map<int, RequestProcessor*>;
  using TransportMap = std::map<std::string, TcpTransportPtr>;

  ProcessorMap processor_table_;  // code -> processor

  TransportMap transport_table_;  // addr -> transport
  std::timed_mutex transport_table_mutex_;

  // FIXME: not strict thread-safe in abnormal scence
  std::vector<RPCHookPtr> rpc_hooks_;  // for Acl / ONS

  int64_t connect_timeout_millis_;            // ms
  int64_t try_lock_transport_table_timeout_;  // s

  // NameServer
  std::timed_mutex namesrv_lock_;
  std::vector<std::string> nameserver_address_list_;
  std::string namesrv_addr_choosed_;
  size_t namesrv_index_{0};

  thread_pool_executor dispatch_executor_;
  thread_pool_executor handle_executor_;
  scheduled_thread_pool_executor timeout_executor_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_TRANSPORT_TCPREMOTINGCLIENT_H_
