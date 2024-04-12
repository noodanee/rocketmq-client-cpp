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
#include "TcpRemotingClient.h"

#include <cstddef>

#include <exception>
#include <memory>
#include <utility>  // std::move

#include "Logging.h"
#include "UtilAll.h"
#include "concurrent/executor.hpp"
#include "utility/MakeUnique.hpp"

namespace {

constexpr int64_t kSecondToMilliseconds = 1000;

}

namespace rocketmq {

namespace {

class ResponseFutureInfo : public TcpTransportInfo {
 public:
  using FutureMap = std::map<int, ResponseFuturePtr>;

 public:
  ResponseFutureInfo() = default;
  ~ResponseFutureInfo() override {
    // FIXME: Can optimize?
    ActiveAllResponseFutures();
  }

  // disable copy
  ResponseFutureInfo(const ResponseFutureInfo&) = delete;
  ResponseFutureInfo& operator=(const ResponseFutureInfo&) = delete;

  // disable move
  ResponseFutureInfo(ResponseFutureInfo&&) = delete;
  ResponseFutureInfo& operator=(ResponseFutureInfo&&) = delete;

  void PutResponseFuture(int opaque, ResponseFuturePtr future) {
    std::lock_guard<std::mutex> lock(future_table_mutex_);
    const auto it = future_table_.find(opaque);
    if (it != future_table_.end()) {
      LOG_WARN_NEW("[BUG] response futurn with opaque:{} is exist.", opaque);
    }
    future_table_[opaque] = std::move(future);
  }

  ResponseFuturePtr PopResponseFuture(int opaque) {
    std::lock_guard<std::mutex> lock(future_table_mutex_);
    const auto& it = future_table_.find(opaque);
    if (it != future_table_.end()) {
      auto future = it->second;
      future_table_.erase(it);
      return future;
    }
    return nullptr;
  }

  void ScanTimeoutFuture(int64_t now, std::vector<ResponseFuturePtr>& future_list) {
    std::lock_guard<std::mutex> lock(future_table_mutex_);
    for (auto it = future_table_.begin(); it != future_table_.end();) {
      auto& future = it->second;  // NOTE: future is a reference
      if (future->Timeout(now - kSecondToMilliseconds)) {
        LOG_WARN_NEW("remove timeout request, code:{}, opaque:{}", future->request_code(), future->opaque());
        future_list.push_back(future);
        it = future_table_.erase(it);
      } else {
        ++it;
      }
    }
  }

  void ActiveAllResponseFutures() {
    std::lock_guard<std::mutex> lock(future_table_mutex_);
    for (const auto& it : future_table_) {
      const auto& future = it.second;
      future->PutResponseCommand(nullptr);
    }
    future_table_.clear();
  }

 private:
  FutureMap future_table_;  // opaque -> future
  std::mutex future_table_mutex_;
};

}  // namespace

TcpRemotingClient::TcpRemotingClient(int worker_thread_nums,
                                     int64_t connect_timeout_millis,
                                     int64_t try_lock_transport_table_timeout)
    : connect_timeout_millis_(connect_timeout_millis),
      try_lock_transport_table_timeout_(try_lock_transport_table_timeout),
      dispatch_executor_("MessageDispatchExecutor", 1, false),
      handle_executor_("MessageHandleExecutor", worker_thread_nums, false),
      timeout_executor_("TimeoutScanExecutor", false) {}

TcpRemotingClient::~TcpRemotingClient() {
  LOG_DEBUG_NEW("TcpRemotingClient is destructed.");
}

void TcpRemotingClient::Start() {
  dispatch_executor_.startup();
  handle_executor_.startup();

  LOG_INFO_NEW("tcpConnectTimeout:{}, tcpTransportTryLockTimeout:{}, pullThreadNums:{}", connect_timeout_millis_,
               try_lock_transport_table_timeout_, handle_executor_.thread_nums());

  timeout_executor_.startup();

  // scanResponseTable
  timeout_executor_.schedule([this]() { ScanTimeoutFuturePeriodically(); }, 3 * kSecondToMilliseconds,
                             time_unit::milliseconds);
}

void TcpRemotingClient::ScanTimeoutFuturePeriodically() {
  try {
    ScanTimeoutFuture();
  } catch (std::exception& e) {
    LOG_ERROR_NEW("scanResponseTable exception: {}", e.what());
  }

  // next round
  timeout_executor_.schedule([this]() { ScanTimeoutFuturePeriodically(); }, kSecondToMilliseconds,
                             time_unit::milliseconds);
}

void TcpRemotingClient::ScanTimeoutFuture() {
  std::vector<TcpTransportPtr> channel_list;
  {
    std::lock_guard<std::timed_mutex> lock(transport_table_mutex_);
    for (const auto& trans : transport_table_) {
      channel_list.push_back(trans.second);
    }
  }

  int64_t now = UtilAll::currentTimeMillis();

  std::vector<ResponseFuturePtr> future_list;
  for (const auto& channel : channel_list) {
    static_cast<ResponseFutureInfo*>(channel->extend_information())->ScanTimeoutFuture(now, future_list);
  }
  for (const auto& future : future_list) {
    future->PutResponseCommand(nullptr);
  }
}

void TcpRemotingClient::Shutdown() {
  LOG_INFO_NEW("TcpRemotingClient::shutdown Begin");

  timeout_executor_.shutdown();

  {
    std::lock_guard<std::timed_mutex> lock(transport_table_mutex_);
    for (const auto& trans : transport_table_) {
      trans.second->Disconnect(trans.first);
    }
    transport_table_.clear();
  }

  handle_executor_.shutdown();

  dispatch_executor_.shutdown();

  LOG_INFO_NEW("TcpRemotingClient::shutdown End, transport_table_:{}", transport_table_.size());
}

void TcpRemotingClient::RegisterRPCHook(RPCHookPtr rpc_hook) {
  if (rpc_hook != nullptr) {
    for (auto& hook : rpc_hooks_) {
      if (hook == rpc_hook) {
        return;
      }
    }

    rpc_hooks_.push_back(std::move(rpc_hook));
  }
}

void TcpRemotingClient::UpdateNameServerAddressList(const std::string& addresses) {
  LOG_INFO_NEW("updateNameServerAddressList: [{}]", addresses);

  if (addresses.empty()) {
    return;
  }

  if (!UtilAll::try_lock_for(namesrv_lock_, 10 * kSecondToMilliseconds)) {
    LOG_ERROR_NEW("updateNameServerAddressList get timed_mutex timeout");
    return;
  }
  std::lock_guard<std::timed_mutex> lock(namesrv_lock_, std::adopt_lock);

  // clear first
  nameserver_address_list_.clear();

  std::vector<std::string> out;
  UtilAll::Split(out, addresses, ";");
  for (auto& addr : out) {
    UtilAll::Trim(addr);

    std::string hostName;
    short portNumber = 0;
    if (UtilAll::SplitURL(addr, hostName, portNumber)) {
      LOG_INFO_NEW("update Namesrv:{}", addr);
      nameserver_address_list_.push_back(addr);
    } else {
      LOG_INFO_NEW("This may be invalid namer server: [{}]", addr);
    }
  }
}

void TcpRemotingClient::RegisterProcessor(MQRequestCode request_code, RequestProcessor* request_processor) {
  // replace
  processor_table_[request_code] = request_processor;
}

namespace {

inline void DoBeforeRpcHooks(const std::vector<RPCHookPtr>& rpc_hooks,
                             const std::string& address,
                             RemotingCommand& request,
                             bool to_sent) {
  for (const auto& rpc_hook : rpc_hooks) {
    rpc_hook->doBeforeRequest(address, request, to_sent);
  }
}

inline void DoAfterRpcHooks(const std::vector<RPCHookPtr>& rpc_hooks,
                            const std::string& address,
                            RemotingCommand& request,
                            RemotingCommand* response,
                            bool to_sent) {
  for (const auto& rpc_hook : rpc_hooks) {
    rpc_hook->doAfterResponse(address, request, response, to_sent);
  }
}

void PutResponseFuture(const TcpTransportPtr& channel, int opaque, ResponseFuturePtr future) {
  return static_cast<ResponseFutureInfo*>(channel->extend_information())->PutResponseFuture(opaque, std::move(future));
}

// NOTE: after call this function, shared_ptr of future_table_[opaque] will
// be erased, so caller must ensure the life cycle of returned shared_ptr
ResponseFuturePtr PopResponseFuture(const TcpTransportPtr& channel, int opaque) {
  return static_cast<ResponseFutureInfo*>(channel->extend_information())->PopResponseFuture(opaque);
}

bool SendCommand(const TcpTransportPtr& channel, RemotingCommand& command) noexcept {
  auto package = command.Encode();
  return channel->SendPackage(package->array(), package->size());
}

std::unique_ptr<RemotingCommand> InvokeSyncImpl(const TcpTransportPtr& channel,
                                                RemotingCommand& request,
                                                int64_t timeout_millis) {
  int code = request.code();
  int opaque = request.opaque();

  auto response_future = std::make_shared<ResponseFuture>(code, opaque, timeout_millis);
  PutResponseFuture(channel, opaque, response_future);

  if (SendCommand(channel, request)) {
    response_future->set_send_request_ok(true);
  } else {
    response_future->set_send_request_ok(false);
    PopResponseFuture(channel, opaque);  // clean future
    // wake up potential waitResponse() is unnecessary, so not call putResponse()
    LOG_WARN_NEW("send a request command to channel <{}> failed.", channel->peer_address());
    THROW_MQEXCEPTION(RemotingSendRequestException, "send request to <" + channel->peer_address() + "> failed", -1);
  }

  auto response = response_future->WaitResponseCommand(timeout_millis);
  if (nullptr == response) {             // timeout
    PopResponseFuture(channel, opaque);  // clean future
    THROW_MQEXCEPTION(RemotingTimeoutException, "wait response on the addr <" + channel->peer_address() + "> timeout",
                      -1);
  }

  return response;
}

}  // namespace

std::unique_ptr<RemotingCommand> TcpRemotingClient::InvokeSync(const std::string& address,
                                                               RemotingCommand request,
                                                               int64_t timeout_millis) {
  auto begin_time = UtilAll::currentTimeMillis();

  auto channel = GetTransport(address);

  try {
    DoBeforeRpcHooks(rpc_hooks_, address, request, true);

    auto cost_time = UtilAll::currentTimeMillis() - begin_time;
    if (timeout_millis <= 0 || timeout_millis < cost_time) {
      THROW_MQEXCEPTION(RemotingTimeoutException, "invokeSync call timeout", -1);
    }

    auto response = InvokeSyncImpl(channel, request, timeout_millis);

    DoAfterRpcHooks(rpc_hooks_, address, request, response.get(), false);

    return response;
  } catch (const RemotingSendRequestException& e) {
    LOG_WARN_NEW("invokeSync: send request exception, so close the channel[{}]", channel->peer_address());
    CloseTransport(address, channel);
    throw;
  } catch (const RemotingTimeoutException& e) {
    int code = request.code();
    if (code != GET_CONSUMER_LIST_BY_GROUP) {
      CloseTransport(address, channel);
      LOG_WARN_NEW("invokeSync: close socket because of timeout, {}ms, {}", timeout_millis, channel->peer_address());
    }
    LOG_WARN_NEW("invokeSync: wait response timeout exception, the channel[{}]", channel->peer_address());
    throw;
  }
}

namespace {

void InvokeAsyncImpl(const TcpTransportPtr& channel,
                     RemotingCommand& request,
                     TcpRemotingClient::InvokeCallback invoke_callback,
                     thread_pool_executor& executor,
                     int64_t timeout_millis) {
  int code = request.code();
  int opaque = request.opaque();

  auto response_future = std::make_shared<ResponseFuture>(code, opaque, timeout_millis, std::move(invoke_callback),
                                                          [&executor](std::function<void()> task) {
                                                            try {
                                                              executor.execute(std::move(task));
                                                            } catch (const std::exception& e) {
                                                              LOG_WARN_NEW("{}", e.what());
                                                            }
                                                          });
  PutResponseFuture(channel, opaque, response_future);

  if (SendCommand(channel, request)) {
    response_future->set_send_request_ok(true);
  } else {
    // request fail
    response_future = PopResponseFuture(channel, opaque);
    if (response_future != nullptr) {
      response_future->set_send_request_ok(false);
      response_future->PutResponseCommand(nullptr);
    }

    LOG_WARN_NEW("send a request command to channel <{}> failed.", channel->peer_address());
  }
}

}  // namespace

void TcpRemotingClient::InvokeAsync(const std::string& addr,
                                    RemotingCommand request,
                                    InvokeCallback invoke_callback,
                                    int64_t timeout_millis) {
  auto beginStartTime = UtilAll::currentTimeMillis();
  auto channel = GetTransport(addr);
  if (channel != nullptr) {
    try {
      DoBeforeRpcHooks(rpc_hooks_, addr, request, true);
      auto costTime = UtilAll::currentTimeMillis() - beginStartTime;
      if (timeout_millis <= 0 || timeout_millis < costTime) {
        THROW_MQEXCEPTION(RemotingTooMuchRequestException, "invokeAsync call timeout", -1);
      }
      InvokeAsyncImpl(channel, request, std::move(invoke_callback), handle_executor_, timeout_millis);
    } catch (const RemotingSendRequestException& e) {
      LOG_WARN_NEW("invokeAsync: send request exception, so close the channel[{}]", channel->peer_address());
      CloseTransport(addr, channel);
      throw;
    }
  } else {
    THROW_MQEXCEPTION(RemotingConnectException, "connect to <" + addr + "> failed", -1);
  }
}

namespace {

void InvokeOnewayImpl(const TcpTransportPtr& channel, RemotingCommand& request) {
  request.MarkOneway();
  try {
    if (!SendCommand(channel, request)) {
      LOG_WARN_NEW("send a request command to channel <{}> failed.", channel->peer_address());
    }
  } catch (const std::exception& e) {
    LOG_WARN_NEW("send a request command to channel <{}> Exception.\n{}", channel->peer_address(), e.what());
    THROW_MQEXCEPTION(RemotingSendRequestException, "send request to <" + channel->peer_address() + "> failed", -1);
  }
}

}  // namespace

void TcpRemotingClient::InvokeOneway(const std::string& addr, RemotingCommand request) {
  auto channel = GetTransport(addr);
  if (channel != nullptr) {
    try {
      DoBeforeRpcHooks(rpc_hooks_, addr, request, true);
      InvokeOnewayImpl(channel, request);
    } catch (const RemotingSendRequestException& e) {
      LOG_WARN_NEW("invokeOneway: send request exception, so close the channel[{}]", channel->peer_address());
      CloseTransport(addr, channel);
      throw;
    }
  } else {
    THROW_MQEXCEPTION(RemotingConnectException, "connect to <" + addr + "> failed", -1);
  }
}

TcpTransportPtr TcpRemotingClient::GetTransport(const std::string& address) {
  auto channel = [this, &address]() {
    if (address.empty()) {
      LOG_DEBUG_NEW("Get namesrv transport");
      return CreateNameServerTransport();
    }
    return CreateTransport(address);
  }();
  if (channel == nullptr) {
    THROW_MQEXCEPTION(RemotingConnectException, "connect to <" + address + "> failed", -1);
  }
  return channel;
}

TcpTransportPtr TcpRemotingClient::CreateTransport(const std::string& address) {
  TcpTransportPtr channel;

  {
    // try get transport_table_mutex_ until tcp_transport_try_lock_timeout_ to avoid blocking long time,
    // if could not get transport_table_mutex_, return NULL
    if (!UtilAll::try_lock_for(transport_table_mutex_, try_lock_transport_table_timeout_ * kSecondToMilliseconds)) {
      LOG_ERROR_NEW("GetTransport of:{} get timed_mutex timeout", address);
      return nullptr;
    }
    std::lock_guard<std::timed_mutex> lock(transport_table_mutex_, std::adopt_lock);

    // check for reuse
    const auto& it = transport_table_.find(address);
    if (it != transport_table_.end()) {
      channel = it->second;
    }

    if (channel != nullptr) {
      switch (channel->LoadStatus()) {
        case TcpConnectStatus::kConnected:
          return channel;
        case TcpConnectStatus::kConnecting:
          // wait server answer
          break;
        case TcpConnectStatus::kFailed:
          LOG_WARN_NEW("trnsport with server disconnected, erase server:{}", address);
          channel->Disconnect(address);
          transport_table_.erase(it);
          channel = nullptr;
          break;
        default:  // TcpConnectStatus::kClosed
          LOG_WARN_NEW("go to CLOSED state, erase:{} from transport table, and reconnect it", address);
          transport_table_.erase(it);
          channel = nullptr;
          break;
      }
    }

    if (channel == nullptr) {
      // callback
      auto read_callback = [this](ByteArrayRef package, TcpTransportPtr channel) {
        dispatch_executor_.submit(
#if __cplusplus >= 201402L
            [this, package = std::move(package), channel = std::move(channel)]
#else
            [this, package, channel]
#endif
            () { ProcessPackageReceived(package, channel); });
      };
      auto close_callback = [this](TcpTransportPtr channel) noexcept {
        LOG_DEBUG_NEW("channel of {} is closed.", channel->peer_address());
        handle_executor_.submit(
#if __cplusplus >= 201402L
            [channel = std::move(channel)]
#else
            [channel]
#endif
            () { static_cast<ResponseFutureInfo*>(channel->extend_information())->ActiveAllResponseFutures(); });
      };

      // create new transport, then connect server
      auto information = MakeUnique<ResponseFutureInfo>();
      channel = TcpTransport::CreateTransport(read_callback, close_callback, std::move(information));
      TcpConnectStatus status = channel->Connect(address, 0);  // use non-block
      if (status != TcpConnectStatus::kConnecting) {
        LOG_WARN_NEW("can not connect to:{}", address);
        channel->Disconnect(address);
        return nullptr;
      }

      // even if connecting failed finally, this server transport will be erased by next CreateTransport
      transport_table_[address] = channel;
    }
  }

  // waiting...
  TcpConnectStatus status = channel->WaitConnecting(static_cast<int>(connect_timeout_millis_));
  if (status != TcpConnectStatus::kConnected) {
    LOG_WARN_NEW("can not connect to server:{}", address);
    return nullptr;
  }

  LOG_INFO_NEW("connect server with addr:{} success", address);
  return channel;
}

TcpTransportPtr TcpRemotingClient::CreateNameServerTransport() {
  // namesrv_lock_ was added to avoid operation of NameServer was blocked by
  // m_tcpLock, it was used by single Thread mostly, so no performance impact
  // try get m_tcpLock until tcp_transport_try_lock_timeout_ to avoid blocking long
  // time, if could not get m_namesrvlock, return NULL
  LOG_DEBUG_NEW("create namesrv transport");
  if (!UtilAll::try_lock_for(namesrv_lock_, try_lock_transport_table_timeout_ * kSecondToMilliseconds)) {
    LOG_ERROR_NEW("CreateNameserverTransport get timed_mutex timeout");
    return nullptr;
  }
  std::lock_guard<std::timed_mutex> lock(namesrv_lock_, std::adopt_lock);

  if (!namesrv_addr_choosed_.empty()) {
    auto channel = CreateTransport(namesrv_addr_choosed_);
    if (channel != nullptr) {
      return channel;
    }
    namesrv_addr_choosed_.clear();
  }

  for (unsigned i = 0; i < nameserver_address_list_.size(); i++) {
    auto index = namesrv_index_++ % nameserver_address_list_.size();
    LOG_INFO_NEW("namesrvIndex is:{}, index:{}, namesrvaddrlist size:{}", namesrv_index_, index,
                 nameserver_address_list_.size());
    auto channel = CreateTransport(nameserver_address_list_[index]);
    if (channel != nullptr) {
      namesrv_addr_choosed_ = nameserver_address_list_[index];
      return channel;
    }
  }

  return nullptr;
}

bool TcpRemotingClient::CloseTransport(const std::string& address, const TcpTransportPtr& channel) {
  if (address.empty()) {
    return CloseNameServerTransport(channel);
  }

  if (!UtilAll::try_lock_for(transport_table_mutex_, try_lock_transport_table_timeout_ * kSecondToMilliseconds)) {
    LOG_ERROR_NEW("CloseTransport of:{} get timed_mutex timeout", address);
    return false;
  }
  std::lock_guard<std::timed_mutex> lock(transport_table_mutex_, std::adopt_lock);

  LOG_INFO_NEW("CloseTransport of:{}", address);

  bool removed = true;
  const auto& it = transport_table_.find(address);
  if (it != transport_table_.end()) {
    if (it->second->start_time() != channel->start_time()) {
      LOG_INFO_NEW("tcpTransport with addr:{} has been closed before, and has been created again, nothing to do",
                   address);
      removed = false;
    }
  } else {
    LOG_INFO_NEW("tcpTransport with addr:{} had been removed from tcpTable before", address);
    removed = false;
  }

  if (removed) {
    LOG_WARN_NEW("closeTransport: erase broker: {}", address);
    transport_table_.erase(address);
  }

  LOG_WARN_NEW("closeTransport: disconnect:{} with state:{}", address, toString(channel->LoadStatus()));
  if (channel->LoadStatus() != TcpConnectStatus::kClosed) {
    channel->Disconnect(address);  // avoid coredump when connection with server was broken
  }

  LOG_ERROR_NEW("CloseTransport of:{} end", address);

  return removed;
}

bool TcpRemotingClient::CloseNameServerTransport(const TcpTransportPtr& channel) {
  if (!UtilAll::try_lock_for(namesrv_lock_, try_lock_transport_table_timeout_ * kSecondToMilliseconds)) {
    LOG_ERROR_NEW("CloseNameServerTransport get timed_mutex timeout");
    return false;
  }
  std::lock_guard<std::timed_mutex> lock(namesrv_lock_, std::adopt_lock);

  std::string address = namesrv_addr_choosed_;
  bool removed = CloseTransport(address, channel);
  if (removed) {
    namesrv_addr_choosed_.clear();
  }

  return removed;
}

namespace {

void ProcessResponseCommand(std::unique_ptr<RemotingCommand> response_command, const TcpTransportPtr& channel) {
  int opaque = response_command->opaque();
  auto response_future = PopResponseFuture(channel, opaque);
  if (response_future != nullptr) {
    int code = response_future->request_code();
    LOG_DEBUG_NEW("processResponseCommand, opaque:{}, request code:{}, server:{}", opaque, code,
                  channel->peer_address());
    response_future->PutResponseCommand(std::move(response_command));
  } else {
    LOG_DEBUG_NEW("responseFuture was deleted by timeout of opaque:{}, server:{}", opaque, channel->peer_address());
  }
}

void ProcessRequestCommand(std::unique_ptr<RemotingCommand> request_command,
                           const TcpTransportPtr& channel,
                           const std::map<int, RequestProcessor*>& processor_table,
                           const std::vector<RPCHookPtr>& rpc_hooks) {
  std::unique_ptr<RemotingCommand> response;

  int requestCode = request_command->code();
  const auto& it = processor_table.find(requestCode);
  if (it != processor_table.end()) {
    try {
      auto* processor = it->second;

      DoBeforeRpcHooks(rpc_hooks, channel->peer_address(), *request_command, false);
      response = processor->processRequest(channel, request_command.get());
      DoAfterRpcHooks(rpc_hooks, channel->peer_address(), *response, response.get(), true);
    } catch (std::exception& e) {
      LOG_ERROR_NEW("process request exception. {}", e.what());

      // send SYSTEM_ERROR response
      response = MakeUnique<RemotingCommand>(SYSTEM_ERROR, e.what(), nullptr);
    }
  } else {
    // send REQUEST_CODE_NOT_SUPPORTED response
    std::string error = "request type " + UtilAll::to_string(request_command->code()) + " not supported";
    response = MakeUnique<RemotingCommand>(REQUEST_CODE_NOT_SUPPORTED, error, nullptr);

    LOG_ERROR_NEW("{}: {}", channel->peer_address(), error);
  }

  if (!request_command->IsOneway() && response != nullptr) {
    response->set_opaque(request_command->opaque());
    response->MarkResponse();
    try {
      if (!SendCommand(channel, *response)) {
        LOG_WARN_NEW("send a response command to channel <{}> failed.", channel->peer_address());
      }
    } catch (const std::exception& e) {
      LOG_ERROR_NEW("process request over, but response failed. {}", e.what());
    }
  }
}

}  // namespace

void TcpRemotingClient::ProcessPackageReceived(ByteArrayRef package, TcpTransportPtr channel) {
  std::unique_ptr<RemotingCommand> command;
  try {
    command = RemotingCommand::Decode(std::move(package));
  } catch (...) {
    LOG_ERROR_NEW("processMessageReceived error");
    return;
  }

  if (command->IsResponse()) {
    ProcessResponseCommand(std::move(command), channel);
  } else {
    auto request_command = std::make_shared<std::unique_ptr<RemotingCommand>>(std::move(command));
    handle_executor_.submit(
#if __cplusplus >= 201402L
        [this, request_command = std::move(request_command), channel = std::move(channel)]
#else
        [this, request_command, channel]
#endif
        () { ProcessRequestCommand(std::move(*request_command), channel, processor_table_, rpc_hooks_); });
  }
}

}  // namespace rocketmq
