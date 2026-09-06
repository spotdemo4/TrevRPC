#include "rpc_event_runtime.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <limits>
#include <new>
#include <span>

#ifndef _WIN32
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#endif

namespace trevrpc::detail {

std::size_t RpcEventRuntime::HandleKeyHash::operator()(const HandleKey& key) const noexcept {
  std::size_t value = std::hash<std::uint64_t>{}(key.owner);
  value ^= std::hash<std::uint32_t>{}(key.slot) + 0x9e3779b9U + (value << 6U) + (value >> 2U);
  value ^= std::hash<std::uint32_t>{}(key.generation) + 0x9e3779b9U + (value << 6U) + (value >> 2U);
  return value;
}

RpcEventRuntime::HandleKey RpcEventRuntime::key(trevrpc_rpc_stream_v1 stream) noexcept {
  return HandleKey{stream.owner, stream.slot, stream.generation};
}

RpcEventRuntime::HandleKey RpcEventRuntime::key(trevrpc_rpc_endpoint_v1 endpoint) noexcept {
  return HandleKey{endpoint.owner, endpoint.slot, endpoint.generation};
}

Result<std::shared_ptr<RpcEventRuntime>> RpcEventRuntime::adopt(trevrpc_rpc_runtime* runtime) {
  if (runtime == nullptr) {
    return Error::runtime(-EINVAL, "RPC runtime must not be null");
  }
  std::unique_ptr<RpcEventRuntime> wrapper(new (std::nothrow) RpcEventRuntime(runtime));
  if (!wrapper) {
    RpcEventRuntime cleanup(runtime);
    (void)cleanup.shutdown_without_driver();
    return Error::runtime(-ENOMEM, "failed to allocate C++ RPC event runtime");
  }
  try {
    auto result = std::shared_ptr<RpcEventRuntime>(std::move(wrapper));
    auto started = result->start();
    if (!started) {
      result->request_driver_stop();
      if (result->driver_.joinable()) {
        result->driver_.join();
      }
      (void)result->shutdown_without_driver();
      return started.error();
    }
    return result;
  } catch (...) {
    if (wrapper) {
      wrapper->request_driver_stop();
      if (wrapper->driver_.joinable()) {
        wrapper->driver_.join();
      }
      (void)wrapper->shutdown_without_driver();
    }
    return Error::runtime(-ENOMEM, "failed to allocate C++ RPC event runtime");
  }
}

RpcEventRuntime::~RpcEventRuntime() {
  auto stopped = shutdown();
  if (!stopped) {
    request_driver_stop();
    if (driver_.joinable()) {
      driver_.join();
    }
    (void)shutdown_without_driver();
  }
  close_driver_wake();
}

Result<void> RpcEventRuntime::start() {
  int error = trevrpc_rpc_wake_source_v1_init(&wake_, sizeof(wake_));
  if (error == 0) {
    error = trevrpc_rpc_runtime_get_wake_source_v1(runtime_, &wake_);
  }
#ifndef _WIN32
  if (error == 0 && wake_.kind != TREVRPC_RPC_WAKE_SOURCE_POSIX_FD) {
    error = -ENOTSUP;
  }
#endif
  if (error != 0) {
    return Error::runtime(error);
  }
  auto driver_wake = initialize_driver_wake();
  if (!driver_wake) {
    return driver_wake.error();
  }
  auto shutdown_operation = reserve_operation();
  if (!shutdown_operation) {
    close_driver_wake();
    return shutdown_operation.error();
  }
  {
    std::lock_guard lock(mutex_);
    shutdown_operation_id_ = shutdown_operation.value();
  }
  try {
    driver_ = std::thread([this] { driver_loop(); });
  } catch (...) {
    reject_operation(shutdown_operation.value());
    {
      std::lock_guard lock(mutex_);
      shutdown_operation_id_ = 0;
    }
    close_driver_wake();
    return Error::runtime(-EAGAIN, "failed to start C++ RPC wake-drain thread");
  }
  return {};
}

Result<std::uint64_t> RpcEventRuntime::reserve_operation() {
  std::lock_guard lock(mutex_);
  if (released_ || close_admitted_) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime is stopping");
  }
  if (driver_error_ != 0) {
    return Error::runtime(driver_error_, "RPC event driver failed");
  }
  try {
    for (;;) {
      const std::uint64_t operation_id = next_operation_id_++;
      if (next_operation_id_ == 0) {
        next_operation_id_ = 1;
      }
      if (operation_id != 0 && operations_.emplace(operation_id, OperationSlot{}).second) {
        return operation_id;
      }
    }
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to reserve RPC operation completion");
  }
}

void RpcEventRuntime::reject_operation(std::uint64_t operation_id) noexcept {
  {
    std::lock_guard lock(mutex_);
    operations_.erase(operation_id);
  }
  condition_.notify_all();
}

Result<RpcEvent> RpcEventRuntime::wait_operation(std::uint64_t operation_id) {
  std::unique_lock lock(mutex_);
  auto found = operations_.find(operation_id);
  if (found == operations_.end()) {
    if (released_ || stopped_seen_) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopped");
    }
    return Error::runtime(-EINVAL, "RPC operation was not reserved");
  }
  ++operation_waiters_;
  condition_.notify_all();
  condition_.wait(lock, [this, operation_id] {
    const auto current = operations_.find(operation_id);
    return driver_error_ != 0 || stopped_seen_ || current == operations_.end() ||
           current->second.ready;
  });
  --operation_waiters_;
  condition_.notify_all();
  found = operations_.find(operation_id);
  if (found == operations_.end()) {
    if (released_ || stopped_seen_) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopped");
    }
    return Error::runtime(-ECANCELED, "RPC operation was cancelled");
  }
  if (!found->second.ready) {
    operations_.erase(found);
    if (driver_error_ != 0) {
      return Error::runtime(driver_error_, "RPC event driver failed");
    }
    return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before operation completion");
  }
  RpcEvent event = std::move(found->second.event);
  operations_.erase(found);
  return event;
}

Result<void> RpcEventRuntime::register_stream(trevrpc_rpc_stream_v1 stream) {
  if (stream.owner == 0) {
    return Error::runtime(-EINVAL, "RPC stream handle must not be null");
  }
  std::lock_guard lock(mutex_);
  if (released_ || close_admitted_) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime is stopping");
  }
  const HandleKey stream_key = key(stream);
  try {
    if (!streams_.emplace(stream_key, StreamSlot{}).second) {
      return Error::runtime(-EALREADY, "RPC stream is already registered");
    }
    const auto pending = pending_streams_.find(stream_key);
    if (pending != pending_streams_.end()) {
      auto registered = streams_.find(stream_key);
      registered->second = std::move(pending->second);
      pending_stream_event_count_ -= registered->second.events.size();
      pending_streams_.erase(pending);
    }
  } catch (...) {
    streams_.erase(stream_key);
    return Error::runtime(-ENOMEM, "failed to register RPC stream");
  }
  condition_.notify_all();
  return {};
}

void RpcEventRuntime::unregister_stream(trevrpc_rpc_stream_v1 stream) noexcept {
  {
    std::lock_guard lock(mutex_);
    streams_.erase(key(stream));
  }
  condition_.notify_all();
}

Result<RpcEvent> RpcEventRuntime::wait_stream(trevrpc_rpc_stream_v1 stream) {
  const HandleKey stream_key = key(stream);
  std::unique_lock lock(mutex_);
  auto found = streams_.find(stream_key);
  if (found == streams_.end()) {
    if (released_ || stopped_seen_) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopped");
    }
    return Error::runtime(-EINVAL, "RPC stream is not registered");
  }
  ++stream_waiters_;
  condition_.notify_all();
  condition_.wait(lock, [this, stream_key] {
    const auto current = streams_.find(stream_key);
    return driver_error_ != 0 || stopped_seen_ || current == streams_.end() ||
           !current->second.events.empty();
  });
  --stream_waiters_;
  condition_.notify_all();
  found = streams_.find(stream_key);
  if (found == streams_.end()) {
    if (released_ || stopped_seen_) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopped");
    }
    return Error::runtime(-ECANCELED, "RPC stream was unregistered");
  }
  if (found->second.events.empty()) {
    if (driver_error_ != 0) {
      return Error::runtime(driver_error_, "RPC event driver failed");
    }
    return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before the stream event arrived");
  }
  RpcEvent event = std::move(found->second.events.front());
  found->second.events.pop_front();
  if (event.kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
    found->second.readable_pending = false;
  }
  return event;
}

Result<void> RpcEventRuntime::register_endpoint(trevrpc_rpc_endpoint_v1 endpoint) {
  if (endpoint.owner == 0) {
    return Error::runtime(-EINVAL, "RPC endpoint handle must not be null");
  }
  std::lock_guard lock(mutex_);
  if (released_ || close_admitted_) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime is stopping");
  }
  const HandleKey endpoint_key = key(endpoint);
  try {
    if (!endpoints_.emplace(endpoint_key, EndpointSlot{}).second) {
      return Error::runtime(-EALREADY, "RPC endpoint is already registered");
    }
    const auto pending = pending_endpoints_.find(endpoint_key);
    if (pending != pending_endpoints_.end()) {
      auto registered = endpoints_.find(endpoint_key);
      registered->second = std::move(pending->second);
      pending_endpoint_event_count_ -= registered->second.events.size();
      pending_endpoints_.erase(pending);
    }
  } catch (...) {
    endpoints_.erase(endpoint_key);
    return Error::runtime(-ENOMEM, "failed to register RPC endpoint");
  }
  condition_.notify_all();
  return {};
}

void RpcEventRuntime::unregister_endpoint(trevrpc_rpc_endpoint_v1 endpoint) noexcept {
  {
    std::lock_guard lock(mutex_);
    endpoints_.erase(key(endpoint));
  }
  condition_.notify_all();
}

Result<RpcEvent> RpcEventRuntime::wait_endpoint(trevrpc_rpc_endpoint_v1 endpoint) {
  const HandleKey endpoint_key = key(endpoint);
  std::unique_lock lock(mutex_);
  auto found = endpoints_.find(endpoint_key);
  if (found == endpoints_.end()) {
    if (released_ || stopped_seen_) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopped");
    }
    return Error::runtime(-EINVAL, "RPC endpoint is not registered");
  }
  ++endpoint_waiters_;
  condition_.notify_all();
  condition_.wait(lock, [this, endpoint_key] {
    const auto current = endpoints_.find(endpoint_key);
    return driver_error_ != 0 || stopped_seen_ || current == endpoints_.end() ||
           !current->second.events.empty();
  });
  --endpoint_waiters_;
  condition_.notify_all();
  found = endpoints_.find(endpoint_key);
  if (found == endpoints_.end()) {
    if (released_ || stopped_seen_) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopped");
    }
    return Error::runtime(-ECANCELED, "RPC endpoint was unregistered");
  }
  if (found->second.events.empty()) {
    if (driver_error_ != 0) {
      return Error::runtime(driver_error_, "RPC event driver failed");
    }
    return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before the endpoint event arrived");
  }
  RpcEvent event = std::move(found->second.events.front());
  found->second.events.pop_front();
  return event;
}

Result<RpcIncomingCall> RpcEventRuntime::wait_incoming() {
  std::unique_lock lock(mutex_);
  condition_.wait(lock,
                  [this] { return driver_error_ != 0 || stopped_seen_ || incoming_count_ != 0; });
  if (driver_error_ != 0) {
    return Error::runtime(driver_error_, "RPC event driver failed");
  }
  if (released_ || stopped_seen_) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before an incoming call arrived");
  }
  RpcIncomingCall incoming = std::move(incoming_calls_[incoming_head_]);
  incoming_calls_[incoming_head_] = {};
  incoming_head_ = (incoming_head_ + 1) % max_pending_incoming_calls;
  --incoming_count_;
  return incoming;
}

int RpcEventRuntime::take_incoming(trevrpc_rpc_event* raw_event,
                                   const trevrpc_rpc_event_info_v1& info) noexcept {
  {
    std::lock_guard lock(mutex_);
    if (incoming_count_ == max_pending_incoming_calls) {
      return -ENOBUFS;
    }
  }

  RpcIncomingCall incoming;
  try {
    incoming.rpc_kind = info.rpc_kind;
    if (info.service != nullptr) {
      incoming.service.assign(info.service, info.service_len);
    }
    if (info.method != nullptr) {
      incoming.method.assign(info.method, info.method_len);
    }
  } catch (...) {
    return -ENOMEM;
  }

  trevrpc_rpc_receive* initial = nullptr;
  int error =
      trevrpc_rpc_event_take_incoming_call(raw_event, &incoming.call, &incoming.stream, &initial);
  if (error != 0) {
    return error;
  }

  trevrpc_rpc_receive_info_v1 receive_info{};
  error = trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info));
  if (error == 0) {
    error = trevrpc_rpc_receive_get_info_v1(initial, &receive_info);
  }
  incoming.preparation_status = error;
  if (error == 0) {
    try {
      incoming.initial_message.resize(receive_info.data_len);
      if (receive_info.data_len != 0) {
        std::memcpy(incoming.initial_message.data(), receive_info.data, receive_info.data_len);
      }
      for (std::uint32_t index = 0; index < receive_info.metadata_count; ++index) {
        const auto& entry = receive_info.metadata[index];
        std::span<const std::byte> value;
        if (entry.value_len != 0) {
          value = std::span(reinterpret_cast<const std::byte*>(entry.value), entry.value_len);
        }
        incoming.metadata.set(std::string(entry.key, entry.key_len), value);
      }
    } catch (...) {
      incoming.preparation_status = -ENOMEM;
    }
  }
  int context_error =
      trevrpc_rpc_call_context_info_v1_init(&incoming.context, sizeof(incoming.context));
  if (context_error == 0) {
    context_error = trevrpc_rpc_call_get_context_v1(runtime_, incoming.call, &incoming.context);
  }
  if (incoming.preparation_status == 0 && context_error != 0) {
    incoming.preparation_status = context_error;
  }
  trevrpc_rpc_receive_release(initial);

  {
    std::lock_guard lock(mutex_);
    incoming_calls_[incoming_tail_] = std::move(incoming);
    incoming_tail_ = (incoming_tail_ + 1) % max_pending_incoming_calls;
    ++incoming_count_;
  }
  condition_.notify_all();
  return 0;
}

void RpcEventRuntime::dispatch(RpcEvent event) noexcept {
  bool overflow = false;
  {
    std::lock_guard lock(mutex_);
    if (event.kind == TREVRPC_RPC_EVENT_STOPPED) {
      stopped_seen_ = true;
    }
    if (event.operation_id != 0) {
      const auto operation = operations_.find(event.operation_id);
      if (operation != operations_.end() && !operation->second.ready) {
        try {
          operation->second.event = event;
          operation->second.ready = true;
        } catch (...) {
          if (driver_error_ == 0) {
            driver_error_ = -ENOMEM;
          }
        }
      }
    }
    const bool stream_event = event.kind == TREVRPC_RPC_EVENT_CALL_FAILED ||
                              event.kind == TREVRPC_RPC_EVENT_STREAM_READABLE ||
                              event.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN ||
                              event.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED ||
                              event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED;
    if (stream_event && event.stream.owner != 0) {
      const HandleKey stream_key = key(event.stream);
      const auto stream = streams_.find(stream_key);
      if (stream != streams_.end()) {
        if (event.kind == TREVRPC_RPC_EVENT_STREAM_READABLE && stream->second.readable_pending) {
          // Readability is a hint, so one queued hint is sufficient.
        } else if (stream->second.events.size() == max_pending_stream_events) {
          overflow = true;
        } else {
          try {
            stream->second.events.push_back(std::move(event));
            stream->second.readable_pending =
                stream->second.readable_pending ||
                stream->second.events.back().kind == TREVRPC_RPC_EVENT_STREAM_READABLE;
          } catch (...) {
            overflow = true;
          }
        }
      } else {
        auto preregistered = pending_streams_.find(stream_key);
        if (preregistered == pending_streams_.end()) {
          if (pending_stream_event_count_ == max_pending_stream_events) {
            overflow = true;
          } else {
            try {
              preregistered = pending_streams_.emplace(stream_key, StreamSlot{}).first;
            } catch (...) {
              overflow = true;
            }
          }
        }
        if (!overflow) {
          auto& slot = preregistered->second;
          if (event.kind == TREVRPC_RPC_EVENT_STREAM_READABLE && slot.readable_pending) {
            // Readability is a hint, so one queued hint is sufficient.
          } else if (slot.events.size() == max_pending_stream_events ||
                     pending_stream_event_count_ == max_pending_stream_events) {
            overflow = true;
          } else {
            try {
              slot.events.push_back(std::move(event));
              slot.readable_pending = slot.readable_pending ||
                                      slot.events.back().kind == TREVRPC_RPC_EVENT_STREAM_READABLE;
              ++pending_stream_event_count_;
            } catch (...) {
              overflow = true;
            }
          }
        }
      }
    }
    const bool unsolicited_endpoint_close = event.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED &&
                                            event.operation_id == 0 && event.endpoint.owner != 0;
    if (unsolicited_endpoint_close) {
      const HandleKey endpoint_key = key(event.endpoint);
      auto endpoint = endpoints_.find(endpoint_key);
      if (endpoint != endpoints_.end()) {
        if (endpoint->second.events.size() == max_pending_endpoint_events) {
          overflow = true;
        } else {
          try {
            endpoint->second.events.push_back(std::move(event));
          } catch (...) {
            overflow = true;
          }
        }
      } else {
        auto preregistered = pending_endpoints_.find(endpoint_key);
        if (preregistered == pending_endpoints_.end()) {
          if (pending_endpoint_event_count_ == max_pending_endpoint_events) {
            overflow = true;
          } else {
            try {
              preregistered = pending_endpoints_.emplace(endpoint_key, EndpointSlot{}).first;
            } catch (...) {
              overflow = true;
            }
          }
        }
        if (!overflow) {
          auto& slot = preregistered->second;
          if (slot.events.size() == max_pending_endpoint_events ||
              pending_endpoint_event_count_ == max_pending_endpoint_events) {
            overflow = true;
          } else {
            try {
              slot.events.push_back(std::move(event));
              ++pending_endpoint_event_count_;
            } catch (...) {
              overflow = true;
            }
          }
        }
      }
    }
    if (overflow && driver_error_ == 0) {
      driver_error_ = -ENOBUFS;
    }
  }
  condition_.notify_all();
}

int RpcEventRuntime::drain_events() noexcept {
  for (;;) {
    trevrpc_rpc_event* raw_event = nullptr;
    const int next_error = trevrpc_rpc_runtime_next_event(runtime_, &raw_event);
    if (next_error != 0) {
      return next_error;
    }
    if (raw_event == nullptr) {
      return -EIO;
    }
    trevrpc_rpc_event_info_v1 info{};
    int error = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
    if (error == 0) {
      error = trevrpc_rpc_event_get_info_v1(raw_event, &info);
    }
    if (error != 0) {
      trevrpc_rpc_event_release(raw_event);
      return error;
    }
    if (info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING) {
      error = take_incoming(raw_event, info);
      trevrpc_rpc_event_release(raw_event);
      if (error == -ENOBUFS) {
        continue;
      }
      if (error != 0) {
        return error;
      }
      continue;
    }
    try {
      RpcEvent event;
      event.kind = info.kind;
      event.flags = info.flags;
      event.status = info.status;
      event.subject_kind = info.subject_kind;
      event.sequence = info.sequence;
      event.endpoint = info.endpoint;
      event.call = info.call;
      event.stream = info.stream;
      event.cancellation = info.cancellation;
      event.operation_id = info.operation_id;
      event.rpc_status = info.rpc_status;
      event.rpc_kind = info.rpc_kind;
      event.application_error_code = info.application_error_code;
      event.provider_error_code = info.provider_error_code;
      if (info.service != nullptr) {
        event.service.assign(info.service, info.service_len);
      }
      if (info.method != nullptr) {
        event.method.assign(info.method, info.method_len);
      }
      dispatch(std::move(event));
    } catch (...) {
      trevrpc_rpc_event_release(raw_event);
      return -ENOMEM;
    }
    trevrpc_rpc_event_release(raw_event);
  }
}

void RpcEventRuntime::fail_driver(int error) noexcept {
  {
    std::lock_guard lock(mutex_);
    if (driver_error_ == 0) {
      driver_error_ = error == 0 ? -EIO : error;
    }
  }
  condition_.notify_all();
}

Result<void> RpcEventRuntime::initialize_driver_wake() {
#ifdef _WIN32
  return {};
#else
  int descriptors[2];
  if (::pipe(descriptors) != 0) {
    return Error::runtime(-errno, "failed to create RPC driver control pipe");
  }
  const int read_flags = ::fcntl(descriptors[0], F_GETFL, 0);
  const int write_flags = ::fcntl(descriptors[1], F_GETFL, 0);
  if (read_flags < 0 || write_flags < 0 ||
      ::fcntl(descriptors[0], F_SETFL, read_flags | O_NONBLOCK) != 0 ||
      ::fcntl(descriptors[1], F_SETFL, write_flags | O_NONBLOCK) != 0 ||
      ::fcntl(descriptors[0], F_SETFD, FD_CLOEXEC) != 0 ||
      ::fcntl(descriptors[1], F_SETFD, FD_CLOEXEC) != 0) {
    const int error = errno;
    ::close(descriptors[0]);
    ::close(descriptors[1]);
    return Error::runtime(-error, "failed to configure RPC driver control pipe");
  }
  driver_wake_read_ = descriptors[0];
  driver_wake_write_ = descriptors[1];
  return {};
#endif
}

void RpcEventRuntime::request_driver_stop() noexcept {
  int wake = -1;
  {
    std::lock_guard lock(mutex_);
    driver_stop_requested_ = true;
    wake = driver_wake_write_;
  }
#ifndef _WIN32
  if (wake >= 0) {
    const std::uint8_t byte = 1;
    const ssize_t written = ::write(wake, &byte, sizeof(byte));
    (void)written;
  }
#else
  (void)wake;
#endif
  condition_.notify_all();
}

void RpcEventRuntime::close_driver_wake() noexcept {
#ifndef _WIN32
  if (driver_wake_read_ >= 0) {
    ::close(driver_wake_read_);
    driver_wake_read_ = -1;
  }
  if (driver_wake_write_ >= 0) {
    ::close(driver_wake_write_);
    driver_wake_write_ = -1;
  }
#endif
}

bool RpcEventRuntime::should_stop() const noexcept {
  std::lock_guard lock(mutex_);
  return driver_error_ != 0 || driver_stop_requested_ ||
         (close_admitted_ && stopped_seen_);
}

void RpcEventRuntime::driver_loop() noexcept {
  for (;;) {
    const int drain_error = drain_events();
    if (drain_error != -EAGAIN) {
      fail_driver(drain_error);
      return;
    }
    if (should_stop()) {
      return;
    }
#ifdef _WIN32
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
#else
    pollfd descriptors[2] = {
        {static_cast<int>(wake_.native_handle), POLLIN, 0},
        {driver_wake_read_, POLLIN, 0},
    };
    int poll_error;
    do {
      poll_error = ::poll(descriptors, 2, -1);
    } while (poll_error < 0 && errno == EINTR);
    if (poll_error < 0) {
      fail_driver(-errno);
      return;
    }
    if ((descriptors[1].revents & POLLIN) != 0) {
      return;
    }
#endif
  }
}

Result<void> RpcEventRuntime::shutdown_without_driver() {
  bool close_admitted;
  std::uint64_t close_operation_id = 0;
  {
    std::lock_guard lock(mutex_);
    if (released_) {
      return {};
    }
    close_admitted = close_admitted_;
    while (!close_admitted && close_operation_id == 0) {
      const std::uint64_t candidate = next_operation_id_++;
      if (next_operation_id_ == 0) {
        next_operation_id_ = 1;
      }
      if (candidate != 0 && operations_.find(candidate) == operations_.end()) {
        close_operation_id = candidate;
      }
    }
  }

  if (!close_admitted) {
    const int close_error = trevrpc_rpc_runtime_close(runtime_, close_operation_id);
    if (close_error != 0) {
      return Error::runtime(close_error);
    }
    std::lock_guard lock(mutex_);
    close_admitted_ = true;
  }

  int stopped_status = 0;
  for (;;) {
    {
      std::lock_guard lock(mutex_);
      if (stopped_seen_) {
        break;
      }
    }
    trevrpc_rpc_event* raw_event = nullptr;
    int error = trevrpc_rpc_runtime_next_event(runtime_, &raw_event);
    if (error == -EAGAIN) {
#ifdef _WIN32
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
#else
      if (wake_.kind == TREVRPC_RPC_WAKE_SOURCE_POSIX_FD) {
        pollfd descriptor{static_cast<int>(wake_.native_handle), POLLIN, 0};
        do {
          error = ::poll(&descriptor, 1, -1);
        } while (error < 0 && errno == EINTR);
        if (error < 0) {
          return Error::runtime(-errno);
        }
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
#endif
      continue;
    }
    if (error != 0) {
      return Error::runtime(error);
    }
    if (raw_event == nullptr) {
      return Error::runtime(-EIO, "RPC runtime returned a null event during shutdown");
    }
    trevrpc_rpc_event_info_v1 info{};
    error = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
    if (error == 0) {
      error = trevrpc_rpc_event_get_info_v1(raw_event, &info);
    }
    if (error == 0 && info.kind == TREVRPC_RPC_EVENT_STOPPED) {
      stopped_status = info.status;
      std::lock_guard lock(mutex_);
      stopped_seen_ = true;
    }
    trevrpc_rpc_event_release(raw_event);
    if (error != 0) {
      return Error::runtime(error);
    }
  }

  int error = trevrpc_rpc_runtime_drain(runtime_);
  if (error == 0) {
    error = trevrpc_rpc_runtime_release(runtime_);
  }
  if (error != 0) {
    return Error::runtime(error);
  }
  {
    std::lock_guard lock(mutex_);
    released_ = true;
    runtime_ = nullptr;
    streams_.clear();
    pending_streams_.clear();
    endpoints_.clear();
    pending_endpoints_.clear();
    pending_stream_event_count_ = 0;
    pending_endpoint_event_count_ = 0;
    operations_.clear();
    incoming_calls_ = {};
    incoming_head_ = 0;
    incoming_tail_ = 0;
    incoming_count_ = 0;
  }
  close_driver_wake();
  condition_.notify_all();
  if (stopped_status != 0) {
    return Error::runtime(stopped_status, "RPC runtime did not stop cleanly");
  }
  return {};
}

Result<void> RpcEventRuntime::shutdown_impl() {
  bool driver_failed;
  {
    std::lock_guard lock(mutex_);
    if (released_) {
      return {};
    }
    driver_failed = driver_error_ != 0;
  }
  if (!driver_.joinable() || driver_failed) {
    if (driver_.joinable()) {
      driver_.join();
    }
    return shutdown_without_driver();
  }

  std::uint64_t operation_id = 0;
  {
    std::lock_guard lock(mutex_);
    operation_id = shutdown_operation_id_;
  }
  if (operation_id == 0) {
    auto reserved = reserve_operation();
    if (!reserved) {
      return reserved.error();
    }
    operation_id = reserved.value();
    std::lock_guard lock(mutex_);
    shutdown_operation_id_ = operation_id;
  }
  const int close_error = trevrpc_rpc_runtime_close(runtime_, operation_id);
  if (close_error != 0) {
    return Error::runtime(close_error);
  }
  {
    std::lock_guard lock(mutex_);
    shutdown_operation_id_ = 0;
    close_admitted_ = true;
  }
  condition_.notify_all();
  auto stopped = wait_operation(operation_id);
  if (driver_.joinable()) {
    driver_.join();
  }
  auto cleaned = shutdown_without_driver();
  if (!cleaned) {
    return cleaned.error();
  }
  if (!stopped) {
    return stopped.error();
  }
  if (stopped.value().kind != TREVRPC_RPC_EVENT_STOPPED || stopped.value().status != 0) {
    return Error::runtime(stopped.value().status == 0 ? -EIO : stopped.value().status,
                          "RPC runtime did not stop cleanly");
  }
  return {};
}

Result<void> RpcEventRuntime::shutdown() {
  std::uint64_t generation = 0;
  {
    std::unique_lock lock(mutex_);
    if (shutdown_in_progress_ || shutdown_waiters_ != 0) {
      generation = shutdown_generation_;
      ++shutdown_waiters_;
      condition_.notify_all();
      condition_.wait(lock, [this, generation] {
        return !shutdown_in_progress_ && shutdown_completed_generation_ == generation;
      });
      const int status = shutdown_status_;
      --shutdown_waiters_;
      lock.unlock();
      return status == 0 ? Result<void>{}
                         : Result<void>{Error::runtime(status, "RPC runtime shutdown failed")};
    }
    if (released_) {
      return {};
    }
    shutdown_in_progress_ = true;
    generation = ++shutdown_generation_;
    shutdown_status_ = 0;
  }

  int status = 0;
  try {
    auto result = shutdown_impl();
    if (!result) {
      status = result.error().code();
    }
  } catch (...) {
    status = -ENOMEM;
  }

  {
    std::lock_guard lock(mutex_);
    shutdown_status_ = status;
    shutdown_completed_generation_ = generation;
    shutdown_in_progress_ = false;
  }
  condition_.notify_all();
  return status == 0 ? Result<void>{}
                     : Result<void>{Error::runtime(status, "RPC runtime shutdown failed")};
}

} // namespace trevrpc::detail
