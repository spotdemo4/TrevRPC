#include "rpc_client.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace trevrpc::detail {
namespace {

[[nodiscard]] bool fits_u32(std::size_t value) noexcept {
  return value <= std::numeric_limits<std::uint32_t>::max();
}

[[nodiscard]] Metadata copy_metadata(const trevrpc_rpc_receive_info_v1& info) {
  Metadata metadata;
  for (std::uint32_t index = 0; index < info.metadata_count; ++index) {
    const auto& entry = info.metadata[index];
    std::span<const std::byte> value;
    if (entry.value_len != 0) {
      value = std::span(reinterpret_cast<const std::byte*>(entry.value), entry.value_len);
    }
    metadata.set(std::string(entry.key, entry.key_len), value);
  }
  return metadata;
}

[[nodiscard]] StatusCode status_code(std::uint32_t code) noexcept {
  return code <= TREVRPC_RPC_STATUS_UNAUTHENTICATED ? static_cast<StatusCode>(code)
                                                    : StatusCode::Unknown;
}

} // namespace

Result<std::unique_ptr<RpcSyncClient>>
RpcSyncClient::connect(std::string_view host, std::uint16_t port, const ChannelConfig& config) {
  if (!fits_u32(host.size()) || config.max_idle_timeout.count() < 0 ||
      config.keep_alive.count() < 0 ||
      config.keep_alive.count() > std::numeric_limits<std::uint32_t>::max()) {
    return Error::runtime(-EINVAL, "RPC client configuration is out of range");
  }
  if (!config.cert_file.empty() || !config.key_file.empty() ||
      config.max_stateless_operations != 0 ||
      config.max_binding_stateless_operations != 0 || config.lifecycle_observer) {
    return Error::runtime(-ENOTSUP,
                          "channel option is not supported by the RPC ABI 1 client");
  }

  trevrpc_rpc_runtime_config_v1 runtime_config{};
  trevrpc_rpc_msquic_config_v1 provider_config{};
  int error = trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config));
  if (error == 0) {
    error = trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config));
  }
  trevrpc_rpc_runtime* raw_runtime = nullptr;
  if (error == 0) {
    error = trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &raw_runtime);
  }
  if (error != 0) {
    return Error::runtime(error);
  }
  auto adopted = RpcEventRuntime::adopt(raw_runtime);
  if (!adopted) {
    return adopted.error();
  }
  auto runtime = std::move(adopted).value();

  trevrpc_rpc_msquic_endpoint_config_v1 endpoint_config{};
  error = trevrpc_rpc_msquic_endpoint_config_v1_init(&endpoint_config, sizeof(endpoint_config));
  if (error != 0) {
    (void)runtime->shutdown();
    return Error::runtime(error);
  }
  endpoint_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
  endpoint_config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
  endpoint_config.host = host.data();
  endpoint_config.host_len = static_cast<std::uint32_t>(host.size());
  endpoint_config.port = port;
  endpoint_config.server_name = host.data();
  endpoint_config.server_name_len = static_cast<std::uint32_t>(host.size());
  if (config.skip_certificate_validation) {
    endpoint_config.flags &= ~TREVRPC_RPC_MSQUIC_VERIFY_PEER;
  }
  if (!config.ca_cert_file.empty()) {
    if (!fits_u32(config.ca_cert_file.size())) {
      (void)runtime->shutdown();
      return Error::runtime(-EOVERFLOW, "CA certificate path is too long");
    }
    endpoint_config.ca_cert_file = config.ca_cert_file.data();
    endpoint_config.ca_cert_file_len = static_cast<std::uint32_t>(config.ca_cert_file.size());
  }
  if (config.max_idle_timeout.count() != 0) {
    endpoint_config.max_idle_timeout_ms =
        static_cast<std::uint64_t>(config.max_idle_timeout.count());
  }
  if (config.keep_alive.count() != 0) {
    endpoint_config.keep_alive_ms = static_cast<std::uint32_t>(config.keep_alive.count());
  }
  if (config.peer_bidi_stream_count != 0) {
    endpoint_config.peer_bidi_stream_count = config.peer_bidi_stream_count;
  }
  if (config.max_pending_send_bytes != 0) {
    endpoint_config.max_pending_send_bytes = config.max_pending_send_bytes;
  }
  if (config.max_pending_send_count != 0) {
    endpoint_config.max_pending_send_count =
        static_cast<std::uint32_t>(std::min<std::size_t>(
            config.max_pending_send_count, std::numeric_limits<std::uint32_t>::max()));
  }
  if (config.max_frame_size != 0) {
    endpoint_config.max_frame_size = config.max_frame_size;
  }
  if (config.stream_recv_window != 0) {
    endpoint_config.stream_recv_window = config.stream_recv_window;
  }
  if (config.conn_flow_control_window != 0) {
    endpoint_config.conn_flow_control_window = config.conn_flow_control_window;
  }

  auto reserved = runtime->reserve_operation();
  if (!reserved) {
    (void)runtime->shutdown();
    return reserved.error();
  }
  trevrpc_rpc_endpoint_v1 endpoint{};
  error = trevrpc_rpc_msquic_endpoint_start_v1(runtime->native_handle(), &endpoint_config,
                                                reserved.value(), &endpoint);
  if (error != 0) {
    runtime->reject_operation(reserved.value());
    (void)runtime->shutdown();
    return Error::runtime(error);
  }
  auto ready = runtime->wait_operation(reserved.value());
  if (!ready) {
    (void)runtime->shutdown();
    return ready.error();
  }
  if (ready.value().kind != TREVRPC_RPC_EVENT_ENDPOINT_READY || ready.value().status != 0) {
    (void)trevrpc_rpc_endpoint_release(runtime->native_handle(), endpoint);
    (void)runtime->shutdown();
    return Error::runtime(ready.value().status == 0 ? -ECONNREFUSED : ready.value().status,
                          "RPC client endpoint failed to become ready");
  }

  try {
    return std::unique_ptr<RpcSyncClient>(new RpcSyncClient(std::move(runtime), endpoint));
  } catch (...) {
    (void)trevrpc_rpc_endpoint_release(runtime->native_handle(), endpoint);
    (void)runtime->shutdown();
    return Error::runtime(-ENOMEM, "failed to allocate C++ RPC client");
  }
}

RpcSyncClient::~RpcSyncClient() { (void)close(); }

Result<trevrpc_rpc_receive*> RpcSyncClient::receive(trevrpc_rpc_stream_v1 stream,
                                                     bool& stream_closed,
                                                     bool& call_closed) {
  int terminal_status = 0;
  for (;;) {
    trevrpc_rpc_receive* receive = nullptr;
    const int error = trevrpc_rpc_stream_receive(runtime_->native_handle(), stream, &receive);
    if (error == 0) {
      return receive;
    }
    if (error != -EAGAIN) {
      return Error::runtime(error);
    }
    if (stream_closed) {
      return Error::runtime(terminal_status == 0 ? -EPIPE : terminal_status,
                            "RPC response stream closed before terminal status");
    }
    auto event = runtime_->wait_stream(stream);
    if (!event) {
      return event.error();
    }
    if (event.value().kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
      stream_closed = true;
      terminal_status = event.value().status;
    } else if (event.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
      call_closed = true;
    }
  }
}

Result<ByteResponse> RpcSyncClient::call_unary(std::string_view service,
                                               std::string_view method,
                                               std::span<const std::byte> body,
                                               const CallOptions& options) {
  std::lock_guard lifecycle_lock(lifecycle_mutex_);
  if (closed_ || !runtime_) {
    return Error::runtime(-ESHUTDOWN, "RPC client is closed");
  }
  if (!fits_u32(service.size()) || !fits_u32(method.size()) ||
      options.timeout.count() < 0 || options.response_idle_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "RPC call configuration is out of range");
  }
  if (options.cancellation != nullptr) {
    return Error::runtime(-ENOTSUP,
                          "standalone C++ cancellation is not yet bound to the RPC runtime");
  }

  std::vector<trevrpc_rpc_metadata_entry_v1> metadata;
  try {
    metadata.reserve(options.metadata.entries().size());
    for (const auto& entry : options.metadata.entries()) {
      if (!fits_u32(entry.key.size())) {
        return Error::runtime(-EOVERFLOW, "RPC metadata key is too long");
      }
      metadata.push_back(trevrpc_rpc_metadata_entry_v1{
          entry.key.data(), static_cast<std::uint32_t>(entry.key.size()), 0,
          reinterpret_cast<const std::uint8_t*>(entry.value.data()), entry.value.size()});
    }
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to construct RPC metadata");
  }

  trevrpc_rpc_call_config_v1 call_config{};
  int error = trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config));
  if (error != 0) {
    return Error::runtime(error);
  }
  call_config.kind = TREVRPC_RPC_KIND_UNARY;
  call_config.service = service.data();
  call_config.service_len = static_cast<std::uint32_t>(service.size());
  call_config.method = method.data();
  call_config.method_len = static_cast<std::uint32_t>(method.size());
  call_config.metadata = metadata.empty() ? nullptr : metadata.data();
  call_config.metadata_count = static_cast<std::uint32_t>(metadata.size());
  call_config.initial_message = reinterpret_cast<const std::uint8_t*>(body.data());
  call_config.initial_message_len = body.size();
  if (options.timeout.count() != 0) {
    call_config.timeout_nanos = static_cast<std::uint64_t>(options.timeout.count());
  }
  if (options.max_response_body_size != 0) {
    call_config.max_response_body_size = options.max_response_body_size;
  }
  if (options.max_response_messages != 0) {
    call_config.max_response_messages = options.max_response_messages;
  }
  if (options.max_response_stream_body_size != 0) {
    call_config.max_response_stream_body_size = options.max_response_stream_body_size;
  }
  if (options.response_idle_timeout.count() != 0) {
    call_config.response_idle_timeout_nanos =
        static_cast<std::uint64_t>(options.response_idle_timeout.count());
  }

  auto reserved = runtime_->reserve_operation();
  if (!reserved) {
    return reserved.error();
  }
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  error = trevrpc_rpc_call_open_v1(runtime_->native_handle(), endpoint_, &call_config,
                                   reserved.value(), &call, &stream);
  if (error != 0) {
    runtime_->reject_operation(reserved.value());
    return Error::runtime(error);
  }
  auto registered = runtime_->register_stream(stream);
  if (!registered) {
    (void)close_call(call, stream, false);
    return registered.error();
  }
  auto ready = runtime_->wait_operation(reserved.value());
  if (!ready) {
    (void)close_call(call, stream);
    return ready.error();
  }
  if (ready.value().kind != TREVRPC_RPC_EVENT_CALL_READY || ready.value().status != 0) {
    const int ready_error = ready.value().status == 0 ? -EIO : ready.value().status;
    (void)close_call(call, stream);
    return Error::runtime(ready_error, "RPC call failed to become ready");
  }

  ByteResponse response;
  Result<ByteResponse> result(Error::runtime(-EIO, "RPC response did not contain a result"));
  bool stream_closed = false;
  bool call_closed = false;
  auto received = receive(stream, stream_closed, call_closed);
  if (!received) {
    result = received.error();
  } else {
    trevrpc_rpc_receive* raw_receive = received.value();
    trevrpc_rpc_receive_info_v1 info{};
    error = trevrpc_rpc_receive_info_v1_init(&info, sizeof(info));
    if (error == 0) {
      error = trevrpc_rpc_receive_get_info_v1(raw_receive, &info);
    }
    if (error != 0) {
      result = Error::runtime(error);
    } else if (info.kind != TREVRPC_RPC_RECEIVE_MESSAGE &&
               info.kind != TREVRPC_RPC_RECEIVE_STATUS) {
      result = Error::protobuf("unary RPC returned an unexpected receive kind");
    } else {
      Metadata response_metadata = copy_metadata(info);
      std::string message;
      if (info.message != nullptr) {
        message.assign(info.message, info.message_len);
      }
      response.status = Status(status_code(info.rpc_status), std::move(message), response_metadata);
      response.metadata = std::move(response_metadata);
      response.body.resize(info.data_len);
      if (info.data_len != 0) {
        std::memcpy(response.body.data(), info.data, info.data_len);
      }
      if (response.status.is_ok() && info.kind != TREVRPC_RPC_RECEIVE_MESSAGE) {
        result = Error::protobuf("successful unary RPC did not return a response message");
      } else {
        result = std::move(response);
      }
    }
    trevrpc_rpc_receive_release(raw_receive);
  }

  auto closed = close_call(call, stream, true, stream_closed, call_closed);
  if (!closed && result) {
    return closed.error();
  }
  return result;
}

Result<void> RpcSyncClient::close_call(trevrpc_rpc_call_v1 call,
                                       trevrpc_rpc_stream_v1 stream,
                                       bool stream_registered,
                                       bool stream_closed,
                                       bool call_closed) {
  auto reserved = runtime_->reserve_operation();
  if (!reserved) {
    if (stream_registered) {
      runtime_->unregister_stream(stream);
    }
    return reserved.error();
  }
  const int close_error = trevrpc_rpc_call_close(runtime_->native_handle(), call, reserved.value(),
                                                 TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
  if (close_error == 0) {
    auto closed = runtime_->wait_operation(reserved.value());
    if (!closed) {
      if (stream_registered) {
        runtime_->unregister_stream(stream);
      }
      return closed.error();
    }
    if (closed.value().kind != TREVRPC_RPC_EVENT_CALL_CLOSED) {
      if (stream_registered) {
        runtime_->unregister_stream(stream);
      }
      return Error::runtime(-EIO, "RPC call close produced an unexpected completion");
    }
  } else {
    runtime_->reject_operation(reserved.value());
    if (close_error != -EALREADY || !stream_registered) {
      if (stream_registered) {
        runtime_->unregister_stream(stream);
      }
      return Error::runtime(close_error);
    }
    while (!stream_closed || !call_closed) {
      auto terminal = runtime_->wait_stream(stream);
      if (!terminal) {
        runtime_->unregister_stream(stream);
        return terminal.error();
      }
      stream_closed = stream_closed || terminal.value().kind == TREVRPC_RPC_EVENT_STREAM_CLOSED;
      call_closed = call_closed || terminal.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED;
    }
  }
  if (stream_registered) {
    runtime_->unregister_stream(stream);
  }
  int error = trevrpc_rpc_stream_release(runtime_->native_handle(), stream);
  if (error == 0) {
    error = trevrpc_rpc_call_release(runtime_->native_handle(), call);
  }
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

Result<void> RpcSyncClient::close() {
  std::lock_guard lifecycle_lock(lifecycle_mutex_);
  if (closed_) {
    return {};
  }
  if (!runtime_) {
    closed_ = true;
    return {};
  }

  std::optional<Error> close_failure;
  auto reserved = runtime_->reserve_operation();
  if (!reserved) {
    close_failure = reserved.error();
  } else {
    int error = trevrpc_rpc_endpoint_close(runtime_->native_handle(), endpoint_, reserved.value());
    if (error != 0) {
      runtime_->reject_operation(reserved.value());
      close_failure = Error::runtime(error);
    } else {
      auto endpoint_closed = runtime_->wait_operation(reserved.value());
      if (!endpoint_closed) {
        close_failure = endpoint_closed.error();
      } else if (endpoint_closed.value().kind != TREVRPC_RPC_EVENT_ENDPOINT_CLOSED) {
        close_failure =
            Error::runtime(-EIO, "RPC endpoint close produced an unexpected completion");
      } else {
        error = trevrpc_rpc_endpoint_release(runtime_->native_handle(), endpoint_);
        if (error != 0) {
          close_failure = Error::runtime(error);
        } else {
          endpoint_ = {};
        }
      }
    }
  }

  auto stopped = runtime_->shutdown();
  if (!stopped) {
    return stopped.error();
  }
  runtime_.reset();
  endpoint_ = {};
  closed_ = true;
  if (close_failure.has_value()) {
    return std::move(close_failure).value();
  }
  return {};
}

} // namespace trevrpc::detail
