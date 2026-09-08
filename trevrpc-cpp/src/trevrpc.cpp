#include <trevrpc/trevrpc.hpp>

#include <trevrpc_rpc_msquic.h>

#include "detail/async_core.hpp"
#include "detail/callbacks.hpp"
#include "detail/channel_core.hpp"
#include "detail/lifecycle.hpp"
#include "detail/rpc_client.hpp"
#include "detail/rpc_server_core.hpp"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <new>
#include <system_error>

namespace trevrpc {
namespace {

[[nodiscard]] std::uint32_t lifecycle_kind(detail::ChannelCoreEventKind kind) noexcept {
  switch (kind) {
  case detail::ChannelCoreEventKind::StateChanged:
    return static_cast<std::uint32_t>(ChannelLifecycleKind::StateChanged);
  case detail::ChannelCoreEventKind::ConnectFailed:
    return static_cast<std::uint32_t>(ChannelLifecycleKind::ConnectFailed);
  case detail::ChannelCoreEventKind::ConnectionShutdown:
    return static_cast<std::uint32_t>(ChannelLifecycleKind::ConnectionShutdown);
  }
  return 0;
}

[[nodiscard]] std::uint32_t lifecycle_state(detail::ChannelCorePhase phase) noexcept {
  switch (phase) {
  case detail::ChannelCorePhase::Connecting:
    return static_cast<std::uint32_t>(ChannelLifecycleState::Connecting);
  case detail::ChannelCorePhase::Ready:
    return static_cast<std::uint32_t>(ChannelLifecycleState::Ready);
  case detail::ChannelCorePhase::Reconnecting:
    return static_cast<std::uint32_t>(ChannelLifecycleState::Reconnecting);
  case detail::ChannelCorePhase::Closed:
    return static_cast<std::uint32_t>(ChannelLifecycleState::Closed);
  }
  return 0;
}

} // namespace

void Metadata::set(std::string key, std::span<const std::byte> value) {
  auto existing = std::find_if(entries_.begin(), entries_.end(),
                               [&key](const Entry& entry) { return entry.key == key; });
  std::vector<std::byte> owned(value.begin(), value.end());
  if (existing == entries_.end()) {
    entries_.push_back(Entry{std::move(key), std::move(owned)});
  } else {
    existing->value = std::move(owned);
  }
}

void Metadata::set(std::string key, std::string_view value) {
  set(std::move(key), std::span(reinterpret_cast<const std::byte*>(value.data()), value.size()));
}

std::optional<std::span<const std::byte>> Metadata::get(std::string_view key) const {
  const auto found = std::find_if(entries_.begin(), entries_.end(),
                                  [key](const Entry& entry) { return entry.key == key; });
  if (found == entries_.end()) {
    return std::nullopt;
  }
  return std::span<const std::byte>(found->value);
}

Error Error::runtime(int code, std::string message) {
  if (message.empty()) {
    if (code == std::numeric_limits<int>::min()) {
      message = "runtime error " + std::to_string(code);
    } else {
      const int native_code = code < 0 ? -code : code;
      message = std::error_code(native_code, std::generic_category()).message();
    }
  }
  return Error(Kind::Runtime, code, std::move(message));
}

Error Error::rpc(Status status) {
  if (status.is_ok()) {
    status = Status::internal("OK status cannot represent a failed Result");
  }
  const int code = static_cast<int>(status.code());
  const std::string message = status.message();
  return Error(Kind::Rpc, code, message, std::move(status));
}

Error Error::protobuf(std::string message) {
  return Error(Kind::Protobuf, -EBADMSG, std::move(message));
}

Cancellation::Cancellation() : state_(std::make_shared<detail::RpcCancellation>()) {}

Cancellation::~Cancellation() = default;

void Cancellation::cancel() noexcept {
  if (state_) {
    state_->cancel();
  }
}

bool Cancellation::cancelled() const noexcept { return state_ && state_->cancelled(); }

bool CallContext::has_deadline() const noexcept {
  return (rpc_context_.flags & TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE) != 0;
}

bool CallContext::deadline_expired() const noexcept {
  return (rpc_context_.flags & TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED) != 0;
}

bool CallContext::cancelled() const noexcept {
  return (rpc_context_.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) != 0;
}

std::optional<std::chrono::nanoseconds> CallContext::time_remaining() const noexcept {
  if (!has_deadline()) {
    return std::nullopt;
  }
  constexpr auto maximum = static_cast<std::uint64_t>(
      std::chrono::nanoseconds::max().count());
  return std::chrono::nanoseconds(
      static_cast<std::chrono::nanoseconds::rep>(
          std::min(rpc_context_.time_remaining_nanos, maximum)));
}

namespace detail {

ClientStream::~ClientStream() { close(); }

ClientStream::ClientStream(ClientStream&& other) noexcept
    : stream_(std::move(other.stream_)) {}

ClientStream& ClientStream::operator=(ClientStream&& other) noexcept {
  if (this != &other) {
    close();
    stream_ = std::move(other.stream_);
  }
  return *this;
}

Result<void> ClientStream::send(std::span<const std::byte> body) {
  return stream_ ? stream_->send(body)
                 : Result<void>{Error::runtime(-EPIPE, "stream send side is closed")};
}

Result<void> ClientStream::finish_send() {
  return stream_ ? stream_->finish_send()
                 : Result<void>{Error::runtime(-EINVAL, "stream is closed")};
}

Result<StreamFrame> ClientStream::receive() {
  return stream_ ? stream_->receive()
                 : Result<StreamFrame>{Error::runtime(-EINVAL, "stream is closed")};
}

void ClientStream::cancel() noexcept {
  if (stream_) {
    stream_->cancel();
  }
}

void ClientStream::close() noexcept {
  if (!stream_) {
    return;
  }
  auto stream = std::move(stream_);
  if (!stream->close_result()) {
    RpcClientStream::schedule_cleanup(std::move(stream));
  }
}

Status error_status(const Error& error) {
  if (error.status().has_value()) {
    return *error.status();
  }
  if (error.kind() == Error::Kind::Protobuf) {
    return Status::invalid_argument(error.message());
  }
  return Status::internal(error.message());
}

} // namespace detail

Channel::~Channel() { close(); }

Result<std::shared_ptr<Channel>> Channel::connect(std::string_view host, std::uint16_t port,
                                                  const ChannelConfig& config,
                                                  std::chrono::nanoseconds timeout,
                                                  Cancellation* cancellation) {
  if (timeout.count() < 0 || config.max_idle_timeout.count() < 0 || config.keep_alive.count() < 0 ||
      static_cast<std::uint64_t>(config.keep_alive.count()) >
          std::numeric_limits<std::uint32_t>::max()) {
    return Error::runtime(-EINVAL, "channel durations are out of range");
  }
  detail::ChannelCoreConfig native;
  native.host = std::string(host);
  native.port = port;
  native.cert_file = config.cert_file;
  native.key_file = config.key_file;
  native.ca_cert_file = config.ca_cert_file;
  native.skip_certificate_validation = config.skip_certificate_validation;
  native.max_idle_timeout = config.max_idle_timeout;
  native.keep_alive = config.keep_alive;
  native.peer_bidi_stream_count = config.peer_bidi_stream_count;
  native.max_pending_send_bytes = config.max_pending_send_bytes;
  native.max_pending_send_count = config.max_pending_send_count;
  native.max_frame_size = config.max_frame_size;
  native.stream_recv_window = config.stream_recv_window;
  native.conn_flow_control_window = config.conn_flow_control_window;
  if (config.lifecycle_observer) {
    auto observer = config.lifecycle_observer;
    native.lifecycle_observer = [observer =
                                     std::move(observer)](const detail::ChannelCoreEvent& event) {
      ChannelLifecycleEvent mapped;
      mapped.kind = lifecycle_kind(event.kind);
      mapped.state = lifecycle_state(event.phase);
      mapped.generation = event.generation;
      mapped.error_code = event.error_code;
      observer->channel_event(mapped);
    };
  }
  if (config.callback_exception_sink) {
    auto sink = config.callback_exception_sink;
    native.callback_exception_sink = [sink = std::move(sink)](std::exception_ptr exception) {
      sink->callback_exception("channel_lifecycle", std::move(exception));
    };
  }
  auto core = detail::ChannelCore::connect(std::move(native));
  if (!core) {
    return core.error();
  }
  auto channel = std::shared_ptr<Channel>(new Channel(std::move(core).value()));
  auto ready = channel->wait_ready(timeout, cancellation);
  if (!ready) {
    channel->close();
    return ready.error();
  }
  return channel;
}

Result<void> Channel::wait_ready(std::chrono::nanoseconds timeout, Cancellation* cancellation) {
  std::shared_ptr<detail::ChannelCore> core;
  {
    std::lock_guard lock(mutex_);
    core = core_;
  }
  if (!core) {
    return Error::runtime(-EINVAL, "channel must not be null");
  }
  auto ready =
      core->wait_ready(timeout, cancellation == nullptr ? nullptr : cancellation->state_.get());
  if (!ready) {
    return ready.error();
  }
  return {};
}

void Channel::close() noexcept {
  std::shared_ptr<detail::ChannelCore> core;
  {
    std::lock_guard lock(mutex_);
    core = std::exchange(core_, {});
  }
  if (core) {
    (void)core->request_close();
  }
}

Result<detail::ByteResponse> Channel::call_unary(std::string_view service, std::string_view method,
                                                 std::span<const std::byte> body,
                                                 const CallOptions& options) {
  std::shared_ptr<detail::ChannelCore> core;
  {
    std::lock_guard lock(mutex_);
    core = core_;
  }
  if (!core) {
    return Error::runtime(-EINVAL, "channel must not be null");
  }
  auto stream = detail::RpcClientStream::open(core, service, method, TREVRPC_RPC_KIND_UNARY, body,
                                              options);
  if (!stream) {
    return stream.error();
  }
  auto state = std::move(stream).value();
  auto frame = state->receive();
  if (!frame) {
    return frame.error();
  }
  if (!frame.value().terminal || (frame.value().status.is_ok() && !frame.value().message)) {
    return Error::protobuf("unary RPC did not return exactly one response message");
  }
  detail::ByteResponse response;
  response.status = std::move(frame.value().status);
  response.metadata = response.status.metadata();
  response.body = std::move(frame.value().body);
  return response;
}

Result<detail::ClientStream> Channel::start_stream(std::string_view service,
                                                   std::string_view method, std::uint32_t kind,
                                                   std::span<const std::byte> body,
                                                   const CallOptions& options) {
  std::shared_ptr<detail::ChannelCore> core;
  {
    std::lock_guard lock(mutex_);
    core = core_;
  }
  if (!core) {
    return Error::runtime(-EINVAL, "channel must not be null");
  }
  auto stream = detail::RpcClientStream::open(core, service, method, kind, body, options);
  if (!stream) {
    return stream.error();
  }
  return detail::ClientStream(std::move(stream).value());
}

Server::~Server() {
  if (state_) {
    detail::abandon_server(std::move(state_));
  }
}

Server::Server(std::shared_ptr<detail::ServerState> state) noexcept : state_(std::move(state)) {}

Server::Server(Server&& other) noexcept : state_(std::move(other.state_)) {}

Server& Server::operator=(Server&& other) noexcept {
  if (this != &other) {
    if (state_) {
      detail::abandon_server(std::move(state_));
    }
    state_ = std::move(other.state_);
  }
  return *this;
}

Result<Server> Server::listen(const ServerConfig& config) {
  if (config.enable_http3 && !config.webtransport_path.empty()) {
    return Error::runtime(-ENOTSUP,
                          "server cannot combine HTTP/3 and WebTransport listeners");
  }
  if (!config.enable_http3 && !config.http3_path.empty()) {
    return Error::runtime(-EINVAL, "HTTP/3 path requires HTTP/3 to be enabled");
  }
  if (config.webtransport_path.empty() && !config.webtransport_origin.empty()) {
    return Error::runtime(-EINVAL, "WebTransport origin requires a WebTransport path");
  }
  if (config.max_idle_timeout.count() < 0 || config.keep_alive.count() < 0 ||
      config.initial_request_timeout.count() < 0 || config.stream_idle_timeout.count() < 0 ||
      static_cast<std::uint64_t>(config.keep_alive.count()) >
          std::numeric_limits<std::uint32_t>::max()) {
    return Error::runtime(-EINVAL, "server durations are out of range");
  }
  if (config.max_stream_messages < -1 || config.max_stream_body_size < -1 ||
      config.worker_count == 0 || config.worker_queue_capacity == 0) {
    return Error::runtime(-EINVAL, "server execution limits are invalid");
  }
  const auto string_too_large = [](const std::string& value) {
    return value.size() > std::numeric_limits<std::uint32_t>::max();
  };
  if (string_too_large(config.host) || string_too_large(config.cert_file) ||
      string_too_large(config.key_file) || string_too_large(config.webtransport_path) ||
      string_too_large(config.webtransport_origin) || string_too_large(config.http3_path) ||
      config.max_pending_send_count > std::numeric_limits<std::uint32_t>::max()) {
    return Error::runtime(-EOVERFLOW, "server configuration exceeds native ABI limits");
  }

  trevrpc_rpc_runtime_config_v1 runtime_config{};
  trevrpc_rpc_msquic_config_v1 provider_config{};
  int error = trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config));
  if (error == 0) {
    error = trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config));
  }
  if (error != 0) {
    return Error::runtime(error);
  }
  if (config.max_frame_size != 0) {
    runtime_config.max_receive_owned_bytes = static_cast<std::uint64_t>(config.max_frame_size);
    runtime_config.max_message_size = static_cast<std::uint64_t>(config.max_frame_size);
  }
  runtime_config.event_capacity = 1024;
  runtime_config.endpoint_capacity = 8;
  runtime_config.call_capacity = 1024;
  runtime_config.stream_capacity = 1024;
  runtime_config.max_receive_owned_count = 1024;
  runtime_config.initial_request_timeout_nanos =
      static_cast<std::uint64_t>(config.initial_request_timeout.count());
  runtime_config.max_stream_messages = config.max_stream_messages;
  runtime_config.max_stream_body_size = config.max_stream_body_size;
  runtime_config.stream_idle_timeout_nanos =
      static_cast<std::uint64_t>(config.stream_idle_timeout.count());

  trevrpc_rpc_runtime* raw_runtime = nullptr;
  error = trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &raw_runtime);
  if (error != 0) {
    return Error::runtime(error);
  }
  auto adopted = detail::RpcEventRuntime::adopt(raw_runtime, runtime_config);
  if (!adopted) {
    (void)trevrpc_rpc_runtime_close(raw_runtime, std::numeric_limits<std::uint64_t>::max());
    (void)trevrpc_rpc_runtime_drain(raw_runtime);
    (void)trevrpc_rpc_runtime_release(raw_runtime);
    return adopted.error();
  }
  auto rpc_runtime = std::move(adopted).value();

  trevrpc_rpc_msquic_endpoint_config_v1 endpoint_config{};
  error = trevrpc_rpc_msquic_endpoint_config_v1_init(&endpoint_config, sizeof(endpoint_config));
  if (error != 0) {
    (void)rpc_runtime->shutdown();
    return Error::runtime(error);
  }
  endpoint_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
  endpoint_config.transport = config.webtransport_path.empty()
                                  ? (config.enable_http3 ? TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3
                                                         : TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE)
                                  : TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT;
  endpoint_config.host = config.host.empty() ? nullptr : config.host.data();
  endpoint_config.host_len = static_cast<std::uint32_t>(config.host.size());
  endpoint_config.port = config.port;
  if (config.peer_bidi_stream_count != 0) {
    endpoint_config.peer_bidi_stream_count = config.peer_bidi_stream_count;
  }
  endpoint_config.cert_file = config.cert_file.empty() ? nullptr : config.cert_file.data();
  endpoint_config.cert_file_len = static_cast<std::uint32_t>(config.cert_file.size());
  endpoint_config.key_file = config.key_file.empty() ? nullptr : config.key_file.data();
  endpoint_config.key_file_len = static_cast<std::uint32_t>(config.key_file.size());
  endpoint_config.path = config.webtransport_path.empty()
                             ? (config.http3_path.empty() ? nullptr : config.http3_path.data())
                             : config.webtransport_path.data();
  endpoint_config.path_len = static_cast<std::uint32_t>(
      config.webtransport_path.empty() ? config.http3_path.size()
                                       : config.webtransport_path.size());
  endpoint_config.origin =
      config.webtransport_origin.empty() ? nullptr : config.webtransport_origin.data();
  endpoint_config.origin_len = static_cast<std::uint32_t>(config.webtransport_origin.size());
  endpoint_config.webtransport_profiles = TREVRPC_RPC_MSQUIC_PROFILE_ALL_SUPPORTED;
  if (config.max_sessions_per_connection != 0) {
    endpoint_config.max_sessions = config.max_sessions_per_connection;
  }
  if (config.max_pending_send_bytes != 0) {
    endpoint_config.max_pending_send_bytes = config.max_pending_send_bytes;
  }
  if (config.max_pending_send_count != 0) {
    endpoint_config.max_pending_send_count =
        static_cast<std::uint32_t>(config.max_pending_send_count);
  }
  if (config.max_frame_size != 0) {
    const auto max_frame_size = static_cast<std::uint64_t>(config.max_frame_size);
    if (max_frame_size > (std::numeric_limits<std::uint64_t>::max() - 4096u) / 2u) {
      (void)rpc_runtime->shutdown();
      return Error::runtime(-EOVERFLOW, "server max frame size exceeds receive budget limits");
    }
    endpoint_config.max_frame_size = max_frame_size;
    endpoint_config.max_pending_receive_bytes = max_frame_size * 2u + 4096u;
  }
  if (config.max_idle_timeout.count() != 0) {
    endpoint_config.max_idle_timeout_ms =
        static_cast<std::uint64_t>(config.max_idle_timeout.count());
  }
  if (config.keep_alive.count() != 0) {
    endpoint_config.keep_alive_ms = static_cast<std::uint32_t>(config.keep_alive.count());
  }
  if (config.stream_recv_window != 0) {
    endpoint_config.stream_recv_window = config.stream_recv_window;
  }
  if (config.conn_flow_control_window != 0) {
    endpoint_config.conn_flow_control_window = config.conn_flow_control_window;
  }

  auto operation = rpc_runtime->reserve_operation();
  if (!operation) {
    (void)rpc_runtime->shutdown();
    return operation.error();
  }
  trevrpc_rpc_endpoint_v1 endpoint{};
  error = trevrpc_rpc_msquic_endpoint_start_v1(rpc_runtime->native_handle(), &endpoint_config,
                                                operation.value(), &endpoint);
  if (error != 0) {
    rpc_runtime->reject_operation(operation.value());
    (void)rpc_runtime->shutdown();
    return Error::runtime(error);
  }
  auto registered = rpc_runtime->register_endpoint(endpoint);
  if (!registered) {
    (void)rpc_runtime->shutdown();
    return registered.error();
  }
  auto ready = rpc_runtime->wait_operation(operation.value());
  if (!ready) {
    (void)rpc_runtime->shutdown();
    return ready.error();
  }
  if (ready.value().status != 0) {
    (void)rpc_runtime->shutdown();
    return Error::runtime(ready.value().status);
  }

  auto core = detail::RpcServerCore::create(rpc_runtime, endpoint, config.worker_count,
                                            config.worker_queue_capacity);
  if (!core) {
    (void)rpc_runtime->shutdown();
    return core.error();
  }
  try {
    return Server(std::make_shared<detail::ServerState>(std::move(core).value()));
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate server state");
  }
}

Result<std::uint16_t> Server::port() const {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->port();
}

Result<void> Server::set_authorizer(std::shared_ptr<Authorizer> callback,
                                    std::shared_ptr<CallbackExceptionSink> exception_sink) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->set_authorizer(
      detail::make_authorizer_state(std::move(callback), std::move(exception_sink)));
}

Result<void> Server::set_metrics(std::shared_ptr<MetricsObserver> callback,
                                 std::shared_ptr<CallbackExceptionSink> exception_sink) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->set_metrics(
      detail::make_metrics_state(std::move(callback), std::move(exception_sink)));
}

Result<void> Server::set_logger(std::shared_ptr<Logger> callback,
                                std::shared_ptr<CallbackExceptionSink> exception_sink) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->set_logger(
      detail::make_logger_state(std::move(callback), std::move(exception_sink)));
}

Result<void> Server::clear_authorizer() {
  return state_ ? state_->clear_authorizer()
                : Result<void>{Error::runtime(-EINVAL, "server is moved from")};
}

Result<void> Server::clear_metrics() {
  return state_ ? state_->clear_metrics()
                : Result<void>{Error::runtime(-EINVAL, "server is moved from")};
}

Result<void> Server::clear_logger() {
  return state_ ? state_->clear_logger()
                : Result<void>{Error::runtime(-EINVAL, "server is moved from")};
}

Result<void> Server::serve() {
  return state_ ? state_->freeze()
                : Result<void>{Error::runtime(-EINVAL, "server is moved from")};
}

Result<void> Server::request_stop() {
  return state_ ? state_->request_stop()
                : Result<void>{Error::runtime(-EINVAL, "server is moved from")};
}

Result<ShutdownReport> Server::shutdown(const ShutdownOptions& options) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->shutdown(options);
}

void Server::shutdown() noexcept { (void)request_stop(); }

void Server::close() noexcept {
  if (!state_) {
    return;
  }
  auto report = state_->shutdown(ShutdownOptions{});
  if (!report) {
    detail::abandon_server(std::move(state_));
  }
}

Result<ServerPhase> Server::phase() const {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->phase();
}

Result<void> Server::register_rpc_route(std::string_view service, std::string_view method,
                                        std::uint32_t kind, RpcHandler handler) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  std::shared_ptr<void> route;
  try {
    route = std::make_shared<RpcHandler>(std::move(handler));
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate server route");
  }
  auto callback = *static_cast<RpcHandler*>(route.get());
  return state_->register_rpc_route(service, method, kind, std::move(callback), route);
}

void Server::respond(const std::shared_ptr<detail::ServerCallState>& state,
                     const Status& status, std::span<const std::byte> body) noexcept {
  if (!state) {
    return;
  }
  try {
    (void)sync_wait(state->respond(std::vector<std::byte>(body.begin(), body.end()), status,
                                   status.metadata()));
  } catch (...) {
    state->stop(detail::ServerStopReason::LocalClose);
  }
}

void Server::finish(const std::shared_ptr<detail::ServerCallState>& state,
                    const Status& status) noexcept {
  if (!state) {
    return;
  }
  try {
    (void)sync_wait(state->finish(status));
  } catch (...) {
    state->stop(detail::ServerStopReason::LocalClose);
  }
}

} // namespace trevrpc
