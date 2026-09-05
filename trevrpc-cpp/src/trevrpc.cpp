#include <trevrpc/trevrpc.hpp>

#include <trevrpc_msquic.h>
#include <trevrpc_webtransport.h>

#include "detail/abi6_bridge.hpp"
#include "detail/callbacks.hpp"
#include "detail/lifecycle.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <system_error>

static_assert(TREVRPC_C_ABI_VERSION == 6u, "trevrpc-cpp requires TrevRPC C ABI 6");

namespace trevrpc {
namespace {

const bool trevrpc_c_abi_6_linked = [] {
  trevrpc_c_abi_6_anchor();
  return true;
}();

[[nodiscard]] const char* nullable(const std::string& value) {
  return value.empty() ? nullptr : value.c_str();
}

using detail::copy_metadata;
using detail::finish_borrowed;
using detail::native_request;
using detail::NativeInboundFrame;
using detail::NativeInboundResponse;
using detail::NativeMetadata;
using detail::normalize_call_options;
using detail::receive_inbound_frame;
using detail::respond_borrowed;
using detail::status_from;

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
    message = trevrpc_error(code);
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
  return Error(Kind::Protobuf, TREVRPC_ERR_INVALID_FRAME, std::move(message));
}

Cancellation::Cancellation() : cancellation_(trevrpc_cancellation_new()) {
  if (cancellation_ == nullptr) {
    throw std::bad_alloc();
  }
}

Cancellation::~Cancellation() { trevrpc_cancellation_release(cancellation_); }

Cancellation::Cancellation(const Cancellation& other) : cancellation_(other.cancellation_) {
  const int error = trevrpc_cancellation_retain(cancellation_);
  if (error != 0) {
    cancellation_ = nullptr;
    throw std::system_error(-error, std::generic_category(),
                            "failed to retain TrevRPC cancellation");
  }
}

Cancellation& Cancellation::operator=(const Cancellation& other) {
  if (this != &other) {
    trevrpc_cancellation* incoming = other.cancellation_;
    const int error = trevrpc_cancellation_retain(incoming);
    if (error != 0) {
      throw std::system_error(-error, std::generic_category(),
                              "failed to retain TrevRPC cancellation");
    }
    trevrpc_cancellation_release(cancellation_);
    cancellation_ = incoming;
  }
  return *this;
}

Cancellation::Cancellation(Cancellation&& other) noexcept
    : cancellation_(std::exchange(other.cancellation_, nullptr)) {}

Cancellation& Cancellation::operator=(Cancellation&& other) noexcept {
  if (this != &other) {
    trevrpc_cancellation_release(cancellation_);
    cancellation_ = std::exchange(other.cancellation_, nullptr);
  }
  return *this;
}

void Cancellation::cancel() noexcept { trevrpc_cancellation_cancel(cancellation_); }

bool Cancellation::cancelled() const noexcept {
  return trevrpc_cancellation_cancelled(cancellation_) != 0;
}

bool CallContext::has_deadline() const noexcept {
  return trevrpc_call_context_has_deadline(context_) != 0;
}

bool CallContext::deadline_expired() const noexcept {
  return trevrpc_call_context_deadline_expired(context_) != 0;
}

bool CallContext::cancelled() const noexcept {
  return trevrpc_call_context_cancelled(context_) != 0;
}

std::optional<std::chrono::nanoseconds> CallContext::time_remaining() const noexcept {
  std::uint64_t remaining = 0;
  if (trevrpc_call_context_time_remaining_nanos(context_, &remaining) != 0) {
    return std::nullopt;
  }
  return std::chrono::nanoseconds(remaining);
}

CallContext::CallContext(const trevrpc_call_context* context, const trevrpc_request* request)
    : context_(context),
      metadata_(request == nullptr ? Metadata{} : copy_metadata(request->metadata)) {}

namespace detail {

ClientStream::~ClientStream() { close(); }

ClientStream::ClientStream(ClientStream&& other) noexcept
    : stream_(std::exchange(other.stream_, nullptr)),
      send_finished_(std::exchange(other.send_finished_, false)),
      receive_finished_(std::exchange(other.receive_finished_, false)) {}

ClientStream& ClientStream::operator=(ClientStream&& other) noexcept {
  if (this != &other) {
    close();
    stream_ = std::exchange(other.stream_, nullptr);
    send_finished_ = std::exchange(other.send_finished_, false);
    receive_finished_ = std::exchange(other.receive_finished_, false);
  }
  return *this;
}

Result<void> ClientStream::send(std::span<const std::byte> body) {
  if (stream_ == nullptr || send_finished_) {
    return Error::runtime(-EPIPE, "stream send side is closed");
  }
  const int error = trevrpc_stream_send_message_copy_wait(
      stream_, reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

Result<void> ClientStream::finish_send() {
  if (stream_ == nullptr) {
    return Error::runtime(-EINVAL, "stream is closed");
  }
  if (send_finished_) {
    return {};
  }
  const int error = trevrpc_stream_finish_send(stream_);
  if (error != 0) {
    return Error::runtime(error);
  }
  send_finished_ = true;
  return {};
}

Result<StreamFrame> ClientStream::receive() {
  if (stream_ == nullptr) {
    return Error::runtime(-EINVAL, "stream is closed");
  }
  if (receive_finished_) {
    return Error::runtime(-EPIPE, "stream receive side is closed");
  }
  auto result = receive_inbound_frame(stream_);
  if ((result && result.value().terminal) ||
      (!result && result.error().kind() == Error::Kind::Protobuf)) {
    receive_finished_ = true;
  }
  return result;
}

void ClientStream::cancel() noexcept {
  if (stream_ != nullptr) {
    trevrpc_stream_cancel(stream_);
  }
}

void ClientStream::close() noexcept {
  if (stream_ != nullptr) {
    trevrpc_stream_close(stream_);
    stream_ = nullptr;
  }
}

Result<void> send_server_message(trevrpc_stream* stream, std::span<const std::byte> body) {
  const int error = trevrpc_stream_send_message_copy_wait(
      stream, reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

Result<std::optional<std::vector<std::byte>>> receive_server_message(trevrpc_stream* stream) {
  trevrpc_inbound_stream_frame* raw_frame = nullptr;
  const int error = trevrpc_stream_recv_inbound(stream, &raw_frame);
  NativeInboundFrame frame(raw_frame);
  if (error != 0) {
    return Error::runtime(error);
  }
  if (frame.get() == nullptr) {
    return std::optional<std::vector<std::byte>>{};
  }

  std::uint32_t kind = 0;
  const int kind_error = trevrpc_inbound_stream_frame_get_kind(frame.get(), &kind);
  if (kind_error != 0) {
    return Error::runtime(kind_error);
  }
  if (kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
    if (kind != TREVRPC_STREAM_FRAME_KIND_STATUS) {
      return Error::protobuf("request stream contained an unknown frame kind");
    }
    Status status;
    const int status_error = status_from(frame.get(), &status);
    if (status_error != 0) {
      return Error::runtime(status_error);
    }
    if (status.is_ok()) {
      return std::optional<std::vector<std::byte>>{};
    }
    return Error::rpc(std::move(status));
  }

  trevrpc_bytes_view body_view{};
  const int body_error = trevrpc_inbound_stream_frame_get_body(frame.get(), &body_view);
  if (body_error != 0) {
    return Error::runtime(body_error);
  }
  if (body_view.data == nullptr && body_view.len > 0) {
    return Error::runtime(-EINVAL, "runtime returned an invalid request stream body view");
  }
  std::vector<std::byte> body(body_view.len);
  if (body_view.len > 0) {
    std::memcpy(body.data(), body_view.data, body_view.len);
  }
  return std::optional<std::vector<std::byte>>(std::move(body));
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
  trevrpc_client_config_v1 native{};
  const int init_error = trevrpc_client_config_v1_init(&native, sizeof(native));
  if (init_error != 0) {
    return Error::runtime(init_error);
  }
  native.cert_file = nullable(config.cert_file);
  native.key_file = nullable(config.key_file);
  native.ca_cert_file = nullable(config.ca_cert_file);
  native.skip_certificate_validation = config.skip_certificate_validation;
  if (config.max_idle_timeout.count() != 0) {
    native.max_idle_timeout_ms = static_cast<std::uint64_t>(config.max_idle_timeout.count());
  }
  if (config.keep_alive.count() != 0) {
    native.keep_alive_ms = static_cast<std::uint32_t>(config.keep_alive.count());
  }
  if (config.peer_bidi_stream_count != 0) {
    native.peer_bidi_stream_count = config.peer_bidi_stream_count;
  }
  if (config.max_stateless_operations != 0) {
    native.max_stateless_operations = config.max_stateless_operations;
  }
  if (config.max_binding_stateless_operations != 0) {
    native.max_binding_stateless_operations = config.max_binding_stateless_operations;
  }
  if (config.max_pending_send_bytes != 0) {
    native.max_pending_send_bytes = config.max_pending_send_bytes;
  }
  if (config.max_pending_send_count != 0) {
    native.max_pending_send_count = config.max_pending_send_count;
  }
  if (config.max_frame_size != 0) {
    native.max_frame_size = config.max_frame_size;
  }
  if (config.stream_recv_window != 0) {
    native.stream_recv_window = config.stream_recv_window;
  }
  if (config.conn_flow_control_window != 0) {
    native.conn_flow_control_window = config.conn_flow_control_window;
  }

  std::shared_ptr<detail::ChannelLifecycleCallbackState> lifecycle_callback;
  std::unique_ptr<trevrpc_channel_options, decltype(&trevrpc_channel_options_free)> channel_options(
      nullptr, trevrpc_channel_options_free);
  if (config.lifecycle_observer) {
    lifecycle_callback = detail::make_channel_lifecycle_state(config.lifecycle_observer,
                                                              config.callback_exception_sink);
    channel_options.reset(trevrpc_channel_options_new());
    if (!channel_options) {
      return Error::runtime(-ENOMEM, "failed to allocate channel options");
    }
    const int callback_error = trevrpc_channel_options_set_lifecycle_callback(
        channel_options.get(), detail::channel_lifecycle_trampoline, lifecycle_callback.get());
    if (callback_error != 0) {
      return Error::runtime(callback_error);
    }
  }

  trevrpc_channel* channel = nullptr;
  const std::string host_string(host);
  const int error = trevrpc_channel_connect_v1(
      host_string.c_str(), port, &native, channel_options.get(),
      static_cast<std::uint64_t>(timeout.count()),
      cancellation == nullptr ? nullptr : cancellation->native_handle(), &channel);
  if (error != 0) {
    return Error::runtime(error);
  }
  try {
    auto state = std::make_shared<detail::ChannelState>(channel, std::move(lifecycle_callback));
    return std::shared_ptr<Channel>(new Channel(std::move(state)));
  } catch (...) {
    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    throw;
  }
}

Result<void> Channel::wait_ready(std::chrono::nanoseconds timeout, Cancellation* cancellation) {
  if (timeout.count() < 0) {
    return Error::runtime(-EINVAL, "readiness timeout must not be negative");
  }
  if (!state_) {
    return Error::runtime(-EINVAL, "channel must not be null");
  }
  auto admission = state_->admit();
  if (!admission) {
    return admission.error();
  }
  std::uint64_t generation = 0;
  const int error = trevrpc_channel_wait_ready(
      admission.value().native_handle(), static_cast<std::uint64_t>(timeout.count()),
      cancellation == nullptr ? nullptr : cancellation->native_handle(), &generation);
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

void Channel::close() noexcept {
  if (state_) {
    state_->close();
  }
}

trevrpc_channel* Channel::native_handle() const noexcept {
  return state_ ? state_->native_handle() : nullptr;
}

Result<detail::ByteResponse> Channel::call_unary(std::string_view service, std::string_view method,
                                                 std::span<const std::byte> body,
                                                 const CallOptions& options) {
  if (!state_) {
    return Error::runtime(-EINVAL, "channel must not be null");
  }
  auto native_options = normalize_call_options(options);
  if (!native_options) {
    return native_options.error();
  }
  auto admission = state_->admit();
  if (!admission) {
    return admission.error();
  }
  const trevrpc_request request = native_request(service, method, TREVRPC_RPC_KIND_UNARY, body);
  trevrpc_inbound_response* raw_response = nullptr;
  const int error = trevrpc_channel_call_request_inbound_v1(
      admission.value().native_handle(), &request, &native_options.value().options, &raw_response);
  NativeInboundResponse response(raw_response);
  if (error != 0) {
    return Error::runtime(error);
  }
  if (response.get() == nullptr) {
    return Error::runtime(-EIO, "runtime returned no unary response");
  }

  auto metadata = copy_metadata(response.get());
  if (!metadata) {
    return metadata.error();
  }
  Status status;
  const int status_error = status_from(response.get(), metadata.value(), &status);
  if (status_error != 0) {
    return Error::runtime(status_error);
  }
  trevrpc_bytes_view body_view{};
  const int body_error = trevrpc_inbound_response_get_body(response.get(), &body_view);
  if (body_error != 0) {
    return Error::runtime(body_error);
  }
  if (body_view.data == nullptr && body_view.len > 0) {
    return Error::runtime(-EINVAL, "runtime returned an invalid unary body view");
  }

  detail::ByteResponse result;
  result.status = std::move(status);
  result.metadata = std::move(metadata).value();
  result.body.resize(body_view.len);
  if (body_view.len > 0) {
    std::memcpy(result.body.data(), body_view.data, body_view.len);
  }
  return result;
}

Result<detail::ClientStream> Channel::start_stream(std::string_view service,
                                                   std::string_view method, std::uint32_t kind,
                                                   std::span<const std::byte> body,
                                                   const CallOptions& options) {
  if (!state_) {
    return Error::runtime(-EINVAL, "channel must not be null");
  }
  auto native_options = normalize_call_options(options);
  if (!native_options) {
    return native_options.error();
  }
  auto admission = state_->admit();
  if (!admission) {
    return admission.error();
  }
  const trevrpc_request request = native_request(service, method, kind, body);
  trevrpc_stream* stream = nullptr;
  const int error = trevrpc_channel_start_stream_request_v1(
      admission.value().native_handle(), &request, &native_options.value().options, &stream);
  if (error != 0) {
    return Error::runtime(error);
  }
  return detail::ClientStream(stream);
}

struct Server::Route {
  std::uint32_t kind;
  Handler handler;
};

int Server::dispatch_route(void* user_data, trevrpc_call* call) noexcept {
  auto* route = static_cast<Route*>(user_data);
  detail::ServerCallbackContextGuard callback_context;
  try {
    route->handler(call);
  } catch (const std::exception&) {
    if (route->kind == TREVRPC_RPC_KIND_UNARY) {
      Server::respond(call, Status::internal("service handler threw"), {});
    } else {
      Server::finish(call, Status::internal("service handler threw"));
    }
  } catch (...) {
    if (route->kind == TREVRPC_RPC_KIND_UNARY) {
      Server::respond(call, Status::internal("service handler threw"), {});
    } else {
      Server::finish(call, Status::internal("service handler threw"));
    }
  }
  return 0;
}

Server::~Server() {
  if (state_ && state_->native_handle() != nullptr) {
    detail::abandon_server(std::move(state_));
  }
}

Server::Server(std::shared_ptr<detail::ServerState> state) noexcept : state_(std::move(state)) {}

Server::Server(Server&& other) noexcept : state_(std::move(other.state_)) {}

Server& Server::operator=(Server&& other) noexcept {
  if (this != &other) {
    if (state_ && state_->native_handle() != nullptr) {
      detail::abandon_server(std::move(state_));
    }
    state_ = std::move(other.state_);
  }
  return *this;
}

Result<Server> Server::listen(const ServerConfig& config) {
  if (config.max_idle_timeout.count() < 0 || config.keep_alive.count() < 0 ||
      static_cast<std::uint64_t>(config.keep_alive.count()) >
          std::numeric_limits<std::uint32_t>::max()) {
    return Error::runtime(-EINVAL, "server durations are out of range");
  }
  trevrpc_server_config_v1 native{};
  const int init_error = trevrpc_server_config_v1_init(&native, sizeof(native));
  if (init_error != 0) {
    return Error::runtime(init_error);
  }
  if (!config.host.empty()) {
    native.host = config.host.c_str();
  }
  native.port = config.port;
  native.enable_native = config.enable_native;
  native.cert_file = nullable(config.cert_file);
  native.key_file = nullable(config.key_file);
  if (!config.enable_webtransport) {
    native.webtransport_path = "";
  } else if (!config.webtransport_path.empty()) {
    native.webtransport_path = config.webtransport_path.c_str();
  }
  if (!config.webtransport_origin.empty()) {
    native.webtransport_origin = config.webtransport_origin.c_str();
  }
  native.enable_http3 = config.enable_http3;
  if (!config.http3_path.empty()) {
    native.http3_path = config.http3_path.c_str();
  }
  if (config.max_idle_timeout.count() != 0) {
    native.max_idle_timeout_ms = static_cast<std::uint64_t>(config.max_idle_timeout.count());
  }
  if (config.keep_alive.count() != 0) {
    native.keep_alive_ms = static_cast<std::uint32_t>(config.keep_alive.count());
  }
  if (config.peer_bidi_stream_count != 0) {
    native.peer_bidi_stream_count = config.peer_bidi_stream_count;
  }
  if (config.max_stateless_operations != 0) {
    native.max_stateless_operations = config.max_stateless_operations;
  }
  if (config.max_binding_stateless_operations != 0) {
    native.max_binding_stateless_operations = config.max_binding_stateless_operations;
  }
  if (config.max_pending_send_bytes != 0) {
    native.max_pending_send_bytes = config.max_pending_send_bytes;
  }
  if (config.max_pending_send_count != 0) {
    native.max_pending_send_count = config.max_pending_send_count;
  }
  if (config.max_sessions_per_connection != 0) {
    native.max_sessions_per_connection = config.max_sessions_per_connection;
  }
  if (config.max_streams_per_session != 0) {
    native.max_streams_per_session = config.max_streams_per_session;
  }
  if (config.stream_recv_window != 0) {
    native.stream_recv_window = config.stream_recv_window;
  }
  if (config.conn_flow_control_window != 0) {
    native.conn_flow_control_window = config.conn_flow_control_window;
  }
  if (config.max_frame_size != 0) {
    native.max_frame_size = config.max_frame_size;
  }

  std::shared_ptr<detail::WebTransportAdmissionState> webtransport_admission;
  if (config.webtransport_admission) {
    webtransport_admission = detail::make_webtransport_admission_state(
        config.webtransport_admission, config.callback_exception_sink);
    native.webtransport_admission = detail::webtransport_admission_trampoline;
    native.webtransport_admission_user_data = webtransport_admission.get();
  }
  std::shared_ptr<detail::Http3AdmissionState> http3_admission;
  if (config.http3_admission) {
    http3_admission =
        detail::make_http3_admission_state(config.http3_admission, config.callback_exception_sink);
    native.http3_admission = detail::http3_admission_trampoline;
    native.http3_admission_user_data = http3_admission.get();
  }

  trevrpc_server* server = nullptr;
  const int error = trevrpc_server_listen_v1(&native, &server);
  if (error != 0) {
    return Error::runtime(error);
  }
  try {
    return Server(std::make_shared<detail::ServerState>(server, std::move(webtransport_admission),
                                                        std::move(http3_admission)));
  } catch (...) {
    (void)trevrpc_server_cancel(server);
    (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
    (void)trevrpc_server_release(server);
    throw;
  }
}

Result<std::uint16_t> Server::port() const {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  auto admission = state_->admit();
  if (!admission) {
    return admission.error();
  }
  std::uint16_t value = 0;
  const int error = trevrpc_server_port(admission.value().native_handle(), &value);
  return error == 0 ? Result<std::uint16_t>{value} : Result<std::uint16_t>{Error::runtime(error)};
}

Result<void> Server::set_options(const ServerOptions& options) {
  const auto negative = [](const auto& value) { return value.has_value() && value->count() < 0; };
  if (negative(options.graceful_shutdown_timeout) || negative(options.initial_request_timeout) ||
      negative(options.stream_idle_timeout)) {
    return Error::runtime(-EINVAL, "server option durations must not be negative");
  }
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  auto admission = state_->admit();
  if (!admission) {
    return admission.error();
  }
  trevrpc_server_options_v1 native{};
  const int init_error = trevrpc_server_options_v1_init(&native, sizeof(native));
  if (init_error != 0) {
    return Error::runtime(init_error);
  }
  if (options.max_concurrent_connections) {
    native.max_concurrent_connections = *options.max_concurrent_connections;
  }
  if (options.max_concurrent_streams_per_connection) {
    native.max_concurrent_streams_per_connection = *options.max_concurrent_streams_per_connection;
  }
  if (options.max_concurrent_requests) {
    native.max_concurrent_requests = *options.max_concurrent_requests;
  }
  if (options.worker_count) {
    native.worker_count = *options.worker_count;
  }
  if (options.worker_queue_capacity) {
    native.worker_queue_capacity = *options.worker_queue_capacity;
  }
  if (options.graceful_shutdown_timeout) {
    native.graceful_shutdown_timeout_nanos =
        static_cast<std::uint64_t>(options.graceful_shutdown_timeout->count());
  }
  if (options.initial_request_timeout) {
    native.initial_request_timeout_nanos =
        static_cast<std::uint64_t>(options.initial_request_timeout->count());
  }
  if (options.max_stream_messages) {
    native.max_stream_messages = *options.max_stream_messages;
  }
  if (options.max_stream_body_size) {
    native.max_stream_body_size = *options.max_stream_body_size;
  }
  if (options.stream_idle_timeout) {
    native.stream_idle_timeout_nanos =
        static_cast<std::uint64_t>(options.stream_idle_timeout->count());
  }
  const int error = trevrpc_server_set_options_v1(admission.value().native_handle(), &native);
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
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

Result<void> Server::set_transport_observer(std::shared_ptr<TransportObserver> callback,
                                            std::shared_ptr<CallbackExceptionSink> exception_sink) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->set_transport_observer(
      detail::make_transport_state(std::move(callback), std::move(exception_sink)));
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

Result<void> Server::clear_transport_observer() {
  return state_ ? state_->clear_transport_observer()
                : Result<void>{Error::runtime(-EINVAL, "server is moved from")};
}

Result<void> Server::serve() {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  auto admission = state_->admit();
  if (!admission) {
    return admission.error();
  }
  trevrpc_server* native = admission.value().native_handle();
  int error = trevrpc_server_freeze(native);
  if (error == 0) {
    error = trevrpc_server_serve(native);
  }
  if (error == TREV_MSQUIC_ERR_CLOSED || error == TREV_WT_ERR_CLOSED) {
    std::uint32_t phase = TREVRPC_SERVER_PHASE_CONFIGURING;
    if (trevrpc_server_get_phase(native, &phase) == 0 && phase >= TREVRPC_SERVER_PHASE_STOPPING) {
      error = 0;
    }
  }
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
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
  if (!report && state_->native_handle() != nullptr) {
    detail::abandon_server(std::move(state_));
  }
}

trevrpc_server* Server::native_handle() const noexcept {
  return state_ ? state_->native_handle() : nullptr;
}

Result<void> Server::register_route(std::string_view service, std::string_view method,
                                    std::uint32_t kind, Handler handler) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  std::shared_ptr<Route> route;
  try {
    route = std::make_shared<Route>(Route{kind, std::move(handler)});
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate server route");
  }
  return state_->register_route(service, method, kind, Server::dispatch_route, route, route.get());
}

Result<void>
Server::register_native_route(std::string_view service, std::string_view method, std::uint32_t kind,
                              trevrpc_call_handler callback, const std::shared_ptr<void>& route,
                              void* user_data,
                              const std::shared_ptr<detail::AsyncServerScopeControl>& async_scope) {
  if (!state_) {
    return Error::runtime(-EINVAL, "server is moved from");
  }
  return state_->register_route(service, method, kind, callback, route, user_data, async_scope);
}

void Server::respond(trevrpc_call* call, const Status& status,
                     std::span<const std::byte> body) noexcept {
  NativeMetadata metadata;
  if (metadata.assign(status.metadata()) != 0) {
    constexpr std::string_view message = "failed to construct C++ service response";
    (void)respond_borrowed(call, TREVRPC_STATUS_INTERNAL, message, {}, nullptr);
    return;
  }
  (void)respond_borrowed(call, static_cast<std::uint32_t>(status.code()), status.message(), body,
                         status.metadata().empty() ? nullptr : metadata.get());
}

void Server::finish(trevrpc_call* call, const Status& status) noexcept {
  NativeMetadata metadata;
  if (metadata.assign(status.metadata()) != 0) {
    constexpr std::string_view message = "failed to construct C++ service terminal metadata";
    (void)finish_borrowed(call, TREVRPC_STATUS_INTERNAL, message, nullptr);
    return;
  }
  (void)finish_borrowed(call, static_cast<std::uint32_t>(status.code()), status.message(),
                        status.metadata().empty() ? nullptr : metadata.get());
}

} // namespace trevrpc
