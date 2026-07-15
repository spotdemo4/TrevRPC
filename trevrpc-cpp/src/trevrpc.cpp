#include <trevrpc/trevrpc.hpp>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>

namespace trevrpc {
namespace {

[[nodiscard]] const char* nullable(const std::string& value) {
  return value.empty() ? nullptr : value.c_str();
}

[[nodiscard]] Metadata copy_metadata(const trevrpc_metadata& metadata) {
  Metadata result;
  for (std::size_t index = 0; index < metadata.entries_len; ++index) {
    const trevrpc_metadata_entry& entry = metadata.entries[index];
    result.set(std::string(entry.key, entry.key_len),
               std::span(reinterpret_cast<const std::byte*>(entry.value), entry.value_len));
  }
  return result;
}

class NativeMetadata {
public:
  NativeMetadata() = default;
  ~NativeMetadata() { trevrpc_metadata_reset(&metadata_); }
  NativeMetadata(const NativeMetadata&) = delete;
  NativeMetadata& operator=(const NativeMetadata&) = delete;
  NativeMetadata(NativeMetadata&& other) noexcept : metadata_(std::exchange(other.metadata_, {})) {}
  NativeMetadata& operator=(NativeMetadata&& other) noexcept {
    if (this != &other) {
      trevrpc_metadata_reset(&metadata_);
      metadata_ = std::exchange(other.metadata_, {});
    }
    return *this;
  }

  [[nodiscard]] int assign(const Metadata& metadata) {
    for (const Metadata::Entry& entry : metadata.entries()) {
      const int error = trevrpc_metadata_set(
          &metadata_, entry.key.data(), entry.key.size(),
          reinterpret_cast<const std::uint8_t*>(entry.value.data()), entry.value.size());
      if (error != 0) {
        return error;
      }
    }
    return 0;
  }

  [[nodiscard]] trevrpc_metadata* get() noexcept { return &metadata_; }
  [[nodiscard]] const trevrpc_metadata* get() const noexcept { return &metadata_; }

private:
  trevrpc_metadata metadata_{};
};

struct NativeCallOptions {
  trevrpc_call_options options = trevrpc_default_call_options();
  NativeMetadata metadata;

  NativeCallOptions() = default;
  NativeCallOptions(const NativeCallOptions&) = delete;
  NativeCallOptions& operator=(const NativeCallOptions&) = delete;
  NativeCallOptions(NativeCallOptions&& other) noexcept
      : options(other.options), metadata(std::move(other.metadata)) {
    if (options.metadata != nullptr) {
      options.metadata = metadata.get();
    }
  }
  NativeCallOptions& operator=(NativeCallOptions&& other) noexcept {
    if (this != &other) {
      options = other.options;
      metadata = std::move(other.metadata);
      if (options.metadata != nullptr) {
        options.metadata = metadata.get();
      }
    }
    return *this;
  }
};

[[nodiscard]] Result<NativeCallOptions> native_call_options(const CallOptions& options) {
  if (options.timeout.count() < 0 || options.response_idle_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "call durations must not be negative");
  }
  NativeCallOptions result;
  const int error = result.metadata.assign(options.metadata);
  if (error != 0) {
    return Error::runtime(error);
  }
  result.options.metadata = options.metadata.empty() ? nullptr : result.metadata.get();
  result.options.timeout_nanos = static_cast<std::uint64_t>(options.timeout.count());
  result.options.cancellation =
      options.cancellation == nullptr ? nullptr : options.cancellation->native_handle();
  result.options.max_response_body_size = options.max_response_body_size;
  result.options.max_response_messages = options.max_response_messages;
  result.options.max_response_stream_body_size = options.max_response_stream_body_size;
  result.options.response_idle_timeout_nanos =
      static_cast<std::uint64_t>(options.response_idle_timeout.count());
  return result;
}

[[nodiscard]] Status status_from(std::uint32_t code, const char* message, std::size_t message_len,
                                 const trevrpc_metadata& metadata) {
  return Status(static_cast<StatusCode>(trevrpc_status_code_from_uint32(code)),
                message == nullptr ? std::string{} : std::string(message, message_len),
                copy_metadata(metadata));
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
    message = trevrpc_error(code);
  }
  return Error(Kind::Runtime, code, std::move(message));
}

Error Error::rpc(Status status) {
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

Cancellation::~Cancellation() { trevrpc_cancellation_free(cancellation_); }

Cancellation::Cancellation(Cancellation&& other) noexcept
    : cancellation_(std::exchange(other.cancellation_, nullptr)) {}

Cancellation& Cancellation::operator=(Cancellation&& other) noexcept {
  if (this != &other) {
    trevrpc_cancellation_free(cancellation_);
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
      send_finished_(std::exchange(other.send_finished_, false)) {}

ClientStream& ClientStream::operator=(ClientStream&& other) noexcept {
  if (this != &other) {
    close();
    stream_ = std::exchange(other.stream_, nullptr);
    send_finished_ = std::exchange(other.send_finished_, false);
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
  trevrpc_stream_frame* frame = nullptr;
  const int error = trevrpc_stream_recv(stream_, &frame);
  if (error != 0) {
    return Error::runtime(error);
  }
  if (frame == nullptr) {
    return Error::protobuf("stream ended without a terminal status");
  }
  StreamFrame result;
  result.terminal = frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS;
  if (result.terminal) {
    result.status = status_from(frame->status, frame->message, frame->message_len, frame->metadata);
  } else {
    result.body.resize(frame->body_len);
    if (frame->body_len > 0) {
      std::memcpy(result.body.data(), frame->body, frame->body_len);
    }
  }
  trevrpc_stream_frame_free(frame);
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
  trevrpc_stream_frame* frame = nullptr;
  const int error = trevrpc_stream_recv(stream, &frame);
  if (error != 0) {
    return Error::runtime(error);
  }
  if (frame == nullptr) {
    return std::optional<std::vector<std::byte>>{};
  }
  if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
    const Status status =
        status_from(frame->status, frame->message, frame->message_len, frame->metadata);
    trevrpc_stream_frame_free(frame);
    if (status.is_ok()) {
      return std::optional<std::vector<std::byte>>{};
    }
    return Error::rpc(status);
  }
  std::vector<std::byte> body(frame->body_len);
  if (frame->body_len > 0) {
    std::memcpy(body.data(), frame->body, frame->body_len);
  }
  trevrpc_stream_frame_free(frame);
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

Channel::~Channel() {
  close();
  if (channel_ != nullptr) {
    trevrpc_channel_release(channel_);
  }
}

Result<std::shared_ptr<Channel>> Channel::connect(std::string_view host, std::uint16_t port,
                                                  const ChannelConfig& config,
                                                  std::chrono::nanoseconds timeout,
                                                  Cancellation* cancellation) {
  if (timeout.count() < 0 || config.max_idle_timeout.count() < 0 || config.keep_alive.count() < 0 ||
      static_cast<std::uint64_t>(config.keep_alive.count()) >
          std::numeric_limits<std::uint32_t>::max()) {
    return Error::runtime(-EINVAL, "channel durations are out of range");
  }
  trevrpc_config native = trevrpc_default_config();
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

  trevrpc_channel* channel = nullptr;
  const std::string host_string(host);
  const int error = trevrpc_channel_connect(
      host_string.c_str(), port, &native, nullptr, static_cast<std::uint64_t>(timeout.count()),
      cancellation == nullptr ? nullptr : cancellation->native_handle(), &channel);
  if (error != 0) {
    return Error::runtime(error);
  }
  try {
    return std::shared_ptr<Channel>(new Channel(channel));
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
  std::uint64_t generation = 0;
  const int error = trevrpc_channel_wait_ready(
      channel_, static_cast<std::uint64_t>(timeout.count()),
      cancellation == nullptr ? nullptr : cancellation->native_handle(), &generation);
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

void Channel::close() noexcept {
  if (channel_ != nullptr) {
    trevrpc_channel_close(channel_);
  }
}

Result<detail::ByteResponse> Channel::call_unary(std::string_view service, std::string_view method,
                                                 std::span<const std::byte> body,
                                                 const CallOptions& options) {
  auto native_options = native_call_options(options);
  if (!native_options) {
    return native_options.error();
  }
  const std::string service_string(service);
  const std::string method_string(method);
  trevrpc_response* response = nullptr;
  const int error = trevrpc_channel_call_unary_with_options(
      channel_, service_string.c_str(), method_string.c_str(),
      reinterpret_cast<const std::uint8_t*>(body.data()), body.size(),
      &native_options.value().options, &response);
  if (error != 0) {
    return Error::runtime(error);
  }
  detail::ByteResponse result;
  result.status =
      status_from(response->status, response->message, response->message_len, response->metadata);
  result.metadata = copy_metadata(response->metadata);
  result.body.resize(response->body_len);
  if (response->body_len > 0) {
    std::memcpy(result.body.data(), response->body, response->body_len);
  }
  trevrpc_response_free(response);
  return result;
}

Result<detail::ClientStream> Channel::start_stream(std::string_view service,
                                                   std::string_view method, std::uint32_t kind,
                                                   std::span<const std::byte> body,
                                                   const CallOptions& options) {
  auto native_options = native_call_options(options);
  if (!native_options) {
    return native_options.error();
  }
  const std::string service_string(service);
  const std::string method_string(method);
  trevrpc_stream* stream = nullptr;
  const int error = trevrpc_channel_start_stream_with_options(
      channel_, service_string.c_str(), method_string.c_str(), kind,
      reinterpret_cast<const std::uint8_t*>(body.data()), body.size(),
      &native_options.value().options, &stream);
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
  try {
    route->handler(call);
  } catch (const std::exception& exception) {
    if (route->kind == TREVRPC_RPC_KIND_UNARY) {
      Server::respond(call, Status::internal(exception.what()), {});
    } else {
      Server::finish(call, Status::internal(exception.what()));
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

Server::~Server() { close(); }

Server::Server(trevrpc_server* server) noexcept : server_(server) {}

Server::Server(Server&& other) noexcept
    : server_(std::exchange(other.server_, nullptr)), routes_(std::move(other.routes_)) {}

Server& Server::operator=(Server&& other) noexcept {
  if (this != &other) {
    close();
    server_ = std::exchange(other.server_, nullptr);
    routes_ = std::move(other.routes_);
  }
  return *this;
}

Result<Server> Server::listen(const ServerConfig& config) {
  if (config.max_idle_timeout.count() < 0 || config.keep_alive.count() < 0 ||
      static_cast<std::uint64_t>(config.keep_alive.count()) >
          std::numeric_limits<std::uint32_t>::max()) {
    return Error::runtime(-EINVAL, "server durations are out of range");
  }
  trevrpc_server_config native = trevrpc_default_server_config();
  if (!config.host.empty()) {
    native.host = config.host.c_str();
  }
  native.port = config.port;
  native.cert_file = nullable(config.cert_file);
  native.key_file = nullable(config.key_file);
  if (!config.webtransport_path.empty()) {
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

  trevrpc_server* server = nullptr;
  const int error = trevrpc_server_listen(&native, &server);
  if (error != 0) {
    return Error::runtime(error);
  }
  return Server(server);
}

Result<std::uint16_t> Server::port() const {
  std::uint16_t value = 0;
  const int error = trevrpc_server_port(server_, &value);
  return error == 0 ? Result<std::uint16_t>{value} : Result<std::uint16_t>{Error::runtime(error)};
}

Result<void> Server::set_options(const ServerOptions& options) {
  if (options.graceful_shutdown_timeout.count() < 0 ||
      options.initial_request_timeout.count() < 0 || options.stream_idle_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "server option durations must not be negative");
  }
  trevrpc_server_options native = trevrpc_default_server_options();
  native.max_concurrent_connections = options.max_concurrent_connections;
  native.max_concurrent_streams_per_connection = options.max_concurrent_streams_per_connection;
  native.max_concurrent_requests = options.max_concurrent_requests;
  native.worker_count = options.worker_count;
  native.worker_queue_capacity = options.worker_queue_capacity;
  native.graceful_shutdown_timeout_nanos =
      static_cast<std::uint64_t>(options.graceful_shutdown_timeout.count());
  native.initial_request_timeout_nanos =
      static_cast<std::uint64_t>(options.initial_request_timeout.count());
  native.max_stream_messages = options.max_stream_messages;
  native.max_stream_body_size = options.max_stream_body_size;
  native.stream_idle_timeout_nanos =
      static_cast<std::uint64_t>(options.stream_idle_timeout.count());
  const int error = trevrpc_server_set_options(server_, &native);
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

Result<void> Server::serve() {
  const int error = trevrpc_server_serve(server_);
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

void Server::shutdown() noexcept {
  if (server_ != nullptr) {
    trevrpc_server_shutdown(server_);
  }
}

void Server::close() noexcept {
  if (server_ != nullptr) {
    trevrpc_server_close(server_);
    server_ = nullptr;
    routes_.clear();
  }
}

Result<void> Server::register_route(std::string_view service, std::string_view method,
                                    std::uint32_t kind, Handler handler) {
  auto route = std::make_unique<Route>(Route{kind, std::move(handler)});
  const std::string service_string(service);
  const std::string method_string(method);
  const int error =
      trevrpc_server_register_call(server_, service_string.c_str(), method_string.c_str(), kind,
                                   Server::dispatch_route, route.get());
  if (error != 0) {
    return Error::runtime(error);
  }
  routes_.push_back(std::move(route));
  return {};
}

void Server::respond(trevrpc_call* call, const Status& status,
                     std::span<const std::byte> body) noexcept {
  trevrpc_response response{};
  NativeMetadata metadata;
  if (metadata.assign(status.metadata()) != 0 ||
      trevrpc_response_set_status(
          &response, trevrpc_status_new(static_cast<std::uint32_t>(status.code()),
                                        status.message().data(), status.message().size())) != 0 ||
      trevrpc_response_set_body(&response, reinterpret_cast<const std::uint8_t*>(body.data()),
                                body.size()) != 0) {
    trevrpc_response_reset(&response);
    trevrpc_response fallback{};
    const char message[] = "failed to construct C++ service response";
    trevrpc_response_set_status(&fallback, trevrpc_status_internal(message, sizeof(message) - 1));
    trevrpc_call_respond(call, &fallback);
    trevrpc_response_reset(&fallback);
    return;
  }
  response.metadata = *metadata.get();
  *metadata.get() = {};
  trevrpc_call_respond(call, &response);
  trevrpc_response_reset(&response);
}

void Server::finish(trevrpc_call* call, const Status& status) noexcept {
  NativeMetadata metadata;
  const trevrpc_metadata* native_metadata = nullptr;
  const int metadata_error = metadata.assign(status.metadata());
  if (metadata_error != 0) {
    constexpr std::string_view message = "failed to construct C++ service terminal metadata";
    trevrpc_call_finish_stream(call, TREVRPC_STATUS_INTERNAL, message.data(), message.size());
    return;
  }
  if (!status.metadata().empty()) {
    native_metadata = metadata.get();
  }
  trevrpc_call_finish_stream_with_metadata(call, static_cast<std::uint32_t>(status.code()),
                                           status.message().data(), status.message().size(),
                                           native_metadata);
}

} // namespace trevrpc
