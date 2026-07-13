#pragma once

#include <trevrpc.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace trevrpc {

enum class StatusCode : std::uint32_t {
  Ok = TREVRPC_STATUS_OK,
  Cancelled = TREVRPC_STATUS_CANCELLED,
  Unknown = TREVRPC_STATUS_UNKNOWN,
  InvalidArgument = TREVRPC_STATUS_INVALID_ARGUMENT,
  DeadlineExceeded = TREVRPC_STATUS_DEADLINE_EXCEEDED,
  NotFound = TREVRPC_STATUS_NOT_FOUND,
  AlreadyExists = TREVRPC_STATUS_ALREADY_EXISTS,
  PermissionDenied = TREVRPC_STATUS_PERMISSION_DENIED,
  ResourceExhausted = TREVRPC_STATUS_RESOURCE_EXHAUSTED,
  FailedPrecondition = TREVRPC_STATUS_FAILED_PRECONDITION,
  Aborted = TREVRPC_STATUS_ABORTED,
  OutOfRange = TREVRPC_STATUS_OUT_OF_RANGE,
  Unimplemented = TREVRPC_STATUS_UNIMPLEMENTED,
  Internal = TREVRPC_STATUS_INTERNAL,
  Unavailable = TREVRPC_STATUS_UNAVAILABLE,
  DataLoss = TREVRPC_STATUS_DATA_LOSS,
  Unauthenticated = TREVRPC_STATUS_UNAUTHENTICATED,
};

class Metadata {
public:
  struct Entry {
    std::string key;
    std::vector<std::byte> value;
  };

  void set(std::string key, std::span<const std::byte> value);
  void set(std::string key, std::string_view value);
  [[nodiscard]] std::optional<std::span<const std::byte>> get(std::string_view key) const;
  [[nodiscard]] const std::vector<Entry>& entries() const noexcept { return entries_; }
  [[nodiscard]] bool empty() const noexcept { return entries_.empty(); }

private:
  std::vector<Entry> entries_;
};

class Status {
public:
  Status() = default;
  explicit Status(StatusCode code, std::string message = {}, Metadata metadata = {})
      : code_(code), message_(std::move(message)), metadata_(std::move(metadata)) {}

  [[nodiscard]] static Status ok() { return Status{}; }
  [[nodiscard]] static Status invalid_argument(std::string message) {
    return Status(StatusCode::InvalidArgument, std::move(message));
  }
  [[nodiscard]] static Status internal(std::string message) {
    return Status(StatusCode::Internal, std::move(message));
  }
  [[nodiscard]] static Status data_loss(std::string message) {
    return Status(StatusCode::DataLoss, std::move(message));
  }
  [[nodiscard]] bool is_ok() const noexcept { return code_ == StatusCode::Ok; }
  [[nodiscard]] StatusCode code() const noexcept { return code_; }
  [[nodiscard]] const std::string& message() const noexcept { return message_; }
  [[nodiscard]] const Metadata& metadata() const noexcept { return metadata_; }

private:
  StatusCode code_ = StatusCode::Ok;
  std::string message_;
  Metadata metadata_;
};

class Error {
public:
  enum class Kind { Runtime, Rpc, Protobuf };

  [[nodiscard]] static Error runtime(int code, std::string message = {});
  [[nodiscard]] static Error rpc(Status status);
  [[nodiscard]] static Error protobuf(std::string message);

  [[nodiscard]] Kind kind() const noexcept { return kind_; }
  [[nodiscard]] int code() const noexcept { return code_; }
  [[nodiscard]] const std::string& message() const noexcept { return message_; }
  [[nodiscard]] const std::optional<Status>& status() const noexcept { return status_; }

private:
  Error(Kind kind, int code, std::string message, std::optional<Status> status = {})
      : kind_(kind), code_(code), message_(std::move(message)), status_(std::move(status)) {}

  Kind kind_;
  int code_;
  std::string message_;
  std::optional<Status> status_;
};

template <typename T> class [[nodiscard]] Result {
public:
  Result(T value) : value_(std::move(value)) {}
  Result(Error error) : value_(std::move(error)) {}
  Result(Status status) : value_(Error::rpc(std::move(status))) {}

  [[nodiscard]] bool has_value() const noexcept { return std::holds_alternative<T>(value_); }
  [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
  [[nodiscard]] T& value() & { return std::get<T>(value_); }
  [[nodiscard]] const T& value() const& { return std::get<T>(value_); }
  [[nodiscard]] T&& value() && { return std::get<T>(std::move(value_)); }
  [[nodiscard]] Error& error() & { return std::get<Error>(value_); }
  [[nodiscard]] const Error& error() const& { return std::get<Error>(value_); }

private:
  std::variant<T, Error> value_;
};

template <> class [[nodiscard]] Result<void> {
public:
  Result() = default;
  Result(Error error) : error_(std::move(error)) {}
  Result(Status status) {
    if (!status.is_ok()) {
      error_ = Error::rpc(std::move(status));
    }
  }

  [[nodiscard]] bool has_value() const noexcept { return !error_.has_value(); }
  [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
  [[nodiscard]] Error& error() & { return error_.value(); }
  [[nodiscard]] const Error& error() const& { return error_.value(); }

private:
  std::optional<Error> error_;
};

class Cancellation {
public:
  Cancellation();
  ~Cancellation();
  Cancellation(const Cancellation&) = delete;
  Cancellation& operator=(const Cancellation&) = delete;
  Cancellation(Cancellation&& other) noexcept;
  Cancellation& operator=(Cancellation&& other) noexcept;

  void cancel() noexcept;
  [[nodiscard]] bool cancelled() const noexcept;
  [[nodiscard]] trevrpc_cancellation* native_handle() const noexcept { return cancellation_; }

private:
  trevrpc_cancellation* cancellation_ = nullptr;
};

struct ChannelConfig {
  std::string cert_file;
  std::string key_file;
  std::string ca_cert_file;
  bool skip_certificate_validation = false;
  std::chrono::milliseconds max_idle_timeout{0};
  std::chrono::milliseconds keep_alive{0};
  std::uint16_t peer_bidi_stream_count = 0;
  std::uint32_t max_stateless_operations = 0;
  std::uint16_t max_binding_stateless_operations = 0;
  std::size_t max_pending_send_bytes = 0;
  std::size_t max_pending_send_count = 0;
  std::size_t max_frame_size = 0;
  std::uint32_t stream_recv_window = 0;
  std::uint32_t conn_flow_control_window = 0;
};

struct CallOptions {
  Metadata metadata;
  std::chrono::nanoseconds timeout{0};
  Cancellation* cancellation = nullptr;
  std::int64_t max_response_body_size = 0;
  std::int64_t max_response_messages = 0;
  std::int64_t max_response_stream_body_size = 0;
  std::chrono::nanoseconds response_idle_timeout{0};
};

struct ServerConfig {
  std::string host;
  std::uint16_t port = 0;
  std::string cert_file;
  std::string key_file;
  std::string webtransport_path;
  std::string webtransport_origin;
  bool enable_http3 = false;
  std::string http3_path;
  std::chrono::milliseconds max_idle_timeout{0};
  std::chrono::milliseconds keep_alive{0};
  std::uint16_t peer_bidi_stream_count = 0;
  std::uint32_t max_stateless_operations = 0;
  std::uint16_t max_binding_stateless_operations = 0;
  std::size_t max_pending_send_bytes = 0;
  std::size_t max_pending_send_count = 0;
  std::uint32_t max_sessions_per_connection = 0;
  std::uint32_t max_streams_per_session = 0;
  std::uint32_t stream_recv_window = 0;
  std::uint32_t conn_flow_control_window = 0;
  std::size_t max_frame_size = 0;
};

struct ServerOptions {
  std::int64_t max_concurrent_connections = 0;
  std::int64_t max_concurrent_streams_per_connection = 0;
  std::int64_t max_concurrent_requests = 0;
  std::int64_t worker_count = 0;
  std::int64_t worker_queue_capacity = 0;
  std::chrono::nanoseconds graceful_shutdown_timeout{0};
  std::chrono::nanoseconds initial_request_timeout{0};
  std::int64_t max_stream_messages = 0;
  std::int64_t max_stream_body_size = 0;
  std::chrono::nanoseconds stream_idle_timeout{0};
};

class CallContext {
public:
  [[nodiscard]] bool has_deadline() const noexcept;
  [[nodiscard]] bool deadline_expired() const noexcept;
  [[nodiscard]] bool cancelled() const noexcept;
  [[nodiscard]] std::optional<std::chrono::nanoseconds> time_remaining() const noexcept;
  [[nodiscard]] const Metadata& metadata() const noexcept { return metadata_; }

private:
  friend class Server;
  CallContext(const trevrpc_call_context* context, const trevrpc_request* request);

  const trevrpc_call_context* context_ = nullptr;
  Metadata metadata_;
};

namespace detail {

struct ByteResponse {
  Status status;
  std::vector<std::byte> body;
  Metadata metadata;
};

struct StreamFrame {
  bool terminal = false;
  Status status;
  std::vector<std::byte> body;
};

class ClientStream {
public:
  ClientStream() = default;
  explicit ClientStream(trevrpc_stream* stream) noexcept : stream_(stream) {}
  ~ClientStream();
  ClientStream(const ClientStream&) = delete;
  ClientStream& operator=(const ClientStream&) = delete;
  ClientStream(ClientStream&& other) noexcept;
  ClientStream& operator=(ClientStream&& other) noexcept;

  [[nodiscard]] Result<void> send(std::span<const std::byte> body);
  [[nodiscard]] Result<void> finish_send();
  [[nodiscard]] Result<StreamFrame> receive();
  void cancel() noexcept;
  void close() noexcept;

private:
  trevrpc_stream* stream_ = nullptr;
  bool send_finished_ = false;
};

template <typename Message>
[[nodiscard]] Result<std::vector<std::byte>> serialize(const Message& message) {
  const std::size_t encoded_size = message.ByteSizeLong();
  if (encoded_size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return Error::protobuf("protobuf message exceeds the serializer size limit");
  }
  std::vector<std::byte> body(encoded_size);
  std::byte empty_buffer{};
  void* data = body.empty() ? static_cast<void*>(&empty_buffer) : body.data();
  if (!message.SerializeToArray(data, static_cast<int>(body.size()))) {
    return Error::protobuf("failed to serialize protobuf message");
  }
  return body;
}

template <typename Message>
[[nodiscard]] Result<Message> parse(std::span<const std::byte> body,
                                    std::string_view failure_message) {
  if (body.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return Error::protobuf("protobuf message exceeds the parser size limit");
  }
  Message message;
  const void* data = body.empty() ? nullptr : body.data();
  if (!message.ParseFromArray(data, static_cast<int>(body.size()))) {
    return Error::protobuf(std::string(failure_message));
  }
  return message;
}

[[nodiscard]] Result<void> send_server_message(trevrpc_stream* stream,
                                               std::span<const std::byte> body);
[[nodiscard]] Result<std::optional<std::vector<std::byte>>>
receive_server_message(trevrpc_stream* stream);
[[nodiscard]] Status error_status(const Error& error);

} // namespace detail

class Channel {
public:
  ~Channel();
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  [[nodiscard]] static Result<std::shared_ptr<Channel>>
  connect(std::string_view host, std::uint16_t port, const ChannelConfig& config = {},
          std::chrono::nanoseconds timeout = std::chrono::nanoseconds{0},
          Cancellation* cancellation = nullptr);

  [[nodiscard]] Result<void>
  wait_ready(std::chrono::nanoseconds timeout = std::chrono::nanoseconds{0},
             Cancellation* cancellation = nullptr);
  void close() noexcept;
  [[nodiscard]] trevrpc_channel* native_handle() const noexcept { return channel_; }

  [[nodiscard]] Result<detail::ByteResponse> call_unary(std::string_view service,
                                                        std::string_view method,
                                                        std::span<const std::byte> body,
                                                        const CallOptions& options);
  [[nodiscard]] Result<detail::ClientStream>
  start_stream(std::string_view service, std::string_view method, std::uint32_t kind,
               std::span<const std::byte> body, const CallOptions& options);

private:
  explicit Channel(trevrpc_channel* channel) noexcept : channel_(channel) {}
  trevrpc_channel* channel_ = nullptr;
};

template <typename T> struct UnaryResponse {
  T message;
  Metadata metadata;
};

template <typename T> class StreamEvent {
public:
  [[nodiscard]] static StreamEvent message(T value) { return StreamEvent(std::move(value)); }
  [[nodiscard]] static StreamEvent terminal(Status status) {
    return StreamEvent(std::move(status));
  }
  [[nodiscard]] bool is_message() const noexcept { return std::holds_alternative<T>(value_); }
  [[nodiscard]] T& message() & { return std::get<T>(value_); }
  [[nodiscard]] const T& message() const& { return std::get<T>(value_); }
  [[nodiscard]] const Status& status() const { return std::get<Status>(value_); }

private:
  explicit StreamEvent(T value) : value_(std::move(value)) {}
  explicit StreamEvent(Status status) : value_(std::move(status)) {}
  std::variant<T, Status> value_;
};

template <typename Response> class ServerStreamingCall {
public:
  explicit ServerStreamingCall(detail::ClientStream stream) : stream_(std::move(stream)) {}
  [[nodiscard]] Result<StreamEvent<Response>> receive() {
    auto frame = stream_.receive();
    if (!frame) {
      return frame.error();
    }
    if (frame.value().terminal) {
      return StreamEvent<Response>::terminal(std::move(frame.value().status));
    }
    auto message = detail::parse<Response>(frame.value().body, "failed to parse stream response");
    if (!message) {
      return message.error();
    }
    return StreamEvent<Response>::message(std::move(message).value());
  }
  void cancel() noexcept { stream_.cancel(); }
  void close() noexcept { stream_.close(); }

private:
  detail::ClientStream stream_;
};

template <typename Request, typename Response> class ClientStreamingCall {
public:
  explicit ClientStreamingCall(detail::ClientStream stream) : stream_(std::move(stream)) {}
  [[nodiscard]] Result<void> send(const Request& request) {
    auto body = detail::serialize(request);
    if (!body) {
      return body.error();
    }
    return stream_.send(body.value());
  }
  [[nodiscard]] Result<void> finish_send() { return stream_.finish_send(); }
  [[nodiscard]] Result<StreamEvent<Response>> receive() {
    auto frame = stream_.receive();
    if (!frame) {
      return frame.error();
    }
    if (frame.value().terminal) {
      return StreamEvent<Response>::terminal(std::move(frame.value().status));
    }
    auto message = detail::parse<Response>(frame.value().body, "failed to parse stream response");
    if (!message) {
      return message.error();
    }
    return StreamEvent<Response>::message(std::move(message).value());
  }
  void cancel() noexcept { stream_.cancel(); }
  void close() noexcept { stream_.close(); }

private:
  detail::ClientStream stream_;
};

template <typename Request, typename Response>
using BidirectionalStreamingCall = ClientStreamingCall<Request, Response>;

template <typename Response> class ServerWriter {
public:
  [[nodiscard]] Result<void> send(const Response& response) {
    auto body = detail::serialize(response);
    if (!body) {
      return body.error();
    }
    return detail::send_server_message(stream_, body.value());
  }

private:
  friend class Server;
  template <typename, typename> friend class ServerReaderWriter;
  explicit ServerWriter(trevrpc_stream* stream) noexcept : stream_(stream) {}
  trevrpc_stream* stream_;
};

template <typename Request> class ServerReader {
public:
  [[nodiscard]] Result<std::optional<Request>> receive() {
    auto body = detail::receive_server_message(stream_);
    if (!body) {
      return body.error();
    }
    if (!body.value().has_value()) {
      return std::optional<Request>{};
    }
    auto message = detail::parse<Request>(body.value().value(), "failed to parse stream request");
    if (!message) {
      return Error::rpc(Status::invalid_argument(message.error().message()));
    }
    return std::optional<Request>(std::move(message).value());
  }

private:
  friend class Server;
  template <typename, typename> friend class ServerReaderWriter;
  explicit ServerReader(trevrpc_stream* stream) noexcept : stream_(stream) {}
  trevrpc_stream* stream_;
};

template <typename Request, typename Response> class ServerReaderWriter {
public:
  [[nodiscard]] Result<std::optional<Request>> receive() { return reader_.receive(); }
  [[nodiscard]] Result<void> send(const Response& response) { return writer_.send(response); }

private:
  friend class Server;
  explicit ServerReaderWriter(trevrpc_stream* stream) noexcept : reader_(stream), writer_(stream) {}
  ServerReader<Request> reader_;
  ServerWriter<Response> writer_;
};

template <typename Request, typename Response>
[[nodiscard]] Result<UnaryResponse<Response>> unary(Channel& channel, std::string_view service,
                                                    std::string_view method, const Request& request,
                                                    const CallOptions& options = {}) {
  auto body = detail::serialize(request);
  if (!body) {
    return body.error();
  }
  auto response = channel.call_unary(service, method, body.value(), options);
  if (!response) {
    return response.error();
  }
  if (!response.value().status.is_ok()) {
    Status status(response.value().status.code(), response.value().status.message(),
                  std::move(response.value().metadata));
    return Error::rpc(std::move(status));
  }
  auto message = detail::parse<Response>(response.value().body, "failed to parse unary response");
  if (!message) {
    return message.error();
  }
  return UnaryResponse<Response>{std::move(message).value(), std::move(response.value().metadata)};
}

template <typename Request, typename Response>
[[nodiscard]] Result<ServerStreamingCall<Response>>
server_streaming(Channel& channel, std::string_view service, std::string_view method,
                 const Request& request, const CallOptions& options = {}) {
  auto body = detail::serialize(request);
  if (!body) {
    return body.error();
  }
  auto stream = channel.start_stream(service, method, TREVRPC_RPC_KIND_SERVER_STREAMING,
                                     body.value(), options);
  if (!stream) {
    return stream.error();
  }
  auto finished = stream.value().finish_send();
  if (!finished) {
    return finished.error();
  }
  return ServerStreamingCall<Response>(std::move(stream).value());
}

template <typename Request, typename Response>
[[nodiscard]] Result<ClientStreamingCall<Request, Response>>
client_streaming(Channel& channel, std::string_view service, std::string_view method,
                 const CallOptions& options = {}) {
  auto stream =
      channel.start_stream(service, method, TREVRPC_RPC_KIND_CLIENT_STREAMING, {}, options);
  if (!stream) {
    return stream.error();
  }
  return ClientStreamingCall<Request, Response>(std::move(stream).value());
}

template <typename Request, typename Response>
[[nodiscard]] Result<BidirectionalStreamingCall<Request, Response>>
bidirectional_streaming(Channel& channel, std::string_view service, std::string_view method,
                        const CallOptions& options = {}) {
  auto stream =
      channel.start_stream(service, method, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, {}, options);
  if (!stream) {
    return stream.error();
  }
  return BidirectionalStreamingCall<Request, Response>(std::move(stream).value());
}

class Server {
public:
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  Server(Server&& other) noexcept;
  Server& operator=(Server&& other) noexcept;

  [[nodiscard]] static Result<Server> listen(const ServerConfig& config);
  [[nodiscard]] Result<std::uint16_t> port() const;
  [[nodiscard]] Result<void> set_options(const ServerOptions& options);
  [[nodiscard]] Result<void> serve();
  void shutdown() noexcept;
  void close() noexcept;
  [[nodiscard]] trevrpc_server* native_handle() const noexcept { return server_; }

  template <typename Request, typename Response, typename Handler>
  [[nodiscard]] Result<void> register_unary(std::string_view service, std::string_view method,
                                            Handler handler) {
    return register_route(
        service, method, TREVRPC_RPC_KIND_UNARY,
        [handler = std::move(handler)](trevrpc_call* call) mutable {
          const trevrpc_request* request = trevrpc_call_request(call);
          CallContext context(trevrpc_call_get_context(call), request);
          auto decoded = detail::parse<Request>(
              std::span(reinterpret_cast<const std::byte*>(request->body), request->body_len),
              "failed to parse unary request");
          if (!decoded) {
            respond(call, Status::invalid_argument(decoded.error().message()), {});
            return;
          }
          auto response = handler(context, decoded.value());
          if (!response) {
            respond(call, detail::error_status(response.error()), {});
            return;
          }
          auto body = detail::serialize(response.value());
          if (!body) {
            respond(call, Status::internal(body.error().message()), {});
            return;
          }
          respond(call, Status::ok(), body.value());
        });
  }

  template <typename Request, typename Response, typename Handler>
  [[nodiscard]] Result<void> register_server_streaming(std::string_view service,
                                                       std::string_view method, Handler handler) {
    return register_route(
        service, method, TREVRPC_RPC_KIND_SERVER_STREAMING,
        [handler = std::move(handler)](trevrpc_call* call) mutable {
          const trevrpc_request* request = trevrpc_call_request(call);
          CallContext context(trevrpc_call_get_context(call), request);
          auto decoded = detail::parse<Request>(
              std::span(reinterpret_cast<const std::byte*>(request->body), request->body_len),
              "failed to parse server-streaming request");
          if (!decoded) {
            finish(call, Status::invalid_argument(decoded.error().message()));
            return;
          }
          ServerWriter<Response> writer(trevrpc_call_stream(call));
          finish(call, handler(context, decoded.value(), writer));
        });
  }

  template <typename Request, typename Response, typename Handler>
  [[nodiscard]] Result<void> register_client_streaming(std::string_view service,
                                                       std::string_view method, Handler handler) {
    return register_route(service, method, TREVRPC_RPC_KIND_CLIENT_STREAMING,
                          [handler = std::move(handler)](trevrpc_call* call) mutable {
                            const trevrpc_request* request = trevrpc_call_request(call);
                            CallContext context(trevrpc_call_get_context(call), request);
                            ServerReader<Request> reader(trevrpc_call_stream(call));
                            auto response = handler(context, reader);
                            if (!response) {
                              finish(call, detail::error_status(response.error()));
                              return;
                            }
                            auto body = detail::serialize(response.value());
                            if (!body) {
                              finish(call, Status::internal(body.error().message()));
                              return;
                            }
                            auto sent = detail::send_server_message(trevrpc_call_stream(call),
                                                                    body.value());
                            finish(call, sent ? Status::ok() : detail::error_status(sent.error()));
                          });
  }

  template <typename Request, typename Response, typename Handler>
  [[nodiscard]] Result<void> register_bidirectional_streaming(std::string_view service,
                                                              std::string_view method,
                                                              Handler handler) {
    return register_route(service, method, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
                          [handler = std::move(handler)](trevrpc_call* call) mutable {
                            const trevrpc_request* request = trevrpc_call_request(call);
                            CallContext context(trevrpc_call_get_context(call), request);
                            ServerReaderWriter<Request, Response> stream(trevrpc_call_stream(call));
                            finish(call, handler(context, stream));
                          });
  }

private:
  using Handler = std::function<void(trevrpc_call*)>;
  struct Route;

  explicit Server(trevrpc_server* server) noexcept;
  [[nodiscard]] Result<void> register_route(std::string_view service, std::string_view method,
                                            std::uint32_t kind, Handler handler);
  static int dispatch_route(void* user_data, trevrpc_call* call) noexcept;
  static void respond(trevrpc_call* call, const Status& status,
                      std::span<const std::byte> body) noexcept;
  static void finish(trevrpc_call* call, const Status& status) noexcept;

  trevrpc_server* server_ = nullptr;
  std::vector<std::unique_ptr<Route>> routes_;
};

} // namespace trevrpc
