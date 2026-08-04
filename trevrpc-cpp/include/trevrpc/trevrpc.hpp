#pragma once

#include <trevrpc.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
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

class Authorizer;
class MetricsObserver;
class Logger;
class TransportObserver;
class WebTransportAdmission;
class Http3Admission;
class ChannelLifecycleObserver;
class CallbackExceptionSink;

namespace detail {
struct AsyncRegistrationAccess;
struct CallbackAccess;
} // namespace detail

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
  Cancellation(const Cancellation& other);
  Cancellation& operator=(const Cancellation& other);
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
  std::shared_ptr<ChannelLifecycleObserver> lifecycle_observer;
  std::shared_ptr<CallbackExceptionSink> callback_exception_sink;
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
  std::shared_ptr<WebTransportAdmission> webtransport_admission;
  std::shared_ptr<Http3Admission> http3_admission;
  std::shared_ptr<CallbackExceptionSink> callback_exception_sink;
};

struct ServerOptions {
  std::optional<std::int64_t> max_concurrent_connections;
  std::optional<std::int64_t> max_concurrent_streams_per_connection;
  std::optional<std::int64_t> max_concurrent_requests;
  std::optional<std::int64_t> worker_count;
  std::optional<std::int64_t> worker_queue_capacity;
  std::optional<std::chrono::nanoseconds> graceful_shutdown_timeout;
  std::optional<std::chrono::nanoseconds> initial_request_timeout;
  std::optional<std::int64_t> max_stream_messages;
  std::optional<std::int64_t> max_stream_body_size;
  std::optional<std::chrono::nanoseconds> stream_idle_timeout;
};

enum class ServerPhase : std::uint32_t {
  Configuring = TREVRPC_SERVER_PHASE_CONFIGURING,
  Frozen = TREVRPC_SERVER_PHASE_FROZEN,
  Serving = TREVRPC_SERVER_PHASE_SERVING,
  Stopping = TREVRPC_SERVER_PHASE_STOPPING,
  Cancelling = TREVRPC_SERVER_PHASE_CANCELLING,
  Stopped = TREVRPC_SERVER_PHASE_STOPPED,
  Released,
};

enum class ShutdownOutcome { Graceful, Cancelled, TimedOut };

struct ShutdownOptions {
  std::optional<std::chrono::nanoseconds> graceful_timeout;
  std::chrono::nanoseconds cancellation_timeout{std::chrono::seconds(10)};
};

struct ShutdownReport {
  ShutdownOutcome outcome;
  ServerPhase final_phase;
  bool released;
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
  friend struct detail::AsyncRegistrationAccess;
  friend struct detail::CallbackAccess;
  CallContext(const trevrpc_call_context* context, const trevrpc_request* request);

  const trevrpc_call_context* context_ = nullptr;
  Metadata metadata_;
};

namespace detail {

class AsyncServerScopeControl;
class ChannelState;
class ServerState;

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
  [[nodiscard]] trevrpc_stream* release_native_handle() noexcept {
    return std::exchange(stream_, nullptr);
  }

private:
  trevrpc_stream* stream_ = nullptr;
  bool send_finished_ = false;
  bool receive_finished_ = false;
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

inline bool consume_protobuf_varint(std::span<const std::byte> data, std::size_t& offset,
                                    std::uint64_t& value) {
  value = 0;
  for (unsigned int shift = 0; shift < 64; shift += 7) {
    if (offset >= data.size()) {
      return false;
    }
    const auto byte = std::to_integer<std::uint8_t>(data[offset++]);
    if (shift == 63 && byte > 1) {
      return false;
    }
    value |= static_cast<std::uint64_t>(byte & 0x7fU) << shift;
    if (byte < 0x80U) {
      return true;
    }
  }
  return false;
}

inline bool skip_protobuf_field(std::span<const std::byte> data, std::size_t& offset,
                                std::uint64_t wire_type, std::uint64_t field_number) {
  std::uint64_t value = 0;
  switch (wire_type) {
  case 0:
    return consume_protobuf_varint(data, offset, value);
  case 1:
    if (data.size() - offset < 8) {
      return false;
    }
    offset += 8;
    return true;
  case 2:
    if (!consume_protobuf_varint(data, offset, value) || value > data.size() - offset) {
      return false;
    }
    offset += static_cast<std::size_t>(value);
    return true;
  case 3:
    while (offset < data.size()) {
      std::uint64_t tag = 0;
      if (!consume_protobuf_varint(data, offset, tag) || tag == 0) {
        return false;
      }
      const std::uint64_t nested_field = tag >> 3;
      const std::uint64_t nested_wire_type = tag & 0x7U;
      if (nested_field == 0 || nested_field > 0x1fffffffU) {
        return false;
      }
      if (nested_wire_type == 4) {
        return nested_field == field_number;
      }
      if (!skip_protobuf_field(data, offset, nested_wire_type, nested_field)) {
        return false;
      }
    }
    return false;
  case 5:
    if (data.size() - offset < 4) {
      return false;
    }
    offset += 4;
    return true;
  default:
    return false;
  }
}

inline std::optional<std::uint64_t> protobuf_wire_type_for_field_type(int field_type) {
  switch (field_type) {
  case 1:  // double
  case 6:  // fixed64
  case 16: // sfixed64
    return 1;
  case 2:  // float
  case 7:  // fixed32
  case 15: // sfixed32
    return 5;
  case 9:  // string
  case 11: // message
  case 12: // bytes
    return 2;
  case 10: // group
    return 3;
  case 3:  // int64
  case 4:  // uint64
  case 5:  // int32
  case 8:  // bool
  case 13: // uint32
  case 14: // enum
  case 17: // sint32
  case 18: // sint64
    return 0;
  default:
    return std::nullopt;
  }
}

template <typename Message>
[[nodiscard]] bool valid_known_protobuf_wire_types(std::span<const std::byte> body) {
  if constexpr (!requires { Message::descriptor(); }) {
    return true;
  } else {
    const auto* descriptor = Message::descriptor();
    for (std::size_t offset = 0; offset < body.size();) {
      std::uint64_t tag = 0;
      if (!consume_protobuf_varint(body, offset, tag) || tag == 0) {
        return false;
      }
      const std::uint64_t field_number = tag >> 3;
      const std::uint64_t wire_type = tag & 0x7U;
      if (field_number == 0 || field_number > 0x1fffffffU || wire_type == 4) {
        return false;
      }

      if (const auto* field = descriptor->FindFieldByNumber(static_cast<int>(field_number));
          field != nullptr) {
        const auto expected = protobuf_wire_type_for_field_type(static_cast<int>(field->type()));
        if (!expected.has_value()) {
          return false;
        }
        const bool packed = field->is_repeated() && field->is_packable() && wire_type == 2;
        if (!packed && wire_type != expected.value()) {
          return false;
        }
      }

      if (!skip_protobuf_field(body, offset, wire_type, field_number)) {
        return false;
      }
    }
    return true;
  }
}

template <typename Message>
[[nodiscard]] Result<Message> parse(std::span<const std::byte> body,
                                    std::string_view failure_message) {
  if (body.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return Error::protobuf("protobuf message exceeds the parser size limit");
  }
  if (!valid_known_protobuf_wire_types<Message>(body)) {
    return Error::protobuf(std::string(failure_message));
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

template <typename T> [[nodiscard]] const auto& response_message(const T& value) {
  if constexpr (requires {
                  value.message;
                  value.metadata;
                }) {
    return value.message;
  } else {
    return value;
  }
}

template <typename T> [[nodiscard]] Metadata response_metadata(const T& value) {
  if constexpr (requires {
                  value.message;
                  value.metadata;
                }) {
    return value.metadata;
  } else {
    return {};
  }
}

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
  [[nodiscard]] trevrpc_channel* native_handle() const noexcept;

  [[nodiscard]] Result<detail::ByteResponse> call_unary(std::string_view service,
                                                        std::string_view method,
                                                        std::span<const std::byte> body,
                                                        const CallOptions& options);
  [[nodiscard]] Result<detail::ClientStream>
  start_stream(std::string_view service, std::string_view method, std::uint32_t kind,
               std::span<const std::byte> body, const CallOptions& options);

private:
  explicit Channel(std::shared_ptr<detail::ChannelState> state) noexcept
      : state_(std::move(state)) {}
  std::shared_ptr<detail::ChannelState> state_;
};

template <typename T> struct Response {
  T message;
  Metadata metadata;
};

template <typename T> using UnaryResponse = Response<T>;

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
  explicit ClientStreamingCall(detail::ClientStream stream, bool exactly_one_response = true)
      : stream_(std::move(stream)), exactly_one_response_(exactly_one_response) {}
  [[nodiscard]] Result<void> send(const Request& request) {
    auto body = detail::serialize(request);
    if (!body) {
      return body.error();
    }
    return stream_.send(body.value());
  }
  [[nodiscard]] Result<void> finish_send() { return stream_.finish_send(); }
  [[nodiscard]] Result<trevrpc::Response<Response>> finish_and_receive() {
    auto finished = finish_send();
    if (!finished) {
      return finished.error();
    }
    std::optional<Response> response;
    bool multiple_responses = false;
    for (;;) {
      auto event = receive();
      if (!event) {
        return event.error();
      }
      if (event.value().is_message()) {
        if (response.has_value()) {
          multiple_responses = true;
        } else {
          response = std::move(event.value().message());
        }
        continue;
      }
      if (!event.value().status().is_ok()) {
        return Error::rpc(event.value().status());
      }
      if (multiple_responses) {
        return Error::protobuf("client-streaming RPC returned more than one response message");
      }
      if (!response.has_value()) {
        return Error::protobuf("client-streaming RPC did not return exactly one response message");
      }
      return trevrpc::Response<Response>{std::move(response).value(),
                                         event.value().status().metadata()};
    }
  }
  [[nodiscard]] Result<StreamEvent<Response>> receive() {
    auto frame = stream_.receive();
    if (!frame) {
      return frame.error();
    }
    if (frame.value().terminal) {
      if (!frame.value().status.is_ok()) {
        return Error::rpc(std::move(frame.value().status));
      }
      if (exactly_one_response_ && response_count_ != 1) {
        return Error::protobuf("client-streaming RPC did not return exactly one response message");
      }
      return StreamEvent<Response>::terminal(std::move(frame.value().status));
    }
    auto message = detail::parse<Response>(frame.value().body, "failed to parse stream response");
    if (!message) {
      return message.error();
    }
    if (response_count_ != std::numeric_limits<std::size_t>::max()) {
      ++response_count_;
    }
    return StreamEvent<Response>::message(std::move(message).value());
  }
  void cancel() noexcept { stream_.cancel(); }
  void close() noexcept { stream_.close(); }

private:
  detail::ClientStream stream_;
  std::size_t response_count_ = 0;
  bool exactly_one_response_ = true;
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
  return BidirectionalStreamingCall<Request, Response>(std::move(stream).value(), false);
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
  [[nodiscard]] Result<void>
  set_authorizer(std::shared_ptr<Authorizer> callback,
                 std::shared_ptr<CallbackExceptionSink> exception_sink = {});
  [[nodiscard]] Result<void>
  set_metrics(std::shared_ptr<MetricsObserver> callback,
              std::shared_ptr<CallbackExceptionSink> exception_sink = {});
  [[nodiscard]] Result<void> set_logger(std::shared_ptr<Logger> callback,
                                        std::shared_ptr<CallbackExceptionSink> exception_sink = {});
  [[nodiscard]] Result<void>
  set_transport_observer(std::shared_ptr<TransportObserver> callback,
                         std::shared_ptr<CallbackExceptionSink> exception_sink = {});
  [[nodiscard]] Result<void> clear_authorizer();
  [[nodiscard]] Result<void> clear_metrics();
  [[nodiscard]] Result<void> clear_logger();
  [[nodiscard]] Result<void> clear_transport_observer();
  [[nodiscard]] Result<void> serve();
  [[nodiscard]] Result<void> request_stop();
  [[nodiscard]] Result<ShutdownReport> shutdown(const ShutdownOptions& options);
  [[deprecated("use request_stop() or shutdown(ShutdownOptions)")]] void shutdown() noexcept;
  [[deprecated("use shutdown(ShutdownOptions)")]] void close() noexcept;
  [[nodiscard]] trevrpc_server* native_handle() const noexcept;

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
          auto body = detail::serialize(detail::response_message(response.value()));
          if (!body) {
            respond(call, Status::internal(body.error().message()), {});
            return;
          }
          respond(call, Status(StatusCode::Ok, {}, detail::response_metadata(response.value())),
                  body.value());
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
    return register_route(
        service, method, TREVRPC_RPC_KIND_CLIENT_STREAMING,
        [handler = std::move(handler)](trevrpc_call* call) mutable {
          const trevrpc_request* request = trevrpc_call_request(call);
          CallContext context(trevrpc_call_get_context(call), request);
          ServerReader<Request> reader(trevrpc_call_stream(call));
          auto response = handler(context, reader);
          if (!response) {
            finish(call, detail::error_status(response.error()));
            return;
          }
          auto body = detail::serialize(detail::response_message(response.value()));
          if (!body) {
            finish(call, Status::internal(body.error().message()));
            return;
          }
          auto sent = detail::send_server_message(trevrpc_call_stream(call), body.value());
          finish(call, sent
                           ? Status(StatusCode::Ok, {}, detail::response_metadata(response.value()))
                           : detail::error_status(sent.error()));
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
  friend struct detail::AsyncRegistrationAccess;
  using Handler = std::function<void(trevrpc_call*)>;
  struct Route;

  explicit Server(std::shared_ptr<detail::ServerState> state) noexcept;
  [[nodiscard]] Result<void> register_route(std::string_view service, std::string_view method,
                                            std::uint32_t kind, Handler handler);
  [[nodiscard]] Result<void>
  register_native_route(std::string_view service, std::string_view method, std::uint32_t kind,
                        trevrpc_call_handler callback, const std::shared_ptr<void>& route,
                        void* user_data,
                        const std::shared_ptr<detail::AsyncServerScopeControl>& async_scope = {});
  static int dispatch_route(void* user_data, trevrpc_call* call) noexcept;
  static void respond(trevrpc_call* call, const Status& status,
                      std::span<const std::byte> body) noexcept;
  static void finish(trevrpc_call* call, const Status& status) noexcept;

  std::shared_ptr<detail::ServerState> state_;
};

} // namespace trevrpc
