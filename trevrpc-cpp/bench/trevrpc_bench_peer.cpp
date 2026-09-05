#include "benchmark.trevrpc.hpp"
#include "benchmark_peer.hpp"

#include <trevrpc/trevrpc.hpp>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace benchmark = trevrpc::benchmark::v1;

namespace {

using namespace trevrpc_bench;

using Clock = std::chrono::steady_clock;
using Nanoseconds = std::chrono::nanoseconds;

constexpr std::size_t kMaximumControlLine = 1024;
constexpr std::string_view kHttp3Path = "/trevrpc";
constexpr std::string_view kWebTransportPath = "/trevrpc";

volatile std::sig_atomic_t stop_requested = 0;

void handle_signal(int) { stop_requested = 1; }

[[nodiscard]] std::string json_string(std::string_view value) {
  constexpr char hexadecimal[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(value.size() + 2);
  encoded.push_back('"');
  for (const unsigned char character : value) {
    switch (character) {
    case '"':
      encoded += "\\\"";
      break;
    case '\\':
      encoded += "\\\\";
      break;
    case '\b':
      encoded += "\\b";
      break;
    case '\f':
      encoded += "\\f";
      break;
    case '\n':
      encoded += "\\n";
      break;
    case '\r':
      encoded += "\\r";
      break;
    case '\t':
      encoded += "\\t";
      break;
    default:
      if (character < 0x20) {
        encoded += "\\u00";
        encoded.push_back(hexadecimal[character >> 4]);
        encoded.push_back(hexadecimal[character & 0x0f]);
      } else {
        encoded.push_back(static_cast<char>(character));
      }
      break;
    }
  }
  encoded.push_back('"');
  return encoded;
}

void emit(std::string_view event) {
  std::cout << event << '\n';
  std::cout.flush();
}

void emit_error(std::string_view phase, std::string_view code, std::string_view message) {
  emit("{\"schema_version\":" + std::to_string(kSchemaVersion) +
       ",\"event\":\"error\",\"peer\":\"cpp\",\"phase\":" + json_string(phase) +
       ",\"code\":" + json_string(code) + ",\"message\":" + json_string(message) + '}');
}

void emit_stopped() {
  emit("{\"schema_version\":" + std::to_string(kSchemaVersion) +
       ",\"event\":\"stopped\",\"peer\":\"cpp\"}");
}

[[nodiscard]] std::string describe(const trevrpc::Error& error) {
  if (error.status().has_value()) {
    return "RPC status " + std::to_string(static_cast<std::uint32_t>(error.status()->code())) +
           ": " + error.message();
  }
  return error.message().empty() ? "TrevRPC error " + std::to_string(error.code())
                                 : error.message() + " (code " + std::to_string(error.code()) + ')';
}

[[nodiscard]] std::uint64_t parse_unsigned(std::string_view name, std::string_view value) {
  std::uint64_t parsed = 0;
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw PeerError("config", "invalid_argument",
                    std::string(name) + " must be an unsigned decimal integer");
  }
  return parsed;
}

[[nodiscard]] Endpoint parse_endpoint(std::string_view name, std::string_view value,
                                      bool allow_zero_port) {
  std::string_view host;
  std::string_view port_text;
  if (value.starts_with('[')) {
    const std::size_t closing = value.find(']');
    if (closing == std::string_view::npos || closing + 1 >= value.size() ||
        value[closing + 1] != ':') {
      throw PeerError("config", "invalid_argument",
                      std::string(name) + " must use [HOST]:PORT for IPv6");
    }
    host = value.substr(1, closing - 1);
    port_text = value.substr(closing + 2);
  } else {
    const std::size_t colon = value.rfind(':');
    if (colon == std::string_view::npos || value.find(':') != colon) {
      throw PeerError("config", "invalid_argument", std::string(name) + " must be HOST:PORT");
    }
    host = value.substr(0, colon);
    port_text = value.substr(colon + 1);
  }
  const std::uint64_t port = parse_unsigned(name, port_text);
  if (host.empty() || port > std::numeric_limits<std::uint16_t>::max() ||
      (!allow_zero_port && port == 0)) {
    throw PeerError("config", "invalid_argument",
                    std::string(name) + " has an invalid host or port");
  }
  return Endpoint{std::string(host), static_cast<std::uint16_t>(port)};
}

using Arguments = std::map<std::string, std::string, std::less<>>;

[[nodiscard]] Arguments parse_arguments(int argc, char** argv) {
  Arguments arguments;
  for (int index = 2; index < argc; index += 2) {
    const std::string_view name(argv[index]);
    if (!name.starts_with("--") || name.size() == 2 || index + 1 >= argc) {
      throw PeerError("config", "invalid_argument", "options must be supplied as --name value");
    }
    const auto [unused, inserted] = arguments.emplace(std::string(name.substr(2)), argv[index + 1]);
    if (!inserted) {
      throw PeerError("config", "invalid_argument", "duplicate option: " + std::string(name));
    }
  }
  return arguments;
}

[[nodiscard]] std::string take_required(Arguments& arguments, std::string_view name) {
  const auto found = arguments.find(name);
  if (found == arguments.end()) {
    throw PeerError("config", "invalid_argument", "missing required option --" + std::string(name));
  }
  std::string value = std::move(found->second);
  arguments.erase(found);
  return value;
}

void reject_unknown(const Arguments& arguments) {
  if (!arguments.empty()) {
    throw PeerError("config", "invalid_argument", "unknown option --" + arguments.begin()->first);
  }
}

[[nodiscard]] Stack parse_stack(std::string_view value) {
  if (value == "trevrpc_native_quic") {
    return Stack::TrevrpcNativeQuic;
  }
  if (value == "trevrpc_http3") {
    return Stack::TrevrpcHttp3;
  }
  if (value == "trevrpc_webtransport") {
    return Stack::TrevrpcWebTransport;
  }
  throw PeerError("config", "invalid_argument",
                  "--stack must be trevrpc_native_quic, trevrpc_http3, or trevrpc_webtransport");
}

[[nodiscard]] RpcKind parse_rpc_kind(std::string_view value) {
  if (value == "unary") {
    return RpcKind::Unary;
  }
  if (value == "client_stream") {
    return RpcKind::ClientStream;
  }
  if (value == "server_stream") {
    return RpcKind::ServerStream;
  }
  if (value == "bidi") {
    return RpcKind::Bidi;
  }
  throw PeerError("config", "invalid_argument",
                  "--rpc must be unary, client_stream, server_stream, or bidi");
}

[[nodiscard]] std::string_view rpc_kind_name(RpcKind kind) {
  switch (kind) {
  case RpcKind::Unary:
    return "unary";
  case RpcKind::ClientStream:
    return "client_stream";
  case RpcKind::ServerStream:
    return "server_stream";
  case RpcKind::Bidi:
    return "bidi";
  }
  return "unknown";
}

[[nodiscard]] ServerConfig parse_server_config(int argc, char** argv) {
  Arguments arguments = parse_arguments(argc, argv);
  ServerConfig config;
  config.stack = parse_stack(take_required(arguments, "stack"));
  config.endpoint = parse_endpoint("--listen", take_required(arguments, "listen"), true);
  config.certificate = take_required(arguments, "cert");
  config.private_key = take_required(arguments, "key");
  if (config.stack == Stack::TrevrpcWebTransport) {
    config.webtransport_origin = take_required(arguments, "webtransport-origin");
    if (config.webtransport_origin.empty()) {
      throw PeerError("config", "invalid_argument", "--webtransport-origin must not be empty");
    }
  } else if (arguments.contains("webtransport-origin")) {
    throw PeerError("config", "invalid_argument",
                    "--webtransport-origin is only valid for a trevrpc_webtransport server");
  }
  reject_unknown(arguments);
  if (config.certificate.empty() || config.private_key.empty()) {
    throw PeerError("config", "invalid_argument", "--cert and --key must not be empty");
  }
  return config;
}

[[nodiscard]] ClientConfig parse_client_config(int argc, char** argv) {
  Arguments arguments = parse_arguments(argc, argv);
  ClientConfig config;
  config.stack = parse_stack(take_required(arguments, "stack"));
  if (config.stack == Stack::TrevrpcHttp3) {
    throw PeerError("config", "invalid_argument", "trevrpc_http3 is server-only");
  }
  if (config.stack == Stack::TrevrpcWebTransport) {
    throw PeerError("config", "invalid_argument", "trevrpc_webtransport is server-only");
  }
  config.endpoint = parse_endpoint("--address", take_required(arguments, "address"), false);
  config.certificate = take_required(arguments, "cert");
  config.rpc_kind = parse_rpc_kind(take_required(arguments, "rpc"));

  const std::uint64_t concurrency =
      parse_unsigned("--concurrency", take_required(arguments, "concurrency"));
  config.warmup_ms = parse_unsigned("--warmup-ms", take_required(arguments, "warmup-ms"));
  config.measurement_ms =
      parse_unsigned("--measurement-ms", take_required(arguments, "measurement-ms"));
  const std::uint64_t request_bytes =
      parse_unsigned("--request-bytes", take_required(arguments, "request-bytes"));
  const std::uint64_t response_bytes =
      parse_unsigned("--response-bytes", take_required(arguments, "response-bytes"));
  const std::uint64_t messages =
      parse_unsigned("--messages-per-stream", take_required(arguments, "messages-per-stream"));
  reject_unknown(arguments);

  if (config.certificate.empty()) {
    throw PeerError("config", "invalid_argument", "--cert must not be empty");
  }
  if (concurrency == 0 || concurrency > std::numeric_limits<std::size_t>::max() ||
      concurrency > kPeerStreamLimit) {
    throw PeerError("config", "invalid_argument", "--concurrency is out of range");
  }
  if (config.warmup_ms > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
      config.measurement_ms >
          static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
      config.measurement_ms > std::numeric_limits<std::uint64_t>::max() / 1'000'000) {
    throw PeerError("config", "invalid_argument", "benchmark duration is out of range");
  }
  if (request_bytes > kMaximumPayloadBytes || response_bytes > kMaximumPayloadBytes) {
    throw PeerError("config", "invalid_argument", "payload size exceeds the benchmark peer limit");
  }
  if (messages == 0 || messages > kMaximumMessagesPerStream) {
    throw PeerError("config", "invalid_argument", "--messages-per-stream is out of range");
  }
  config.concurrency = static_cast<std::size_t>(concurrency);
  config.request_bytes = static_cast<std::size_t>(request_bytes);
  config.response_bytes = static_cast<std::uint32_t>(response_bytes);
  config.messages_per_stream = static_cast<std::uint32_t>(messages);
  return config;
}

class BenchmarkService final : public benchmark::BenchmarkServiceService {
public:
  trevrpc::Result<trevrpc::Response<benchmark::BenchmarkResponse>>
  Unary(const trevrpc::CallContext&, const benchmark::BenchmarkRequest& request) override {
    if (request.payload().size() > kMaximumPayloadBytes) {
      return trevrpc::Status::invalid_argument("request payload exceeds the benchmark peer limit");
    }
    if (request.response_bytes() > kMaximumPayloadBytes) {
      return trevrpc::Status::invalid_argument("response_bytes exceeds the benchmark peer limit");
    }
    benchmark::BenchmarkResponse response;
    response.set_sequence(request.sequence());
    response.set_payload(make_payload(request.response_bytes(), request.sequence()));
    return trevrpc::Response<benchmark::BenchmarkResponse>{std::move(response), {}};
  }

  trevrpc::Result<trevrpc::Response<benchmark::BenchmarkSummary>>
  ClientStream(const trevrpc::CallContext&,
               trevrpc::ServerReader<benchmark::BenchmarkRequest>& reader) override {
    std::uint64_t message_count = 0;
    std::uint64_t payload_bytes = 0;
    for (;;) {
      auto request = reader.receive();
      if (!request) {
        return request.error();
      }
      if (!request.value().has_value()) {
        break;
      }
      const std::size_t size = request.value()->payload().size();
      if (size > kMaximumPayloadBytes) {
        return trevrpc::Status::invalid_argument(
            "request payload exceeds the benchmark peer limit");
      }
      if (message_count >= kMaximumMessagesPerStream) {
        return trevrpc::Status(trevrpc::StatusCode::ResourceExhausted,
                               "message count exceeds the benchmark peer limit");
      }
      if (size > std::numeric_limits<std::uint64_t>::max() - payload_bytes) {
        return trevrpc::Status(trevrpc::StatusCode::OutOfRange, "stream summary overflow");
      }
      ++message_count;
      payload_bytes += static_cast<std::uint64_t>(size);
    }
    benchmark::BenchmarkSummary summary;
    summary.set_message_count(message_count);
    summary.set_payload_bytes(payload_bytes);
    return trevrpc::Response<benchmark::BenchmarkSummary>{std::move(summary), {}};
  }

  trevrpc::Status
  ServerStream(const trevrpc::CallContext&, const benchmark::StreamRequest& request,
               trevrpc::ServerWriter<benchmark::BenchmarkResponse>& writer) override {
    if (request.payload().size() > kMaximumPayloadBytes || request.message_count() == 0 ||
        request.message_count() > kMaximumMessagesPerStream ||
        request.response_bytes() > kMaximumPayloadBytes) {
      return trevrpc::Status::invalid_argument(
          "stream response settings exceed the benchmark peer limits");
    }
    for (std::uint32_t index = 0; index < request.message_count(); ++index) {
      benchmark::BenchmarkResponse response;
      response.set_sequence(index);
      response.set_payload(make_payload(request.response_bytes(), index));
      auto sent = writer.send(response);
      if (!sent) {
        return trevrpc::Status::internal(describe(sent.error()));
      }
    }
    return trevrpc::Status::ok();
  }

  trevrpc::Status Bidi(const trevrpc::CallContext&,
                       trevrpc::ServerReaderWriter<benchmark::BenchmarkRequest,
                                                   benchmark::BenchmarkResponse>& stream) override {
    std::uint32_t message_count = 0;
    for (;;) {
      auto request = stream.receive();
      if (!request) {
        return trevrpc::Status::internal(describe(request.error()));
      }
      if (!request.value().has_value()) {
        return trevrpc::Status::ok();
      }
      if (message_count >= kMaximumMessagesPerStream) {
        return trevrpc::Status(trevrpc::StatusCode::ResourceExhausted,
                               "message count exceeds the benchmark peer limit");
      }
      if (request.value()->payload().size() > kMaximumPayloadBytes) {
        return trevrpc::Status::invalid_argument(
            "request payload exceeds the benchmark peer limit");
      }
      ++message_count;
      if (request.value()->response_bytes() > kMaximumPayloadBytes) {
        return trevrpc::Status::invalid_argument("response_bytes exceeds the benchmark peer limit");
      }
      benchmark::BenchmarkResponse response;
      response.set_sequence(request.value()->sequence());
      response.set_payload(
          make_payload(request.value()->response_bytes(), request.value()->sequence()));
      auto sent = stream.send(response);
      if (!sent) {
        return trevrpc::Status::internal(describe(sent.error()));
      }
    }
  }
};

enum class ControlCommand { Start, Shutdown, EndOfInput, Interrupted, ServeEnded };

[[nodiscard]] ControlCommand wait_for_control(BenchmarkServer* server = nullptr) {
  std::string pending;
  for (;;) {
    if (stop_requested != 0) {
      return ControlCommand::Interrupted;
    }
    if (server != nullptr && server->stopped()) {
      return ControlCommand::ServeEnded;
    }

    pollfd input{STDIN_FILENO, POLLIN, 0};
    const int polled = poll(&input, 1, 100);
    if (polled < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw PeerError("control", "stdin_failed", "failed to poll standard input");
    }
    if (polled == 0) {
      continue;
    }
    if ((input.revents & (POLLERR | POLLNVAL)) != 0) {
      throw PeerError("control", "stdin_failed", "standard input reported an error");
    }
    if ((input.revents & (POLLIN | POLLHUP)) == 0) {
      continue;
    }

    char buffer[256];
    // poll established readability; no benchmark mutex is held here.
    // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection)
    const ssize_t received = read(STDIN_FILENO, buffer, sizeof(buffer));
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw PeerError("control", "stdin_failed", "failed to read standard input");
    }
    if (received == 0) {
      if (pending.empty()) {
        return ControlCommand::EndOfInput;
      }
      pending.push_back('\n');
    } else {
      pending.append(buffer, static_cast<std::size_t>(received));
    }
    if (pending.size() > kMaximumControlLine) {
      throw PeerError("control", "invalid_command", "standard input command is too long");
    }

    const std::size_t newline = pending.find('\n');
    if (newline == std::string::npos) {
      continue;
    }
    std::string_view command(pending.data(), newline);
    if (command.ends_with('\r')) {
      command.remove_suffix(1);
    }
    if (command == "START") {
      return ControlCommand::Start;
    }
    if (command == "SHUTDOWN") {
      return ControlCommand::Shutdown;
    }
    throw PeerError("control", "invalid_command", "expected START or SHUTDOWN");
  }
}

class BenchmarkServerImpl final : public BenchmarkServer {
public:
  BenchmarkServerImpl(trevrpc::Server server, std::uint16_t port)
      : state_(std::make_shared<State>(std::move(server))), port_(port),
        serve_thread_([state = state_] {
          state->serve_result = state->server.serve();
          state->stopped.store(true, std::memory_order_release);
        }) {}

  ~BenchmarkServerImpl() override {
    shutdown();
    if (serve_thread_.joinable()) {
      serve_thread_.detach();
    }
  }

  [[nodiscard]] std::uint16_t port() const override { return port_; }

  [[nodiscard]] bool stopped() const override {
    return state_->stopped.load(std::memory_order_acquire);
  }

  void shutdown() override {
    if (!serve_thread_.joinable()) {
      return;
    }
    trevrpc::ShutdownOptions options;
    options.graceful_timeout = std::chrono::seconds(10);
    options.cancellation_timeout = std::chrono::seconds(10);
    auto report = state_->server.shutdown(options);
    if (!report) {
      shutdown_error_ = describe(report.error());
      return;
    }
    if (!report.value().released) {
      shutdown_error_ = "native server shutdown timed out before release";
      return;
    }
    shutdown_error_.reset();
    serve_thread_.join();
  }

  [[nodiscard]] std::optional<std::string> finish_error() override {
    if (shutdown_error_) {
      return shutdown_error_;
    }
    if (!state_->serve_result) {
      return describe(state_->serve_result.error());
    }
    return std::nullopt;
  }

private:
  struct State {
    explicit State(trevrpc::Server value) : server(std::move(value)) {}

    trevrpc::Server server;
    trevrpc::Result<void> serve_result;
    std::atomic<bool> stopped{false};
  };

  std::shared_ptr<State> state_;
  std::uint16_t port_;
  std::optional<std::string> shutdown_error_;
  std::thread serve_thread_;
};

[[nodiscard]] std::unique_ptr<BenchmarkServer> start_server(const ServerConfig& peer_config) {
  trevrpc::ServerConfig config;
  config.host = peer_config.endpoint.host;
  config.port = peer_config.endpoint.port;
  config.cert_file = peer_config.certificate;
  config.key_file = peer_config.private_key;
  config.enable_native = peer_config.stack == Stack::TrevrpcNativeQuic;
  config.enable_webtransport = peer_config.stack == Stack::TrevrpcWebTransport;
  if (peer_config.stack == Stack::TrevrpcWebTransport) {
    config.webtransport_path = kWebTransportPath;
    config.webtransport_origin = peer_config.webtransport_origin;
  } else {
    config.enable_webtransport = false;
  }
  if (peer_config.stack == Stack::TrevrpcHttp3) {
    config.enable_http3 = true;
    config.http3_path = kHttp3Path;
  }
  config.peer_bidi_stream_count = kPeerStreamLimit;
  config.max_frame_size = kMaximumFrameSize;
  auto listening = trevrpc::Server::listen(config);
  if (!listening) {
    throw PeerError("setup", "listen_failed", describe(listening.error()));
  }
  trevrpc::Server server = std::move(listening).value();

  trevrpc::ServerOptions options;
  options.worker_queue_capacity = std::numeric_limits<std::int64_t>::max();
  options.initial_request_timeout = kConnectTimeout;
  options.max_stream_messages = kMaximumMessagesPerStream;
  options.max_stream_body_size = std::numeric_limits<std::int64_t>::max();
  auto configured = server.set_options(options);
  if (!configured) {
    throw PeerError("setup", "server_config_failed", describe(configured.error()));
  }
  auto registered =
      benchmark::RegisterBenchmarkService(server, std::make_shared<BenchmarkService>());
  if (!registered) {
    throw PeerError("setup", "registration_failed", describe(registered.error()));
  }
  auto port = server.port();
  if (!port) {
    throw PeerError("setup", "listen_failed", describe(port.error()));
  }

  return std::make_unique<BenchmarkServerImpl>(std::move(server), port.value());
}

int run_server(int argc, char** argv) {
  const ServerConfig config = parse_server_config(argc, argv);
  std::unique_ptr<BenchmarkServer> server = start_server(config);

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  if (server->stopped()) {
    const std::optional<std::string> error = server->finish_error();
    throw PeerError("serve", "serve_failed", error.value_or("server stopped during startup"));
  }

  emit("{\"schema_version\":" + std::to_string(kSchemaVersion) +
       ",\"event\":\"ready\",\"peer\":\"cpp\",\"address\":" +
       json_string(config.endpoint.address(server->port())) +
       ",\"pid\":" + std::to_string(getpid()) + '}');

  ControlCommand command = ControlCommand::EndOfInput;
  try {
    command = wait_for_control(server.get());
  } catch (...) {
    server->shutdown();
    throw;
  }
  server->shutdown();
  if (const std::optional<std::string> error = server->finish_error(); error.has_value()) {
    throw PeerError("serve", "serve_failed", *error);
  }
  if (command == ControlCommand::ServeEnded) {
    throw PeerError("serve", "serve_stopped", "server stopped before SHUTDOWN");
  }
  if (command == ControlCommand::Start) {
    throw PeerError("control", "invalid_command", "server expected SHUTDOWN");
  }
  if (command == ControlCommand::Shutdown || command == ControlCommand::EndOfInput) {
    emit_stopped();
  }
  return 0;
}

[[nodiscard]] std::optional<std::string> expect_terminal(
    trevrpc::ClientStreamingCall<benchmark::BenchmarkRequest, benchmark::BenchmarkSummary>& call) {
  auto event = call.receive();
  if (!event) {
    return describe(event.error());
  }
  if (event.value().is_message()) {
    return "received an extra client-stream response";
  }
  if (!event.value().status().is_ok()) {
    return "terminal RPC status: " + event.value().status().message();
  }
  return std::nullopt;
}

template <typename Call>
[[nodiscard]] std::optional<std::string> expect_stream_terminal(Call& call) {
  auto event = call.receive();
  if (!event) {
    return describe(event.error());
  }
  if (event.value().is_message()) {
    return "received more responses than expected";
  }
  if (!event.value().status().is_ok()) {
    return "terminal RPC status: " + event.value().status().message();
  }
  return std::nullopt;
}

[[nodiscard]] trevrpc::CallOptions benchmark_call_options() {
  trevrpc::CallOptions options;
  options.max_response_body_size = std::numeric_limits<std::int64_t>::max();
  options.max_response_messages = std::numeric_limits<std::int64_t>::max();
  options.max_response_stream_body_size = std::numeric_limits<std::int64_t>::max();
  return options;
}

[[nodiscard]] std::optional<std::string> run_unary(benchmark::BenchmarkServiceClient& client,
                                                   const ClientConfig& config,
                                                   std::uint64_t sequence) {
  benchmark::BenchmarkRequest request;
  request.set_sequence(sequence);
  request.set_payload(make_payload(config.request_bytes, sequence));
  request.set_response_bytes(config.response_bytes);
  auto response = client.Unary(request, benchmark_call_options());
  if (!response) {
    return describe(response.error());
  }
  if (response.value().message.sequence() != sequence ||
      !valid_payload(response.value().message.payload(), config.response_bytes, sequence)) {
    return "unary response validation failed";
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string>
run_client_stream(benchmark::BenchmarkServiceClient& client, const ClientConfig& config) {
  auto started = client.ClientStream(benchmark_call_options());
  if (!started) {
    return describe(started.error());
  }
  auto& call = started.value();
  for (std::uint32_t index = 0; index < config.messages_per_stream; ++index) {
    benchmark::BenchmarkRequest request;
    request.set_sequence(index);
    request.set_payload(make_payload(config.request_bytes, index));
    request.set_response_bytes(config.response_bytes);
    auto sent = call.send(request);
    if (!sent) {
      return describe(sent.error());
    }
  }
  auto finished = call.finish_send();
  if (!finished) {
    return describe(finished.error());
  }
  auto response = call.receive();
  if (!response) {
    return describe(response.error());
  }
  if (!response.value().is_message()) {
    return "client stream ended before its summary: " + response.value().status().message();
  }
  const std::uint64_t expected_bytes =
      static_cast<std::uint64_t>(config.request_bytes) * config.messages_per_stream;
  if (response.value().message().message_count() != config.messages_per_stream ||
      response.value().message().payload_bytes() != expected_bytes) {
    return "client-stream summary validation failed";
  }
  return expect_terminal(call);
}

[[nodiscard]] std::optional<std::string>
run_server_stream(benchmark::BenchmarkServiceClient& client, const ClientConfig& config) {
  benchmark::StreamRequest request;
  request.set_message_count(config.messages_per_stream);
  request.set_payload(make_payload(config.request_bytes, 0));
  request.set_response_bytes(config.response_bytes);
  auto started = client.ServerStream(request, benchmark_call_options());
  if (!started) {
    return describe(started.error());
  }
  auto& call = started.value();
  for (std::uint32_t index = 0; index < config.messages_per_stream; ++index) {
    auto event = call.receive();
    if (!event) {
      return describe(event.error());
    }
    if (!event.value().is_message()) {
      return "server stream ended before all responses arrived: " +
             event.value().status().message();
    }
    if (event.value().message().sequence() != index ||
        !valid_payload(event.value().message().payload(), config.response_bytes, index)) {
      return "server-stream response validation failed";
    }
  }
  return expect_stream_terminal(call);
}

[[nodiscard]] std::optional<std::string> run_bidi(benchmark::BenchmarkServiceClient& client,
                                                  const ClientConfig& config) {
  auto started = client.Bidi(benchmark_call_options());
  if (!started) {
    return describe(started.error());
  }
  auto& call = started.value();
  std::string sender_error;
  std::thread sender([&] {
    try {
      for (std::uint32_t index = 0; index < config.messages_per_stream; ++index) {
        benchmark::BenchmarkRequest request;
        request.set_sequence(index);
        request.set_payload(make_payload(config.request_bytes, index));
        request.set_response_bytes(config.response_bytes);
        auto sent = call.send(request);
        if (!sent) {
          sender_error = describe(sent.error());
          call.cancel();
          return;
        }
      }
      auto finished = call.finish_send();
      if (!finished) {
        sender_error = describe(finished.error());
        call.cancel();
      }
    } catch (const std::exception& error) {
      sender_error = error.what();
      call.cancel();
    } catch (...) {
      sender_error = "bidi sender failed";
      call.cancel();
    }
  });

  std::optional<std::string> receive_error;
  for (std::uint32_t index = 0; index < config.messages_per_stream; ++index) {
    auto event = call.receive();
    if (!event) {
      receive_error = describe(event.error());
      break;
    }
    if (!event.value().is_message()) {
      receive_error =
          "bidi stream ended before all responses arrived: " + event.value().status().message();
      break;
    }
    if (event.value().message().sequence() != index ||
        !valid_payload(event.value().message().payload(), config.response_bytes, index)) {
      receive_error = "bidi response validation failed";
      break;
    }
  }
  if (receive_error.has_value()) {
    call.cancel();
  } else {
    receive_error = expect_stream_terminal(call);
    if (receive_error.has_value()) {
      call.cancel();
    }
  }
  sender.join();
  if (!sender_error.empty()) {
    return sender_error;
  }
  return receive_error;
}

[[nodiscard]] std::optional<std::string> run_operation(benchmark::BenchmarkServiceClient& client,
                                                       const ClientConfig& config,
                                                       std::uint64_t sequence) {
  switch (config.rpc_kind) {
  case RpcKind::Unary:
    return run_unary(client, config, sequence);
  case RpcKind::ClientStream:
    return run_client_stream(client, config);
  case RpcKind::ServerStream:
    return run_server_stream(client, config);
  case RpcKind::Bidi:
    return run_bidi(client, config);
  }
  return "unknown RPC kind";
}

class NativeBenchmarkClient final : public BenchmarkClient {
public:
  explicit NativeBenchmarkClient(const std::shared_ptr<trevrpc::Channel>& channel)
      : client_(channel) {}

  [[nodiscard]] std::optional<std::string> run(const ClientConfig& config,
                                               std::uint64_t sequence) override {
    return run_operation(client_, config, sequence);
  }

private:
  benchmark::BenchmarkServiceClient client_;
};

class NativeClientFactory final : public ClientFactory {
public:
  explicit NativeClientFactory(std::shared_ptr<trevrpc::Channel> channel)
      : channel_(std::move(channel)) {}

  ~NativeClientFactory() override { close(); }

  [[nodiscard]] std::unique_ptr<BenchmarkClient> create() override {
    std::lock_guard lock(mutex_);
    if (channel_ == nullptr) {
      throw std::runtime_error("TrevRPC channel is closed");
    }
    return std::make_unique<NativeBenchmarkClient>(channel_);
  }

  void close() override {
    std::lock_guard lock(mutex_);
    if (channel_ != nullptr) {
      channel_->close();
      channel_.reset();
    }
  }

private:
  std::mutex mutex_;
  std::shared_ptr<trevrpc::Channel> channel_;
};

[[nodiscard]] std::shared_ptr<ClientFactory> connect_native_client(const ClientConfig& config) {
  trevrpc::ChannelConfig channel_config;
  channel_config.ca_cert_file = config.certificate;
  channel_config.skip_certificate_validation = false;
  channel_config.max_frame_size = kMaximumFrameSize;
  auto connected = trevrpc::Channel::connect(config.endpoint.host, config.endpoint.port,
                                             channel_config, kConnectTimeout);
  if (!connected) {
    throw PeerError("setup", "connect_failed", describe(connected.error()));
  }
  return std::make_shared<NativeClientFactory>(std::move(connected).value());
}

[[nodiscard]] std::pair<std::uint64_t, std::uint64_t>
messages_per_operation(RpcKind kind, std::uint32_t messages) {
  switch (kind) {
  case RpcKind::Unary:
    return {1, 1};
  case RpcKind::ClientStream:
    return {messages, 1};
  case RpcKind::ServerStream:
    return {1, messages};
  case RpcKind::Bidi:
    return {messages, messages};
  }
  return {0, 0};
}

[[nodiscard]] std::uint64_t histogram_upper_bound(std::uint64_t value) {
  value = std::max<std::uint64_t>(value, 1);
  const unsigned floor_log2 = std::bit_width(value) - 1;
  const unsigned shift = floor_log2 > 9 ? floor_log2 - 9 : 0;
  return (((value >> shift) + 1) << shift) - 1;
}

struct LaneResult {
  std::uint64_t completed = 0;
  std::uint64_t failed = 0;
  std::uint64_t request_messages = 0;
  std::uint64_t response_messages = 0;
  std::map<std::uint64_t, std::uint64_t> histogram;
  std::string error;
};

struct PhaseControl {
  std::mutex mutex;
  std::condition_variable ready_condition;
  std::condition_variable start_condition;
  std::size_t ready = 0;
  bool started = false;
  bool cancelled = false;
  Clock::time_point start;
};

struct PreparedPhase {
  std::shared_ptr<PhaseControl> control;
  std::shared_ptr<std::vector<LaneResult>> lanes;
  std::vector<std::thread> threads;
};

struct PhaseResult {
  std::uint64_t completed = 0;
  std::uint64_t failed = 0;
  std::uint64_t request_messages = 0;
  std::uint64_t response_messages = 0;
  std::map<std::uint64_t, std::uint64_t> histogram;
};

[[nodiscard]] PreparedPhase prepare_phase(const std::shared_ptr<ClientFactory>& factory,
                                          const ClientConfig& config, std::uint64_t duration_ms,
                                          bool record_histogram) {
  PreparedPhase phase;
  phase.control = std::make_shared<PhaseControl>();
  phase.lanes = std::make_shared<std::vector<LaneResult>>(config.concurrency);
  phase.threads.reserve(config.concurrency);
  try {
    for (std::size_t lane_index = 0; lane_index < config.concurrency; ++lane_index) {
      phase.threads.emplace_back([factory, &config, duration_ms, record_histogram, lane_index,
                                  control = phase.control, lanes = phase.lanes] {
        std::unique_ptr<BenchmarkClient> client;
        try {
          client = factory->create();
        } catch (const std::exception& error) {
          LaneResult& lane = (*lanes)[lane_index];
          lane.failed = 1;
          lane.error = error.what();
        } catch (...) {
          LaneResult& lane = (*lanes)[lane_index];
          lane.failed = 1;
          lane.error = "benchmark client creation failed";
        }
        {
          std::unique_lock lock(control->mutex);
          ++control->ready;
          control->ready_condition.notify_one();
          if (client == nullptr) {
            return;
          }
          control->start_condition.wait(lock,
                                        [&] { return control->started || control->cancelled; });
          if (control->cancelled) {
            return;
          }
        }

        LaneResult& lane = (*lanes)[lane_index];
        const auto [request_messages, response_messages] =
            messages_per_operation(config.rpc_kind, config.messages_per_stream);
        const auto deadline = control->start + std::chrono::milliseconds(duration_ms);
        std::uint64_t sequence = 0;
        for (;;) {
          const auto operation_start = Clock::now();
          if (operation_start >= deadline) {
            break;
          }
          try {
            auto error = client->run(config, sequence++);
            if (error.has_value()) {
              lane.failed = 1;
              lane.error = std::move(error).value();
              break;
            }
            ++lane.completed;
            lane.request_messages += request_messages;
            lane.response_messages += response_messages;
            if (record_histogram) {
              const auto elapsed =
                  std::chrono::duration_cast<Nanoseconds>(Clock::now() - operation_start);
              const std::uint64_t latency =
                  elapsed.count() <= 0 ? 1 : static_cast<std::uint64_t>(elapsed.count());
              ++lane.histogram[histogram_upper_bound(latency)];
            }
          } catch (const std::exception& error) {
            lane.failed = 1;
            lane.error = error.what();
            break;
          } catch (...) {
            lane.failed = 1;
            lane.error = "benchmark operation threw an unknown exception";
            break;
          }
        }
      });
    }
  } catch (...) {
    {
      std::lock_guard lock(phase.control->mutex);
      phase.control->cancelled = true;
    }
    phase.control->start_condition.notify_all();
    for (std::thread& thread : phase.threads) {
      thread.join();
    }
    throw;
  }
  return phase;
}

void wait_until_ready(PreparedPhase& phase, std::size_t concurrency) {
  std::unique_lock lock(phase.control->mutex);
  phase.control->ready_condition.wait(lock, [&] { return phase.control->ready == concurrency; });
  lock.unlock();
}

[[nodiscard]] Clock::time_point start_phase(PreparedPhase& phase) {
  Clock::time_point start;
  {
    std::lock_guard lock(phase.control->mutex);
    start = Clock::now();
    phase.control->start = start;
    phase.control->started = true;
  }
  phase.control->start_condition.notify_all();
  return start;
}

void cancel_phase(PreparedPhase& phase) {
  {
    std::lock_guard lock(phase.control->mutex);
    phase.control->cancelled = true;
  }
  phase.control->start_condition.notify_all();
}

[[nodiscard]] PhaseResult finish_phase(PreparedPhase& phase) {
  for (std::thread& thread : phase.threads) {
    thread.join();
  }
  PhaseResult result;
  for (const LaneResult& lane : *phase.lanes) {
    result.completed += lane.completed;
    result.failed += lane.failed;
    result.request_messages += lane.request_messages;
    result.response_messages += lane.response_messages;
    for (const auto& [upper_bound, count] : lane.histogram) {
      result.histogram[upper_bound] += count;
    }
    if (!lane.error.empty()) {
      std::cerr << "benchmark operation failed: " << lane.error << '\n';
    }
  }
  return result;
}

[[nodiscard]] PhaseResult run_warmup(const std::shared_ptr<ClientFactory>& factory,
                                     const ClientConfig& config) {
  PreparedPhase phase = prepare_phase(factory, config, config.warmup_ms, false);
  wait_until_ready(phase, config.concurrency);
  static_cast<void>(start_phase(phase));
  return finish_phase(phase);
}

void emit_sample(const ClientConfig& config, const PhaseResult& result, std::uint64_t elapsed_ns) {
  const std::uint64_t admission_ns = config.measurement_ms * 1'000'000;
  const std::uint64_t drain_ns = elapsed_ns > admission_ns ? elapsed_ns - admission_ns : 0;
  std::string event = "{\"schema_version\":" + std::to_string(kSchemaVersion) +
                      ",\"event\":\"sample\",\"peer\":\"cpp\",\"rpc_kind\":" +
                      json_string(rpc_kind_name(config.rpc_kind)) + ",\"admission_ns\":\"" +
                      std::to_string(admission_ns) + "\",\"elapsed_ns\":\"" +
                      std::to_string(elapsed_ns) + "\",\"drain_ns\":\"" + std::to_string(drain_ns) +
                      "\",\"completed\":\"" + std::to_string(result.completed) +
                      "\",\"failed\":\"" + std::to_string(result.failed) +
                      "\",\"request_messages\":\"" + std::to_string(result.request_messages) +
                      "\",\"response_messages\":\"" + std::to_string(result.response_messages) +
                      "\",\"histogram\":[";
  bool first = true;
  for (const auto& [upper_bound, count] : result.histogram) {
    if (!first) {
      event.push_back(',');
    }
    first = false;
    event += "{\"upper_bound_ns\":\"" + std::to_string(upper_bound) + "\",\"count\":\"" +
             std::to_string(count) + "\"}";
  }
  event += "]}";
  emit(event);
}

int run_client(int argc, char** argv) {
  const ClientConfig config = parse_client_config(argc, argv);
  const std::shared_ptr<ClientFactory> factory = connect_native_client(config);
  std::unique_ptr<BenchmarkClient> validation_client = factory->create();
  auto validation_error = validation_client->run(config, 0);
  if (validation_error.has_value()) {
    throw PeerError("validate", "rpc_failed", std::move(validation_error).value());
  }
  validation_client.reset();

  if (config.warmup_ms > 0) {
    const PhaseResult warmup = run_warmup(factory, config);
    if (warmup.failed != 0) {
      throw PeerError("warmup", "rpc_failed",
                      "warmup recorded " + std::to_string(warmup.failed) + " failed operations");
    }
  }

  PreparedPhase measurement = prepare_phase(factory, config, config.measurement_ms, true);
  wait_until_ready(measurement, config.concurrency);
  emit("{\"schema_version\":" + std::to_string(kSchemaVersion) +
       ",\"event\":\"armed\",\"peer\":\"cpp\",\"pid\":" + std::to_string(getpid()) + '}');

  ControlCommand command;
  try {
    command = wait_for_control();
  } catch (...) {
    cancel_phase(measurement);
    static_cast<void>(finish_phase(measurement));
    throw;
  }
  if (command != ControlCommand::Start) {
    cancel_phase(measurement);
    static_cast<void>(finish_phase(measurement));
    if (command == ControlCommand::Shutdown) {
      emit_stopped();
      return 0;
    }
    if (command == ControlCommand::Interrupted) {
      return 130;
    }
    throw PeerError("control", "stdin_closed", "standard input closed before START");
  }

  const Clock::time_point phase_start = start_phase(measurement);
  const PhaseResult result = finish_phase(measurement);
  const auto elapsed = std::chrono::duration_cast<Nanoseconds>(Clock::now() - phase_start);
  const std::uint64_t elapsed_ns =
      elapsed.count() <= 0 ? 0 : static_cast<std::uint64_t>(elapsed.count());
  if (result.failed != 0) {
    throw PeerError("measure", "rpc_failed",
                    "measurement recorded " + std::to_string(result.failed) +
                        " failed operation(s)");
  }
  emit_sample(config, result, elapsed_ns);
  factory->close();
  return 0;
}

} // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);
  try {
    if (argc < 2) {
      throw PeerError("config", "invalid_argument",
                      "usage: trevrpc-bench-peer-cpp capabilities|server|client [options]");
    }
    const std::string_view command(argv[1]);
    if (command == "capabilities") {
      if (argc != 2) {
        throw PeerError("config", "invalid_argument", "capabilities does not accept options");
      }
      emit("{\"schema_version\":" + std::to_string(kSchemaVersion) +
           ",\"event\":\"capabilities\",\"peer\":\"cpp\","
           "\"roles\":{\"client\":[\"trevrpc_native_quic\"],"
           "\"server\":[\"trevrpc_native_quic\",\"trevrpc_http3\",\"trevrpc_webtransport\"]},"
           "\"rpc_kinds\":[\"unary\",\"client_stream\",\"server_stream\",\"bidi\"],"
           "\"histogram\":\"log_linear_v1\"}");
      return 0;
    }
    if (command == "server") {
      return run_server(argc, argv);
    }
    if (command == "client") {
      return run_client(argc, argv);
    }
    throw PeerError("config", "invalid_argument", "unknown command: " + std::string(command));
  } catch (const PeerError& error) {
    std::cerr << error.what() << '\n';
    emit_error(error.phase(), error.code(), error.what());
    return 1;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    emit_error("internal", "internal_error", error.what());
    return 1;
  } catch (...) {
    std::cerr << "unknown peer failure\n";
    emit_error("internal", "internal_error", "unknown peer failure");
    return 1;
  }
}
