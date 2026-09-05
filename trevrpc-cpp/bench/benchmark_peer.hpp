#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace trevrpc_bench {

constexpr std::uint32_t kSchemaVersion = 5;
constexpr auto kConnectTimeout = std::chrono::seconds(10);
constexpr std::size_t kMaximumPayloadBytes = std::size_t{64} * 1024 * 1024;
constexpr std::uint32_t kMaximumMessagesPerStream = 1'000'000;
constexpr std::size_t kMaximumFrameSize = kMaximumPayloadBytes + 1024;
constexpr std::uint16_t kPeerStreamLimit = 1024;

class PeerError final : public std::runtime_error {
public:
  PeerError(std::string phase, std::string code, std::string_view message);

  [[nodiscard]] const std::string& phase() const noexcept;
  [[nodiscard]] const std::string& code() const noexcept;

private:
  std::string phase_;
  std::string code_;
};

struct Endpoint {
  std::string host;
  std::uint16_t port = 0;

  [[nodiscard]] std::string address(std::uint16_t actual_port) const;
};

enum class Stack { TrevrpcNativeQuic, TrevrpcHttp3, TrevrpcWebTransport };
enum class RpcKind { Unary, ClientStream, ServerStream, Bidi };

struct ServerConfig {
  Stack stack = Stack::TrevrpcNativeQuic;
  Endpoint endpoint;
  std::string certificate;
  std::string private_key;
  std::string webtransport_origin;
};

struct ClientConfig {
  Stack stack = Stack::TrevrpcNativeQuic;
  Endpoint endpoint;
  std::string certificate;
  RpcKind rpc_kind = RpcKind::Unary;
  std::size_t concurrency = 0;
  std::uint64_t warmup_ms = 0;
  std::uint64_t measurement_ms = 0;
  std::size_t request_bytes = 0;
  std::uint32_t response_bytes = 0;
  std::uint32_t messages_per_stream = 0;
};

[[nodiscard]] std::string make_payload(std::size_t size, std::uint64_t sequence);
[[nodiscard]] bool valid_payload(std::string_view payload, std::size_t size,
                                 std::uint64_t sequence);

class BenchmarkClient {
public:
  virtual ~BenchmarkClient() = default;
  [[nodiscard]] virtual std::optional<std::string> run(const ClientConfig& config,
                                                       std::uint64_t sequence) = 0;
};

class ClientFactory {
public:
  virtual ~ClientFactory() = default;
  [[nodiscard]] virtual std::unique_ptr<BenchmarkClient> create() = 0;
  virtual void close() = 0;
};

class BenchmarkServer {
public:
  virtual ~BenchmarkServer() = default;
  [[nodiscard]] virtual std::uint16_t port() const = 0;
  [[nodiscard]] virtual bool stopped() const = 0;
  virtual void shutdown() = 0;
  [[nodiscard]] virtual std::optional<std::string> finish_error() = 0;
};

} // namespace trevrpc_bench
