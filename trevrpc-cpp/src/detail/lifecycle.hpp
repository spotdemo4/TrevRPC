#pragma once

#include <trevrpc/trevrpc.hpp>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace trevrpc::detail {

struct AuthorizerCallbackState;
struct MetricsCallbackState;
struct LoggerCallbackState;
struct TransportCallbackState;
struct WebTransportAdmissionState;
struct Http3AdmissionState;
struct ChannelLifecycleCallbackState;

class AsyncServerScopeControl {
public:
  virtual ~AsyncServerScopeControl() = default;
  virtual void cancel() noexcept = 0;
  [[nodiscard]] virtual Result<void>
  drain_until(std::chrono::steady_clock::time_point deadline) noexcept = 0;
};

class ChannelState final : public std::enable_shared_from_this<ChannelState> {
public:
  class Admission final {
  public:
    Admission() = default;
    explicit Admission(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}
    ~Admission();
    Admission(const Admission&) = delete;
    Admission& operator=(const Admission&) = delete;
    Admission(Admission&& other) noexcept = default;
    Admission& operator=(Admission&& other) noexcept;

    [[nodiscard]] trevrpc_channel* native_handle() const noexcept;

  private:
    std::shared_ptr<ChannelState> state_;
  };

  explicit ChannelState(
      trevrpc_channel* channel,
      std::shared_ptr<ChannelLifecycleCallbackState> lifecycle_callback = {}) noexcept
      : channel_(channel), lifecycle_callback_(std::move(lifecycle_callback)) {}
  ~ChannelState();
  ChannelState(const ChannelState&) = delete;
  ChannelState& operator=(const ChannelState&) = delete;

  [[nodiscard]] Result<Admission> admit();
  void close() noexcept;
  [[nodiscard]] trevrpc_channel* native_handle() const noexcept;

private:
  friend class Admission;
  void leave() noexcept;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  trevrpc_channel* channel_ = nullptr;
  bool closing_ = false;
  bool released_ = false;
  bool release_deferred_ = false;
  std::size_t entered_ = 0;
  std::shared_ptr<ChannelLifecycleCallbackState> lifecycle_callback_;
};

class ServerState final : public std::enable_shared_from_this<ServerState> {
public:
  class Admission final {
  public:
    Admission() = default;
    explicit Admission(std::shared_ptr<ServerState> state) noexcept : state_(std::move(state)) {}
    ~Admission();
    Admission(const Admission&) = delete;
    Admission& operator=(const Admission&) = delete;
    Admission(Admission&& other) noexcept = default;
    Admission& operator=(Admission&& other) noexcept;

    [[nodiscard]] trevrpc_server* native_handle() const noexcept;

  private:
    std::shared_ptr<ServerState> state_;
  };

  explicit ServerState(trevrpc_server* server,
                       std::shared_ptr<WebTransportAdmissionState> webtransport_admission = {},
                       std::shared_ptr<Http3AdmissionState> http3_admission = {}) noexcept
      : server_(server), webtransport_admission_(std::move(webtransport_admission)),
        http3_admission_(std::move(http3_admission)) {}
  ~ServerState();
  ServerState(const ServerState&) = delete;
  ServerState& operator=(const ServerState&) = delete;

  [[nodiscard]] trevrpc_server* native_handle() const noexcept;
  [[nodiscard]] Result<Admission> admit();
  [[nodiscard]] Result<ServerPhase> phase() const;
  [[nodiscard]] Result<void> request_stop();
  [[nodiscard]] Result<ShutdownReport> shutdown(const ShutdownOptions& options);
  void cancel_for_abandonment() noexcept;
  [[nodiscard]] Result<void>
  register_route(std::string_view service, std::string_view method, std::uint32_t kind,
                 trevrpc_call_handler callback, const std::shared_ptr<void>& route, void* user_data,
                 const std::shared_ptr<AsyncServerScopeControl>& async_scope = {});
  [[nodiscard]] Result<void> set_authorizer(std::shared_ptr<AuthorizerCallbackState> callback);
  [[nodiscard]] Result<void> set_metrics(std::shared_ptr<MetricsCallbackState> callback);
  [[nodiscard]] Result<void> set_logger(std::shared_ptr<LoggerCallbackState> callback);
  [[nodiscard]] Result<void>
  set_transport_observer(std::shared_ptr<TransportCallbackState> callback);
  [[nodiscard]] Result<void> clear_authorizer();
  [[nodiscard]] Result<void> clear_metrics();
  [[nodiscard]] Result<void> clear_logger();
  [[nodiscard]] Result<void> clear_transport_observer();

private:
  friend class Admission;
  void leave() noexcept;
  [[nodiscard]] Result<std::uint64_t>
  graceful_deadline(const std::optional<std::chrono::nanoseconds>& timeout) const;
  [[nodiscard]] static Result<std::uint64_t> deadline_after(std::chrono::nanoseconds timeout);
  [[nodiscard]] Result<ShutdownReport> release(ShutdownOutcome outcome);
  void cancel_async_scopes() noexcept;
  [[nodiscard]] Result<void>
  drain_async_scopes_until(std::chrono::steady_clock::time_point deadline) noexcept;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::mutex shutdown_mutex_;
  trevrpc_server* server_ = nullptr;
  std::size_t entered_ = 0;
  bool releasing_ = false;
  bool cancellation_requested_ = false;
  std::vector<std::shared_ptr<void>> routes_;
  std::vector<std::shared_ptr<AsyncServerScopeControl>> async_scopes_;
  std::shared_ptr<WebTransportAdmissionState> webtransport_admission_;
  std::shared_ptr<Http3AdmissionState> http3_admission_;
  std::shared_ptr<AuthorizerCallbackState> authorizer_;
  std::shared_ptr<MetricsCallbackState> metrics_;
  std::shared_ptr<LoggerCallbackState> logger_;
  std::shared_ptr<TransportCallbackState> transport_observer_;
  std::optional<ShutdownReport> final_report_;
};

void abandon_channel(std::shared_ptr<ChannelState> state) noexcept;
[[nodiscard]] bool
drain_lifecycle_reaper_until(std::chrono::steady_clock::time_point deadline) noexcept;
void abandon_server(std::shared_ptr<ServerState> state) noexcept;

class ServerCallbackContextGuard final {
public:
  ServerCallbackContextGuard() noexcept;
  ~ServerCallbackContextGuard();
  ServerCallbackContextGuard(const ServerCallbackContextGuard&) = delete;
  ServerCallbackContextGuard& operator=(const ServerCallbackContextGuard&) = delete;

private:
  bool previous_;
};

class ChannelCallbackContextGuard final {
public:
  ChannelCallbackContextGuard() noexcept;
  ~ChannelCallbackContextGuard();
  ChannelCallbackContextGuard(const ChannelCallbackContextGuard&) = delete;
  ChannelCallbackContextGuard& operator=(const ChannelCallbackContextGuard&) = delete;

private:
  bool previous_;
};

class ExecutorContextGuard final {
public:
  explicit ExecutorContextGuard(const void* executor) noexcept;
  ~ExecutorContextGuard();
  ExecutorContextGuard(const ExecutorContextGuard&) = delete;
  ExecutorContextGuard& operator=(const ExecutorContextGuard&) = delete;

private:
  const void* previous_;
};

[[nodiscard]] bool running_in_server_callback() noexcept;
[[nodiscard]] bool running_in_channel_callback() noexcept;
[[nodiscard]] bool running_in_executor_context() noexcept;
[[nodiscard]] bool running_in_executor_context(const void* executor) noexcept;

} // namespace trevrpc::detail
