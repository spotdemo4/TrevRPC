#pragma once

#include <trevrpc/trevrpc.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

namespace trevrpc::detail {

struct AuthorizerCallbackState;
struct MetricsCallbackState;
struct LoggerCallbackState;
class RpcServerCore;

class AsyncServerScopeControl {
public:
  virtual ~AsyncServerScopeControl() = default;
  virtual void cancel() noexcept = 0;
  [[nodiscard]] virtual Result<void>
  drain_until(std::chrono::steady_clock::time_point deadline) noexcept = 0;
};

class ServerState final {
public:
  explicit ServerState(std::shared_ptr<RpcServerCore> rpc_core) noexcept
      : rpc_core_(std::move(rpc_core)) {}
  ~ServerState();
  ServerState(const ServerState&) = delete;
  ServerState& operator=(const ServerState&) = delete;

  [[nodiscard]] Result<std::uint16_t> port() const;
  [[nodiscard]] Result<ServerPhase> phase() const;
  [[nodiscard]] Result<void> freeze();
  [[nodiscard]] Result<void> request_stop();
  [[nodiscard]] Result<ShutdownReport> shutdown(const ShutdownOptions& options);
  void cancel_for_abandonment() noexcept;
  [[nodiscard]] Result<void>
  register_rpc_route(std::string_view service, std::string_view method, std::uint32_t kind,
                     std::function<void(std::shared_ptr<ServerCallState>)> callback,
                     const std::shared_ptr<void>& route,
                     const std::shared_ptr<AsyncServerScopeControl>& async_scope = {});
  [[nodiscard]] Result<void> set_authorizer(std::shared_ptr<AuthorizerCallbackState> callback);
  [[nodiscard]] Result<void> set_metrics(std::shared_ptr<MetricsCallbackState> callback);
  [[nodiscard]] Result<void> set_logger(std::shared_ptr<LoggerCallbackState> callback);
  [[nodiscard]] Result<void> clear_authorizer();
  [[nodiscard]] Result<void> clear_metrics();
  [[nodiscard]] Result<void> clear_logger();

private:
  mutable std::mutex mutex_;
  std::mutex shutdown_mutex_;
  std::shared_ptr<AuthorizerCallbackState> authorizer_;
  std::shared_ptr<MetricsCallbackState> metrics_;
  std::shared_ptr<LoggerCallbackState> logger_;
  std::optional<ShutdownReport> final_report_;
  std::shared_ptr<RpcServerCore> rpc_core_;
  bool rpc_frozen_ = false;
  bool stop_requested_ = false;
};

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
