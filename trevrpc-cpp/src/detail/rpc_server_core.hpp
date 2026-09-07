#pragma once

#include "async_core.hpp"
#include "rpc_event_runtime.hpp"

#include <trevrpc_rpc.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>

namespace trevrpc::detail {

struct AuthorizerCallbackState;
struct MetricsCallbackState;
struct LoggerCallbackState;

class RpcServerCore final : public std::enable_shared_from_this<RpcServerCore> {
public:
  using Handler = std::function<void(std::shared_ptr<ServerCallState>)>;

  [[nodiscard]] static Result<std::shared_ptr<RpcServerCore>>
  create(std::shared_ptr<RpcEventRuntime> runtime, trevrpc_rpc_endpoint_v1 endpoint,
         std::size_t worker_count = 1, std::size_t queue_capacity = 64);
  ~RpcServerCore();
  RpcServerCore(const RpcServerCore&) = delete;
  RpcServerCore& operator=(const RpcServerCore&) = delete;

  [[nodiscard]] Result<void>
  register_route(std::string_view service, std::string_view method, std::uint32_t kind,
                 Handler handler, std::shared_ptr<void> lifetime,
                 std::shared_ptr<AsyncServerScopeControl> async_scope = {});
  [[nodiscard]] Result<void> start();
  void set_authorizer(std::shared_ptr<AuthorizerCallbackState> callback) noexcept;
  void set_metrics(std::shared_ptr<MetricsCallbackState> callback) noexcept;
  void set_logger(std::shared_ptr<LoggerCallbackState> callback) noexcept;
  [[nodiscard]] Result<std::uint16_t> port();
  [[nodiscard]] Result<void> request_stop();
  [[nodiscard]] Result<ShutdownReport> shutdown(const ShutdownOptions& options);
  void cancel_for_abandonment() noexcept;
  [[nodiscard]] std::size_t active() const noexcept;

private:
  RpcServerCore(std::shared_ptr<RpcEventRuntime> runtime, trevrpc_rpc_endpoint_v1 endpoint,
                std::shared_ptr<ThreadPoolExecutor> executor,
                std::shared_ptr<AsyncRuntime> async_runtime);
  [[nodiscard]] Result<void> close_endpoint();
  void dispatch_loop() noexcept;
  void dispatch_one(RpcIncomingCall incoming) noexcept;
  [[nodiscard]] Handler find_handler(const RpcIncomingCall& incoming) const;
  [[nodiscard]] static bool valid_kind(std::uint32_t kind) noexcept;

  struct State;
  std::shared_ptr<State> state_;
};

} // namespace trevrpc::detail
