#include "rpc_server_core.hpp"

#include "callbacks.hpp"
#include "lifecycle.hpp"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace trevrpc::detail {
namespace {

void callback_exception(const CallbackExceptionState& state, std::string_view name,
                        const std::exception_ptr& exception) noexcept {
  if (!state.sink) {
    return;
  }
  try {
    state.sink->callback_exception(name, exception);
  } catch (...) {
    (void)std::current_exception();
  }
}

Deadline deadline_after(std::chrono::nanoseconds timeout) noexcept {
  if (timeout == std::chrono::nanoseconds::max()) {
    return Deadline::max();
  }
  const auto now = Deadline::clock::now();
  const auto requested = std::chrono::duration_cast<Deadline::duration>(timeout);
  if (requested >= Deadline::max() - now) {
    return Deadline::max();
  }
  return now + requested;
}

void cancel_server_scopes(
    const std::shared_ptr<ServerScope>& scope,
    const std::vector<std::shared_ptr<AsyncServerScopeControl>>& child_scopes) noexcept {
  scope->request_stop(ServerStopReason::ServerCancellation);
  for (const auto& child_scope : child_scopes) {
    child_scope->cancel();
  }
}

Result<void>
drain_server_work(const std::shared_ptr<ThreadPoolExecutor>& executor,
                  const std::shared_ptr<ServerScope>& scope,
                  const std::vector<std::shared_ptr<AsyncServerScopeControl>>& child_scopes,
                  Deadline deadline) noexcept {
  auto executor_drained = executor->drain_until(deadline);
  if (!executor_drained) {
    return executor_drained.error();
  }
  auto scope_drained = scope->drain_until(deadline);
  if (!scope_drained) {
    return scope_drained.error();
  }
  for (const auto& child_scope : child_scopes) {
    auto child_drained = child_scope->drain_until(deadline);
    if (!child_drained) {
      return child_drained.error();
    }
  }
  return {};
}

void log_event(const std::shared_ptr<LoggerCallbackState>& state, std::uint32_t level,
               std::string_view event, std::string_view message, const RpcIncomingCall& incoming,
               int error_code) noexcept {
  if (!state || !state->callback) {
    return;
  }
  try {
    ServerCallbackContextGuard guard;
    state->callback->log(LogEvent{level, std::string(event), std::string(message), incoming.service,
                                  incoming.method, error_code});
  } catch (...) {
    callback_exception(*state, "logger", std::current_exception());
  }
}

} // namespace

struct RpcServerCore::State final {
  struct RouteKey {
    std::string service;
    std::string method;
    std::uint32_t kind = 0;
    bool operator==(const RouteKey&) const noexcept = default;
  };
  struct RouteHash {
    std::size_t operator()(const RouteKey& key) const noexcept {
      std::size_t value = std::hash<std::string>{}(key.service);
      value ^= std::hash<std::string>{}(key.method) + 0x9e3779b9U + (value << 6U) + (value >> 2U);
      value ^= std::hash<std::uint32_t>{}(key.kind) + 0x9e3779b9U + (value << 6U) + (value >> 2U);
      return value;
    }
  };
  struct RouteEntry {
    Handler handler;
    std::shared_ptr<void> lifetime;
    std::shared_ptr<AsyncServerScopeControl> async_scope;
  };

  std::shared_ptr<RpcEventRuntime> runtime;
  trevrpc_rpc_endpoint_v1 endpoint{};
  std::shared_ptr<ThreadPoolExecutor> executor;
  std::shared_ptr<AsyncRuntime> async_runtime;
  std::shared_ptr<ServerScope> scope = std::make_shared<ServerScope>();
  mutable std::mutex mutex;
  std::mutex stop_mutex;
  std::mutex shutdown_mutex;
  std::condition_variable condition;
  std::unordered_map<RouteKey, RouteEntry, RouteHash> routes;
  std::shared_ptr<AuthorizerCallbackState> authorizer;
  std::shared_ptr<MetricsCallbackState> metrics;
  std::shared_ptr<LoggerCallbackState> logger;
  std::thread dispatcher;
  bool started = false;
  bool stopping = false;
  bool endpoint_released = false;
  bool joined = false;
  bool cancellation_started = false;
  std::optional<std::uint64_t> deferred_endpoint_close_operation;
  bool deferred_endpoint_close_submitted = false;
  std::optional<ShutdownReport> final_report;
};

RpcServerCore::RpcServerCore(std::shared_ptr<RpcEventRuntime> runtime,
                             trevrpc_rpc_endpoint_v1 endpoint,
                             std::shared_ptr<ThreadPoolExecutor> executor,
                             std::shared_ptr<AsyncRuntime> async_runtime)
    : state_(std::make_shared<State>()) {
  state_->runtime = std::move(runtime);
  state_->endpoint = endpoint;
  state_->executor = std::move(executor);
  state_->async_runtime = std::move(async_runtime);
}

RpcServerCore::~RpcServerCore() { cancel_for_abandonment(); }

bool RpcServerCore::valid_kind(std::uint32_t kind) noexcept {
  return kind == TREVRPC_RPC_KIND_UNARY || kind == TREVRPC_RPC_KIND_SERVER_STREAMING ||
         kind == TREVRPC_RPC_KIND_CLIENT_STREAMING ||
         kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING;
}

Result<std::shared_ptr<RpcServerCore>>
RpcServerCore::create(std::shared_ptr<RpcEventRuntime> runtime, trevrpc_rpc_endpoint_v1 endpoint,
                      std::size_t worker_count, std::size_t queue_capacity) {
  if (!runtime || endpoint.owner == 0 || worker_count == 0 || queue_capacity == 0) {
    return Error::runtime(-EINVAL, "invalid ABI1 server core configuration");
  }
  auto executor = ThreadPoolExecutor::create({worker_count, queue_capacity});
  if (!executor) {
    return executor.error();
  }
  std::shared_ptr<Executor> continuation;
  try {
    continuation = make_inline_executor();
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate server continuation executor");
  }
  auto async_runtime = AsyncRuntime::create(std::move(continuation));
  if (!async_runtime) {
    return async_runtime.error();
  }
  try {
    auto core = std::shared_ptr<RpcServerCore>(new RpcServerCore(std::move(runtime), endpoint,
                                                                 std::move(executor).value(),
                                                                 std::move(async_runtime).value()));
    return core;
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate ABI1 server core");
  }
}

Result<void> RpcServerCore::start() {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  if (state->started) {
    return {};
  }
  if (state->stopping || state->joined) {
    return Error::runtime(-ESHUTDOWN, "server is stopping");
  }
  auto self = shared_from_this();
  try {
    state->dispatcher = std::thread([self] { self->dispatch_loop(); });
    state->started = true;
    return {};
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to start ABI1 server dispatcher");
  }
}

Result<void> RpcServerCore::register_route(std::string_view service, std::string_view method,
                                           std::uint32_t kind, Handler handler,
                                           std::shared_ptr<void> lifetime,
                                           std::shared_ptr<AsyncServerScopeControl> async_scope) {
  if (service.empty() || method.empty() || !valid_kind(kind) || !handler || !lifetime) {
    return Error::runtime(-EINVAL, "invalid ABI1 server route");
  }
  std::lock_guard lock(state_->mutex);
  if (state_->stopping) {
    return Error::runtime(-ESHUTDOWN, "server is stopping");
  }
  try {
    State::RouteKey key{std::string(service), std::string(method), kind};
    if (state_->routes.contains(key)) {
      return Error::runtime(-EEXIST, "server route is already registered");
    }
    state_->routes.emplace(
        std::move(key),
        State::RouteEntry{std::move(handler), std::move(lifetime), std::move(async_scope)});
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to retain ABI1 server route");
  }
  return {};
}

void RpcServerCore::set_authorizer(std::shared_ptr<AuthorizerCallbackState> callback) noexcept {
  std::lock_guard lock(state_->mutex);
  state_->authorizer = std::move(callback);
}

void RpcServerCore::set_metrics(std::shared_ptr<MetricsCallbackState> callback) noexcept {
  std::lock_guard lock(state_->mutex);
  state_->metrics = std::move(callback);
}

void RpcServerCore::set_logger(std::shared_ptr<LoggerCallbackState> callback) noexcept {
  std::lock_guard lock(state_->mutex);
  state_->logger = std::move(callback);
}

RpcServerCore::Handler RpcServerCore::find_handler(const RpcIncomingCall& incoming) const {
  std::lock_guard lock(state_->mutex);
  auto found =
      state_->routes.find(State::RouteKey{incoming.service, incoming.method, incoming.rpc_kind});
  return found == state_->routes.end() ? Handler{} : found->second.handler;
}

void RpcServerCore::dispatch_loop() noexcept {
  for (;;) {
    auto incoming = state_->runtime->wait_incoming();
    if (!incoming) {
      std::lock_guard lock(state_->mutex);
      state_->stopping = true;
      return;
    }
    auto self = shared_from_this();
    auto state = state_;
    auto reservation = state->executor->try_reserve();
    if (!reservation) {
      const int admission_error = reservation.error().code();
      auto rejected = state->runtime->reject_incoming(incoming.value());
      if (admission_error == -EAGAIN && rejected) {
        // Queue pressure is transient. Keep the dispatcher alive so later
        // incoming calls can be admitted once a worker drains the queue.
        std::this_thread::yield();
        continue;
      }
      std::lock_guard lock(state->mutex);
      state->stopping = true;
      return;
    }
    std::shared_ptr<RpcIncomingCall> pending;
    try {
      // Allocate the holder before transferring native handles. If allocation
      // fails, the Result still owns the untouched incoming call.
      pending = std::make_shared<RpcIncomingCall>();
    } catch (...) {
      (void)state->runtime->reject_incoming(incoming.value());
      std::lock_guard lock(state->mutex);
      state->stopping = true;
      return;
    }
    *pending = std::move(incoming).value();
    auto submitted = std::move(reservation).value().commit([self, pending]() mutable {
      self->dispatch_one(std::move(*pending));
      pending->call = {};
      pending->stream = {};
    });
    if (!submitted) {
      (void)state->runtime->reject_incoming(*pending);
      std::lock_guard lock(state->mutex);
      state->stopping = true;
      return;
    }
  }
}

void RpcServerCore::dispatch_one(RpcIncomingCall incoming) noexcept {
  Handler handler;
  try {
    handler = find_handler(incoming);
  } catch (...) {
    RpcIncomingCall rejected;
    rejected.call = incoming.call;
    rejected.stream = incoming.stream;
    (void)state_->runtime->reject_incoming(rejected);
    return;
  }
  bool accepted = false;
  auto call = ServerCallState::create_rpc(std::move(incoming), state_->runtime,
                                          state_->async_runtime, &accepted);
  if (!call) {
    return;
  }
  auto call_state = std::move(call).value();
  auto scope_id = state_->scope->add(call_state);
  if (!scope_id) {
    call_state->stop(ServerStopReason::ServerCancellation);
    return;
  }

  std::shared_ptr<AuthorizerCallbackState> authorizer;
  std::shared_ptr<MetricsCallbackState> metrics;
  std::shared_ptr<LoggerCallbackState> logger;
  {
    std::lock_guard lock(state_->mutex);
    authorizer = state_->authorizer;
    metrics = state_->metrics;
    logger = state_->logger;
  }
  Work cleanup_observer;
  try {
    cleanup_observer = [weak_scope = std::weak_ptr<ServerScope>(state_->scope),
                        scope_id = scope_id.value()]() noexcept {
      if (auto scope = weak_scope.lock()) {
        scope->complete(scope_id);
      }
    };
  } catch (...) {
    call_state->stop(ServerStopReason::ServerCancellation);
    state_->scope->complete(scope_id.value());
    return;
  }
  call_state->set_cleanup_observer(std::move(cleanup_observer));

  const auto* request = call_state->incoming();
  if (request == nullptr) {
    call_state->stop(ServerStopReason::ServerCancellation);
    return;
  }
  const auto started_at = std::chrono::steady_clock::now();
  ServerCallTerminalObserver terminal_observer;
  try {
    terminal_observer = [metrics, service = request->service, method = request->method,
                         request_body_size = request->initial_message.size(),
                         started_at](ServerCallTerminal terminal) noexcept {
      if (metrics && metrics->callback) {
        try {
          const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - started_at);
          ServerCallbackContextGuard guard;
          metrics->callback->rpc_finished(RpcFinishedEvent{service, method, request_body_size,
                                                           terminal.response_body_size,
                                                           terminal.status, elapsed});
        } catch (...) {
          callback_exception(*metrics, "metrics.rpc_finished", std::current_exception());
        }
      }
    };
  } catch (...) {
    call_state->stop(ServerStopReason::ServerCancellation);
    return;
  }
  if (metrics && metrics->callback) {
    try {
      ServerCallbackContextGuard guard;
      metrics->callback->rpc_started(
          RpcStartedEvent{request->service, request->method, request->initial_message.size()});
    } catch (...) {
      callback_exception(*metrics, "metrics.rpc_started", std::current_exception());
    }
  }
  call_state->set_terminal_observer(std::move(terminal_observer));

  auto send_status = [&](const Status& status) noexcept {
    try {
      if (request->rpc_kind == TREVRPC_RPC_KIND_UNARY ||
          request->rpc_kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
        (void)sync_wait(call_state->respond({}, status));
      } else {
        (void)sync_wait(call_state->finish(status));
      }
    } catch (...) {
      call_state->stop(ServerStopReason::LocalClose);
    }
  };

  if (authorizer && authorizer->callback) {
    Status authorization;
    try {
      ServerCallbackContextGuard guard;
      authorization = authorizer->callback->authorize(
          server_call_context(call_state),
          AuthorizationRequest{request->service, request->method, request->metadata,
                               request->initial_message.size(), request->rpc_kind});
    } catch (...) {
      callback_exception(*authorizer, "authorizer", std::current_exception());
      authorization = Status::internal("authorizer callback threw");
    }
    if (!authorization.is_ok()) {
      log_event(logger, 2, "rpc.authorization_denied", authorization.message(), *request,
                static_cast<int>(authorization.code()));
      send_status(authorization);
      return;
    }
  }

  if (!handler) {
    const Status status = Status(StatusCode::Unimplemented, "RPC route is not registered");
    log_event(logger, 2, "rpc.route_not_found", status.message(), *request,
              static_cast<int>(status.code()));
    send_status(status);
  } else {
    try {
      handler(call_state);
    } catch (...) {
      const Status status = Status::internal("service handler threw");
      log_event(logger, 3, "rpc.handler_failed", status.message(), *request,
                static_cast<int>(status.code()));
      send_status(status);
    }
  }
}

Result<std::uint16_t> RpcServerCore::port() {
  std::uint16_t port = 0;
  const int error =
      trevrpc_rpc_endpoint_get_port_v1(state_->runtime->native_handle(), state_->endpoint, &port);
  return error == 0 ? Result<std::uint16_t>(port) : Result<std::uint16_t>(Error::runtime(error));
}

Result<void> RpcServerCore::close_endpoint() {
  std::uint64_t operation_id = 0;
  bool close_submitted = false;
  {
    std::lock_guard lock(state_->mutex);
    if (state_->deferred_endpoint_close_operation) {
      operation_id = *state_->deferred_endpoint_close_operation;
      close_submitted = state_->deferred_endpoint_close_submitted;
    }
  }
  if (operation_id != 0 && state_->runtime->stopped()) {
    state_->runtime->reject_operation(operation_id);
    {
      std::lock_guard lock(state_->mutex);
      state_->deferred_endpoint_close_operation.reset();
      state_->deferred_endpoint_close_submitted = false;
    }
    state_->runtime->unregister_endpoint(state_->endpoint);
    return {};
  }
  if (operation_id == 0) {
    auto operation = state_->runtime->reserve_operation();
    if (!operation) {
      if (state_->runtime->stopped()) {
        std::optional<std::uint64_t> deferred;
        {
          std::lock_guard lock(state_->mutex);
          deferred = state_->deferred_endpoint_close_operation;
          state_->deferred_endpoint_close_operation.reset();
          state_->deferred_endpoint_close_submitted = false;
        }
        if (deferred) {
          state_->runtime->reject_operation(*deferred);
        }
        state_->runtime->unregister_endpoint(state_->endpoint);
        return {};
      }
      return operation.error();
    }
    operation_id = operation.value();
  }

  auto retain_operation = [&](bool submitted) {
    std::lock_guard lock(state_->mutex);
    state_->deferred_endpoint_close_operation = operation_id;
    state_->deferred_endpoint_close_submitted = submitted;
  };
  auto forget_operation = [&] {
    std::lock_guard lock(state_->mutex);
    state_->deferred_endpoint_close_operation.reset();
    state_->deferred_endpoint_close_submitted = false;
  };

  if (!close_submitted) {
    auto native = state_->runtime->native_handle();
    const int close_error = trevrpc_rpc_endpoint_close(static_cast<trevrpc_rpc_runtime*>(native),
                                                       state_->endpoint, operation_id);
    if (close_error == -EDEADLK) {
      retain_operation(false);
      return Error::runtime(close_error, "server endpoint close cannot run here");
    }
    if (close_error == -EALREADY) {
      state_->runtime->reject_operation(operation_id);
    } else if (close_error != 0) {
      state_->runtime->reject_operation(operation_id);
      forget_operation();
      return Error::runtime(close_error, "failed to close ABI1 server endpoint");
    } else {
      close_submitted = true;
    }
  }

  if (close_submitted) {
    auto closed = state_->runtime->wait_operation(operation_id);
    if (!closed) {
      if (closed.error().code() == -EDEADLK) {
        retain_operation(true);
      } else {
        forget_operation();
      }
      return closed.error();
    }
    if (closed.value().status != 0) {
      forget_operation();
      return Error::runtime(closed.value().status, "ABI1 server endpoint close failed");
    }
  }

  auto release_native = state_->runtime->native_handle();
  const int release_error = trevrpc_rpc_endpoint_release(
      static_cast<trevrpc_rpc_runtime*>(release_native), state_->endpoint);
  if (release_error != 0 && release_error != -ESTALE) {
    forget_operation();
    return Error::runtime(release_error, "failed to release ABI1 server endpoint");
  }
  forget_operation();
  state_->runtime->unregister_endpoint(state_->endpoint);
  return {};
}

Result<void> RpcServerCore::request_stop() {
  std::lock_guard stop_lock(state_->stop_mutex);
  bool endpoint_released = false;
  {
    std::lock_guard lock(state_->mutex);
    state_->stopping = true;
    endpoint_released = state_->endpoint_released;
  }
  if (!endpoint_released) {
    auto endpoint = close_endpoint();
    if (!endpoint) {
      return endpoint.error();
    }
    std::lock_guard lock(state_->mutex);
    state_->endpoint_released = true;
  }
  state_->runtime->stop_incoming();
  return {};
}

Result<ShutdownReport> RpcServerCore::shutdown(const ShutdownOptions& options) {
  if ((options.graceful_timeout && options.graceful_timeout->count() < 0) ||
      options.cancellation_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "shutdown durations must not be negative");
  }
  std::unique_lock shutdown_lock(state_->shutdown_mutex);
  if (state_->final_report) {
    return *state_->final_report;
  }
  auto stopped = request_stop();
  if (!stopped) {
    return stopped.error();
  }
  bool join_dispatcher = false;
  {
    std::lock_guard lock(state_->mutex);
    if (!state_->joined) {
      state_->joined = true;
      join_dispatcher = true;
    }
  }
  if (join_dispatcher && state_->dispatcher.joinable()) {
    state_->dispatcher.join();
  }
  state_->executor->request_stop();

  std::vector<std::shared_ptr<AsyncServerScopeControl>> child_scopes;
  try {
    std::lock_guard lock(state_->mutex);
    child_scopes.reserve(state_->routes.size());
    for (const auto& [key, route] : state_->routes) {
      (void)key;
      if (!route.async_scope || route.async_scope.get() == state_->scope.get()) {
        continue;
      }
      const auto duplicate =
          std::find_if(child_scopes.begin(), child_scopes.end(), [&](const auto& existing) {
            return existing.get() == route.async_scope.get();
          });
      if (duplicate == child_scopes.end()) {
        child_scopes.push_back(route.async_scope);
      }
    }
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to snapshot ABI1 server route scopes");
  }

  const auto graceful_timeout = options.graceful_timeout.value_or(std::chrono::seconds(10));
  auto drained = drain_server_work(state_->executor, state_->scope, child_scopes,
                                   deadline_after(graceful_timeout));
  if (!drained && drained.error().code() != -ETIMEDOUT) {
    return drained.error();
  }

  if (!drained) {
    cancel_server_scopes(state_->scope, child_scopes);
    state_->cancellation_started = true;
    auto cancelled = drain_server_work(state_->executor, state_->scope, child_scopes,
                                       deadline_after(options.cancellation_timeout));
    if (!cancelled) {
      if (cancelled.error().code() != -ETIMEDOUT) {
        return cancelled.error();
      }
      return ShutdownReport{ShutdownOutcome::TimedOut, ServerPhase::Cancelling, false};
    }
  }

  auto runtime = state_->runtime->shutdown();
  if (!runtime) {
    return runtime.error();
  }
  state_->final_report = ShutdownReport{state_->cancellation_started ? ShutdownOutcome::Cancelled
                                                                     : ShutdownOutcome::Graceful,
                                        ServerPhase::Released, true};
  return *state_->final_report;
}

void RpcServerCore::cancel_for_abandonment() noexcept {
  if (!state_) {
    return;
  }
  (void)request_stop();
  state_->executor->request_stop();
  state_->scope->request_stop(ServerStopReason::ServerCancellation);
  try {
    std::vector<std::shared_ptr<AsyncServerScopeControl>> child_scopes;
    {
      std::lock_guard lock(state_->mutex);
      child_scopes.reserve(state_->routes.size());
      for (const auto& [key, route] : state_->routes) {
        (void)key;
        if (route.async_scope) {
          child_scopes.push_back(route.async_scope);
        }
      }
    }
    for (const auto& child_scope : child_scopes) {
      child_scope->cancel();
    }
  } catch (...) {
    (void)std::current_exception();
  }
  state_->runtime->request_abandon();
  if (state_->dispatcher.joinable()) {
    state_->dispatcher.detach();
  }
}

std::size_t RpcServerCore::active() const noexcept { return state_ ? state_->scope->active() : 0; }

} // namespace trevrpc::detail
