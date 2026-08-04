#include <trevrpc/async.hpp>

#include "detail/async_core.hpp"
#include "detail/lifecycle.hpp"

#include <cerrno>
#include <deque>
#include <thread>
#include <vector>

namespace trevrpc::detail {

class ThreadPoolState final : public std::enable_shared_from_this<ThreadPoolState> {
public:
  explicit ThreadPoolState(ThreadPoolExecutorOptions options) : options_(options) {}
  ~ThreadPoolState() {
    request_stop();
    const std::thread::id current = std::this_thread::get_id();
    for (std::thread& worker : workers_) {
      if (!worker.joinable()) {
        continue;
      }
      if (worker.get_id() == current) {
        worker.detach();
      } else {
        worker.join();
      }
    }
  }

  void start() {
    workers_.reserve(options_.worker_count);
    auto self = shared_from_this();
    try {
      for (std::size_t index = 0; index < options_.worker_count; ++index) {
        workers_.emplace_back([self] { self->worker_loop(); });
      }
    } catch (...) {
      request_stop();
      for (std::thread& worker : workers_) {
        if (worker.joinable()) {
          worker.join();
        }
      }
      throw;
    }
  }

  [[nodiscard]] Result<std::shared_ptr<ExecutorReservationState>> reserve() noexcept;
  [[nodiscard]] Result<void> commit(Work work) noexcept;
  void cancel_reservation() noexcept;

  void request_stop() noexcept {
    std::lock_guard lock(mutex_);
    stopping_ = true;
    condition_.notify_all();
    drained_.notify_all();
  }

  [[nodiscard]] Result<void> drain_until(Deadline deadline) noexcept {
    std::unique_lock lock(mutex_);
    const auto drained = [this] { return queue_.empty() && running_ == 0 && reserved_ == 0; };
    if (deadline == Deadline::max()) {
      drained_.wait(lock, drained);
      return {};
    }
    if (!drained_.wait_until(lock, deadline, drained)) {
      return Error::runtime(-ETIMEDOUT, "executor drain timed out");
    }
    return {};
  }

  [[nodiscard]] ExecutorSnapshot snapshot() const noexcept {
    std::lock_guard lock(mutex_);
    return ExecutorSnapshot{queue_.size(), reserved_,   running_, completed_,
                            rejected_,     exceptions_, stopping_};
  }

private:
  void worker_loop() noexcept {
    ExecutorContextGuard context(this);
    for (;;) {
      Work work;
      {
        std::unique_lock lock(mutex_);
        condition_.wait(lock, [this] { return !queue_.empty() || (stopping_ && reserved_ == 0); });
        if (queue_.empty()) {
          if (stopping_ && reserved_ == 0) {
            return;
          }
          continue;
        }
        work = std::move(queue_.front());
        queue_.pop_front();
        ++running_;
      }
      try {
        work();
      } catch (...) {
        std::lock_guard lock(mutex_);
        ++exceptions_;
      }
      {
        std::lock_guard lock(mutex_);
        --running_;
        ++completed_;
        drained_.notify_all();
        condition_.notify_all();
      }
    }
  }

  friend class ExecutorReservationState;
  ThreadPoolExecutorOptions options_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::condition_variable drained_;
  std::deque<Work> queue_;
  std::vector<std::thread> workers_;
  std::size_t reserved_ = 0;
  std::size_t running_ = 0;
  std::uint64_t completed_ = 0;
  std::uint64_t rejected_ = 0;
  std::uint64_t exceptions_ = 0;
  bool stopping_ = false;
};

class ExecutorReservationState final {
public:
  explicit ExecutorReservationState(std::shared_ptr<ThreadPoolState> pool)
      : pool_(std::move(pool)) {}
  ~ExecutorReservationState() { cancel(); }

  [[nodiscard]] Result<void> commit(Work work) noexcept {
    std::shared_ptr<ThreadPoolState> pool;
    {
      std::lock_guard lock(mutex_);
      if (consumed_) {
        return Error::runtime(-EINVAL, "executor reservation was consumed");
      }
      consumed_ = true;
      pool = std::move(pool_);
    }
    if (!work) {
      pool->cancel_reservation();
      return Error::runtime(-EINVAL, "executor work must not be empty");
    }
    return pool->commit(std::move(work));
  }

  void cancel() noexcept {
    std::shared_ptr<ThreadPoolState> pool;
    {
      std::lock_guard lock(mutex_);
      if (consumed_) {
        return;
      }
      consumed_ = true;
      pool = std::move(pool_);
    }
    if (pool) {
      pool->cancel_reservation();
    }
  }

private:
  std::mutex mutex_;
  bool consumed_ = false;
  std::shared_ptr<ThreadPoolState> pool_;
};

Result<std::shared_ptr<ExecutorReservationState>> ThreadPoolState::reserve() noexcept {
  try {
    std::lock_guard lock(mutex_);
    if (stopping_) {
      ++rejected_;
      return Error::runtime(-ESHUTDOWN, "executor is stopping");
    }
    if (queue_.size() + reserved_ >= options_.queue_capacity) {
      ++rejected_;
      return Error::runtime(-EAGAIN, "executor queue is full");
    }
    ++reserved_;
    return std::make_shared<ExecutorReservationState>(shared_from_this());
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate executor reservation");
  }
}

Result<void> ThreadPoolState::commit(Work work) noexcept {
  std::lock_guard lock(mutex_);
  if (reserved_ == 0) {
    return Error::runtime(-EINVAL, "executor reservation accounting underflow");
  }
  try {
    queue_.push_back(std::move(work));
  } catch (...) {
    --reserved_;
    condition_.notify_all();
    drained_.notify_all();
    return Error::runtime(-ENOMEM, "failed to commit executor work");
  }
  --reserved_;
  condition_.notify_one();
  drained_.notify_all();
  return {};
}

void ThreadPoolState::cancel_reservation() noexcept {
  std::lock_guard lock(mutex_);
  if (reserved_ > 0) {
    --reserved_;
  }
  condition_.notify_all();
  drained_.notify_all();
}

} // namespace trevrpc::detail

namespace trevrpc {

ExecutorReservation::~ExecutorReservation() { cancel(); }

Result<void> ExecutorReservation::commit(Work work) && noexcept {
  auto state = std::move(state_);
  if (!state) {
    return Error::runtime(-EINVAL, "executor reservation is empty");
  }
  return state->commit(std::move(work));
}

void ExecutorReservation::cancel() noexcept {
  auto state = std::move(state_);
  if (state) {
    state->cancel();
  }
}

ThreadPoolExecutor::~ThreadPoolExecutor() {
  request_stop();
  if (state_ && !detail::running_in_executor_context(state_.get())) {
    (void)state_->drain_until(Deadline::max());
  }
}

Result<std::shared_ptr<ThreadPoolExecutor>>
ThreadPoolExecutor::create(const ThreadPoolExecutorOptions& options) {
  if (options.worker_count == 0 || options.queue_capacity == 0) {
    return Error::runtime(-EINVAL, "executor worker count and queue capacity must be positive");
  }
  try {
    auto state = std::make_shared<detail::ThreadPoolState>(options);
    state->start();
    return std::shared_ptr<ThreadPoolExecutor>(new ThreadPoolExecutor(std::move(state)));
  } catch (const std::system_error& error) {
    return Error::runtime(-error.code().value(), error.what());
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to create executor workers");
  }
}

Result<ExecutorReservation> ThreadPoolExecutor::try_reserve() noexcept {
  if (!state_) {
    return Error::runtime(-ESHUTDOWN, "executor is closed");
  }
  auto reservation = state_->reserve();
  if (!reservation) {
    return reservation.error();
  }
  return ExecutorReservation(std::move(reservation).value());
}

Result<void> ThreadPoolExecutor::execute(Work work) noexcept {
  auto reservation = try_reserve();
  if (!reservation) {
    return reservation.error();
  }
  return std::move(reservation).value().commit(std::move(work));
}

void ThreadPoolExecutor::request_stop() noexcept {
  if (state_) {
    state_->request_stop();
  }
}

Result<void> ThreadPoolExecutor::drain_until(Deadline deadline) noexcept {
  return state_ ? state_->drain_until(deadline)
                : Result<void>{Error::runtime(-ESHUTDOWN, "executor is closed")};
}

bool ThreadPoolExecutor::running_in_this_executor() const noexcept {
  return state_ && detail::running_in_executor_context(state_.get());
}

ExecutorSnapshot ThreadPoolExecutor::snapshot() const noexcept {
  return state_ ? state_->snapshot() : ExecutorSnapshot{};
}

AsyncRuntime::~AsyncRuntime() = default;

Result<std::shared_ptr<AsyncRuntime>>
AsyncRuntime::create(std::shared_ptr<Executor> continuation_executor,
                     const AsyncRuntimeOptions& options) {
  if (!continuation_executor || options.native_io_worker_count == 0 ||
      options.native_io_queue_capacity == 0 || options.max_pending_sends_per_stream == 0 ||
      options.max_pending_send_bytes_per_stream == 0 ||
      options.max_waiting_senders_per_stream == 0 || options.receive_poll_min.count() <= 0 ||
      options.receive_poll_max < options.receive_poll_min) {
    return Error::runtime(-EINVAL, "invalid asynchronous runtime options");
  }
  auto native = ThreadPoolExecutor::create(
      {options.native_io_worker_count, options.native_io_queue_capacity});
  if (!native) {
    return native.error();
  }
  try {
    auto state = std::make_shared<detail::AsyncRuntimeState>(std::move(continuation_executor),
                                                             std::move(native).value(), options);
    return std::shared_ptr<AsyncRuntime>(new AsyncRuntime(std::move(state)));
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to create asynchronous runtime");
  }
}

std::shared_ptr<Executor> AsyncRuntime::continuation_executor() const noexcept {
  return state_ ? state_->continuation() : nullptr;
}

const AsyncRuntimeOptions& AsyncRuntime::options() const noexcept {
  static const AsyncRuntimeOptions empty{};
  return state_ ? state_->options() : empty;
}

CancellationSource::CancellationSource() : state_(std::make_shared<detail::CancellationState>()) {}

CancellationToken CancellationSource::token() const noexcept { return CancellationToken(state_); }

void CancellationSource::cancel() noexcept {
  if (state_) {
    state_->cancel();
  }
}

bool CancellationToken::cancelled() const noexcept { return state_ && state_->cancelled(); }

namespace detail {

Result<OwnedAsyncCallOptions> own_async_call_options(const AsyncCallOptions& options) {
  if (options.call_options.timeout.count() < 0 ||
      options.call_options.response_idle_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "async call durations must not be negative");
  }
  try {
    OwnedAsyncCallOptions owned;
    owned.call_options = options.call_options;
    owned.cancellation = options.cancellation;
    owned.deadline = options.deadline;
    if (options.call_options.cancellation != nullptr) {
      owned.retained_cancellation =
          std::make_shared<Cancellation>(*options.call_options.cancellation);
      owned.call_options.cancellation = owned.retained_cancellation.get();
    }
    if (options.call_options.timeout.count() != 0) {
      const auto now = Deadline::clock::now();
      const auto relative =
          std::chrono::duration_cast<Deadline::duration>(options.call_options.timeout);
      const Deadline timeout_deadline =
          relative >= Deadline::max() - now ? Deadline::max() : now + relative;
      if (!owned.deadline || timeout_deadline < *owned.deadline) {
        owned.deadline = timeout_deadline;
      }
    }
    return owned;
  } catch (const std::system_error& error) {
    return Error::runtime(-error.code().value(), error.what());
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to own async call options");
  }
}

Task<Result<ByteResponse>> async_unary_bytes(std::shared_ptr<Channel> channel,
                                             std::shared_ptr<AsyncRuntime> runtime,
                                             std::string service, std::string method,
                                             std::vector<std::byte> request,
                                             OwnedAsyncCallOptions options) {
  co_return co_await OperationState::run_unary(std::move(channel), std::move(runtime),
                                               std::move(service), std::move(method),
                                               std::move(request), std::move(options));
}

Task<Result<std::shared_ptr<OperationState>>>
async_start_stream_bytes(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
                         std::string service, std::string method, std::uint32_t kind,
                         std::vector<std::byte> request, OwnedAsyncCallOptions options) {
  co_return co_await OperationState::start_stream(std::move(channel), std::move(runtime),
                                                  std::move(service), std::move(method), kind,
                                                  std::move(request), std::move(options));
}

Task<Result<void>> operation_send(const std::shared_ptr<OperationState>& operation,
                                  std::size_t encoded_size,
                                  std::function<Result<std::vector<std::byte>>()> serializer,
                                  SendOptions options) {
  if (!operation) {
    co_return Error::runtime(-EINVAL, "async stream is empty");
  }
  co_return co_await operation->send(encoded_size, std::move(serializer), options, false);
}

Task<Result<void>> operation_finish_send(const std::shared_ptr<OperationState>& operation,
                                         SendOptions options) {
  if (!operation) {
    co_return Error::runtime(-EINVAL, "async stream is empty");
  }
  co_return co_await operation->send(
      0, [] { return Result<std::vector<std::byte>>(std::vector<std::byte>{}); }, options, true);
}

Task<Result<StreamFrame>> operation_receive(const std::shared_ptr<OperationState>& operation) {
  if (!operation) {
    co_return Error::runtime(-EINVAL, "async stream is empty");
  }
  co_return co_await operation->receive();
}

void operation_cancel(const std::shared_ptr<OperationState>& operation) noexcept {
  if (operation) {
    operation->cancel();
  }
}

void operation_close(const std::shared_ptr<OperationState>& operation) noexcept {
  if (operation) {
    operation->close();
  }
}

CallContext AsyncRegistrationAccess::context(const trevrpc_call_context* context,
                                             const trevrpc_request* request) {
  return CallContext(context, request);
}

Result<void> AsyncRegistrationAccess::register_route(
    Server& server, std::string_view service, std::string_view method, std::uint32_t kind,
    trevrpc_call_handler callback, const std::shared_ptr<void>& route, void* user_data,
    const std::shared_ptr<AsyncServerScopeControl>& async_scope) {
  return server.register_native_route(service, method, kind, callback, route, user_data,
                                      async_scope);
}

namespace {

using AsyncRouteHandler =
    std::variant<AsyncUnaryBytesHandler, AsyncServerStreamingBytesHandler,
                 AsyncClientStreamingBytesHandler, AsyncBidirectionalBytesHandler>;

class AsyncRoute final : public std::enable_shared_from_this<AsyncRoute> {
public:
  AsyncRoute(std::uint32_t kind, std::shared_ptr<AsyncRuntime> runtime, AsyncRouteHandler handler)
      : kind_(kind), runtime_(std::move(runtime)), handler_(std::move(handler)),
        scope_(std::make_shared<ServerScope>()) {}

  [[nodiscard]] std::shared_ptr<ServerScope> scope() const noexcept { return scope_; }

  [[nodiscard]] int dispatch(trevrpc_call* call) noexcept {
    ServerCallbackContextGuard callback_context;
    auto executor = runtime_->continuation_executor();
    auto reservation = executor->try_reserve();
    if (!reservation) {
      complete_immediately(
          call, Status(StatusCode::ResourceExhausted, "async service executor is saturated"));
      return 0;
    }

    const trevrpc_request* request = trevrpc_call_request(call);
    if (request == nullptr || request->kind != kind_) {
      complete_immediately(call, Status::internal("async service request kind mismatch"));
      return 0;
    }

    std::vector<std::byte> body;
    std::optional<CallContext> context;
    try {
      context.emplace(AsyncRegistrationAccess::context(trevrpc_call_get_context(call), request));
      const auto* begin = reinterpret_cast<const std::byte*>(request->body);
      body.assign(begin, begin + request->body_len);
    } catch (...) {
      complete_immediately(call, Status::internal("failed to copy async service request"));
      return 0;
    }

    bool deferred = false;
    auto state =
        ServerCallState::create(call, kind_, runtime_, production_server_call_ops(), {}, &deferred);
    if (!state) {
      if (!deferred) {
        complete_immediately(call, Status::internal("failed to defer async service call"));
        return 0;
      }
      return TREVRPC_CALL_DEFERRED;
    }
    auto call_state = std::move(state).value();
    auto scope_id = scope_->add(call_state);
    if (!scope_id) {
      call_state->stop(ServerStopReason::ServerCancellation);
      return TREVRPC_CALL_DEFERRED;
    }

    Work starter;
    try {
      starter = [self = shared_from_this(), state = call_state, scope_id = scope_id.value(),
                 context = std::move(context).value(), body = std::move(body)]() mutable {
        if (state->snapshot().final_selected) {
          self->scope_->complete(scope_id);
          return;
        }
        auto terminal_state = state;
        auto task = self->run(std::move(state), std::move(context), std::move(body));
        task.associate_executor(self->runtime_->continuation_executor());
        auto started =
            spawn(std::move(task),
                  [self, scope_id, terminal_state](const TaskCompletion<void>& completion) {
                    if (completion.exception) {
                      terminal_state->stop(ServerStopReason::LocalClose);
                    }
                    self->scope_->complete(scope_id);
                  });
        if (!started) {
          terminal_state->stop(ServerStopReason::LocalClose);
          self->scope_->complete(scope_id);
        }
      };
    } catch (...) {
      call_state->stop(ServerStopReason::LocalClose);
      scope_->complete(scope_id.value());
      return TREVRPC_CALL_DEFERRED;
    }

    auto committed = std::move(reservation).value().commit(std::move(starter));
    if (!committed) {
      call_state->stop(ServerStopReason::LocalClose);
      scope_->complete(scope_id.value());
    }
    return TREVRPC_CALL_DEFERRED;
  }

private:
  void complete_immediately(trevrpc_call* call, const Status& status) noexcept {
    auto call_ops = production_server_call_ops();
    if (kind_ == TREVRPC_RPC_KIND_UNARY || kind_ == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
      (void)call_ops->respond(call, status, {}, status.metadata());
    } else {
      (void)call_ops->finish(call, status);
    }
  }

  [[nodiscard]] Task<void> run(std::shared_ptr<ServerCallState> state, CallContext context,
                               std::vector<std::byte> body) {
    std::optional<ByteResponse> response;
    Status terminal = Status::internal("service handler threw");
    try {
      switch (kind_) {
      case TREVRPC_RPC_KIND_UNARY: {
        auto result = co_await std::get<AsyncUnaryBytesHandler>(handler_)(std::move(context),
                                                                          std::move(body));
        if (result) {
          response.emplace(std::move(result).value());
        } else {
          terminal = error_status(result.error());
        }
        break;
      }
      case TREVRPC_RPC_KIND_SERVER_STREAMING:
        terminal = co_await std::get<AsyncServerStreamingBytesHandler>(handler_)(
            std::move(context), std::move(body), state->operation());
        break;
      case TREVRPC_RPC_KIND_CLIENT_STREAMING: {
        auto result = co_await std::get<AsyncClientStreamingBytesHandler>(handler_)(
            std::move(context), state->operation());
        if (result) {
          response.emplace(std::move(result).value());
        } else {
          terminal = error_status(result.error());
        }
        break;
      }
      case TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING:
        terminal = co_await std::get<AsyncBidirectionalBytesHandler>(handler_)(std::move(context),
                                                                               state->operation());
        break;
      default:
        terminal = Status::internal("async service request kind mismatch");
        break;
      }
    } catch (...) {
      terminal = Status::internal("service handler threw");
    }

    if (kind_ == TREVRPC_RPC_KIND_UNARY || kind_ == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
      if (response.has_value()) {
        auto completed = co_await state->respond(
            std::move(response->body), std::move(response->status), std::move(response->metadata));
        (void)completed;
      } else {
        auto completed = co_await state->respond({}, std::move(terminal));
        (void)completed;
      }
    } else {
      auto completed = co_await state->finish(std::move(terminal));
      (void)completed;
    }
  }

  std::uint32_t kind_;
  std::shared_ptr<AsyncRuntime> runtime_;
  AsyncRouteHandler handler_;
  std::shared_ptr<ServerScope> scope_;
};

int dispatch_async_route(void* user_data, trevrpc_call* call) noexcept {
  return static_cast<AsyncRoute*>(user_data)->dispatch(call);
}

template <typename Handler>
Result<void> register_async_route(Server& server, std::string_view service, std::string_view method,
                                  std::uint32_t kind, std::shared_ptr<AsyncRuntime> runtime,
                                  Handler handler) {
  if (!runtime || !handler) {
    return Error::runtime(-EINVAL, "async service and runtime must not be null");
  }
  try {
    auto route = std::make_shared<AsyncRoute>(kind, std::move(runtime),
                                              AsyncRouteHandler(std::move(handler)));
    return AsyncRegistrationAccess::register_route(
        server, service, method, kind, dispatch_async_route, route, route.get(), route->scope());
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate async service route");
  }
}

} // namespace

Result<void> register_async_unary_bytes(Server& server, std::string_view service,
                                        std::string_view method,
                                        std::shared_ptr<AsyncRuntime> runtime,
                                        AsyncUnaryBytesHandler handler) {
  return register_async_route(server, service, method, TREVRPC_RPC_KIND_UNARY, std::move(runtime),
                              std::move(handler));
}

Result<void> register_async_server_streaming_bytes(Server& server, std::string_view service,
                                                   std::string_view method,
                                                   std::shared_ptr<AsyncRuntime> runtime,
                                                   AsyncServerStreamingBytesHandler handler) {
  return register_async_route(server, service, method, TREVRPC_RPC_KIND_SERVER_STREAMING,
                              std::move(runtime), std::move(handler));
}

Result<void> register_async_client_streaming_bytes(Server& server, std::string_view service,
                                                   std::string_view method,
                                                   std::shared_ptr<AsyncRuntime> runtime,
                                                   AsyncClientStreamingBytesHandler handler) {
  return register_async_route(server, service, method, TREVRPC_RPC_KIND_CLIENT_STREAMING,
                              std::move(runtime), std::move(handler));
}

Result<void> register_async_bidirectional_bytes(Server& server, std::string_view service,
                                                std::string_view method,
                                                std::shared_ptr<AsyncRuntime> runtime,
                                                AsyncBidirectionalBytesHandler handler) {
  return register_async_route(server, service, method, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
                              std::move(runtime), std::move(handler));
}

} // namespace detail

} // namespace trevrpc
