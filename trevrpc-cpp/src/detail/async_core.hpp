#pragma once

#include <trevrpc/async.hpp>

#include "abi6_bridge.hpp"
#include "lifecycle.hpp"

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

namespace trevrpc::detail {

template <typename T>
class AsyncCompletion final : public std::enable_shared_from_this<AsyncCompletion<T>> {
public:
  [[nodiscard]] static Result<std::shared_ptr<AsyncCompletion>>
  create(std::shared_ptr<Executor> executor) noexcept {
    std::optional<ExecutorReservation> reservation;
    if (executor) {
      auto reserved = executor->try_reserve();
      if (!reserved) {
        return reserved.error();
      }
      reservation.emplace(std::move(reserved).value());
    }
    try {
      return std::shared_ptr<AsyncCompletion>(
          new AsyncCompletion(std::move(executor), std::move(reservation)));
    } catch (...) {
      return Error::runtime(-ENOMEM, "failed to reserve async continuation");
    }
  }

  class Awaiter final {
  public:
    explicit Awaiter(std::shared_ptr<AsyncCompletion> state) : state_(std::move(state)) {}
    [[nodiscard]] bool await_ready() const noexcept {
      std::lock_guard lock(state_->mutex_);
      return state_->ready_;
    }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      std::lock_guard lock(state_->mutex_);
      if (state_->ready_) {
        return false;
      }
      state_->continuation_ = continuation;
      return true;
    }
    T await_resume() {
      std::lock_guard lock(state_->mutex_);
      if (state_->exception_) {
        std::rethrow_exception(state_->exception_);
      }
      return std::move(state_->value_).value();
    }

  private:
    std::shared_ptr<AsyncCompletion> state_;
  };

  [[nodiscard]] Awaiter operator co_await() { return Awaiter(this->shared_from_this()); }

  void complete(T value) noexcept {
    std::coroutine_handle<> continuation;
    {
      std::lock_guard lock(mutex_);
      if (ready_) {
        return;
      }
      value_.emplace(std::move(value));
      ready_ = true;
      continuation = continuation_;
    }
    schedule(continuation);
  }

  void fail(const std::exception_ptr& exception) noexcept {
    std::coroutine_handle<> continuation;
    {
      std::lock_guard lock(mutex_);
      if (ready_) {
        return;
      }
      exception_ = exception;
      ready_ = true;
      continuation = continuation_;
    }
    schedule(continuation);
  }

private:
  AsyncCompletion(std::shared_ptr<Executor> executor,
                  std::optional<ExecutorReservation> reservation) noexcept
      : executor_(std::move(executor)), reservation_(std::move(reservation)) {}
  void schedule(std::coroutine_handle<> continuation) noexcept;

  std::mutex mutex_;
  bool ready_ = false;
  std::optional<T> value_;
  std::exception_ptr exception_;
  std::coroutine_handle<> continuation_;
  std::shared_ptr<Executor> executor_;
  std::optional<ExecutorReservation> reservation_;
};

template <typename T>
void AsyncCompletion<T>::schedule(std::coroutine_handle<> continuation) noexcept {
  if (!continuation) {
    return;
  }
  if (!executor_ || executor_->running_in_this_executor()) {
    continuation.resume();
    return;
  }
  std::optional<ExecutorReservation> reservation;
  {
    std::lock_guard lock(mutex_);
    reservation = std::move(reservation_);
  }
  if (!reservation) {
    std::terminate();
  }
  auto committed = std::move(*reservation).commit([continuation] { continuation.resume(); });
  if (!committed) {
    std::terminate();
  }
}

template <typename T>
[[nodiscard]] Result<std::shared_ptr<AsyncCompletion<T>>>
make_completion(std::shared_ptr<Executor> executor) noexcept {
  return AsyncCompletion<T>::create(std::move(executor));
}

class CancellationState final {
public:
  [[nodiscard]] bool cancelled() const noexcept;
  void cancel() noexcept;
  [[nodiscard]] std::uint64_t register_callback(Work callback);
  void unregister_callback(std::uint64_t id) noexcept;

private:
  mutable std::mutex mutex_;
  bool cancelled_ = false;
  std::uint64_t next_id_ = 1;
  std::unordered_map<std::uint64_t, Work> callbacks_;
};

class AsyncRuntimeState final {
public:
  AsyncRuntimeState(std::shared_ptr<Executor> continuation,
                    std::shared_ptr<ThreadPoolExecutor> native_io, AsyncRuntimeOptions options);
  ~AsyncRuntimeState();
  AsyncRuntimeState(const AsyncRuntimeState&) = delete;
  AsyncRuntimeState& operator=(const AsyncRuntimeState&) = delete;

  [[nodiscard]] Result<void> submit_native(Work work) noexcept;
  [[nodiscard]] Result<void> schedule_at(Deadline deadline, Work work) noexcept;
  [[nodiscard]] std::shared_ptr<Executor> continuation() const noexcept { return continuation_; }
  [[nodiscard]] const AsyncRuntimeOptions& options() const noexcept { return options_; }

private:
  void timer_loop() noexcept;

  std::shared_ptr<Executor> continuation_;
  std::shared_ptr<ThreadPoolExecutor> native_io_;
  AsyncRuntimeOptions options_;
  std::mutex timer_mutex_;
  std::condition_variable timer_condition_;
  std::multimap<Deadline, Work> timers_;
  bool timer_stop_ = false;
  std::thread timer_thread_;
};

class NativeOps {
public:
  virtual ~NativeOps() = default;
  [[nodiscard]] virtual int send(trevrpc_stream* stream,
                                 std::span<const std::byte> body) noexcept = 0;
  [[nodiscard]] virtual int finish_send(trevrpc_stream* stream) noexcept = 0;
  [[nodiscard]] virtual Result<std::optional<StreamFrame>>
  receive_ready_since(trevrpc_stream* stream, std::uint64_t wait_started) noexcept = 0;
  virtual void cancel(trevrpc_stream* stream) noexcept = 0;
  virtual void close(trevrpc_stream* stream) noexcept = 0;
};

using NativeSendAction = std::function<int(trevrpc_stream*, std::span<const std::byte>)>;

[[nodiscard]] std::shared_ptr<NativeOps> production_native_ops();

class OperationState final : public std::enable_shared_from_this<OperationState> {
public:
  [[nodiscard]] static std::shared_ptr<OperationState>
  create(trevrpc_stream* stream, const std::shared_ptr<AsyncRuntime>& runtime,
         std::shared_ptr<NativeOps> native_ops, std::optional<Deadline> deadline,
         std::shared_ptr<Cancellation> cancellation_bridge = {},
         std::shared_ptr<CancellationState> cancellation_state = {},
         std::uint64_t cancellation_registration = 0, bool terminal_stops_send = false);
  [[nodiscard]] static Task<Result<ByteResponse>> run_unary(std::shared_ptr<Channel> channel,
                                                            std::shared_ptr<AsyncRuntime> runtime,
                                                            std::string service, std::string method,
                                                            std::vector<std::byte> request,
                                                            OwnedAsyncCallOptions options);
  [[nodiscard]] static Task<Result<std::shared_ptr<OperationState>>>
  start_stream(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
               std::string service, std::string method, std::uint32_t kind,
               std::vector<std::byte> request, OwnedAsyncCallOptions options);

  OperationState(trevrpc_stream* stream, std::shared_ptr<AsyncRuntimeState> runtime,
                 std::shared_ptr<NativeOps> native_ops, std::optional<Deadline> deadline = {},
                 std::shared_ptr<Cancellation> cancellation_bridge = {},
                 std::shared_ptr<CancellationState> cancellation_state = {},
                 std::uint64_t cancellation_registration = 0, bool terminal_stops_send = false);
  ~OperationState();
  OperationState(const OperationState&) = delete;
  OperationState& operator=(const OperationState&) = delete;

  [[nodiscard]] Task<Result<void>> send(std::size_t encoded_size,
                                        std::function<Result<std::vector<std::byte>>()> serializer,
                                        SendOptions options, bool finish);
  [[nodiscard]] Task<Result<void>>
  send_action(std::size_t encoded_size, std::function<Result<std::vector<std::byte>>()> serializer,
              SendOptions options, bool seal_send, NativeSendAction action);
  [[nodiscard]] Task<Result<StreamFrame>> receive();
  void cancel() noexcept;
  void close() noexcept;
  void retire(const Error& reason) noexcept;
  void when_send_idle(Work callback) noexcept;
  void set_cancellation_registration(std::uint64_t registration) noexcept;
  [[nodiscard]] std::shared_ptr<Executor> continuation_executor() const noexcept;

private:
  struct SendItem {
    std::size_t bytes_reserved = 0;
    bool seal_send = false;
    bool ready = false;
    std::vector<std::byte> bytes;
    std::optional<Error> preparation_error;
    NativeSendAction action;
    std::shared_ptr<AsyncCompletion<Result<void>>> completion;
  };
  struct SendWaiter {
    std::size_t bytes = 0;
    bool seal_send = false;
    std::optional<Deadline> deadline;
    std::function<Result<std::vector<std::byte>>()> serializer;
    NativeSendAction action;
    std::shared_ptr<AsyncCompletion<Result<void>>> completion;
    std::shared_ptr<SendItem> item;
    bool settled = false;
  };

  [[nodiscard]] bool has_capacity_locked(std::size_t bytes) const noexcept;
  void
  prepare_admitted_item(const std::shared_ptr<SendItem>& item,
                        const std::function<Result<std::vector<std::byte>>()>& serializer) noexcept;
  void start_send_if_ready() noexcept;
  void finish_send_item(const std::shared_ptr<SendItem>& item, int error) noexcept;
  void admit_waiters_locked(std::vector<std::shared_ptr<SendWaiter>>& admitted);
  void timeout_waiter(const std::shared_ptr<SendWaiter>& waiter) noexcept;
  void start_receive_attempt(std::shared_ptr<AsyncCompletion<Result<StreamFrame>>> completion,
                             std::uint64_t wait_started, std::chrono::nanoseconds delay) noexcept;
  void settle_receive(const std::shared_ptr<AsyncCompletion<Result<StreamFrame>>>& completion,
                      Result<StreamFrame> result) noexcept;
  void finish_native_work() noexcept;
  void collect_idle_callbacks_locked(std::vector<Work>& callbacks);

  trevrpc_stream* stream_;
  std::shared_ptr<AsyncRuntimeState> runtime_;
  std::shared_ptr<NativeOps> native_ops_;
  std::optional<Deadline> deadline_;
  std::shared_ptr<Cancellation> cancellation_bridge_;
  std::shared_ptr<CancellationState> cancellation_state_;
  std::uint64_t cancellation_registration_ = 0;

  std::mutex mutex_;
  std::deque<std::shared_ptr<SendItem>> send_queue_;
  std::deque<std::shared_ptr<SendWaiter>> send_waiters_;
  std::size_t pending_items_ = 0;
  std::size_t pending_bytes_ = 0;
  bool send_running_ = false;
  std::size_t native_work_ = 0;
  bool close_requested_ = false;
  bool receive_pending_ = false;
  bool receive_terminal_ = false;
  std::shared_ptr<AsyncCompletion<Result<StreamFrame>>> receive_completion_;
  std::vector<Work> send_idle_callbacks_;
  bool cancelled_ = false;
  bool closed_ = false;
  bool send_sealed_ = false;
  bool send_cancelled_ = false;
  bool terminal_stops_send_ = false;
};

enum class ServerStopReason { Deadline, PeerCancellation, LocalClose, ServerCancellation };

enum class ServerTerminalPhase { Open, ApplicationPending, Settled };

struct ServerCallSnapshot {
  ServerTerminalPhase terminal = ServerTerminalPhase::Open;
  std::optional<ServerStopReason> external_stop;
  bool final_selected = false;
  bool pin_released = false;
};

class ServerCallOps {
public:
  virtual ~ServerCallOps() = default;
  [[nodiscard]] virtual int defer(trevrpc_call* call) noexcept = 0;
  [[nodiscard]] virtual int retain(trevrpc_call* call) noexcept = 0;
  virtual void release(trevrpc_call* call) noexcept = 0;
  [[nodiscard]] virtual trevrpc_stream* stream(trevrpc_call* call) noexcept = 0;
  [[nodiscard]] virtual int respond(trevrpc_call* call, const Status& status,
                                    std::span<const std::byte> body,
                                    const Metadata& metadata) noexcept = 0;
  [[nodiscard]] virtual int finish(trevrpc_call* call, const Status& status) noexcept = 0;
  virtual void cancel(trevrpc_call* call) noexcept = 0;
  virtual void close(trevrpc_call* call) noexcept = 0;
};

[[nodiscard]] std::shared_ptr<ServerCallOps> production_server_call_ops();

class ServerCallState final : public std::enable_shared_from_this<ServerCallState> {
public:
  [[nodiscard]] static Result<std::shared_ptr<ServerCallState>>
  create(trevrpc_call* call, std::uint32_t kind, const std::shared_ptr<AsyncRuntime>& runtime,
         const std::shared_ptr<ServerCallOps>& call_ops = production_server_call_ops(),
         std::shared_ptr<NativeOps> stream_ops = {}, bool* deferred = nullptr);
  ~ServerCallState();
  ServerCallState(const ServerCallState&) = delete;
  ServerCallState& operator=(const ServerCallState&) = delete;

  [[nodiscard]] Task<Result<void>> respond(std::vector<std::byte> body, Status status,
                                           Metadata metadata = {});
  [[nodiscard]] Task<Result<void>> finish(Status status);
  void stop(ServerStopReason reason) noexcept;
  [[nodiscard]] std::shared_ptr<OperationState> operation() const noexcept { return operation_; }
  [[nodiscard]] ServerCallSnapshot snapshot() const noexcept;

private:
  ServerCallState(trevrpc_call* call, std::uint32_t kind,
                  std::shared_ptr<ServerCallOps> call_ops) noexcept;
  [[nodiscard]] static Task<Result<void>> respond_owned(std::shared_ptr<ServerCallState> self,
                                                        std::vector<std::byte> body, Status status,
                                                        Metadata metadata);
  [[nodiscard]] static Task<Result<void>> finish_owned(std::shared_ptr<ServerCallState> self,
                                                       Status status);
  [[nodiscard]] Task<Result<void>>
  terminal(std::size_t encoded_size, std::function<Result<std::vector<std::byte>>()> serializer,
           NativeSendAction action);
  void record_native_terminal_result(int error) noexcept;
  void settle_application(Result<void> result) noexcept;
  void settle_without_application(const Error& result) noexcept;
  void publish_final_locked(std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>>& waiters,
                            std::optional<Error>& error) noexcept;
  void cleanup_after_settlement(bool close_call, Error retire_reason) noexcept;
  [[nodiscard]] static Error stop_error(ServerStopReason reason);

  trevrpc_call* call_ = nullptr;
  std::uint32_t kind_ = 0;
  std::shared_ptr<ServerCallOps> call_ops_;
  std::shared_ptr<OperationState> operation_;

  mutable std::mutex mutex_;
  ServerTerminalPhase terminal_phase_ = ServerTerminalPhase::Open;
  std::optional<ServerStopReason> external_stop_;
  std::optional<Error> final_error_;
  bool final_selected_ = false;
  bool pin_released_ = false;
  std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>> terminal_waiters_;
};

class ServerScope final : public AsyncServerScopeControl {
public:
  [[nodiscard]] Result<std::uint64_t> add(std::shared_ptr<ServerCallState> call);
  void complete(std::uint64_t id) noexcept;
  void request_stop(ServerStopReason reason = ServerStopReason::ServerCancellation) noexcept;
  void cancel() noexcept override { request_stop(ServerStopReason::ServerCancellation); }
  [[nodiscard]] Result<void> drain_until(Deadline deadline) noexcept override;
  [[nodiscard]] std::size_t active() const noexcept;

private:
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::map<std::uint64_t, std::shared_ptr<ServerCallState>> calls_;
  std::uint64_t next_id_ = 1;
  bool stopping_ = false;
};

[[nodiscard]] std::shared_ptr<OperationState>
create_operation(trevrpc_stream* stream, const std::shared_ptr<AsyncRuntime>& runtime,
                 std::shared_ptr<NativeOps> native_ops = production_native_ops(),
                 std::optional<Deadline> deadline = {});

} // namespace trevrpc::detail
