#pragma once

#include <trevrpc/trevrpc.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

namespace trevrpc {

using Deadline = std::chrono::steady_clock::time_point;
using Work = std::function<void()>;

namespace detail {
class ExecutorReservationState;
class ThreadPoolState;
class AsyncRuntimeState;
class OperationState;
class CancellationState;
} // namespace detail

class ExecutorReservation {
public:
  ExecutorReservation() = default;
  ExecutorReservation(ExecutorReservation&&) noexcept = default;
  ExecutorReservation& operator=(ExecutorReservation&&) noexcept = default;
  ExecutorReservation(const ExecutorReservation&) = delete;
  ExecutorReservation& operator=(const ExecutorReservation&) = delete;
  ~ExecutorReservation();

  [[nodiscard]] Result<void> commit(Work work) && noexcept;
  void cancel() noexcept;
  [[nodiscard]] explicit operator bool() const noexcept { return state_ != nullptr; }

private:
  friend class ThreadPoolExecutor;
  explicit ExecutorReservation(std::shared_ptr<detail::ExecutorReservationState> state) noexcept
      : state_(std::move(state)) {}
  std::shared_ptr<detail::ExecutorReservationState> state_;
};

class Executor {
public:
  virtual ~Executor() = default;
  [[nodiscard]] virtual Result<ExecutorReservation> try_reserve() noexcept = 0;
  [[nodiscard]] virtual Result<void> execute(Work work) noexcept = 0;
  virtual void request_stop() noexcept = 0;
  [[nodiscard]] virtual Result<void> drain_until(Deadline deadline) noexcept = 0;
  [[nodiscard]] virtual bool running_in_this_executor() const noexcept = 0;
};

struct ThreadPoolExecutorOptions {
  std::size_t worker_count = 16;
  std::size_t queue_capacity = 1024;
};

struct ExecutorSnapshot {
  std::size_t queued = 0;
  std::size_t reserved = 0;
  std::size_t running = 0;
  std::uint64_t completed = 0;
  std::uint64_t rejected = 0;
  std::uint64_t exceptions = 0;
  bool stopping = false;
};

class ThreadPoolExecutor final : public Executor {
public:
  ~ThreadPoolExecutor() override;
  ThreadPoolExecutor(const ThreadPoolExecutor&) = delete;
  ThreadPoolExecutor& operator=(const ThreadPoolExecutor&) = delete;

  [[nodiscard]] static Result<std::shared_ptr<ThreadPoolExecutor>>
  create(const ThreadPoolExecutorOptions& options = {});
  [[nodiscard]] Result<ExecutorReservation> try_reserve() noexcept override;
  [[nodiscard]] Result<void> execute(Work work) noexcept override;
  void request_stop() noexcept override;
  [[nodiscard]] Result<void> drain_until(Deadline deadline) noexcept override;
  [[nodiscard]] bool running_in_this_executor() const noexcept override;
  [[nodiscard]] ExecutorSnapshot snapshot() const noexcept;

private:
  explicit ThreadPoolExecutor(std::shared_ptr<detail::ThreadPoolState> state)
      : state_(std::move(state)) {}
  std::shared_ptr<detail::ThreadPoolState> state_;
};

struct AsyncRuntimeOptions {
  std::size_t native_io_worker_count = 4;
  std::size_t native_io_queue_capacity = 1024;
  std::size_t max_pending_sends_per_stream = 64;
  std::size_t max_pending_send_bytes_per_stream = std::size_t{16} * 1024 * 1024;
  std::size_t max_waiting_senders_per_stream = 64;
  std::chrono::nanoseconds receive_poll_min{std::chrono::microseconds(250)};
  std::chrono::nanoseconds receive_poll_max{std::chrono::milliseconds(8)};
};

class AsyncRuntime {
public:
  ~AsyncRuntime();
  AsyncRuntime(const AsyncRuntime&) = delete;
  AsyncRuntime& operator=(const AsyncRuntime&) = delete;

  [[nodiscard]] static Result<std::shared_ptr<AsyncRuntime>>
  create(std::shared_ptr<Executor> continuation_executor, const AsyncRuntimeOptions& options = {});
  [[nodiscard]] std::shared_ptr<Executor> continuation_executor() const noexcept;
  [[nodiscard]] const AsyncRuntimeOptions& options() const noexcept;

private:
  friend class detail::OperationState;
  explicit AsyncRuntime(std::shared_ptr<detail::AsyncRuntimeState> state)
      : state_(std::move(state)) {}
  std::shared_ptr<detail::AsyncRuntimeState> state_;
};

class CancellationToken {
public:
  CancellationToken() = default;
  [[nodiscard]] bool cancelled() const noexcept;

private:
  friend class CancellationSource;
  friend class detail::OperationState;
  explicit CancellationToken(std::shared_ptr<detail::CancellationState> state)
      : state_(std::move(state)) {}
  std::shared_ptr<detail::CancellationState> state_;
};

class CancellationSource {
public:
  CancellationSource();
  [[nodiscard]] CancellationToken token() const noexcept;
  void cancel() noexcept;

private:
  std::shared_ptr<detail::CancellationState> state_;
};

struct AsyncCallOptions {
  CallOptions call_options;
  CancellationToken cancellation;
  std::optional<Deadline> deadline;
};

enum class BackpressureMode { FailFast, Wait };

struct SendOptions {
  BackpressureMode backpressure = BackpressureMode::FailFast;
  std::optional<Deadline> deadline;
};

template <typename T> struct TaskCompletion {
  std::optional<T> value;
  std::exception_ptr exception;
};

template <> struct TaskCompletion<void> {
  bool completed = false;
  std::exception_ptr exception;
};

namespace detail {

template <typename T> struct TaskPromiseStorage {
  std::optional<T> value;
  template <typename U> void return_value(U&& result) { value.emplace(std::forward<U>(result)); }
  [[nodiscard]] T take() { return std::move(value).value(); }
};

template <> struct TaskPromiseStorage<void> {
  void return_void() noexcept {}
  void take() noexcept {}
};

template <typename T> class TaskPromise;

struct FinalScheduleAwaiter {
  [[nodiscard]] bool await_ready() const noexcept { return false; }
  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> handle) const noexcept {
    auto& promise = handle.promise();
    if (!promise.continuation_) {
      return;
    }
    auto continuation = promise.continuation_;
    auto executor = promise.continuation_executor_;
    if (!executor || executor->running_in_this_executor()) {
      continuation.resume();
      return;
    }
    if (!promise.continuation_reservation_) {
      std::terminate();
    }
    auto reservation = std::move(*promise.continuation_reservation_);
    promise.continuation_reservation_.reset();
    auto committed = std::move(reservation).commit([continuation] { continuation.resume(); });
    if (!committed) {
      std::terminate();
    }
  }
  void await_resume() const noexcept {}
};

} // namespace detail

template <typename T> class [[nodiscard]] Task {
public:
  struct promise_type : detail::TaskPromiseStorage<T> {
    [[nodiscard]] Task get_return_object() noexcept {
      return Task(handle_type::from_promise(*this));
    }
    [[nodiscard]] std::suspend_always initial_suspend() const noexcept { return {}; }
    [[nodiscard]] detail::FinalScheduleAwaiter final_suspend() const noexcept { return {}; }
    void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  private:
    friend class Task;
    friend struct detail::FinalScheduleAwaiter;
    std::exception_ptr exception_;
    std::coroutine_handle<> continuation_;
    std::shared_ptr<Executor> continuation_executor_;
    std::optional<ExecutorReservation> continuation_reservation_;
  };

  using handle_type = std::coroutine_handle<promise_type>;

  Task() = default;
  Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        handle_.destroy();
      }
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  class Awaiter {
  public:
    explicit Awaiter(handle_type handle) noexcept : handle_(handle) {}
    Awaiter(const Awaiter&) = delete;
    Awaiter& operator=(const Awaiter&) = delete;
    Awaiter(Awaiter&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    ~Awaiter() {
      if (handle_) {
        handle_.destroy();
      }
    }
    [[nodiscard]] bool await_ready() const noexcept { return !handle_ || handle_.done(); }
    template <typename Promise> void await_suspend(std::coroutine_handle<Promise> continuation) {
      auto& promise = handle_.promise();
      if constexpr (requires { continuation.promise().continuation_executor_; }) {
        if (!promise.continuation_executor_) {
          promise.continuation_executor_ = continuation.promise().continuation_executor_;
        }
      }
      if (promise.continuation_executor_ &&
          !promise.continuation_executor_->running_in_this_executor()) {
        auto reserved = promise.continuation_executor_->try_reserve();
        if (!reserved) {
          throw std::system_error(-reserved.error().code(), std::generic_category(),
                                  reserved.error().message());
        }
        promise.continuation_reservation_.emplace(std::move(reserved).value());
      }
      promise.continuation_ = continuation;
      handle_.resume();
    }
    decltype(auto) await_resume() {
      if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
      }
      if constexpr (std::is_void_v<T>) {
        handle_.promise().take();
        handle_.destroy();
        handle_ = {};
        return;
      } else {
        T value = handle_.promise().take();
        handle_.destroy();
        handle_ = {};
        return value;
      }
    }

  private:
    handle_type handle_{};
  };

  [[nodiscard]] Awaiter operator co_await() && noexcept {
    return Awaiter(std::exchange(handle_, {}));
  }

  [[nodiscard]] bool associated_executor_is_current() const noexcept {
    return handle_ && handle_.promise().continuation_executor_ &&
           handle_.promise().continuation_executor_->running_in_this_executor();
  }

  [[nodiscard]] std::shared_ptr<Executor> associated_executor() const noexcept {
    return handle_ ? handle_.promise().continuation_executor_ : nullptr;
  }

  void associate_executor(std::shared_ptr<Executor> executor) noexcept {
    if (handle_) {
      handle_.promise().continuation_executor_ = std::move(executor);
    }
  }

private:
  explicit Task(handle_type handle) noexcept : handle_(handle) {}
  handle_type handle_{};
};

namespace detail {

struct DetachedTask {
  struct promise_type {
    [[nodiscard]] DetachedTask get_return_object() noexcept {
      return {std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    [[nodiscard]] std::suspend_always initial_suspend() const noexcept { return {}; }
    [[nodiscard]] std::suspend_never final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    void unhandled_exception() const noexcept { std::terminate(); }
  };
  std::coroutine_handle<promise_type> handle;
};

template <typename T, typename CompletionSink>
DetachedTask spawn_runner(Task<T> task, CompletionSink sink) {
  TaskCompletion<T> completion;
  try {
    if constexpr (std::is_void_v<T>) {
      co_await std::move(task);
      completion.completed = true;
    } else {
      completion.value.emplace(co_await std::move(task));
    }
  } catch (...) {
    completion.exception = std::current_exception();
  }
  try {
    std::invoke(sink, std::move(completion));
  } catch (...) {
    (void)std::current_exception();
  }
}

} // namespace detail

template <typename T, typename CompletionSink>
[[nodiscard]] Result<void> spawn(Task<T> task, CompletionSink sink) {
  std::optional<ExecutorReservation> reservation;
  auto executor = task.associated_executor();
  if (executor && !executor->running_in_this_executor()) {
    auto reserved = executor->try_reserve();
    if (!reserved) {
      return reserved.error();
    }
    reservation.emplace(std::move(reserved).value());
  }
  try {
    auto runner = detail::spawn_runner(std::move(task), std::move(sink));
    const auto handle = runner.handle;
    if (!reservation) {
      handle.resume();
      return {};
    }
    Work start;
    try {
      start = [handle] { handle.resume(); };
    } catch (...) {
      handle.destroy();
      throw;
    }
    auto committed = std::move(*reservation).commit(std::move(start));
    if (!committed) {
      handle.destroy();
      return committed.error();
    }
    return {};
  } catch (const std::system_error& error) {
    return Error::runtime(-error.code().value(), error.what());
  } catch (...) {
    return Error::runtime(-EAGAIN, "failed to start task");
  }
}

namespace detail {
template <typename T> struct IsResult : std::false_type {};
template <typename U> struct IsResult<Result<U>> : std::true_type {};
} // namespace detail

template <typename T> T sync_wait(Task<T> task) {
  if (task.associated_executor_is_current()) {
    if constexpr (detail::IsResult<T>::value) {
      using Value = T;
      return Value(Error::runtime(-EDEADLK, "sync_wait called from continuation executor"));
    } else {
      throw std::logic_error("sync_wait called from continuation executor");
    }
  }
  std::mutex mutex;
  std::condition_variable condition;
  bool done = false;
  TaskCompletion<T> completion;
  auto started = spawn(std::move(task), [&](TaskCompletion<T> value) {
    std::lock_guard lock(mutex);
    completion = std::move(value);
    done = true;
    condition.notify_one();
  });
  if (!started) {
    throw std::system_error(-started.error().code(), std::generic_category(),
                            started.error().message());
  }
  std::unique_lock lock(mutex);
  condition.wait(lock, [&] { return done; });
  if (completion.exception) {
    std::rethrow_exception(completion.exception);
  }
  if constexpr (std::is_void_v<T>) {
    return;
  } else {
    return std::move(completion.value).value();
  }
}

namespace detail {
struct OwnedAsyncCallOptions {
  CallOptions call_options;
  CancellationToken cancellation;
  std::optional<Deadline> deadline;
  std::shared_ptr<Cancellation> retained_cancellation;
};

[[nodiscard]] Result<OwnedAsyncCallOptions> own_async_call_options(const AsyncCallOptions& options);
[[nodiscard]] Task<Result<ByteResponse>> async_unary_bytes(std::shared_ptr<Channel> channel,
                                                           std::shared_ptr<AsyncRuntime> runtime,
                                                           std::string service, std::string method,
                                                           std::vector<std::byte> request,
                                                           OwnedAsyncCallOptions options);
[[nodiscard]] Task<Result<std::shared_ptr<OperationState>>>
async_start_stream_bytes(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
                         std::string service, std::string method, std::uint32_t kind,
                         std::vector<std::byte> request, OwnedAsyncCallOptions options);

[[nodiscard]] Task<Result<void>>
operation_send(const std::shared_ptr<OperationState>& operation, std::size_t encoded_size,
               std::function<Result<std::vector<std::byte>>()> serializer, SendOptions options);
[[nodiscard]] Task<Result<void>>
operation_finish_send(const std::shared_ptr<OperationState>& operation, SendOptions options);
[[nodiscard]] Task<Result<StreamFrame>>
operation_receive(const std::shared_ptr<OperationState>& operation);
void operation_cancel(const std::shared_ptr<OperationState>& operation) noexcept;
void operation_close(const std::shared_ptr<OperationState>& operation) noexcept;

using AsyncUnaryBytesHandler =
    std::function<Task<Result<ByteResponse>>(CallContext, std::vector<std::byte>)>;
using AsyncServerStreamingBytesHandler = std::function<Task<Status>(
    CallContext, std::vector<std::byte>, std::shared_ptr<OperationState>)>;
using AsyncClientStreamingBytesHandler =
    std::function<Task<Result<ByteResponse>>(CallContext, std::shared_ptr<OperationState>)>;
using AsyncBidirectionalBytesHandler =
    std::function<Task<Status>(CallContext, std::shared_ptr<OperationState>)>;

struct AsyncRegistrationAccess {
  [[nodiscard]] static CallContext context(const trevrpc_call_context* context,
                                           const trevrpc_request* request);
  [[nodiscard]] static Result<void>
  register_route(Server& server, std::string_view service, std::string_view method,
                 std::uint32_t kind, trevrpc_call_handler callback,
                 const std::shared_ptr<void>& route, void* user_data,
                 const std::shared_ptr<AsyncServerScopeControl>& async_scope);
};

[[nodiscard]] Result<void> register_async_unary_bytes(Server& server, std::string_view service,
                                                      std::string_view method,
                                                      std::shared_ptr<AsyncRuntime> runtime,
                                                      AsyncUnaryBytesHandler handler);
[[nodiscard]] Result<void> register_async_server_streaming_bytes(
    Server& server, std::string_view service, std::string_view method,
    std::shared_ptr<AsyncRuntime> runtime, AsyncServerStreamingBytesHandler handler);
[[nodiscard]] Result<void> register_async_client_streaming_bytes(
    Server& server, std::string_view service, std::string_view method,
    std::shared_ptr<AsyncRuntime> runtime, AsyncClientStreamingBytesHandler handler);
[[nodiscard]] Result<void>
register_async_bidirectional_bytes(Server& server, std::string_view service,
                                   std::string_view method, std::shared_ptr<AsyncRuntime> runtime,
                                   AsyncBidirectionalBytesHandler handler);
} // namespace detail

template <typename T> class AsyncStreamSender {
public:
  AsyncStreamSender() = default;
  AsyncStreamSender(AsyncStreamSender&&) noexcept = default;
  AsyncStreamSender& operator=(AsyncStreamSender&&) noexcept = default;
  AsyncStreamSender(const AsyncStreamSender&) = delete;
  AsyncStreamSender& operator=(const AsyncStreamSender&) = delete;
  [[nodiscard]] Task<Result<void>> send(const T& message, const SendOptions& options = {}) {
    const std::size_t encoded_size = message.ByteSizeLong();
    return detail::operation_send(
        operation_, encoded_size, [message] { return detail::serialize(message); }, options);
  }
  [[nodiscard]] Task<Result<void>> finish_send(const SendOptions& options = {}) {
    return detail::operation_finish_send(operation_, options);
  }
  void cancel() noexcept { detail::operation_cancel(operation_); }
  void close() noexcept { detail::operation_close(operation_); }

  explicit AsyncStreamSender(std::shared_ptr<detail::OperationState> operation)
      : operation_(std::move(operation)) {}

private:
  std::shared_ptr<detail::OperationState> operation_;
};

template <typename T> class AsyncStreamReceiver {
public:
  AsyncStreamReceiver() = default;
  AsyncStreamReceiver(AsyncStreamReceiver&&) noexcept = default;
  AsyncStreamReceiver& operator=(AsyncStreamReceiver&&) noexcept = default;
  AsyncStreamReceiver(const AsyncStreamReceiver&) = delete;
  AsyncStreamReceiver& operator=(const AsyncStreamReceiver&) = delete;
  [[nodiscard]] Task<Result<StreamEvent<T>>> receive() {
    auto frame = co_await detail::operation_receive(operation_);
    if (!frame) {
      co_return frame.error();
    }
    if (frame.value().terminal) {
      co_return StreamEvent<T>::terminal(std::move(frame.value().status));
    }
    auto message = detail::parse<T>(frame.value().body, "failed to parse async stream response");
    if (!message) {
      co_return message.error();
    }
    co_return StreamEvent<T>::message(std::move(message).value());
  }
  void cancel() noexcept { detail::operation_cancel(operation_); }
  void close() noexcept { detail::operation_close(operation_); }

  explicit AsyncStreamReceiver(std::shared_ptr<detail::OperationState> operation)
      : operation_(std::move(operation)) {}

private:
  std::shared_ptr<detail::OperationState> operation_;
};

template <typename Response> struct AsyncServerStreamingCall {
  AsyncStreamReceiver<Response> responses;
};

template <typename Request, typename Response> struct AsyncClientStreamingCall {
  AsyncStreamSender<Request> requests;
  AsyncStreamReceiver<Response> responses;

  [[nodiscard]] Task<Result<trevrpc::Response<Response>>> finish_and_receive() {
    auto finished = co_await requests.finish_send();
    if (!finished) {
      co_return finished.error();
    }
    std::optional<Response> response;
    bool multiple_responses = false;
    for (;;) {
      auto event = co_await responses.receive();
      if (!event) {
        co_return event.error();
      }
      if (event.value().is_message()) {
        if (response.has_value()) {
          multiple_responses = true;
        } else {
          response.emplace(std::move(event.value().message()));
        }
        continue;
      }
      if (!event.value().status().is_ok()) {
        co_return Error::rpc(event.value().status());
      }
      if (multiple_responses) {
        co_return Error::protobuf("client-streaming RPC returned more than one response message");
      }
      if (!response.has_value()) {
        co_return Error::protobuf(
            "client-streaming RPC did not return exactly one response message");
      }
      co_return trevrpc::Response<Response>{std::move(response).value(),
                                            event.value().status().metadata()};
    }
  }
};

template <typename Request, typename Response>
using AsyncBidirectionalStreamingCall = AsyncClientStreamingCall<Request, Response>;

template <typename T> class AsyncServerReader {
public:
  AsyncServerReader() = default;
  AsyncServerReader(AsyncServerReader&&) noexcept = default;
  AsyncServerReader& operator=(AsyncServerReader&&) noexcept = default;
  AsyncServerReader(const AsyncServerReader&) = delete;
  AsyncServerReader& operator=(const AsyncServerReader&) = delete;

  explicit AsyncServerReader(std::shared_ptr<detail::OperationState> operation)
      : operation_(std::move(operation)) {}

  [[nodiscard]] Task<Result<std::optional<T>>> receive() {
    auto frame = co_await detail::operation_receive(operation_);
    if (!frame) {
      co_return frame.error();
    }
    if (frame.value().terminal) {
      if (!frame.value().status.is_ok()) {
        co_return Error::rpc(std::move(frame.value().status));
      }
      co_return std::optional<T>{};
    }
    auto value = detail::parse<T>(frame.value().body, "failed to parse async stream request");
    if (!value) {
      co_return Error::rpc(Status::invalid_argument(value.error().message()));
    }
    co_return std::optional<T>(std::move(value).value());
  }

private:
  std::shared_ptr<detail::OperationState> operation_;
};

template <typename T> class AsyncServerWriter {
public:
  AsyncServerWriter() = default;
  AsyncServerWriter(AsyncServerWriter&&) noexcept = default;
  AsyncServerWriter& operator=(AsyncServerWriter&&) noexcept = default;
  AsyncServerWriter(const AsyncServerWriter&) = delete;
  AsyncServerWriter& operator=(const AsyncServerWriter&) = delete;

  explicit AsyncServerWriter(std::shared_ptr<detail::OperationState> operation)
      : operation_(std::move(operation)) {}

  [[nodiscard]] Task<Result<void>> send(const T& message, const SendOptions& options = {}) {
    const std::size_t encoded_size = message.ByteSizeLong();
    return detail::operation_send(
        operation_, encoded_size, [message] { return detail::serialize(message); }, options);
  }

private:
  std::shared_ptr<detail::OperationState> operation_;
};

template <typename Request, typename ResponseMessage, typename Handler>
[[nodiscard]] Result<void>
register_async_unary(Server& server, std::string_view service, std::string_view method,
                     std::shared_ptr<AsyncRuntime> runtime, Handler handler) {
  try {
    return detail::register_async_unary_bytes(
        server, service, method, std::move(runtime),
        [handler = std::move(handler)](CallContext context, std::vector<std::byte> body)
            -> Task<Result<detail::ByteResponse>> {
          auto request = detail::parse<Request>(body, "failed to parse async unary request");
          if (!request) {
            co_return Error::rpc(Status::invalid_argument(request.error().message()));
          }
          auto response = co_await handler(std::move(context), std::move(request).value());
          if (!response) {
            co_return response.error();
          }
          auto encoded = detail::serialize(response.value().message);
          if (!encoded) {
            co_return encoded.error();
          }
          auto envelope = std::move(response).value();
          co_return detail::ByteResponse{Status::ok(), std::move(encoded).value(),
                                         std::move(envelope.metadata)};
        });
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate async unary route");
  }
}

template <typename Request, typename ResponseMessage, typename Handler>
[[nodiscard]] Result<void>
register_async_server_streaming(Server& server, std::string_view service, std::string_view method,
                                std::shared_ptr<AsyncRuntime> runtime, Handler handler) {
  try {
    return detail::register_async_server_streaming_bytes(
        server, service, method, std::move(runtime),
        [handler = std::move(handler)](
            CallContext context, std::vector<std::byte> body,
            std::shared_ptr<detail::OperationState> operation) -> Task<Status> {
          auto request =
              detail::parse<Request>(body, "failed to parse async server-streaming request");
          if (!request) {
            co_return Status::invalid_argument(request.error().message());
          }
          co_return co_await handler(std::move(context), std::move(request).value(),
                                     AsyncServerWriter<ResponseMessage>(std::move(operation)));
        });
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate async server-streaming route");
  }
}

template <typename Request, typename ResponseMessage, typename Handler>
[[nodiscard]] Result<void>
register_async_client_streaming(Server& server, std::string_view service, std::string_view method,
                                std::shared_ptr<AsyncRuntime> runtime, Handler handler) {
  try {
    return detail::register_async_client_streaming_bytes(
        server, service, method, std::move(runtime),
        [handler = std::move(handler)](CallContext context,
                                       std::shared_ptr<detail::OperationState> operation)
            -> Task<Result<detail::ByteResponse>> {
          auto response = co_await handler(std::move(context),
                                           AsyncServerReader<Request>(std::move(operation)));
          if (!response) {
            co_return response.error();
          }
          auto encoded = detail::serialize(response.value().message);
          if (!encoded) {
            co_return encoded.error();
          }
          auto envelope = std::move(response).value();
          co_return detail::ByteResponse{Status::ok(), std::move(encoded).value(),
                                         std::move(envelope.metadata)};
        });
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate async client-streaming route");
  }
}

template <typename Request, typename ResponseMessage, typename Handler>
[[nodiscard]] Result<void>
register_async_bidirectional_streaming(Server& server, std::string_view service,
                                       std::string_view method,
                                       std::shared_ptr<AsyncRuntime> runtime, Handler handler) {
  try {
    return detail::register_async_bidirectional_bytes(
        server, service, method, std::move(runtime),
        [handler = std::move(handler)](
            CallContext context,
            std::shared_ptr<detail::OperationState> operation) -> Task<Status> {
          auto shared = operation;
          co_return co_await handler(std::move(context), AsyncServerReader<Request>(shared),
                                     AsyncServerWriter<ResponseMessage>(std::move(operation)));
        });
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate async bidirectional route");
  }
}

namespace detail {

template <typename T> Task<Result<T>> ready_error(Error error) { co_return error; }

template <typename ResponseMessage>
Task<Result<trevrpc::Response<ResponseMessage>>>
async_unary_impl(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
                 std::string service, std::string method, std::vector<std::byte> body,
                 OwnedAsyncCallOptions owned) {
  auto response =
      co_await async_unary_bytes(std::move(channel), std::move(runtime), std::move(service),
                                 std::move(method), std::move(body), std::move(owned));
  if (!response) {
    co_return response.error();
  }
  if (!response.value().status.is_ok()) {
    co_return Error::rpc(std::move(response.value().status));
  }
  auto message =
      parse<ResponseMessage>(response.value().body, "failed to parse async unary response");
  if (!message) {
    co_return message.error();
  }
  co_return trevrpc::Response<ResponseMessage>{std::move(message).value(),
                                               std::move(response.value().metadata)};
}

template <typename ResponseMessage>
Task<Result<AsyncServerStreamingCall<ResponseMessage>>>
async_server_streaming_impl(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
                            std::string service, std::string method, std::vector<std::byte> body,
                            OwnedAsyncCallOptions owned) {
  auto operation = co_await async_start_stream_bytes(
      std::move(channel), runtime, std::move(service), std::move(method),
      TREVRPC_RPC_KIND_SERVER_STREAMING, std::move(body), std::move(owned));
  if (!operation) {
    co_return operation.error();
  }
  auto finished = co_await operation_finish_send(operation.value(), {});
  if (!finished) {
    co_return finished.error();
  }
  co_return AsyncServerStreamingCall<ResponseMessage>{
      AsyncStreamReceiver<ResponseMessage>(std::move(operation).value())};
}

template <typename RequestMessage, typename ResponseMessage>
Task<Result<AsyncClientStreamingCall<RequestMessage, ResponseMessage>>>
async_streaming_pair_impl(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
                          std::string service, std::string method, std::uint32_t kind,
                          OwnedAsyncCallOptions owned) {
  auto operation =
      co_await async_start_stream_bytes(std::move(channel), runtime, std::move(service),
                                        std::move(method), kind, {}, std::move(owned));
  if (!operation) {
    co_return operation.error();
  }
  auto state = std::move(operation).value();
  co_return AsyncClientStreamingCall<RequestMessage, ResponseMessage>{
      AsyncStreamSender<RequestMessage>(state), AsyncStreamReceiver<ResponseMessage>(state)};
}

} // namespace detail

template <typename Request, typename Response>
[[nodiscard]] Task<Result<trevrpc::Response<Response>>>
async_unary(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
            std::string_view service, std::string_view method, const Request& request,
            const AsyncCallOptions& options = {}) {
  auto executor = runtime ? runtime->continuation_executor() : nullptr;
  auto body = detail::serialize(request);
  auto owned = detail::own_async_call_options(options);
  if (!body || !owned) {
    auto ready =
        detail::ready_error<trevrpc::Response<Response>>(!body ? body.error() : owned.error());
    ready.associate_executor(executor);
    return ready;
  }
  auto task = detail::async_unary_impl<Response>(std::move(channel), std::move(runtime),
                                                 std::string(service), std::string(method),
                                                 std::move(body).value(), std::move(owned).value());
  task.associate_executor(std::move(executor));
  return task;
}

template <typename Request, typename Response>
[[nodiscard]] Task<Result<AsyncServerStreamingCall<Response>>>
async_server_streaming(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
                       std::string_view service, std::string_view method, const Request& request,
                       const AsyncCallOptions& options = {}) {
  auto executor = runtime ? runtime->continuation_executor() : nullptr;
  auto body = detail::serialize(request);
  auto owned = detail::own_async_call_options(options);
  if (!body || !owned) {
    auto ready = detail::ready_error<AsyncServerStreamingCall<Response>>(!body ? body.error()
                                                                               : owned.error());
    ready.associate_executor(executor);
    return ready;
  }
  auto task = detail::async_server_streaming_impl<Response>(
      std::move(channel), std::move(runtime), std::string(service), std::string(method),
      std::move(body).value(), std::move(owned).value());
  task.associate_executor(std::move(executor));
  return task;
}

template <typename Request, typename Response>
[[nodiscard]] Task<Result<AsyncClientStreamingCall<Request, Response>>>
async_client_streaming(std::shared_ptr<Channel> channel, std::shared_ptr<AsyncRuntime> runtime,
                       std::string_view service, std::string_view method,
                       const AsyncCallOptions& options = {}) {
  auto executor = runtime ? runtime->continuation_executor() : nullptr;
  auto owned = detail::own_async_call_options(options);
  if (!owned) {
    auto ready = detail::ready_error<AsyncClientStreamingCall<Request, Response>>(owned.error());
    ready.associate_executor(executor);
    return ready;
  }
  auto task = detail::async_streaming_pair_impl<Request, Response>(
      std::move(channel), std::move(runtime), std::string(service), std::string(method),
      TREVRPC_RPC_KIND_CLIENT_STREAMING, std::move(owned).value());
  task.associate_executor(std::move(executor));
  return task;
}

template <typename Request, typename Response>
[[nodiscard]] Task<Result<AsyncBidirectionalStreamingCall<Request, Response>>>
async_bidirectional_streaming(std::shared_ptr<Channel> channel,
                              std::shared_ptr<AsyncRuntime> runtime, std::string_view service,
                              std::string_view method, const AsyncCallOptions& options = {}) {
  auto executor = runtime ? runtime->continuation_executor() : nullptr;
  auto owned = detail::own_async_call_options(options);
  if (!owned) {
    auto ready =
        detail::ready_error<AsyncBidirectionalStreamingCall<Request, Response>>(owned.error());
    ready.associate_executor(executor);
    return ready;
  }
  auto task = detail::async_streaming_pair_impl<Request, Response>(
      std::move(channel), std::move(runtime), std::string(service), std::string(method),
      TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, std::move(owned).value());
  task.associate_executor(std::move(executor));
  return task;
}

} // namespace trevrpc
