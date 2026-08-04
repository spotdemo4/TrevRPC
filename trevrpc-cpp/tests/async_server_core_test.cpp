#include "detail/async_core.hpp"

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

struct FakeHandles {
  alignas(void*) std::byte call_storage{};
  alignas(void*) std::byte stream_storage{};

  [[nodiscard]] trevrpc_call* call() noexcept {
    return reinterpret_cast<trevrpc_call*>(&call_storage);
  }
  [[nodiscard]] trevrpc_stream* stream() noexcept {
    return reinterpret_cast<trevrpc_stream*>(&stream_storage);
  }
};

class FakeCallOps final : public trevrpc::detail::ServerCallOps {
public:
  explicit FakeCallOps(trevrpc_stream* stream) : stream_(stream) {}

  int defer(trevrpc_call*) noexcept override {
    ++defer_count;
    return defer_error;
  }
  int retain(trevrpc_call*) noexcept override {
    ++retain_count;
    return retain_error;
  }
  void release(trevrpc_call*) noexcept override {
    ++release_count;
    condition_.notify_all();
  }
  trevrpc_stream* stream(trevrpc_call*) noexcept override { return stream_; }

  int respond(trevrpc_call*, const trevrpc::Status&, std::span<const std::byte> body,
              const trevrpc::Metadata&) noexcept override {
    {
      std::unique_lock lock(mutex_);
      terminal_body.assign(body.begin(), body.end());
      events.emplace_back("respond");
      ++respond_count;
      terminal_entered_ = true;
      condition_.notify_all();
      condition_.wait(lock, [this] { return release_terminal_; });
    }
    return terminal_error;
  }

  int finish(trevrpc_call*, const trevrpc::Status&) noexcept override {
    {
      std::unique_lock lock(mutex_);
      events.emplace_back("finish");
      ++finish_count;
      terminal_entered_ = true;
      condition_.notify_all();
      condition_.wait(lock, [this] { return release_terminal_; });
    }
    return terminal_error;
  }

  void cancel(trevrpc_call*) noexcept override {
    ++cancel_count;
    release_terminal();
  }

  void close(trevrpc_call*) noexcept override {
    ++close_count;
    release_terminal();
  }

  void wait_for_terminal() {
    std::unique_lock lock(mutex_);
    const bool entered = condition_.wait_for(lock, 2s, [this] { return terminal_entered_; });
    assert(entered);
  }

  void release_terminal() noexcept {
    {
      std::lock_guard lock(mutex_);
      release_terminal_ = true;
    }
    condition_.notify_all();
  }

  void wait_for_release() {
    std::unique_lock lock(mutex_);
    const bool released =
        condition_.wait_for(lock, 2s, [this] { return release_count.load() == 1; });
    assert(released);
  }

  int defer_error = 0;
  int retain_error = 0;
  int terminal_error = 0;
  std::atomic<int> defer_count{0};
  std::atomic<int> retain_count{0};
  std::atomic<int> release_count{0};
  std::atomic<int> respond_count{0};
  std::atomic<int> finish_count{0};
  std::atomic<int> cancel_count{0};
  std::atomic<int> close_count{0};
  std::vector<std::byte> terminal_body;
  std::vector<std::string> events;

private:
  trevrpc_stream* stream_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool terminal_entered_ = false;
  bool release_terminal_ = false;
};

class FakeStreamOps final : public trevrpc::detail::NativeOps {
public:
  explicit FakeStreamOps(std::shared_ptr<FakeCallOps> calls) : calls_(std::move(calls)) {}

  int send(trevrpc_stream*, std::span<const std::byte>) noexcept override {
    std::unique_lock lock(mutex_);
    calls_->events.emplace_back("send");
    send_entered_ = true;
    condition_.notify_all();
    condition_.wait(lock, [this] { return release_send_; });
    return send_error;
  }

  int finish_send(trevrpc_stream*) noexcept override { return 0; }

  trevrpc::Result<std::optional<trevrpc::detail::StreamFrame>>
  receive_ready_since(trevrpc_stream*, std::uint64_t) noexcept override {
    return std::optional<trevrpc::detail::StreamFrame>{};
  }

  void cancel(trevrpc_stream*) noexcept override {
    ++cancel_count;
    release_send();
  }

  void close(trevrpc_stream*) noexcept override { ++close_count; }

  void wait_for_send() {
    std::unique_lock lock(mutex_);
    const bool entered = condition_.wait_for(lock, 2s, [this] { return send_entered_; });
    assert(entered);
  }

  void release_send() noexcept {
    {
      std::lock_guard lock(mutex_);
      release_send_ = true;
    }
    condition_.notify_all();
  }

  int send_error = 0;
  std::atomic<int> cancel_count{0};
  std::atomic<int> close_count{0};

private:
  std::shared_ptr<FakeCallOps> calls_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool send_entered_ = false;
  bool release_send_ = false;
};

struct RuntimeFixture {
  RuntimeFixture() {
    auto continuation_result =
        trevrpc::ThreadPoolExecutor::create({.worker_count = 2, .queue_capacity = 64});
    assert(continuation_result);
    continuation = std::move(continuation_result).value();
    trevrpc::AsyncRuntimeOptions options;
    options.native_io_worker_count = 2;
    options.native_io_queue_capacity = 64;
    auto runtime_result = trevrpc::AsyncRuntime::create(continuation, options);
    assert(runtime_result);
    runtime = std::move(runtime_result).value();
  }

  ~RuntimeFixture() {
    runtime.reset();
    continuation->request_stop();
    assert(continuation->drain_until(trevrpc::Deadline::clock::now() + 2s));
  }

  std::shared_ptr<trevrpc::ThreadPoolExecutor> continuation;
  std::shared_ptr<trevrpc::AsyncRuntime> runtime;
};

struct SpawnedResult {
  std::mutex mutex;
  std::condition_variable condition;
  bool done = false;
  std::optional<trevrpc::Result<void>> value;

  void complete(trevrpc::TaskCompletion<trevrpc::Result<void>> completion) {
    assert(!completion.exception);
    assert(completion.value.has_value());
    std::lock_guard lock(mutex);
    value.emplace(std::move(completion.value).value());
    done = true;
    condition.notify_all();
  }

  trevrpc::Result<void> wait() {
    std::unique_lock lock(mutex);
    const bool completed = condition.wait_for(lock, 2s, [this] { return done; });
    assert(completed);
    return std::move(value).value();
  }
};

std::shared_ptr<trevrpc::detail::ServerCallState>
make_call(RuntimeFixture& fixture, FakeHandles& handles, std::uint32_t kind,
          const std::shared_ptr<FakeCallOps>& calls,
          const std::shared_ptr<FakeStreamOps>& streams) {
  auto result = trevrpc::detail::ServerCallState::create(handles.call(), kind, fixture.runtime,
                                                         calls, streams);
  assert(result);
  assert(calls->defer_count.load() == 1);
  assert(calls->retain_count.load() == 1);
  return std::move(result).value();
}

void test_defer_failure_does_not_claim_deferred_ownership() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  calls->defer_error = -EIO;
  auto streams = std::make_shared<FakeStreamOps>(calls);
  bool deferred = true;
  auto result =
      trevrpc::detail::ServerCallState::create(handles.call(), TREVRPC_RPC_KIND_SERVER_STREAMING,
                                               fixture.runtime, calls, streams, &deferred);
  assert(!result);
  assert(result.error().code() == -EIO);
  assert(!deferred);
  assert(calls->defer_count.load() == 1);
  assert(calls->retain_count.load() == 0);
  assert(calls->close_count.load() == 0);
  assert(calls->release_count.load() == 0);
}

void test_post_defer_failure_preserves_deferred_ownership_signal() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  calls->retain_error = -ENOMEM;
  auto streams = std::make_shared<FakeStreamOps>(calls);
  bool deferred = false;
  auto result =
      trevrpc::detail::ServerCallState::create(handles.call(), TREVRPC_RPC_KIND_SERVER_STREAMING,
                                               fixture.runtime, calls, streams, &deferred);
  assert(!result);
  assert(result.error().code() == -ENOMEM);
  assert(deferred);
  assert(calls->defer_count.load() == 1);
  assert(calls->retain_count.load() == 1);
  assert(calls->close_count.load() == 1);
  assert(calls->release_count.load() == 0);
}

void test_terminal_orders_after_messages() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, handles, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  SpawnedResult sent;
  auto send_started = trevrpc::spawn(state->operation()->send(
                                         1,
                                         [] {
                                           return trevrpc::Result<std::vector<std::byte>>(
                                               std::vector<std::byte>{std::byte{1}});
                                         },
                                         {}, false),
                                     [&](trevrpc::TaskCompletion<trevrpc::Result<void>> result) {
                                       sent.complete(std::move(result));
                                     });
  assert(send_started);
  streams->wait_for_send();

  SpawnedResult finished;
  auto finish_started = trevrpc::spawn(state->finish(trevrpc::Status::ok()),
                                       [&](trevrpc::TaskCompletion<trevrpc::Result<void>> result) {
                                         finished.complete(std::move(result));
                                       });
  assert(finish_started);
  assert(calls->finish_count.load() == 0);

  streams->release_send();
  calls->wait_for_terminal();
  assert(calls->events.size() == 2);
  assert(calls->events[0] == "send");
  assert(calls->events[1] == "finish");
  calls->release_terminal();
  assert(sent.wait());
  assert(finished.wait());
  calls->wait_for_release();
  assert(calls->finish_count.load() == 1);
  assert(calls->close_count.load() == 0);
  assert(streams->close_count.load() == 0);
}

void test_duplicate_terminal_attempts_share_winner() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, handles, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  SpawnedResult first;
  SpawnedResult second;
  assert(trevrpc::spawn(state->finish(trevrpc::Status::ok()),
                        [&](auto result) { first.complete(std::move(result)); }));
  calls->wait_for_terminal();
  assert(trevrpc::spawn(state->finish(trevrpc::Status(trevrpc::StatusCode::Internal, "loser")),
                        [&](auto result) { second.complete(std::move(result)); }));
  calls->release_terminal();
  assert(first.wait());
  assert(second.wait());
  calls->wait_for_release();
  assert(calls->finish_count.load() == 1);
  assert(calls->release_count.load() == 1);
}

void test_deadline_beats_unsettled_terminal() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, handles, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  SpawnedResult finished;
  assert(trevrpc::spawn(state->finish(trevrpc::Status::ok()),
                        [&](auto result) { finished.complete(std::move(result)); }));
  calls->wait_for_terminal();
  state->stop(trevrpc::detail::ServerStopReason::Deadline);
  auto result = finished.wait();
  assert(!result);
  assert(result.error().code() == -ETIMEDOUT);
  calls->wait_for_release();
  assert(calls->close_count.load() == 1);
  assert(calls->release_count.load() == 1);
}

void test_settled_terminal_beats_late_deadline() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, handles, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  auto result = trevrpc::sync_wait(state->finish(trevrpc::Status::ok()));
  state->stop(trevrpc::detail::ServerStopReason::Deadline);
  assert(result);
  calls->wait_for_release();
  assert(calls->close_count.load() == 0);
}

void test_terminal_failure_closes_deferred_call() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  calls->terminal_error = -EIO;
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, handles, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  auto result = trevrpc::sync_wait(state->finish(trevrpc::Status::ok()));
  assert(!result);
  assert(result.error().code() == -EIO);
  calls->wait_for_release();
  assert(calls->close_count.load() == 1);
  assert(calls->release_count.load() == 1);
}

void test_native_completion_before_cpp_winner() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  calls->terminal_error = -EALREADY;
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, handles, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  auto result = trevrpc::sync_wait(state->finish(trevrpc::Status::ok()));
  assert(!result);
  assert(result.error().code() == -ECANCELED);
  calls->wait_for_release();
  assert(calls->close_count.load() == 0);
  assert(calls->release_count.load() == 1);
}

void test_lazy_terminal_task_owns_server_state_eagerly() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, handles, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);
  std::weak_ptr<trevrpc::detail::ServerCallState> weak = state;
  auto task = state->finish(trevrpc::Status::ok());
  state.reset();
  assert(!weak.expired());
  assert(trevrpc::sync_wait(std::move(task)));
  calls->wait_for_release();
  assert(weak.expired());
}

void test_abandonment_closes_without_closing_embedded_stream() {
  RuntimeFixture fixture;
  FakeHandles handles;
  auto calls = std::make_shared<FakeCallOps>(handles.stream());
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state =
      make_call(fixture, handles, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, calls, streams);
  state.reset();
  calls->wait_for_release();
  assert(calls->close_count.load() == 1);
  assert(calls->release_count.load() == 1);
  assert(streams->close_count.load() == 0);
}

void test_server_scope_cancels_and_drains() {
  RuntimeFixture fixture;
  FakeHandles first_handles;
  FakeHandles second_handles;
  auto first_calls = std::make_shared<FakeCallOps>(first_handles.stream());
  auto second_calls = std::make_shared<FakeCallOps>(second_handles.stream());
  auto first_streams = std::make_shared<FakeStreamOps>(first_calls);
  auto second_streams = std::make_shared<FakeStreamOps>(second_calls);
  auto first = make_call(fixture, first_handles, TREVRPC_RPC_KIND_SERVER_STREAMING, first_calls,
                         first_streams);
  auto second = make_call(fixture, second_handles, TREVRPC_RPC_KIND_SERVER_STREAMING, second_calls,
                          second_streams);

  trevrpc::detail::ServerScope scope;
  auto first_id = scope.add(first);
  auto second_id = scope.add(second);
  assert(first_id);
  assert(second_id);
  assert(scope.active() == 2);
  scope.request_stop();
  assert(first_calls->close_count.load() == 1);
  assert(second_calls->close_count.load() == 1);
  auto timed_out = scope.drain_until(trevrpc::Deadline::clock::now());
  assert(!timed_out);
  assert(timed_out.error().code() == -ETIMEDOUT);
  scope.complete(first_id.value());
  scope.complete(second_id.value());
  assert(scope.drain_until(trevrpc::Deadline::clock::now() + 1s));
  assert(scope.active() == 0);
  first_calls->wait_for_release();
  second_calls->wait_for_release();
}

} // namespace

int main() {
  test_defer_failure_does_not_claim_deferred_ownership();
  test_post_defer_failure_preserves_deferred_ownership_signal();
  test_terminal_orders_after_messages();
  test_duplicate_terminal_attempts_share_winner();
  test_deadline_beats_unsettled_terminal();
  test_settled_terminal_beats_late_deadline();
  test_terminal_failure_closes_deferred_call();
  test_native_completion_before_cpp_winner();
  test_lazy_terminal_task_owns_server_state_eagerly();
  test_abandonment_closes_without_closing_embedded_stream();
  test_server_scope_cancels_and_drains();
  return 0;
}
