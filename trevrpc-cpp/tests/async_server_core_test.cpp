#include "detail/async_core.hpp"

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

class FakeCallOps final : public trevrpc::detail::RpcServerCallOps {
public:
  void release(trevrpc::Work completion) noexcept override {
    trevrpc::Work ready;
    {
      std::lock_guard lock(mutex_);
      ++release_count;
      if (release_cleanup_) {
        ready = std::move(completion);
      } else {
        cleanup_completion_ = std::move(completion);
      }
    }
    if (ready) {
      ready();
    }
    condition_.notify_all();
  }

  trevrpc::Result<void>
  start_respond(const trevrpc::Status&, std::span<const std::byte> body, const trevrpc::Metadata&,
                trevrpc::detail::NativeCompletion completion) noexcept override {
    trevrpc::detail::NativeCompletion ready;
    int error = 0;
    {
      std::lock_guard lock(mutex_);
      terminal_body.assign(body.begin(), body.end());
      events.emplace_back("respond");
      ++respond_count;
      terminal_entered_ = true;
      if (release_terminal_) {
        ready = std::move(completion);
        error = terminal_error;
      } else {
        terminal_completion_ = std::move(completion);
      }
      condition_.notify_all();
    }
    if (ready) {
      ready(error);
    }
    return {};
  }

  trevrpc::Result<void>
  start_finish(const trevrpc::Status&,
               trevrpc::detail::NativeCompletion completion) noexcept override {
    trevrpc::detail::NativeCompletion ready;
    int error = 0;
    {
      std::lock_guard lock(mutex_);
      events.emplace_back("finish");
      ++finish_count;
      terminal_entered_ = true;
      if (release_terminal_) {
        ready = std::move(completion);
        error = terminal_error;
      } else {
        terminal_completion_ = std::move(completion);
      }
      condition_.notify_all();
    }
    if (ready) {
      ready(error);
    }
    return {};
  }

  void cancel() noexcept override {
    ++cancel_count;
    release_terminal();
  }

  void close() noexcept override {
    ++close_count;
    release_terminal();
  }

  void wait_for_terminal() {
    std::unique_lock lock(mutex_);
    const bool entered = condition_.wait_for(lock, 2s, [this] { return terminal_entered_; });
    assert(entered);
  }

  void release_terminal() noexcept {
    trevrpc::detail::NativeCompletion completion;
    int error = 0;
    {
      std::lock_guard lock(mutex_);
      release_terminal_ = true;
      completion = std::move(terminal_completion_);
      error = terminal_error;
    }
    if (completion) {
      completion(error);
    }
    condition_.notify_all();
  }

  void wait_for_release() {
    std::unique_lock lock(mutex_);
    const bool released =
        condition_.wait_for(lock, 2s, [this] { return release_count.load() == 1; });
    assert(released);
  }

  void hold_cleanup() noexcept {
    std::lock_guard lock(mutex_);
    release_cleanup_ = false;
  }

  void release_cleanup() noexcept {
    trevrpc::Work completion;
    {
      std::lock_guard lock(mutex_);
      release_cleanup_ = true;
      completion = std::move(cleanup_completion_);
    }
    if (completion) {
      completion();
    }
    condition_.notify_all();
  }

  int terminal_error = 0;
  std::atomic<int> release_count{0};
  std::atomic<int> respond_count{0};
  std::atomic<int> finish_count{0};
  std::atomic<int> cancel_count{0};
  std::atomic<int> close_count{0};
  std::vector<std::byte> terminal_body;
  std::vector<std::string> events;

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  bool terminal_entered_ = false;
  bool release_terminal_ = false;
  bool release_cleanup_ = true;
  trevrpc::detail::NativeCompletion terminal_completion_;
  trevrpc::Work cleanup_completion_;
};

class FakeStreamOps final : public trevrpc::detail::NativeOps {
public:
  explicit FakeStreamOps(std::shared_ptr<FakeCallOps> calls) : calls_(std::move(calls)) {}

  trevrpc::Result<void> start_send(std::span<const std::byte>,
                                   trevrpc::detail::NativeCompletion completion) noexcept override {
    trevrpc::detail::NativeCompletion ready;
    int error = 0;
    {
      std::lock_guard lock(mutex_);
      calls_->events.emplace_back("send");
      send_entered_ = true;
      if (release_send_) {
        ready = std::move(completion);
        error = send_error;
      } else {
        send_completion_ = std::move(completion);
      }
      condition_.notify_all();
    }
    if (ready) {
      ready(error);
    }
    return {};
  }

  trevrpc::Result<void>
  start_finish_send(trevrpc::detail::NativeCompletion completion) noexcept override {
    completion(0);
    return {};
  }

  trevrpc::Result<void>
  start_receive(trevrpc::detail::NativeReceiveCompletion completion) noexcept override {
    std::lock_guard lock(mutex_);
    receive_completion_ = std::move(completion);
    return {};
  }

  trevrpc::Result<void> schedule_at(trevrpc::Deadline deadline,
                                    trevrpc::Work work) noexcept override {
    if (!work) {
      return trevrpc::Error::runtime(-EINVAL);
    }
    try {
      std::thread timer([deadline, work = std::move(work)]() mutable noexcept {
        std::this_thread::sleep_until(deadline);
        try {
          work();
        } catch (...) {
          (void)std::current_exception();
        }
      });
      timer.detach();
      return {};
    } catch (...) {
      return trevrpc::Error::runtime(-ENOMEM);
    }
  }

  void cancel() noexcept override {
    ++cancel_count;
    release_send();
    trevrpc::detail::NativeReceiveCompletion completion;
    {
      std::lock_guard lock(mutex_);
      completion = std::move(receive_completion_);
    }
    if (completion) {
      completion(trevrpc::Error::runtime(-ECANCELED));
    }
  }

  trevrpc::Result<void> close() noexcept override {
    ++close_count;
    return {};
  }

  void wait_for_send() {
    std::unique_lock lock(mutex_);
    const bool entered = condition_.wait_for(lock, 2s, [this] { return send_entered_; });
    assert(entered);
  }

  void release_send() noexcept {
    trevrpc::detail::NativeCompletion completion;
    int error = 0;
    {
      std::lock_guard lock(mutex_);
      release_send_ = true;
      completion = std::move(send_completion_);
      error = send_error;
    }
    if (completion) {
      completion(error);
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
  trevrpc::detail::NativeCompletion send_completion_;
  trevrpc::detail::NativeReceiveCompletion receive_completion_;
};

struct RuntimeFixture {
  RuntimeFixture() {
    auto continuation_result =
        trevrpc::ThreadPoolExecutor::create({.worker_count = 2, .queue_capacity = 64});
    assert(continuation_result);
    continuation = std::move(continuation_result).value();
    auto runtime_result = trevrpc::AsyncRuntime::create(continuation);
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

struct TerminalResult {
  std::mutex mutex;
  std::condition_variable condition;
  std::optional<trevrpc::detail::ServerCallTerminal> value;
  std::size_t count = 0;

  void record(trevrpc::detail::ServerCallTerminal terminal) {
    {
      std::lock_guard lock(mutex);
      value = terminal;
      ++count;
    }
    condition.notify_all();
  }

  trevrpc::detail::ServerCallTerminal wait() {
    std::unique_lock lock(mutex);
    const bool completed = condition.wait_for(lock, 2s, [this] { return value.has_value(); });
    assert(completed);
    return *value;
  }
};

std::shared_ptr<trevrpc::detail::ServerCallState>
make_call(RuntimeFixture& fixture, std::uint32_t kind, const std::shared_ptr<FakeCallOps>& calls,
          const std::shared_ptr<FakeStreamOps>& streams) {
  auto result =
      trevrpc::detail::ServerCallState::create_for_test(kind, fixture.runtime, calls, streams);
  assert(result);
  return std::move(result).value();
}

void test_terminal_orders_after_messages() {
  RuntimeFixture fixture;
  auto calls = std::make_shared<FakeCallOps>();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

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

void test_error_responses_observe_zero_body_once() {
  for (const trevrpc::StatusCode status :
       {trevrpc::StatusCode::PermissionDenied, trevrpc::StatusCode::Unimplemented,
        trevrpc::StatusCode::Internal}) {
    RuntimeFixture fixture;
    auto calls = std::make_shared<FakeCallOps>();
    calls->release_terminal();
    auto streams = std::make_shared<FakeStreamOps>(calls);
    auto state = make_call(fixture, TREVRPC_RPC_KIND_UNARY, calls, streams);
    TerminalResult observed;
    auto* raw_state = state.get();
    state->set_terminal_observer([&](trevrpc::detail::ServerCallTerminal terminal) {
      assert(raw_state->snapshot().terminal == trevrpc::detail::ServerTerminalPhase::Settled);
      observed.record(terminal);
    });

    assert(trevrpc::sync_wait(state->respond({}, trevrpc::Status(status, "failed"))));
    const auto terminal = observed.wait();
    assert(terminal.status == status);
    assert(terminal.response_body_size == 0);
    {
      std::lock_guard lock(observed.mutex);
      assert(observed.count == 1);
    }
    calls->wait_for_release();
  }
}

void test_successful_response_observes_exact_body_after_settlement() {
  const std::vector<std::byte> body{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  for (const std::uint32_t kind : {TREVRPC_RPC_KIND_UNARY, TREVRPC_RPC_KIND_CLIENT_STREAMING}) {
    RuntimeFixture fixture;
    auto calls = std::make_shared<FakeCallOps>();
    calls->release_terminal();
    auto streams = std::make_shared<FakeStreamOps>(calls);
    auto state = make_call(fixture, kind, calls, streams);

    assert(trevrpc::sync_wait(state->respond(body, trevrpc::Status::ok())));
    TerminalResult observed;
    state->set_terminal_observer(
        [&](trevrpc::detail::ServerCallTerminal terminal) { observed.record(terminal); });
    const auto terminal = observed.wait();
    assert(terminal.status == trevrpc::StatusCode::Ok);
    assert(terminal.response_body_size == body.size());
    {
      std::lock_guard lock(observed.mutex);
      assert(observed.count == 1);
    }
    calls->wait_for_release();
    std::size_t cleanup_count = 0;
    auto* raw_state = state.get();
    state->set_cleanup_observer([&] {
      assert(raw_state->snapshot().pin_released);
      ++cleanup_count;
    });
    state->set_cleanup_observer([&] { ++cleanup_count; });
    assert(cleanup_count == 1);
  }
}

void test_failed_response_observes_selected_status_with_zero_body() {
  RuntimeFixture fixture;
  auto calls = std::make_shared<FakeCallOps>();
  calls->terminal_error = -EIO;
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_UNARY, calls, streams);
  TerminalResult observed;
  state->set_terminal_observer(
      [&](trevrpc::detail::ServerCallTerminal terminal) { observed.record(terminal); });

  const std::vector<std::byte> body{std::byte{1}, std::byte{2}};
  const auto result = trevrpc::sync_wait(state->respond(body, trevrpc::Status::ok()));
  assert(!result);
  const auto terminal = observed.wait();
  assert(terminal.status == trevrpc::StatusCode::Ok);
  assert(terminal.response_body_size == 0);
  calls->wait_for_release();
}

void test_finish_and_stop_observe_status_with_zero_body() {
  {
    RuntimeFixture fixture;
    auto calls = std::make_shared<FakeCallOps>();
    calls->release_terminal();
    auto streams = std::make_shared<FakeStreamOps>(calls);
    auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);
    TerminalResult observed;
    state->set_terminal_observer(
        [&](trevrpc::detail::ServerCallTerminal terminal) { observed.record(terminal); });

    assert(trevrpc::sync_wait(
        state->finish(trevrpc::Status(trevrpc::StatusCode::Internal, "failed"))));
    const auto terminal = observed.wait();
    assert(terminal.status == trevrpc::StatusCode::Internal);
    assert(terminal.response_body_size == 0);
    calls->wait_for_release();
  }

  const std::vector<std::pair<trevrpc::detail::ServerStopReason, trevrpc::StatusCode>> stops{
      {trevrpc::detail::ServerStopReason::Deadline, trevrpc::StatusCode::DeadlineExceeded},
      {trevrpc::detail::ServerStopReason::PeerCancellation, trevrpc::StatusCode::Cancelled},
      {trevrpc::detail::ServerStopReason::LocalClose, trevrpc::StatusCode::Cancelled},
      {trevrpc::detail::ServerStopReason::ServerCancellation, trevrpc::StatusCode::Cancelled},
  };
  for (const auto& [reason, status] : stops) {
    RuntimeFixture fixture;
    auto calls = std::make_shared<FakeCallOps>();
    auto streams = std::make_shared<FakeStreamOps>(calls);
    auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);
    TerminalResult observed;
    state->set_terminal_observer(
        [&](trevrpc::detail::ServerCallTerminal terminal) { observed.record(terminal); });

    state->stop(reason);
    const auto terminal = observed.wait();
    assert(terminal.status == status);
    assert(terminal.response_body_size == 0);
    calls->wait_for_release();
  }
}

void test_duplicate_terminal_attempts_share_winner() {
  RuntimeFixture fixture;
  auto calls = std::make_shared<FakeCallOps>();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

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
  auto calls = std::make_shared<FakeCallOps>();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

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
  auto calls = std::make_shared<FakeCallOps>();
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  auto result = trevrpc::sync_wait(state->finish(trevrpc::Status::ok()));
  state->stop(trevrpc::detail::ServerStopReason::Deadline);
  assert(result);
  calls->wait_for_release();
  assert(calls->close_count.load() == 0);
}

void test_terminal_failure_closes_deferred_call() {
  RuntimeFixture fixture;
  auto calls = std::make_shared<FakeCallOps>();
  calls->terminal_error = -EIO;
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  auto result = trevrpc::sync_wait(state->finish(trevrpc::Status::ok()));
  assert(!result);
  assert(result.error().code() == -EIO);
  calls->wait_for_release();
  assert(calls->close_count.load() == 1);
  assert(calls->release_count.load() == 1);
}

void test_native_completion_before_cpp_winner() {
  RuntimeFixture fixture;
  auto calls = std::make_shared<FakeCallOps>();
  calls->terminal_error = -EALREADY;
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);

  auto result = trevrpc::sync_wait(state->finish(trevrpc::Status::ok()));
  assert(!result);
  assert(result.error().code() == -ECANCELED);
  calls->wait_for_release();
  assert(calls->close_count.load() == 0);
  assert(calls->release_count.load() == 1);
}

void test_lazy_terminal_task_owns_server_state_eagerly() {
  RuntimeFixture fixture;
  auto calls = std::make_shared<FakeCallOps>();
  calls->release_terminal();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);
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
  auto calls = std::make_shared<FakeCallOps>();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, calls, streams);
  state.reset();
  calls->wait_for_release();
  assert(calls->close_count.load() == 1);
  assert(calls->release_count.load() == 1);
  assert(streams->close_count.load() == 0);
}

void test_scope_remains_active_until_native_cleanup() {
  RuntimeFixture fixture;
  auto calls = std::make_shared<FakeCallOps>();
  calls->hold_cleanup();
  auto streams = std::make_shared<FakeStreamOps>(calls);
  auto state = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, calls, streams);
  auto scope = std::make_shared<trevrpc::detail::ServerScope>();
  const auto scope_id = scope->add(state);
  assert(scope_id);
  std::atomic<std::size_t> terminal_count{0};
  std::atomic<std::size_t> cleanup_count{0};
  state->set_terminal_observer(
      [weak_scope = std::weak_ptr<trevrpc::detail::ServerScope>(scope), &terminal_count](auto) {
        const auto current = weak_scope.lock();
        assert(current);
        assert(current->active() == 1);
        terminal_count.fetch_add(1);
      });
  state->set_cleanup_observer([weak_scope = std::weak_ptr<trevrpc::detail::ServerScope>(scope),
                               id = scope_id.value(), calls, &cleanup_count] {
    assert(calls->release_count.load() == 1);
    cleanup_count.fetch_add(1);
    if (auto current = weak_scope.lock()) {
      current->complete(id);
    }
  });

  assert(scope->active() == 1);
  SpawnedResult finished;
  assert(trevrpc::spawn(state->finish(trevrpc::Status::ok()),
                        [&](auto result) { finished.complete(std::move(result)); }));
  calls->wait_for_terminal();
  assert(scope->active() == 1);
  calls->release_terminal();
  assert(finished.wait());
  calls->wait_for_release();
  const auto pending = scope->drain_until(trevrpc::Deadline::clock::now() + 10ms);
  assert(!pending);
  assert(pending.error().code() == -ETIMEDOUT);
  assert(scope->active() == 1);
  assert(cleanup_count.load() == 0);

  calls->release_cleanup();
  assert(scope->drain_until(trevrpc::Deadline::clock::now() + 1s));
  assert(scope->active() == 0);
  assert(terminal_count.load() == 1);
  assert(cleanup_count.load() == 1);
}

void test_server_scope_cancels_and_drains() {
  RuntimeFixture fixture;
  auto first_calls = std::make_shared<FakeCallOps>();
  auto second_calls = std::make_shared<FakeCallOps>();
  auto first_streams = std::make_shared<FakeStreamOps>(first_calls);
  auto second_streams = std::make_shared<FakeStreamOps>(second_calls);
  auto first = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, first_calls, first_streams);
  auto second = make_call(fixture, TREVRPC_RPC_KIND_SERVER_STREAMING, second_calls, second_streams);

  trevrpc::detail::ServerScope scope;
  auto first_id = scope.add(first);
  auto second_id = scope.add(second);
  assert(first_id);
  assert(second_id);
  first->set_cleanup_observer([&scope, id = first_id.value()] { scope.complete(id); });
  second->set_cleanup_observer([&scope, id = second_id.value()] { scope.complete(id); });
  assert(scope.active() == 2);
  scope.request_stop();
  assert(first_calls->close_count.load() == 1);
  assert(second_calls->close_count.load() == 1);
  assert(scope.drain_until(trevrpc::Deadline::clock::now() + 1s));
  assert(scope.active() == 0);
  first_calls->wait_for_release();
  second_calls->wait_for_release();
}

} // namespace

int main() {
  test_terminal_orders_after_messages();
  test_error_responses_observe_zero_body_once();
  test_successful_response_observes_exact_body_after_settlement();
  test_failed_response_observes_selected_status_with_zero_body();
  test_finish_and_stop_observe_status_with_zero_body();
  test_duplicate_terminal_attempts_share_winner();
  test_deadline_beats_unsettled_terminal();
  test_settled_terminal_beats_late_deadline();
  test_terminal_failure_closes_deferred_call();
  test_native_completion_before_cpp_winner();
  test_lazy_terminal_task_owns_server_state_eagerly();
  test_abandonment_closes_without_closing_embedded_stream();
  test_scope_remains_active_until_native_cleanup();
  test_server_scope_cancels_and_drains();
  return 0;
}
