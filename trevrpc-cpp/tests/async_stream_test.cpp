#include "detail/async_core.hpp"

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

class FakeNativeOps final : public trevrpc::detail::NativeOps {
public:
  int send(trevrpc_stream*, std::span<const std::byte> body) noexcept override {
    std::unique_lock lock(mutex_);
    sends_.emplace_back(body.begin(), body.end());
    send_entered_ = true;
    condition_.notify_all();
    condition_.wait(lock, [this] { return release_send_; });
    return send_error_;
  }

  int finish_send(trevrpc_stream*) noexcept override { return 0; }

  trevrpc::Result<std::optional<trevrpc::detail::StreamFrame>>
  receive_ready_since(trevrpc_stream*, std::uint64_t) noexcept override {
    std::unique_lock lock(mutex_);
    ++receive_polls_;
    condition_.notify_all();
    if (block_receive_) {
      condition_.wait(lock, [this] { return release_receive_; });
    }
    return std::exchange(next_receive_, std::nullopt);
  }

  void cancel(trevrpc_stream*) noexcept override {
    cancelled_ = true;
    condition_.notify_all();
  }

  void close(trevrpc_stream*) noexcept override {
    closed_ = true;
    condition_.notify_all();
  }

  void wait_for_send() {
    std::unique_lock lock(mutex_);
    condition_.wait(lock, [this] { return send_entered_; });
  }

  void release_send() {
    {
      std::lock_guard lock(mutex_);
      release_send_ = true;
    }
    condition_.notify_all();
  }

  void block_receive() {
    std::lock_guard lock(mutex_);
    block_receive_ = true;
  }

  void set_receive(trevrpc::detail::StreamFrame frame) {
    std::lock_guard lock(mutex_);
    next_receive_ = std::move(frame);
  }

  void wait_for_receive_poll() {
    std::unique_lock lock(mutex_);
    condition_.wait_for(lock, 2s, [this] { return receive_polls_.load() > 0; });
  }

  void release_receive() {
    {
      std::lock_guard lock(mutex_);
      release_receive_ = true;
    }
    condition_.notify_all();
  }

  void wait_for_close() {
    std::unique_lock lock(mutex_);
    condition_.wait_for(lock, 2s, [this] { return closed_.load(); });
  }

  [[nodiscard]] bool send_entered() noexcept {
    std::lock_guard lock(mutex_);
    return send_entered_;
  }
  [[nodiscard]] bool cancelled() const noexcept { return cancelled_.load(); }
  [[nodiscard]] bool closed() const noexcept { return closed_.load(); }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<std::vector<std::byte>> sends_;
  bool send_entered_ = false;
  bool release_send_ = false;
  bool block_receive_ = false;
  bool release_receive_ = false;
  std::optional<trevrpc::detail::StreamFrame> next_receive_;
  int send_error_ = 0;
  std::atomic<int> receive_polls_{0};
  std::atomic<bool> cancelled_{false};
  std::atomic<bool> closed_{false};
};

} // namespace

int main() {
  auto saturated_continuation_result =
      trevrpc::ThreadPoolExecutor::create({.worker_count = 1, .queue_capacity = 1});
  assert(saturated_continuation_result);
  auto saturated_continuation = std::move(saturated_continuation_result).value();
  auto held_reservation = saturated_continuation->try_reserve();
  assert(held_reservation);
  trevrpc::AsyncRuntimeOptions saturated_options;
  saturated_options.native_io_worker_count = 1;
  saturated_options.native_io_queue_capacity = 1;
  auto saturated_runtime_result =
      trevrpc::AsyncRuntime::create(saturated_continuation, saturated_options);
  assert(saturated_runtime_result);
  auto saturated_runtime = std::move(saturated_runtime_result).value();
  auto saturated_native = std::make_shared<FakeNativeOps>();
  alignas(void*) std::byte saturated_stream_storage{};
  auto* saturated_stream = reinterpret_cast<trevrpc_stream*>(&saturated_stream_storage);
  auto saturated_operation = trevrpc::detail::OperationState::create(
      saturated_stream, saturated_runtime, saturated_native, {});
  assert(saturated_operation);
  auto executor_saturated = trevrpc::sync_wait(saturated_operation->send(
      1,
      [] { return trevrpc::Result<std::vector<std::byte>>(std::vector<std::byte>{std::byte{0}}); },
      {}, false));
  assert(!executor_saturated);
  assert(executor_saturated.error().code() == -EAGAIN);
  assert(!saturated_native->send_entered());
  saturated_operation->close();
  saturated_runtime.reset();
  std::move(held_reservation).value().cancel();
  saturated_continuation->request_stop();
  assert(saturated_continuation->drain_until(trevrpc::Deadline::clock::now() + 2s));

  auto continuation_result =
      trevrpc::ThreadPoolExecutor::create({.worker_count = 2, .queue_capacity = 32});
  assert(continuation_result);
  auto continuation = std::move(continuation_result).value();

  trevrpc::AsyncRuntimeOptions options;
  options.native_io_worker_count = 2;
  options.native_io_queue_capacity = 32;
  options.max_pending_sends_per_stream = 1;
  options.max_pending_send_bytes_per_stream = 8;
  options.max_waiting_senders_per_stream = 1;
  options.receive_poll_min = 10s;
  options.receive_poll_max = 10s;
  auto runtime_result = trevrpc::AsyncRuntime::create(continuation, options);
  assert(runtime_result);
  auto runtime = std::move(runtime_result).value();

  auto native = std::make_shared<FakeNativeOps>();
  alignas(void*) std::byte stream_storage{};
  auto* fake_stream = reinterpret_cast<trevrpc_stream*>(&stream_storage);
  auto operation = trevrpc::detail::OperationState::create(fake_stream, runtime, native, {});
  assert(operation);

  std::atomic<int> serialized{0};
  std::mutex completion_mutex;
  std::condition_variable completion_condition;
  bool first_done = false;
  auto first_task = operation->send(
      1,
      [&] {
        ++serialized;
        return trevrpc::Result<std::vector<std::byte>>(std::vector<std::byte>{std::byte{1}});
      },
      {}, false);
  auto spawned = trevrpc::spawn(std::move(first_task),
                                [&](trevrpc::TaskCompletion<trevrpc::Result<void>> result) {
                                  assert(!result.exception);
                                  assert(result.value.has_value());
                                  assert(result.value.value());
                                  {
                                    std::lock_guard lock(completion_mutex);
                                    first_done = true;
                                  }
                                  completion_condition.notify_all();
                                });
  assert(spawned);
  native->wait_for_send();

  auto saturated = trevrpc::sync_wait(operation->send(
      1,
      [&] {
        ++serialized;
        return trevrpc::Result<std::vector<std::byte>>(std::vector<std::byte>{std::byte{2}});
      },
      {}, false));
  assert(!saturated);
  assert(saturated.error().code() == -ENOBUFS);
  assert(serialized.load() == 1);

  native->release_send();
  {
    std::unique_lock lock(completion_mutex);
    completion_condition.wait_for(lock, 2s, [&] { return first_done; });
  }
  assert(first_done);

  bool receive_done = false;
  auto receiver = trevrpc::spawn(
      operation->receive(),
      [&](trevrpc::TaskCompletion<trevrpc::Result<trevrpc::detail::StreamFrame>> result) {
        assert(!result.exception);
        assert(result.value.has_value());
        assert(!result.value.value());
        assert(result.value.value().error().code() == -ECANCELED);
        {
          std::lock_guard lock(completion_mutex);
          receive_done = true;
        }
        completion_condition.notify_all();
      });
  assert(receiver);
  native->wait_for_receive_poll();

  auto duplicate = trevrpc::sync_wait(operation->receive());
  assert(!duplicate);
  assert(duplicate.error().code() == -EBUSY);

  operation->cancel();
  {
    std::unique_lock lock(completion_mutex);
    completion_condition.wait_for(lock, 2s, [&] { return receive_done; });
  }
  assert(receive_done);
  assert(native->cancelled());

  operation->close();
  native->wait_for_close();
  assert(native->closed());

  auto close_native = std::make_shared<FakeNativeOps>();
  auto close_operation =
      trevrpc::detail::OperationState::create(fake_stream, runtime, close_native, {});
  assert(close_operation);

  bool close_send_done = false;
  int close_send_error = 0;
  auto close_sender = trevrpc::spawn(close_operation->send(
                                         1,
                                         [] {
                                           return trevrpc::Result<std::vector<std::byte>>(
                                               std::vector<std::byte>{std::byte{3}});
                                         },
                                         {}, false),
                                     [&](trevrpc::TaskCompletion<trevrpc::Result<void>> result) {
                                       assert(!result.exception);
                                       assert(result.value.has_value());
                                       assert(!result.value.value());
                                       {
                                         std::lock_guard lock(completion_mutex);
                                         close_send_error = result.value.value().error().code();
                                         close_send_done = true;
                                       }
                                       completion_condition.notify_all();
                                     });
  assert(close_sender);
  close_native->wait_for_send();

  close_operation->close();
  assert(close_native->cancelled());
  assert(!close_native->closed());

  close_native->release_send();
  {
    std::unique_lock lock(completion_mutex);
    completion_condition.wait_for(lock, 2s, [&] { return close_send_done; });
  }
  close_native->wait_for_close();
  assert(close_send_done);
  assert(close_send_error == -ECANCELED);
  assert(close_native->closed());

  auto receive_close_native = std::make_shared<FakeNativeOps>();
  receive_close_native->block_receive();
  auto receive_close_operation =
      trevrpc::detail::OperationState::create(fake_stream, runtime, receive_close_native, {});
  assert(receive_close_operation);
  bool receive_close_done = false;
  auto receive_close_receiver = trevrpc::spawn(
      receive_close_operation->receive(),
      [&](trevrpc::TaskCompletion<trevrpc::Result<trevrpc::detail::StreamFrame>> result) {
        assert(!result.exception);
        assert(result.value.has_value());
        assert(!result.value.value());
        assert(result.value.value().error().code() == -ECANCELED);
        {
          std::lock_guard lock(completion_mutex);
          receive_close_done = true;
        }
        completion_condition.notify_all();
      });
  assert(receive_close_receiver);
  receive_close_native->wait_for_receive_poll();

  receive_close_operation->close();
  assert(receive_close_native->cancelled());
  assert(!receive_close_native->closed());
  {
    std::unique_lock lock(completion_mutex);
    completion_condition.wait_for(lock, 2s, [&] { return receive_close_done; });
  }
  assert(receive_close_done);
  assert(!receive_close_native->closed());

  receive_close_native->release_receive();
  receive_close_native->wait_for_close();
  assert(receive_close_native->closed());

  auto deadline_native = std::make_shared<FakeNativeOps>();
  auto deadline_operation = trevrpc::detail::OperationState::create(
      fake_stream, runtime, deadline_native, trevrpc::Deadline::clock::now() - 1ms);
  assert(deadline_operation);
  auto timed_out = trevrpc::sync_wait(deadline_operation->receive());
  assert(!timed_out);
  assert(timed_out.error().code() == -ETIMEDOUT);
  for (int i = 0; i < 50 && !deadline_native->cancelled(); ++i) {
    std::this_thread::sleep_for(1ms);
  }
  assert(deadline_native->cancelled());
  deadline_operation->close();

  auto terminal_native = std::make_shared<FakeNativeOps>();
  terminal_native->set_receive(trevrpc::detail::StreamFrame{
      true, trevrpc::Status(trevrpc::StatusCode::PermissionDenied, "terminal"), {}});
  auto terminal_operation = trevrpc::detail::OperationState::create(
      fake_stream, runtime, terminal_native, {}, {}, {}, 0, true);
  assert(terminal_operation);
  bool terminal_send_done = false;
  int terminal_send_error = 0;
  auto terminal_sender = trevrpc::spawn(
      terminal_operation->send(
          1,
          [] {
            return trevrpc::Result<std::vector<std::byte>>(std::vector<std::byte>{std::byte{5}});
          },
          {}, false),
      [&](trevrpc::TaskCompletion<trevrpc::Result<void>> result) {
        assert(!result.exception);
        assert(result.value.has_value());
        assert(!result.value.value());
        {
          std::lock_guard lock(completion_mutex);
          terminal_send_error = result.value.value().error().code();
          terminal_send_done = true;
        }
        completion_condition.notify_all();
      });
  assert(terminal_sender);
  terminal_native->wait_for_send();
  auto terminal = trevrpc::sync_wait(terminal_operation->receive());
  assert(terminal);
  assert(terminal.value().terminal);
  assert(terminal.value().status.code() == trevrpc::StatusCode::PermissionDenied);
  assert(terminal_native->cancelled());
  auto duplicate_terminal = trevrpc::sync_wait(terminal_operation->receive());
  assert(!duplicate_terminal);
  assert(duplicate_terminal.error().code() == -EALREADY);
  terminal_native->release_send();
  {
    std::unique_lock lock(completion_mutex);
    completion_condition.wait_for(lock, 2s, [&] { return terminal_send_done; });
  }
  assert(terminal_send_done);
  assert(terminal_send_error == -ECANCELED);
  terminal_operation->close();

  auto stop_native = std::make_shared<FakeNativeOps>();
  auto stop_operation =
      trevrpc::detail::OperationState::create(fake_stream, runtime, stop_native, {});
  assert(stop_operation);
  bool stop_send_done = false;
  auto stop_sender = trevrpc::spawn(stop_operation->send(
                                        1,
                                        [] {
                                          return trevrpc::Result<std::vector<std::byte>>(
                                              std::vector<std::byte>{std::byte{4}});
                                        },
                                        {}, false),
                                    [&](trevrpc::TaskCompletion<trevrpc::Result<void>> result) {
                                      assert(!result.exception);
                                      assert(result.value.has_value());
                                      assert(result.value.value());
                                      {
                                        std::lock_guard lock(completion_mutex);
                                        stop_send_done = true;
                                      }
                                      completion_condition.notify_all();
                                    });
  assert(stop_sender);
  stop_native->wait_for_send();
  continuation->request_stop();
  stop_native->release_send();
  {
    std::unique_lock lock(completion_mutex);
    completion_condition.wait_for(lock, 2s, [&] { return stop_send_done; });
  }
  assert(stop_send_done);
  stop_operation->close();

  runtime.reset();
  continuation->request_stop();
  assert(continuation->drain_until(trevrpc::Deadline::clock::now() + 2s));
  return 0;
}
