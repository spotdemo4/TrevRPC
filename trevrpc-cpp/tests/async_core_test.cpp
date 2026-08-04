#include <trevrpc/async.hpp>

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <thread>

namespace {

using namespace std::chrono_literals;

struct Message {
  [[nodiscard]] std::size_t ByteSizeLong() const noexcept { return 0; }
  [[nodiscard]] bool SerializeToArray(void*, int) const noexcept { return true; }
  [[nodiscard]] bool ParseFromArray(const void*, int) noexcept { return true; }
};

trevrpc::Task<int> lazy_value(std::atomic<int>& starts) {
  ++starts;
  co_return 7;
}

trevrpc::Task<int> throwing_task() {
  throw std::runtime_error("task failure");
  co_return 0;
}

trevrpc::Task<std::thread::id> continuation_thread(trevrpc::Task<int> child) {
  (void)co_await std::move(child);
  co_return std::this_thread::get_id();
}

trevrpc::Task<std::thread::id> starting_thread(std::atomic<int>& starts) {
  ++starts;
  co_return std::this_thread::get_id();
}

} // namespace

int main() {
  auto executor_result =
      trevrpc::ThreadPoolExecutor::create({.worker_count = 1, .queue_capacity = 1});
  assert(executor_result);
  auto executor = std::move(executor_result).value();

  auto reservation = executor->try_reserve();
  assert(reservation);
  auto full = executor->try_reserve();
  assert(!full);
  assert(full.error().code() == -EAGAIN);

  std::atomic<int> ran{0};
  executor->request_stop();
  auto committed = std::move(reservation).value().commit([&ran] { ++ran; });
  assert(committed);
  assert(executor->drain_until(trevrpc::Deadline::clock::now() + 2s));
  assert(ran.load() == 1);
  const auto snapshot = executor->snapshot();
  assert(snapshot.completed == 1);
  assert(snapshot.rejected >= 1);

  std::atomic<int> starts{0};
  auto lazy = lazy_value(starts);
  assert(starts.load() == 0);
  assert(trevrpc::sync_wait(std::move(lazy)) == 7);
  assert(starts.load() == 1);

  bool threw = false;
  try {
    (void)trevrpc::sync_wait(throwing_task());
  } catch (const std::runtime_error&) {
    threw = true;
  }
  assert(threw);

  auto continuation_executor_result =
      trevrpc::ThreadPoolExecutor::create({.worker_count = 1, .queue_capacity = 8});
  assert(continuation_executor_result);
  auto continuation_executor = std::move(continuation_executor_result).value();
  std::atomic<int> child_starts{0};
  auto child = lazy_value(child_starts);
  child.associate_executor(continuation_executor);
  const auto continuation_id = trevrpc::sync_wait(continuation_thread(std::move(child)));
  std::thread::id executor_id;
  std::atomic<bool> captured{false};
  auto capture_result = continuation_executor->execute([&] {
    executor_id = std::this_thread::get_id();
    captured = true;
  });
  assert(capture_result);
  assert(continuation_executor->drain_until(trevrpc::Deadline::clock::now() + 2s));
  assert(captured.load());
  assert(continuation_id == executor_id);

  std::atomic<int> executor_starts{0};
  auto executor_task = starting_thread(executor_starts);
  executor_task.associate_executor(continuation_executor);
  assert(trevrpc::sync_wait(std::move(executor_task)) == executor_id);
  assert(executor_starts.load() == 1);

  auto saturated_executor_result =
      trevrpc::ThreadPoolExecutor::create({.worker_count = 1, .queue_capacity = 1});
  assert(saturated_executor_result);
  auto saturated_executor = std::move(saturated_executor_result).value();
  auto held = saturated_executor->try_reserve();
  assert(held);
  std::atomic<int> rejected_starts{0};
  auto rejected_task = starting_thread(rejected_starts);
  rejected_task.associate_executor(saturated_executor);
  auto rejected_spawn = trevrpc::spawn(std::move(rejected_task),
                                       [](const trevrpc::TaskCompletion<std::thread::id>&) {});
  assert(!rejected_spawn);
  assert(rejected_spawn.error().code() == -EAGAIN);
  assert(rejected_starts.load() == 0);

  std::atomic<int> rejected_child_starts{0};
  auto rejected_child = lazy_value(rejected_child_starts);
  rejected_child.associate_executor(saturated_executor);
  bool rejected_child_threw = false;
  try {
    (void)trevrpc::sync_wait(continuation_thread(std::move(rejected_child)));
  } catch (const std::system_error& error) {
    rejected_child_threw = error.code().value() == EAGAIN;
  }
  assert(rejected_child_threw);
  assert(rejected_child_starts.load() == 0);

  std::move(held).value().cancel();
  saturated_executor->request_stop();
  assert(saturated_executor->drain_until(trevrpc::Deadline::clock::now() + 2s));

  auto null_unary = trevrpc::sync_wait(
      trevrpc::async_unary<Message, Message>(nullptr, nullptr, "service", "method", Message{}));
  assert(!null_unary);
  assert(null_unary.error().code() == -EINVAL);

  auto null_stream = trevrpc::sync_wait(
      trevrpc::async_client_streaming<Message, Message>(nullptr, nullptr, "service", "method"));
  assert(!null_stream);
  assert(null_stream.error().code() == -EINVAL);

  trevrpc::CancellationSource source;
  const auto first = source.token();
  const auto second = source.token();
  assert(!first.cancelled());
  assert(!second.cancelled());
  source.cancel();
  assert(first.cancelled());
  assert(second.cancelled());

  auto self_release_result =
      trevrpc::ThreadPoolExecutor::create({.worker_count = 1, .queue_capacity = 4});
  assert(self_release_result);
  auto self_release = std::move(self_release_result).value();
  std::weak_ptr<trevrpc::ThreadPoolExecutor> self_release_weak = self_release;
  auto release_signal = std::make_shared<std::pair<std::mutex, std::condition_variable>>();
  auto release_done = std::make_shared<std::atomic<bool>>(false);
  auto release_started =
      self_release->execute([owner = self_release, release_signal, release_done]() mutable {
        owner.reset();
        release_done->store(true);
        release_signal->second.notify_all();
      });
  assert(release_started);
  self_release.reset();
  {
    std::unique_lock lock(release_signal->first);
    const bool completed =
        release_signal->second.wait_for(lock, 2s, [&] { return release_done->load(); });
    assert(completed);
  }
  for (int attempt = 0; attempt < 200 && !self_release_weak.expired(); ++attempt) {
    std::this_thread::sleep_for(1ms);
  }
  assert(self_release_weak.expired());

  continuation_executor->request_stop();
  assert(continuation_executor->drain_until(trevrpc::Deadline::clock::now() + 2s));
  return 0;
}
