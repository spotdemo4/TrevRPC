#include "detail/channel_core.hpp"
#include "detail/lifecycle.hpp"
#include "rpc_event_runtime_fake_fixture.h"

#include <trevrpc_rpc.h>

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace trevrpc::detail {

class RpcEventRuntimeTestPeer {
public:
  static void fail_next_operation_reservation(RpcEventRuntime& runtime, int error) {
    runtime.test_fail_next_operation_reservation(error);
  }

  static void fail_next_endpoint_registration(RpcEventRuntime& runtime, int error) {
    runtime.test_fail_next_endpoint_registration(error);
  }

  static std::thread::id driver_thread_id(const RpcEventRuntime& runtime) {
    return runtime.test_driver_thread_id();
  }

  static void fail_next_adopt_start(int error) {
    RpcEventRuntime::test_fail_next_adopt_start(error);
  }

  static void reserve_unadopted_cleanup() { assert(RpcEventRuntime::reserve_unadopted_cleanup()); }

  static void wait_for_unadopted_cleanup() { RpcEventRuntime::test_wait_for_unadopted_cleanup(); }
};

class ChannelCoreTestPeer {
public:
  static void fail_next_start(int error) { ChannelCore::test_fail_next_start(error); }

  static Result<std::shared_ptr<RpcEventRuntime>>
  adopt_created_runtime(trevrpc_rpc_runtime* runtime, const trevrpc_rpc_runtime_config_v1& config) {
    return ChannelCore::adopt_created_runtime(runtime, config);
  }

  static void wait_for_attempts(ChannelCore& channel, std::size_t count) {
    channel.test_wait_for_attempts(count);
  }

  static void wait_for_retired(ChannelCore& channel, std::size_t count) {
    channel.test_wait_for_retired(count);
  }

  static void wait_for_lifecycle_idle(ChannelCore& channel) {
    channel.test_wait_for_lifecycle_idle();
  }

  static void enqueue_lifecycle(ChannelCore& channel, ChannelCoreEvent event) {
    channel.test_enqueue_lifecycle(event);
  }

  static std::size_t live_generations(const ChannelCore& channel) {
    return channel.test_live_generations();
  }
};

} // namespace trevrpc::detail

namespace {

using trevrpc::detail::ChannelCore;
using trevrpc::detail::ChannelCoreConfig;
using trevrpc::detail::ChannelCoreEvent;
using trevrpc::detail::ChannelCoreEventKind;
using trevrpc::detail::ChannelCorePhase;
using trevrpc::detail::ChannelCoreTestPeer;
using trevrpc::detail::RpcCancellation;
using trevrpc::detail::RpcEventRuntime;
using trevrpc::detail::RpcEventRuntimeTestPeer;

struct TestRuntime {
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  std::shared_ptr<RpcEventRuntime> runtime;
};

TestRuntime make_runtime(std::uint32_t endpoint_capacity = 16) {
  trevrpc_rpc_runtime_config_v1 config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
  config.event_capacity = 128;
  config.endpoint_capacity = endpoint_capacity;
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  trevrpc_rpc_runtime* raw_runtime = nullptr;
  assert(trevrpc_cpp_rpc_fake_fixture_create(&fake, &config, &raw_runtime) == 0);
  auto runtime = RpcEventRuntime::adopt(raw_runtime, config);
  assert(runtime);
  return TestRuntime{fake, std::move(runtime).value()};
}

ChannelCoreConfig test_config() {
  ChannelCoreConfig config;
  config.host = "fake.test";
  config.reconnect_initial_delay = std::chrono::milliseconds(0);
  config.reconnect_max_delay = std::chrono::milliseconds(0);
  return config;
}

struct StarterControl {
  std::mutex mutex;
  std::condition_variable condition;
  std::size_t calls = 0;
  int next_error = 0;
  bool throw_next = false;
  std::vector<trevrpc_rpc_endpoint_v1> endpoints;
};

std::shared_ptr<ChannelCore> make_channel(TestRuntime& test,
                                          std::shared_ptr<StarterControl> control,
                                          ChannelCoreConfig config = test_config(),
                                          ChannelCore::Backoff backoff = {}) {
  auto created =
      ChannelCore::create(test.runtime, std::move(config),
                          [fake = test.fake, control = std::move(control)](
                              trevrpc_rpc_runtime* runtime, std::uint64_t operation_id,
                              trevrpc_rpc_endpoint_v1* endpoint) {
                            int injected_error = 0;
                            bool throw_now = false;
                            {
                              std::lock_guard lock(control->mutex);
                              ++control->calls;
                              injected_error = std::exchange(control->next_error, 0);
                              throw_now = std::exchange(control->throw_next, false);
                              control->condition.notify_all();
                            }
                            if (throw_now) {
                              throw std::bad_alloc();
                            }
                            if (injected_error != 0) {
                              return injected_error;
                            }
                            const int error = trevrpc_cpp_rpc_fake_start_endpoint(
                                fake, runtime, TREVRPC_RPC_ENDPOINT_CLIENT, operation_id, endpoint);
                            if (error == 0) {
                              std::lock_guard lock(control->mutex);
                              control->endpoints.push_back(*endpoint);
                              control->condition.notify_all();
                            }
                            return error;
                          }, std::move(backoff));
  assert(created);
  return std::move(created).value();
}

void wait_for_starter_calls(const std::shared_ptr<StarterControl>& control, std::size_t count) {
  std::unique_lock lock(control->mutex);
  control->condition.wait(lock, [&] { return control->calls >= count; });
}

void wait_for_started_endpoints(const std::shared_ptr<StarterControl>& control, std::size_t count) {
  std::unique_lock lock(control->mutex);
  control->condition.wait(lock, [&] { return control->endpoints.size() >= count; });
}

void test_created_runtime_adoption_failure_is_reaped() {
  trevrpc_rpc_runtime_config_v1 config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  trevrpc_rpc_runtime* raw_runtime = nullptr;
  assert(trevrpc_cpp_rpc_fake_fixture_create(&fake, &config, &raw_runtime) == 0);
  RpcEventRuntimeTestPeer::reserve_unadopted_cleanup();
  RpcEventRuntimeTestPeer::fail_next_adopt_start(-EAGAIN);
  trevrpc_cpp_rpc_fake_set_close_result(fake, -EIO);
  auto adopted = ChannelCoreTestPeer::adopt_created_runtime(raw_runtime, config);
  assert(!adopted);
  assert(adopted.error().code() == -EAGAIN);
  RpcEventRuntimeTestPeer::wait_for_unadopted_cleanup();
}

void test_coordinator_start_failure_returns_without_hanging() {
  auto test = make_runtime();
  ChannelCoreTestPeer::fail_next_start(-EAGAIN);
  auto created = ChannelCore::create(
      test.runtime, test_config(),
      [fake = test.fake](trevrpc_rpc_runtime* runtime, std::uint64_t operation_id,
                         trevrpc_rpc_endpoint_v1* endpoint) {
        return trevrpc_cpp_rpc_fake_start_endpoint(fake, runtime, TREVRPC_RPC_ENDPOINT_CLIENT,
                                                   operation_id, endpoint);
      });
  assert(!created);
  assert(created.error().code() == -EAGAIN);
  assert(test.runtime->shutdown());
}

void test_initial_ready_and_fail_fast_admission() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);

  auto disconnected = channel->acquire_generation();
  assert(!disconnected);
  assert(disconnected.error().code() == -ENOTCONN);

  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  auto ready = channel->wait_ready(std::chrono::seconds(1));
  assert(ready);
  assert(ready.value() == 1);
  assert(channel->phase() == ChannelCorePhase::Ready);

  {
    auto lease = channel->acquire_generation();
    assert(lease);
    assert(lease.value().generation() == 1);
    assert(lease.value().runtime() == test.runtime);
  }
  assert(channel->close());
}

void test_wait_ready_timeout_and_cancellation() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);

  auto timed_out = channel->wait_ready(std::chrono::milliseconds(1));
  assert(!timed_out);
  assert(timed_out.error().code() == -ETIMEDOUT);

  RpcCancellation cancellation;
  auto waiting = std::async(std::launch::async, [&] {
    return channel->wait_ready(std::chrono::seconds(5), &cancellation);
  });
  cancellation.cancel();
  auto cancelled = waiting.get();
  assert(!cancelled);
  assert(cancelled.error().code() == -ECANCELED);
  assert(channel->close());
}

void test_failed_attempt_does_not_commit_generation() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  {
    std::lock_guard lock(control->mutex);
    control->next_error = -ECONNREFUSED;
  }
  auto channel = make_channel(test, control);
  wait_for_starter_calls(control, 2);
  wait_for_started_endpoints(control, 1);
  assert(channel->generation() == 0);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  auto ready = channel->wait_ready(std::chrono::seconds(1));
  assert(ready);
  assert(ready.value() == 1);
  assert(channel->close());
}

void test_endpoint_starter_exception_is_recoverable() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  {
    std::lock_guard lock(control->mutex);
    control->throw_next = true;
  }
  auto channel = make_channel(test, control);
  wait_for_starter_calls(control, 2);
  wait_for_started_endpoints(control, 1);
  assert(channel->generation() == 0);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)).value() == 1);
  assert(channel->close());
}

void test_async_endpoint_failure_does_not_commit_generation() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_closed(test.fake, -ECONNREFUSED) == 0);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 2);
  wait_for_started_endpoints(control, 2);
  assert(channel->generation() == 0);
  assert(channel->phase() == ChannelCorePhase::Connecting);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)).value() == 1);
  assert(channel->close());
}

void test_backoff_callback_is_reentrant() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  {
    std::lock_guard lock(control->mutex);
    control->next_error = -ECONNREFUSED;
  }

  std::mutex backoff_mutex;
  std::condition_variable backoff_condition;
  std::shared_ptr<ChannelCore> channel;
  bool channel_published = false;
  std::size_t backoff_calls = 0;
  std::size_t backoff_completions = 0;
  auto backoff = [&](std::size_t) {
    std::shared_ptr<ChannelCore> current;
    std::size_t call = 0;
    {
      std::unique_lock lock(backoff_mutex);
      backoff_condition.wait(lock, [&] { return channel_published; });
      current = channel;
      call = ++backoff_calls;
      backoff_condition.notify_all();
    }

    assert(current->phase() ==
           (call == 1 ? ChannelCorePhase::Connecting : ChannelCorePhase::Reconnecting));
    auto readiness = current->wait_ready(std::chrono::milliseconds(1));
    assert(!readiness);
    assert(readiness.error().code() == -ETIMEDOUT);
    if (call == 2) {
      assert(current->close());
    }
    {
      std::lock_guard lock(backoff_mutex);
      ++backoff_completions;
    }
    backoff_condition.notify_all();
    return std::chrono::milliseconds(0);
  };

  channel = make_channel(test, control, test_config(), std::move(backoff));
  {
    std::lock_guard lock(backoff_mutex);
    channel_published = true;
  }
  backoff_condition.notify_all();

  wait_for_starter_calls(control, 2);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  assert(trevrpc_cpp_rpc_fake_push_connection_closed(test.fake, -ECONNRESET) == 0);
  {
    std::unique_lock lock(backoff_mutex);
    assert(backoff_condition.wait_for(lock, std::chrono::seconds(1),
                                      [&] { return backoff_completions >= 2; }));
  }
  assert(channel->close());
}

void test_reconnect_generation_pinning_and_retirement() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)).value() == 1);

  auto first_result = channel->acquire_generation();
  assert(first_result);
  std::optional<ChannelCore::GenerationLease> first(std::move(first_result).value());
  const auto first_endpoint = first->endpoint();
  assert(trevrpc_cpp_rpc_fake_push_connection_closed(test.fake, -ECONNRESET) == 0);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 2);
  assert(channel->phase() == ChannelCorePhase::Reconnecting);
  auto reconnecting = channel->acquire_generation();
  assert(!reconnecting);
  assert(reconnecting.error().code() == -ENOTCONN);
  wait_for_started_endpoints(control, 2);

  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)).value() == 2);
  auto second_result = channel->acquire_generation();
  assert(second_result);
  std::optional<ChannelCore::GenerationLease> second(std::move(second_result).value());
  assert(second->generation() == 2);
  assert(first->generation() == 1);
  const auto second_endpoint = second->endpoint();
  assert(first_endpoint.owner != second_endpoint.owner ||
         first_endpoint.slot != second_endpoint.slot ||
         first_endpoint.generation != second_endpoint.generation);

  ChannelCoreTestPeer::wait_for_retired(*channel, 1);
  assert(ChannelCoreTestPeer::live_generations(*channel) >= 2);
  first.reset();
  while (ChannelCoreTestPeer::live_generations(*channel) != 1) {
    std::this_thread::yield();
  }
  second.reset();
  assert(channel->close());
}

void test_registration_failure_releases_endpoint_capacity() {
  auto test = make_runtime(1);
  RpcEventRuntimeTestPeer::fail_next_endpoint_registration(*test.runtime, -ENOBUFS);
  auto control = std::make_shared<StarterControl>();
  const std::size_t releases_before = trevrpc_cpp_rpc_fake_release_handle_count(test.fake);
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 2);
  wait_for_started_endpoints(control, 2);
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) > releases_before);
  assert(channel->generation() == 0);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)).value() == 1);
  assert(channel->close());
}

void test_cancellation_attachments_are_runtime_scoped() {
  auto first_runtime = make_runtime();
  auto second_runtime = make_runtime();
  RpcCancellation cancellation;
  RpcCancellation copied = cancellation;
  auto first_result = cancellation.attach(first_runtime.runtime);
  assert(first_result);
  std::optional<RpcCancellation::AttachmentLease> first(std::move(first_result).value());
  const auto first_handle = first->handle();

  copied.cancel();
  assert(cancellation.cancelled());
  auto second_result = cancellation.attach(second_runtime.runtime);
  assert(second_result);
  std::optional<RpcCancellation::AttachmentLease> second(std::move(second_result).value());
  const auto second_handle = second->handle();
  assert(first_handle.owner != second_handle.owner);
  auto repeated_result = cancellation.attach(first_runtime.runtime);
  assert(repeated_result);
  std::optional<RpcCancellation::AttachmentLease> repeated(std::move(repeated_result).value());
  const auto repeated_handle = repeated->handle();
  assert(repeated_handle.owner == first_handle.owner);
  assert(repeated_handle.slot == first_handle.slot);
  assert(repeated_handle.generation == first_handle.generation);

  repeated.reset();
  first.reset();
  second.reset();
  assert(first_runtime.runtime->shutdown());
  assert(second_runtime.runtime->shutdown());
}

void test_cancellation_submission_retries_after_reservation_failure() {
  auto test = make_runtime();
  RpcCancellation cancellation;
  auto attachment_result = cancellation.attach(test.runtime);
  assert(attachment_result);
  std::optional<RpcCancellation::AttachmentLease> attachment(std::move(attachment_result).value());
  RpcEventRuntimeTestPeer::fail_next_operation_reservation(*test.runtime, -ENOBUFS);
  cancellation.cancel();
  cancellation.cancel();
  attachment.reset();
  assert(test.runtime->shutdown());
}

void test_lifecycle_callbacks_are_off_driver_and_coalesced() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  std::mutex mutex;
  std::condition_variable condition;
  std::vector<ChannelCoreEvent> events;
  std::thread::id callback_thread;
  bool callback_entered = false;
  bool release_callback = false;
  auto config = test_config();
  config.lifecycle_capacity = 2;
  config.lifecycle_observer = [&](const ChannelCoreEvent& event) {
    std::unique_lock lock(mutex);
    callback_thread = std::this_thread::get_id();
    events.push_back(event);
    if (!callback_entered) {
      callback_entered = true;
      condition.notify_all();
      condition.wait(lock, [&] { return release_callback; });
    }
  };
  auto channel = make_channel(test, control, std::move(config));
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  std::thread::id driver_thread;
  const auto driver_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (driver_thread == std::thread::id{} && std::chrono::steady_clock::now() < driver_deadline) {
    driver_thread = RpcEventRuntimeTestPeer::driver_thread_id(*test.runtime);
    std::this_thread::yield();
  }
  assert(driver_thread != std::thread::id{});
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  {
    std::unique_lock lock(mutex);
    assert(condition.wait_for(lock, std::chrono::seconds(1), [&] { return callback_entered; }));
  }
  for (std::uint64_t generation = 1; generation <= 10; ++generation) {
    ChannelCoreTestPeer::enqueue_lifecycle(
        *channel, ChannelCoreEvent{ChannelCoreEventKind::StateChanged,
                                   ChannelCorePhase::Reconnecting, generation, -EAGAIN});
  }
  {
    std::lock_guard lock(mutex);
    release_callback = true;
  }
  condition.notify_all();
  ChannelCoreTestPeer::wait_for_lifecycle_idle(*channel);
  {
    std::lock_guard lock(mutex);
    assert(events.size() == 3);
    assert(callback_thread != driver_thread);
    assert(events.back().kind == ChannelCoreEventKind::StateChanged);
    assert(events.back().generation == 10);
  }
  assert(channel->close());
}

void test_close_fences_admission_and_waits_for_pins() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  auto lease_result = channel->acquire_generation();
  assert(lease_result);
  std::optional<ChannelCore::GenerationLease> lease(std::move(lease_result).value());

  auto closing = std::async(std::launch::async, [&] { return channel->close(); });
  while (channel->phase() != ChannelCorePhase::Closed) {
    std::this_thread::yield();
  }
  auto rejected = channel->acquire_generation();
  assert(!rejected);
  assert(rejected.error().code() == -ESHUTDOWN);
  assert(closing.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout);
  lease.reset();
  assert(closing.get());
  assert(channel->close());
}

void test_final_owner_destruction_does_not_wait_for_pins() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  auto lease_result = channel->acquire_generation();
  assert(lease_result);
  std::optional<ChannelCore::GenerationLease> lease(std::move(lease_result).value());

  std::weak_ptr<ChannelCore> weak = channel;
  auto destroying = std::async(std::launch::async, [channel = std::move(channel)] {});
  const bool destruction_finished =
      destroying.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
  lease.reset();
  assert(destruction_finished);
  destroying.get();
  assert(weak.expired());
  assert(trevrpc::detail::drain_lifecycle_reaper_until(
      std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

void test_external_endpoint_close_is_observed() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));

  {
    auto lease = channel->acquire_generation();
    assert(lease);
    const auto endpoint = lease.value().endpoint();
    auto operation = test.runtime->reserve_operation();
    assert(operation);
    assert(trevrpc_rpc_endpoint_close(test.runtime->native_handle(), endpoint, operation.value()) ==
           0);
    auto closed = test.runtime->wait_operation(operation.value());
    assert(closed);
    assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
  }
  assert(channel->close());
}

void test_persistent_endpoint_close_error_is_retryable() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, -EIO, true);

  auto closing = std::async(std::launch::async, [&] { return channel->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto failed = closing.get();
  assert(!failed);
  assert(failed.error().code() == -EIO);

  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, 0, false);
  assert(channel->close());
}

void test_persistent_duplicate_endpoint_close_is_retryable() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, -EALREADY, true);

  auto closing = std::async(std::launch::async, [&] { return channel->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto failed = closing.get();
  assert(!failed);
  assert(failed.error().code() == -EALREADY);

  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, 0, false);
  assert(channel->close());
}

void test_persistent_registration_cleanup_error_is_retryable() {
  auto test = make_runtime();
  RpcEventRuntimeTestPeer::fail_next_endpoint_registration(*test.runtime, -ENOBUFS);
  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, -EIO, true);
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  wait_for_started_endpoints(control, 1);

  auto closing = std::async(std::launch::async, [&] { return channel->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto failed = closing.get();
  assert(!failed);
  assert(failed.error().code() == -EIO);

  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, 0, false);
  assert(channel->close());
}

void test_persistent_endpoint_release_error_is_retryable() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  trevrpc_cpp_rpc_fake_set_release_handle_result(test.fake, -EIO);

  auto closing = std::async(std::launch::async, [&] { return channel->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto failed = closing.get();
  assert(!failed);
  assert(failed.error().code() == -EIO);

  trevrpc_cpp_rpc_fake_set_release_handle_result(test.fake, 0);
  assert(channel->close());
}

void test_runtime_busy_shutdown_is_retryable() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  auto channel = make_channel(test, control);
  ChannelCoreTestPeer::wait_for_attempts(*channel, 1);
  wait_for_started_endpoints(control, 1);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(channel->wait_ready(std::chrono::seconds(1)));
  const trevrpc_rpc_stream_v1 registered_stream{0x1234, 7, 1};
  assert(test.runtime->register_stream(registered_stream));

  auto closing = std::async(std::launch::async, [&] { return channel->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto failed = closing.get();
  assert(!failed);
  assert(failed.error().code() == -EBUSY);

  test.runtime->unregister_stream(registered_stream);
  assert(channel->close());
}

void test_last_owner_destruction_from_lifecycle_callback() {
  auto test = make_runtime();
  auto control = std::make_shared<StarterControl>();
  std::mutex mutex;
  std::condition_variable condition;
  bool destroyed = false;
  std::shared_ptr<ChannelCore> holder;
  auto config = test_config();
  config.lifecycle_observer = [&](const ChannelCoreEvent& event) {
    if (event.kind != ChannelCoreEventKind::StateChanged ||
        event.phase != ChannelCorePhase::Ready) {
      return;
    }
    holder.reset();
    {
      std::lock_guard lock(mutex);
      destroyed = true;
      condition.notify_all();
    }
  };
  holder = make_channel(test, control, std::move(config));
  std::weak_ptr<ChannelCore> channel_observer = holder;
  std::weak_ptr<RpcEventRuntime> runtime_observer = test.runtime;
  ChannelCoreTestPeer::wait_for_attempts(*holder, 1);
  wait_for_started_endpoints(control, 1);
  test.runtime.reset();
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  {
    std::unique_lock lock(mutex);
    assert(condition.wait_for(lock, std::chrono::seconds(1), [&] { return destroyed; }));
  }
  assert(channel_observer.expired());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!runtime_observer.expired() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  assert(runtime_observer.expired());
}

} // namespace

int main() {
  test_created_runtime_adoption_failure_is_reaped();
  test_coordinator_start_failure_returns_without_hanging();
  test_initial_ready_and_fail_fast_admission();
  test_wait_ready_timeout_and_cancellation();
  test_failed_attempt_does_not_commit_generation();
  test_endpoint_starter_exception_is_recoverable();
  test_async_endpoint_failure_does_not_commit_generation();
  test_backoff_callback_is_reentrant();
  test_reconnect_generation_pinning_and_retirement();
  test_registration_failure_releases_endpoint_capacity();
  test_cancellation_attachments_are_runtime_scoped();
  test_cancellation_submission_retries_after_reservation_failure();
  test_lifecycle_callbacks_are_off_driver_and_coalesced();
  test_close_fences_admission_and_waits_for_pins();
  test_final_owner_destruction_does_not_wait_for_pins();
  test_external_endpoint_close_is_observed();
  test_persistent_endpoint_close_error_is_retryable();
  test_persistent_duplicate_endpoint_close_is_retryable();
  test_persistent_registration_cleanup_error_is_retryable();
  test_persistent_endpoint_release_error_is_retryable();
  test_runtime_busy_shutdown_is_retryable();
  test_last_owner_destruction_from_lifecycle_callback();
}
