#include <trevrpc/trevrpc.hpp>
#include <trevrpc_rpc.h>

#include "detail/rpc_client.hpp"
#include "detail/rpc_event_runtime.hpp"
#include "detail/rpc_server_core.hpp"
#include "rpc_event_runtime_fake_fixture.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <thread>
#include <utility>

namespace trevrpc::detail {

class ServerCallStateTestPeer {
public:
  static void fail_next_rpc_allocation() { ServerCallState::test_fail_next_rpc_allocation(); }
};

struct RpcClientStreamTestPeer {
  static void fail_next_receive_allocation() {
    RpcClientStream::test_fail_next_receive_allocation();
  }

  static void fail_next_cleanup_owner_allocation() {
    RpcClientStream::test_fail_next_cleanup_owner_allocation();
  }

  static void fail_next_cleanup_task_allocation() {
    RpcClientStream::test_fail_next_cleanup_task_allocation();
  }

  static void fail_next_cleanup_requeue_allocation() {
    RpcClientStream::test_fail_next_cleanup_requeue_allocation();
  }

  static void shutdown_cleanup_service() { RpcClientStream::test_shutdown_cleanup_service(); }

  static ClientStream wrap(std::shared_ptr<RpcClientStream> stream) {
    return ClientStream(std::move(stream));
  }

  static void schedule_cleanup(std::shared_ptr<RpcClientStream> stream) {
    RpcClientStream::schedule_cleanup(std::move(stream));
  }

  static void force_no_call_partial(std::shared_ptr<RpcClientStream>& stream) {
    std::lock_guard lock(stream->mutex_);
    stream->runtime_->reject_operation(stream->close_operation_);
    stream->runtime_->unregister_stream(stream->stream_);
    stream->call_ = {};
    stream->close_operation_ = 0;
    stream->close_operation_consumed_ = true;
    stream->stream_registered_ = false;
    stream->stream_unregistered_ = true;
  }

  static void note(std::shared_ptr<RpcClientStream>& stream, const RpcEvent& event) {
    stream->note_event(event);
  }

  static void defer_receive_terminal(std::shared_ptr<RpcClientStream>& stream,
                                     const RpcEvent& event) {
    stream->defer_receive_terminal(event);
  }

  static void drain_pending(std::shared_ptr<RpcClientStream>& stream) {
    std::shared_ptr<RpcEventRuntime> runtime;
    trevrpc_rpc_stream_v1 native_stream{};
    {
      std::lock_guard lock(stream->mutex_);
      runtime = stream->runtime_;
      native_stream = stream->stream_;
    }
    for (;;) {
      auto event = runtime->try_wait_stream(native_stream);
      if (!event || !event.value().has_value()) {
        return;
      }
    }
  }

  static Result<void> subscribe(std::shared_ptr<RpcClientStream>& stream,
                                RpcEventRuntime::EventCallback callback) {
    std::shared_ptr<RpcEventRuntime> runtime;
    trevrpc_rpc_stream_v1 native_stream{};
    {
      std::lock_guard lock(stream->mutex_);
      runtime = stream->runtime_;
      native_stream = stream->stream_;
    }
    return runtime->subscribe_stream(native_stream, std::move(callback));
  }
};

class RpcEventRuntimeTestPeer {
public:
  static void wait_for_operation_waiters(RpcEventRuntime& runtime, std::size_t count) {
    runtime.test_wait_for_operation_waiters(count);
  }

  static void wait_for_stream_waiters(RpcEventRuntime& runtime, std::size_t count) {
    runtime.test_wait_for_stream_waiters(count);
  }

  static void wait_for_pending_stream_events(RpcEventRuntime& runtime, trevrpc_rpc_stream_v1 stream,
                                             std::size_t count) {
    runtime.test_wait_for_pending_stream_events(stream, count);
  }

  static void wait_for_endpoint_waiters(RpcEventRuntime& runtime, std::size_t count) {
    runtime.test_wait_for_endpoint_waiters(count);
  }

  static void wait_for_shutdown_waiters(RpcEventRuntime& runtime, std::size_t count) {
    runtime.test_wait_for_shutdown_waiters(count);
  }

  static bool wait_for_close_submission_waiters(RpcEventRuntime& runtime, std::size_t count,
                                                std::chrono::milliseconds timeout) {
    return runtime.test_wait_for_close_submission_waiters(count, timeout);
  }

  static void block_next_completed_strict_waiter(RpcEventRuntime& runtime) {
    runtime.test_block_next_completed_strict_waiter();
  }

  static void wait_for_blocked_strict_waiter(RpcEventRuntime& runtime) {
    runtime.test_wait_for_blocked_strict_waiter();
  }

  static void release_blocked_strict_waiter(RpcEventRuntime& runtime) {
    runtime.test_release_blocked_strict_waiter();
  }

  static void wait_for_stopped(RpcEventRuntime& runtime) { runtime.test_wait_for_stopped(); }
  static void pause_finalization(RpcEventRuntime& runtime) { runtime.test_pause_finalization(); }
  static void wait_for_finalization_pause(RpcEventRuntime& runtime) {
    runtime.test_wait_for_finalization_pause();
  }
  static void release_finalization(RpcEventRuntime& runtime) {
    runtime.test_release_finalization();
  }

  static std::thread::id driver_thread_id(const RpcEventRuntime& runtime) {
    return runtime.test_driver_thread_id();
  }

  static void wait_for_incoming_backlog(RpcEventRuntime& runtime, std::size_t count) {
    runtime.test_wait_for_incoming_backlog(count);
  }

  static void wait_for_incoming_waiter(RpcEventRuntime& runtime) {
    runtime.test_wait_for_incoming_waiter();
  }

  static void dispatch(RpcEventRuntime& runtime, RpcEvent event) {
    assert(runtime.test_dispatch(std::move(event)) == 0);
  }

  static void wait_for_rejected_streams(RpcEventRuntime& runtime, std::size_t count) {
    runtime.test_wait_for_rejected_streams(count);
  }

  static void fail_driver(RpcEventRuntime& runtime, int error) { runtime.test_fail_driver(error); }

  static void pause_driver(RpcEventRuntime& runtime) { runtime.test_pause_driver(); }

  static void resume_driver(RpcEventRuntime& runtime) { runtime.test_resume_driver(); }

  static void wait_for_terminal_settlement(RpcEventRuntime& runtime) {
    runtime.test_wait_for_terminal_settlement();
  }

  static void settle_driver_failure(RpcEventRuntime& runtime, int error) {
    runtime.test_settle_driver_failure(error);
  }

  static void fail_next_operation_reservation(RpcEventRuntime& runtime, int error) {
    runtime.test_fail_next_operation_reservation(error);
  }

  static void fail_next_endpoint_registration(RpcEventRuntime& runtime, int error) {
    runtime.test_fail_next_endpoint_registration(error);
  }

  static void fail_next_stream_registration(RpcEventRuntime& runtime, int error) {
    runtime.test_fail_next_stream_registration(error);
  }

  static void fail_next_endpoint_registration_commit(RpcEventRuntime& runtime) {
    runtime.test_fail_next_endpoint_registration_commit();
  }

  static Result<void> settle_unregistered_endpoint(RpcEventRuntime& runtime,
                                                   trevrpc_rpc_endpoint_v1 endpoint) {
    return runtime.settle_unregistered_endpoint(endpoint);
  }

  static void fail_next_control_write(RpcEventRuntime& runtime, int error) {
    runtime.test_fail_next_control_write(error);
  }

  static void fail_next_adopt_start(int error) {
    RpcEventRuntime::test_fail_next_adopt_start(error);
  }

  static void reserve_unadopted_cleanup() { assert(RpcEventRuntime::reserve_unadopted_cleanup()); }

  static void reap_unadopted(trevrpc_rpc_runtime* runtime) {
    RpcEventRuntime::reap_unadopted(runtime);
  }

  static void wait_for_unadopted_cleanup() { RpcEventRuntime::test_wait_for_unadopted_cleanup(); }

  static std::weak_ptr<void> state_observer(const RpcEventRuntime& runtime) {
    return runtime.test_state_observer();
  }

  static void drop_last_owner_after_stopped(std::shared_ptr<RpcEventRuntime>& runtime) {
    std::weak_ptr<RpcEventRuntime> observer = runtime;
    runtime->test_wait_for_stopped();
    runtime.reset();
    while (!observer.expired()) {
      std::this_thread::yield();
    }
  }
};

} // namespace trevrpc::detail

namespace {

using trevrpc::CallOptions;
using trevrpc::Result;
using trevrpc::detail::AsyncServerScopeControl;
using trevrpc::detail::ChannelCore;
using trevrpc::detail::ChannelCoreConfig;
using trevrpc::detail::RpcClientStream;
using trevrpc::detail::RpcClientStreamTestPeer;
using trevrpc::detail::RpcEvent;
using trevrpc::detail::RpcEventRuntime;
using trevrpc::detail::RpcEventRuntimeTestPeer;
using trevrpc::detail::RpcIncomingCall;
using trevrpc::detail::RpcServerCore;
using trevrpc::detail::ServerCallState;
using trevrpc::detail::ServerCallStateTestPeer;

struct TestRuntime {
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  std::shared_ptr<RpcEventRuntime> runtime;
};

class BlockingServerScope final : public AsyncServerScopeControl {
public:
  void cancel() noexcept override {
    {
      std::lock_guard lock(mutex_);
      cancelled_ = true;
      released_ = true;
    }
    condition_.notify_all();
  }

  Result<void> drain_until(std::chrono::steady_clock::time_point deadline) noexcept override {
    std::unique_lock lock(mutex_);
    drain_entered_ = true;
    condition_.notify_all();
    const auto released = [this] { return released_; };
    if (deadline == std::chrono::steady_clock::time_point::max()) {
      condition_.wait(lock, released);
      return {};
    }
    if (!condition_.wait_until(lock, deadline, released)) {
      return trevrpc::Error::runtime(-ETIMEDOUT, "blocking test scope drain timed out");
    }
    return {};
  }

  void wait_for_drain() {
    std::unique_lock lock(mutex_);
    const bool entered =
        condition_.wait_for(lock, std::chrono::seconds(2), [this] { return drain_entered_; });
    assert(entered);
  }

  void release() noexcept {
    {
      std::lock_guard lock(mutex_);
      released_ = true;
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool cancelled() const noexcept {
    std::lock_guard lock(mutex_);
    return cancelled_;
  }

private:
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  bool drain_entered_ = false;
  bool released_ = false;
  bool cancelled_ = false;
};

void release_raw_runtime(trevrpc_rpc_runtime* runtime, std::uint64_t close_operation = 1) {
  assert(trevrpc_rpc_runtime_close(runtime, close_operation) == 0);
  bool stopped = false;
  while (!stopped) {
    trevrpc_rpc_event* event = nullptr;
    const int error = trevrpc_rpc_runtime_next_event(runtime, &event);
    if (error == -EAGAIN) {
      std::this_thread::yield();
      continue;
    }
    assert(error == 0);
    assert(event != nullptr);
    trevrpc_rpc_event_info_v1 info{};
    assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    stopped = info.kind == TREVRPC_RPC_EVENT_STOPPED;
    trevrpc_rpc_event_release(event);
  }
  assert(trevrpc_rpc_runtime_drain(runtime) == 0);
  assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

TestRuntime make_runtime(trevrpc_rpc_runtime_config_v1* custom_config = nullptr) {
  trevrpc_rpc_runtime_config_v1 default_config{};
  if (custom_config == nullptr) {
    assert(trevrpc_rpc_runtime_config_v1_init(&default_config, sizeof(default_config)) == 0);
    custom_config = &default_config;
  }
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  trevrpc_rpc_runtime* raw_runtime = nullptr;
  assert(trevrpc_cpp_rpc_fake_fixture_create(&fake, custom_config, &raw_runtime) == 0);
  auto adopted = RpcEventRuntime::adopt(raw_runtime, *custom_config);
  assert(adopted);
  return TestRuntime{fake, std::move(adopted).value()};
}

void test_timer_runs_on_event_driver_at_deadline() {
  auto test = make_runtime();
  using TimerResult = std::pair<std::chrono::steady_clock::time_point, std::thread::id>;
  std::promise<TimerResult> callback_result;
  auto callback_future = callback_result.get_future();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
  assert(test.runtime->schedule_at(deadline, [&callback_result] {
    callback_result.set_value({std::chrono::steady_clock::now(), std::this_thread::get_id()});
  }));
  assert(callback_future.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
  const auto [completed_at, callback_thread] = callback_future.get();
  assert(completed_at >= deadline);
  assert(callback_thread == RpcEventRuntimeTestPeer::driver_thread_id(*test.runtime));
  assert(test.runtime->shutdown());
}

void test_failed_adopt_retains_runtime_ownership() {
  trevrpc_rpc_runtime_config_v1 valid_config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&valid_config, sizeof(valid_config)) == 0);

  {
    trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
    trevrpc_rpc_runtime* raw_runtime = nullptr;
    assert(trevrpc_cpp_rpc_fake_fixture_create(&fake, &valid_config, &raw_runtime) == 0);

    auto invalid_config = valid_config;
    invalid_config.struct_version = 0;
    auto adopted = RpcEventRuntime::adopt(raw_runtime, invalid_config);
    assert(!adopted);
    assert(adopted.error().code() == -EINVAL);
    release_raw_runtime(raw_runtime);
  }

  {
    trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
    trevrpc_rpc_runtime* raw_runtime = nullptr;
    assert(trevrpc_cpp_rpc_fake_fixture_create(&fake, &valid_config, &raw_runtime) == 0);

    RpcEventRuntimeTestPeer::fail_next_adopt_start(-EAGAIN);
    auto adopted = RpcEventRuntime::adopt(raw_runtime, valid_config);
    assert(!adopted);
    assert(adopted.error().code() == -EAGAIN);

    trevrpc_cpp_rpc_fake_set_close_result(fake, -EIO);
    assert(trevrpc_rpc_runtime_close(raw_runtime, 1) == -EIO);
    release_raw_runtime(raw_runtime, 2);
  }

  {
    trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
    trevrpc_rpc_runtime* raw_runtime = nullptr;
    assert(trevrpc_cpp_rpc_fake_fixture_create(&fake, &valid_config, &raw_runtime) == 0);

    RpcEventRuntimeTestPeer::reserve_unadopted_cleanup();
    RpcEventRuntimeTestPeer::fail_next_adopt_start(-EAGAIN);
    auto adopted = RpcEventRuntime::adopt(raw_runtime, valid_config);
    assert(!adopted);
    assert(adopted.error().code() == -EAGAIN);

    trevrpc_cpp_rpc_fake_set_persistent_close_result(fake, -EIO);
    auto submitted = std::async(std::launch::async,
                                [&] { RpcEventRuntimeTestPeer::reap_unadopted(raw_runtime); });
    assert(submitted.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    submitted.get();

    trevrpc_cpp_rpc_fake_set_persistent_close_result(fake, 0);
    RpcEventRuntimeTestPeer::wait_for_unadopted_cleanup();
  }
}

trevrpc_rpc_endpoint_v1 start_endpoint(TestRuntime& test, std::uint32_t mode) {
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  trevrpc_rpc_endpoint_v1 endpoint{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake, test.runtime->native_handle(), mode,
                                             operation.value(), &endpoint) == 0);
  if (mode == TREVRPC_RPC_ENDPOINT_CLIENT) {
    assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  }
  auto ready = test.runtime->wait_operation(operation.value());
  assert(ready);
  assert(ready.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);
  assert(ready.value().endpoint.owner == endpoint.owner);
  assert(ready.value().endpoint.slot == endpoint.slot);
  assert(ready.value().endpoint.generation == endpoint.generation);
  return endpoint;
}

void close_endpoint(TestRuntime& test, trevrpc_rpc_endpoint_v1 endpoint) {
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  assert(trevrpc_rpc_endpoint_close(test.runtime->native_handle(), endpoint, operation.value()) ==
         0);
  auto closed = test.runtime->wait_operation(operation.value());
  assert(closed);
  assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
  assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
}

void test_endpoint_registration_failure_releases_endpoint_capacity() {
  trevrpc_rpc_runtime_config_v1 config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
  config.endpoint_capacity = 1;
  auto test = make_runtime(&config);

  auto startup = test.runtime->reserve_operation();
  assert(startup);
  trevrpc_rpc_endpoint_v1 endpoint{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake, test.runtime->native_handle(),
                                             TREVRPC_RPC_ENDPOINT_CLIENT, startup.value(),
                                             &endpoint) == 0);
  RpcEventRuntimeTestPeer::fail_next_endpoint_registration(*test.runtime, -ENOBUFS);
  auto registered = test.runtime->register_endpoint(endpoint);
  assert(!registered);
  assert(registered.error().code() == -ENOBUFS);
  test.runtime->reject_operation(startup.value());
  assert(RpcEventRuntimeTestPeer::settle_unregistered_endpoint(*test.runtime, endpoint));

  auto replacement_startup = test.runtime->reserve_operation();
  assert(replacement_startup);
  trevrpc_rpc_endpoint_v1 replacement{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake, test.runtime->native_handle(),
                                             TREVRPC_RPC_ENDPOINT_CLIENT,
                                             replacement_startup.value(), &replacement) == 0);
  test.runtime->reject_operation(replacement_startup.value());
  assert(RpcEventRuntimeTestPeer::settle_unregistered_endpoint(*test.runtime, replacement));
  assert(test.runtime->shutdown());
}

void test_unregistered_endpoint_settlement_precedes_cleanup_reservation() {
  auto test = make_runtime();
  auto startup = test.runtime->reserve_operation();
  assert(startup);
  trevrpc_rpc_endpoint_v1 endpoint{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake, test.runtime->native_handle(),
                                             TREVRPC_RPC_ENDPOINT_CLIENT, startup.value(),
                                             &endpoint) == 0);
  test.runtime->reject_operation(startup.value());

  auto close_operation = test.runtime->reserve_operation();
  assert(close_operation);
  assert(trevrpc_rpc_endpoint_close(test.runtime->native_handle(), endpoint,
                                    close_operation.value()) == 0);
  assert(test.runtime->wait_operation(close_operation.value()));
  RpcEventRuntimeTestPeer::fail_next_operation_reservation(*test.runtime, -ENOBUFS);
  assert(RpcEventRuntimeTestPeer::settle_unregistered_endpoint(*test.runtime, endpoint));
  assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
  auto reserved = test.runtime->reserve_operation();
  assert(!reserved);
  assert(reserved.error().code() == -ENOBUFS);
  assert(test.runtime->shutdown());
}

void test_endpoint_registration_commit_failure_preserves_pending_events() {
  auto test = make_runtime();
  auto startup = test.runtime->reserve_operation();
  assert(startup);
  trevrpc_rpc_endpoint_v1 endpoint{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake, test.runtime->native_handle(),
                                             TREVRPC_RPC_ENDPOINT_CLIENT, startup.value(),
                                             &endpoint) == 0);
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  assert(test.runtime->wait_operation(startup.value()));

  RpcEventRuntimeTestPeer::fail_next_endpoint_registration_commit(*test.runtime);
  auto failed = test.runtime->register_endpoint(endpoint);
  assert(!failed);
  assert(failed.error().code() == -ENOMEM);
  assert(test.runtime->register_endpoint(endpoint));
  auto ready = test.runtime->wait_endpoint(endpoint);
  assert(ready);
  assert(ready.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);

  auto close_operation = test.runtime->reserve_operation();
  assert(close_operation);
  assert(trevrpc_rpc_endpoint_close(test.runtime->native_handle(), endpoint,
                                    close_operation.value()) == 0);
  assert(test.runtime->wait_operation(close_operation.value()));
  auto closed = test.runtime->wait_endpoint(endpoint);
  assert(closed);
  assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
  test.runtime->unregister_endpoint(endpoint);
  assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
  assert(test.runtime->shutdown());
}

std::pair<trevrpc_rpc_call_v1, trevrpc_rpc_stream_v1>
open_call(TestRuntime& test, trevrpc_rpc_endpoint_v1 endpoint, std::uint64_t operation_id,
          std::uint32_t kind = TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
  trevrpc_rpc_call_config_v1 config{};
  assert(trevrpc_rpc_call_config_v1_init(&config, sizeof(config)) == 0);
  constexpr std::string_view service = "fake.Service";
  constexpr std::string_view method = "Test";
  constexpr std::string_view request = "request";
  config.kind = kind;
  config.service = service.data();
  config.service_len = static_cast<std::uint32_t>(service.size());
  config.method = method.data();
  config.method_len = static_cast<std::uint32_t>(method.size());
  config.initial_message = reinterpret_cast<const std::uint8_t*>(request.data());
  config.initial_message_len = request.size();
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  assert(trevrpc_rpc_call_open_v1(test.runtime->native_handle(), endpoint, &config, operation_id,
                                  &call, &stream) == 0);
  return {call, stream};
}

std::shared_ptr<ChannelCore> make_client_channel(TestRuntime& test) {
  ChannelCoreConfig config;
  config.host = "fake.test";
  config.reconnect_initial_delay = std::chrono::milliseconds(0);
  config.reconnect_max_delay = std::chrono::milliseconds(0);
  auto channel_result = ChannelCore::create(
      test.runtime, std::move(config),
      [fake = test.fake](trevrpc_rpc_runtime* runtime, std::uint64_t operation_id,
                         trevrpc_rpc_endpoint_v1* endpoint) {
        const int error = trevrpc_cpp_rpc_fake_start_endpoint(
            fake, runtime, TREVRPC_RPC_ENDPOINT_CLIENT, operation_id, endpoint);
        if (error == 0) {
          assert(trevrpc_cpp_rpc_fake_push_connection_ready(fake) == 0);
        }
        return error;
      });
  assert(channel_result);
  auto channel = std::move(channel_result).value();
  assert(channel->wait_ready(std::chrono::seconds(1)));
  return channel;
}

std::shared_ptr<RpcClientStream> open_client_stream(TestRuntime& test,
                                                    const std::shared_ptr<ChannelCore>& channel) {
  CallOptions options;
  const std::array<std::byte, 1> initial{std::byte{0x01}};
  auto opening = std::async(std::launch::async, [&] {
    return RpcClientStream::open(channel, "fake.Service", "Test",
                                 TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, initial, options);
  });
  trevrpc_cpp_rpc_fake_wait_stream_open_calls(test.fake, 1);
  assert(trevrpc_cpp_rpc_fake_push_stream_ready(test.fake) == 0);
  trevrpc_cpp_rpc_fake_wait_stream_send_calls(test.fake, 1);
  assert(trevrpc_cpp_rpc_fake_push_last_send_complete(test.fake) == 0);
  assert(opening.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto opened = opening.get();
  assert(opened);
  return std::move(opened).value();
}

void test_client_registration_failure_closes_unregistered_call() {
  auto test = make_runtime();
  ChannelCoreConfig config;
  config.host = "fake.test";
  config.reconnect_initial_delay = std::chrono::milliseconds(0);
  config.reconnect_max_delay = std::chrono::milliseconds(0);
  auto channel_result = ChannelCore::create(
      test.runtime, std::move(config),
      [fake = test.fake](trevrpc_rpc_runtime* runtime, std::uint64_t operation_id,
                         trevrpc_rpc_endpoint_v1* endpoint) {
        const int error = trevrpc_cpp_rpc_fake_start_endpoint(
            fake, runtime, TREVRPC_RPC_ENDPOINT_CLIENT, operation_id, endpoint);
        if (error == 0) {
          assert(trevrpc_cpp_rpc_fake_push_connection_ready(fake) == 0);
        }
        return error;
      });
  assert(channel_result);
  auto channel = std::move(channel_result).value();
  assert(channel->wait_ready(std::chrono::seconds(1)));

  RpcEventRuntimeTestPeer::fail_next_stream_registration(*test.runtime, -ENOMEM);
  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, -EBUSY, true);
  CallOptions options;
  const std::array<std::byte, 1> initial{std::byte{0x01}};
  auto opening = std::async(std::launch::async, [&] {
    return RpcClientStream::open(channel, "fake.Service", "Test",
                                 TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, initial, options);
  });
  trevrpc_cpp_rpc_fake_wait_stream_open_calls(test.fake, 1);
  assert(opening.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto opened = opening.get();
  assert(!opened);
  assert(opened.error().code() == -ENOMEM);
  for (unsigned int attempt = 0;
       attempt != 1000 && trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 0; ++attempt) {
    std::this_thread::yield();
  }
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) != 0);
  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, 0, false);
  for (unsigned int attempt = 0;
       attempt != 2000 && trevrpc_cpp_rpc_fake_release_handle_count(test.fake) < 2; ++attempt) {
    std::this_thread::yield();
  }
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) >= 2);
  assert(channel->close());
}

void test_client_partial_stream_without_call_does_not_wait_for_close_operation() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  RpcClientStreamTestPeer::force_no_call_partial(stream);
  auto closing = std::async(std::launch::async, [&] { stream->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  closing.get();
  assert(channel->request_close());
}

void test_client_open_owner_allocation_failure_precedes_admission() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  RpcClientStreamTestPeer::fail_next_cleanup_owner_allocation();
  CallOptions options;
  const std::array<std::byte, 1> initial{std::byte{0x01}};
  auto opened = RpcClientStream::open(channel, "fake.Service", "Test",
                                      TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, initial, options);
  assert(!opened);
  assert(opened.error().code() == -ENOMEM);
  assert(channel->close());
}

void test_cleanup_task_allocation_failure_abandons_owner() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto owner = open_client_stream(test, channel);
  std::weak_ptr<RpcClientStream> observer = owner;
  auto stream = RpcClientStreamTestPeer::wrap(std::move(owner));
  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, -EBUSY, false);
  RpcClientStreamTestPeer::fail_next_cleanup_task_allocation();
  stream.close();
  assert(observer.expired());
  assert(channel->request_close());
}

void test_cleanup_requeue_allocation_failure_abandons_owner() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto owner = open_client_stream(test, channel);
  std::weak_ptr<RpcClientStream> observer = owner;
  auto stream = RpcClientStreamTestPeer::wrap(std::move(owner));
  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, -EBUSY, true);
  RpcClientStreamTestPeer::fail_next_cleanup_requeue_allocation();
  stream.close();
  for (unsigned int attempt = 0; attempt != 2000 && !observer.expired(); ++attempt) {
    std::this_thread::yield();
  }
  assert(observer.expired());
  assert(channel->request_close());
}

void test_client_stream_release_retries_independently() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);

  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, -EBUSY, false);
  stream->close();
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 1);

  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, 0, false);
  stream->close();
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 2);
  assert(channel->request_close());
}

void test_client_call_release_persistent_error_retries() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);

  trevrpc_cpp_rpc_fake_set_call_release_result(test.fake, test.runtime->native_handle(), -EIO,
                                               true);
  stream->close();
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 1);

  stream->close();
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 1);

  trevrpc_cpp_rpc_fake_set_call_release_result(test.fake, test.runtime->native_handle(), 0, false);
  stream->close();
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 1);
  assert(channel->request_close());
}

void test_public_client_stream_close_retains_stream_release() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = RpcClientStreamTestPeer::wrap(open_client_stream(test, channel));

  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, -EBUSY, true);
  stream.close();
  for (unsigned int attempt = 0;
       attempt != 1000 && trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 0; ++attempt) {
    std::this_thread::yield();
  }
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) != 0);
  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, 0, false);
  for (unsigned int attempt = 0;
       attempt != 2000 && trevrpc_cpp_rpc_fake_release_handle_count(test.fake) < 2; ++attempt) {
    std::this_thread::yield();
  }
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) >= 2);
  assert(channel->close());
}

void test_public_client_stream_close_retains_call_release() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = RpcClientStreamTestPeer::wrap(open_client_stream(test, channel));

  trevrpc_cpp_rpc_fake_set_call_release_result(test.fake, test.runtime->native_handle(), -EBUSY,
                                               true);
  stream.close();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  trevrpc_cpp_rpc_fake_set_call_release_result(test.fake, test.runtime->native_handle(), 0, false);
  assert(channel->close());
}

void test_persistent_cleanup_error_abandons_on_shutdown() {
  for (const int error : {-EIO, -EBUSY}) {
    auto test = make_runtime();
    auto channel = make_client_channel(test);
    auto stream = RpcClientStreamTestPeer::wrap(open_client_stream(test, channel));
    trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, error, true);

    stream.close();
    auto closing = std::async(std::launch::async, [&] { return channel->close(); });
    assert(closing.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    auto closed = closing.get();
    assert(!closed);
    assert(closed.error().code() == error);
    assert(test.runtime->native_handle() == nullptr);
  }
}

void test_client_close_after_runtime_stop_releases_without_terminal_wait() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, -EIO) == 0);
  auto closing = std::async(std::launch::async, [&] { stream->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  closing.get();
  assert(channel->request_close());
}

void test_client_receive_allocation_failure_releases_and_closes() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  const std::array<std::uint8_t, 1> body{0x42};
  assert(trevrpc_cpp_rpc_fake_push_response_message(test.fake, body.data(), body.size()) == 0);
  assert(trevrpc_cpp_rpc_fake_push_stream_readable(test.fake) == 0);
  RpcClientStreamTestPeer::fail_next_receive_allocation();

  auto received = stream->receive();
  assert(!received);
  assert(received.error().code() == -ENOMEM);

  auto closing = std::async(std::launch::async, [&] { stream->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  closing.get();
  assert(channel->request_close());
}

void test_client_concurrent_close_waits_for_one_cleanup() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  trevrpc_cpp_rpc_fake_set_call_release_result(test.fake, test.runtime->native_handle(), -EIO,
                                               true);

  auto first = std::async(std::launch::async, [&] { stream->close(); });
  for (unsigned int attempt = 0;
       attempt != 1000 && trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 0; ++attempt) {
    std::this_thread::yield();
  }
  auto second = std::async(std::launch::async, [&] { stream->close(); });
  assert(first.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  assert(second.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  first.get();
  second.get();
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 1);

  trevrpc_cpp_rpc_fake_set_call_release_result(test.fake, test.runtime->native_handle(), 0, false);
  stream->close();
  assert(channel->request_close());
}

void test_client_status_fin_waits_for_paused_send() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  trevrpc_cpp_rpc_fake_block_send(test.fake);
  const std::array<std::byte, 1> body{std::byte{0x22}};
  auto sending = std::async(std::launch::async, [&] { return stream->send(body); });
  trevrpc_cpp_rpc_fake_wait_send_entered(test.fake);

  assert(trevrpc_cpp_rpc_fake_push_response_status(test.fake, TREVRPC_RPC_STATUS_OK) == 0);
  assert(trevrpc_cpp_rpc_fake_push_stream_readable(test.fake) == 0);
  assert(trevrpc_cpp_rpc_fake_push_stream_receive_fin(test.fake) == 0);
  auto receiving = std::async(std::launch::async, [&] { return stream->receive(); });
  assert(receiving.wait_for(std::chrono::milliseconds(50)) != std::future_status::ready);

  trevrpc_cpp_rpc_fake_release_send(test.fake);
  trevrpc_cpp_rpc_fake_wait_stream_send_calls(test.fake, 2);
  assert(trevrpc_cpp_rpc_fake_push_last_send_complete(test.fake) == 0);
  assert(sending.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto send_result = sending.get();
  assert(send_result);
  assert(receiving.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  auto receive_result = receiving.get();
  assert(receive_result);
  assert(receive_result.value().terminal);
  assert(receive_result.value().status.is_ok());
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) == 1);
  assert(channel->request_close());
}

void test_client_receive_returns_terminal_before_cleanup_retry() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  trevrpc_cpp_rpc_fake_set_call_release_result(test.fake, test.runtime->native_handle(), -EIO, false);
  assert(trevrpc_cpp_rpc_fake_push_response_status(test.fake, TREVRPC_RPC_STATUS_OK) == 0);
  assert(trevrpc_cpp_rpc_fake_push_stream_readable(test.fake) == 0);
  assert(trevrpc_cpp_rpc_fake_push_stream_receive_fin(test.fake) == 0);

  auto terminal = stream->receive();
  assert(terminal);
  assert(terminal.value().terminal);
  assert(!terminal.value().message);
  assert(terminal.value().status.is_ok());

  stream->close();
  assert(channel->close());
}

void test_client_drains_queued_receives_after_terminal_event() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  const std::array<std::uint8_t, 1> body{0x42};
  assert(trevrpc_cpp_rpc_fake_push_response_message(test.fake, body.data(), body.size()) == 0);
  assert(trevrpc_cpp_rpc_fake_push_response_status(test.fake, TREVRPC_RPC_STATUS_OK) == 0);

  RpcEvent receive_fin;
  receive_fin.kind = TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN;
  receive_fin.flags = TREVRPC_RPC_EVENT_FLAG_CLEAN_FIN;
  RpcClientStreamTestPeer::defer_receive_terminal(stream, receive_fin);

  auto message = stream->receive();
  assert(message);
  assert(message.value().message);
  assert(message.value().body.size() == body.size());
  assert(static_cast<std::uint8_t>(message.value().body[0]) == body[0]);

  auto terminal = stream->receive();
  assert(terminal);
  assert(terminal.value().terminal);
  assert(!terminal.value().message);
  assert(terminal.value().status.is_ok());
  assert(channel->request_close());
}

void test_client_close_retry_exhaustion_releases_reservations() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);

  trevrpc_cpp_rpc_fake_set_stream_abort_result(test.fake, -EAGAIN);
  stream->close();

  trevrpc_cpp_rpc_fake_set_stream_abort_result(test.fake, 0);
  stream->close();
  assert(channel->close());
}

void test_endpoint_preregistration() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);

  assert(trevrpc_cpp_rpc_fake_push_connection_closed(test.fake, -ECONNRESET) == 0);
  const trevrpc_rpc_endpoint_v1 wrong_generation{endpoint.owner, endpoint.slot,
                                                 endpoint.generation + 1};
  assert(test.runtime->register_endpoint(wrong_generation));
  assert(test.runtime->register_endpoint(endpoint));

  auto wrong_waiter =
      std::async(std::launch::async, [&] { return test.runtime->wait_endpoint(wrong_generation); });
  RpcEventRuntimeTestPeer::wait_for_endpoint_waiters(*test.runtime, 1);
  auto ready = test.runtime->wait_endpoint(endpoint);
  assert(ready);
  assert(ready.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);
  auto closed = test.runtime->wait_endpoint(endpoint);
  assert(closed);
  assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
  assert(closed.value().operation_id == 0);
  assert(closed.value().status == -ECONNRESET);
  test.runtime->unregister_endpoint(wrong_generation);
  auto wrong_result = wrong_waiter.get();
  assert(!wrong_result);
  assert(wrong_result.error().code() == -ECANCELED);

  test.runtime->unregister_endpoint(endpoint);
  assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
  assert(test.runtime->shutdown());
}

void test_stream_preregistration_and_terminal_order() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);
  auto open_operation = test.runtime->reserve_operation();
  assert(open_operation);
  const auto [call, stream] = open_call(test, endpoint, open_operation.value());

  assert(trevrpc_cpp_rpc_fake_push_stream_closed(test.fake, -ECONNRESET) == 0);
  auto failed = test.runtime->wait_operation(open_operation.value());
  assert(failed);
  assert(failed.value().kind == TREVRPC_RPC_EVENT_CALL_FAILED);
  assert(failed.value().status == -ECONNRESET);

  const trevrpc_rpc_stream_v1 wrong_generation{stream.owner, stream.slot, stream.generation + 1};
  assert(test.runtime->register_stream(wrong_generation));
  assert(test.runtime->register_stream(stream));
  auto wrong_waiter =
      std::async(std::launch::async, [&] { return test.runtime->wait_stream(wrong_generation); });
  RpcEventRuntimeTestPeer::wait_for_stream_waiters(*test.runtime, 1);

  constexpr std::array expected{
      TREVRPC_RPC_EVENT_CALL_FAILED,
      TREVRPC_RPC_EVENT_STREAM_CLOSED,
      TREVRPC_RPC_EVENT_CALL_CLOSED,
  };
  for (const auto kind : expected) {
    auto event = test.runtime->wait_stream(stream);
    assert(event);
    assert(event.value().kind == kind);
  }
  test.runtime->unregister_stream(wrong_generation);
  auto wrong_result = wrong_waiter.get();
  assert(!wrong_result);
  assert(wrong_result.error().code() == -ECANCELED);

  test.runtime->unregister_stream(stream);
  assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), stream) == 0);
  assert(trevrpc_rpc_call_release(test.runtime->native_handle(), call) == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
}

void test_operation_and_stream_dual_routing() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);
  auto open_operation = test.runtime->reserve_operation();
  assert(open_operation);
  const auto [call, stream] = open_call(test, endpoint, open_operation.value());
  assert(test.runtime->register_stream(stream));
  assert(trevrpc_cpp_rpc_fake_push_stream_ready(test.fake) == 0);

  trevrpc_cpp_rpc_fake_wait_stream_send_calls(test.fake, 1);
  assert(trevrpc_cpp_rpc_fake_push_last_send_complete(test.fake) == 0);
  auto ready = test.runtime->wait_operation(open_operation.value());
  assert(ready);
  assert(ready.value().kind == TREVRPC_RPC_EVENT_CALL_READY);

  for (int index = 0; index != 20; ++index) {
    assert(trevrpc_cpp_rpc_fake_push_stream_readable(test.fake) == 0);
  }
  auto readable = test.runtime->wait_stream(stream);
  assert(readable);
  assert(readable.value().kind == TREVRPC_RPC_EVENT_STREAM_READABLE);

  auto close_operation = test.runtime->reserve_operation();
  assert(close_operation);
  assert(trevrpc_rpc_call_close(test.runtime->native_handle(), call, close_operation.value(),
                                TREVRPC_RPC_CLOSE_FLAG_ABORT, 17) == 0);
  auto call_closed = test.runtime->wait_operation(close_operation.value());
  assert(call_closed);
  assert(call_closed.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED);

  bool stream_closed_seen = false;
  bool call_closed_seen = false;
  while (!call_closed_seen) {
    auto event = test.runtime->wait_stream(stream);
    assert(event);
    if (event.value().kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
      assert(!stream_closed_seen);
      stream_closed_seen = true;
    } else if (event.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
      assert(stream_closed_seen);
      assert(event.value().operation_id == close_operation.value());
      call_closed_seen = true;
    } else {
      assert(event.value().kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    }
  }

  test.runtime->unregister_stream(stream);
  assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), stream) == 0);
  assert(trevrpc_rpc_call_release(test.runtime->native_handle(), call) == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
}

void test_incoming_copy() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  constexpr std::string_view body = "owned incoming body";
  assert(trevrpc_cpp_rpc_fake_push_incoming(
             test.fake, "fake.Service", "Incoming", TREVRPC_RPC_KIND_UNARY,
             reinterpret_cast<const std::uint8_t*>(body.data()), body.size()) == 0);
  auto incoming = test.runtime->wait_incoming();
  assert(incoming);
  assert(incoming.value().service == "fake.Service");
  assert(incoming.value().method == "Incoming");
  assert(incoming.value().rpc_kind == TREVRPC_RPC_KIND_UNARY);
  assert(incoming.value().preparation_status == 0);
  assert(incoming.value().initial_message.size() == body.size());
  const auto empty_metadata = incoming.value().metadata.get("empty-incoming");
  assert(empty_metadata.has_value());
  assert(empty_metadata->empty());
  assert(std::equal(incoming.value().initial_message.begin(),
                    incoming.value().initial_message.end(),
                    reinterpret_cast<const std::byte*>(body.data())));

  assert(test.runtime->register_stream(incoming.value().stream));
  auto accept_operation = test.runtime->reserve_operation();
  assert(accept_operation);
  assert(trevrpc_rpc_call_accept(test.runtime->native_handle(), incoming.value().call,
                                 accept_operation.value()) == 0);
  auto accepted = test.runtime->wait_operation(accept_operation.value());
  assert(accepted);
  assert(accepted.value().kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);

  auto close_operation = test.runtime->reserve_operation();
  assert(close_operation);
  assert(trevrpc_rpc_call_close(test.runtime->native_handle(), incoming.value().call,
                                close_operation.value(), TREVRPC_RPC_CLOSE_FLAG_ABORT, 0) == 0);
  auto closed = test.runtime->wait_operation(close_operation.value());
  assert(closed);
  assert(closed.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
  for (;;) {
    auto event = test.runtime->wait_stream(incoming.value().stream);
    assert(event);
    if (event.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
      break;
    }
  }
  test.runtime->unregister_stream(incoming.value().stream);
  assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), incoming.value().stream) == 0);
  assert(trevrpc_rpc_call_release(test.runtime->native_handle(), incoming.value().call) == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
}

void test_stop_incoming_unblocks_waiter_without_stopping_runtime() {
  auto test = make_runtime();
  auto waiting = std::async(std::launch::async, [&] { return test.runtime->wait_incoming(); });
  RpcEventRuntimeTestPeer::wait_for_incoming_waiter(*test.runtime);

  test.runtime->stop_incoming();
  assert(waiting.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
  auto incoming = waiting.get();
  assert(!incoming);
  assert(incoming.error().code() == -ESHUTDOWN);

  auto operation = test.runtime->reserve_operation();
  assert(operation);
  test.runtime->reject_operation(operation.value());
  assert(test.runtime->shutdown());
}

void test_server_core_drains_and_cancels_child_scopes_before_runtime_shutdown() {
  {
    auto test = make_runtime();
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
    assert(test.runtime->register_endpoint(endpoint));
    auto core = RpcServerCore::create(test.runtime, endpoint, 1, 4);
    assert(core);
    auto server = std::move(core).value();
    auto child_scope = std::make_shared<BlockingServerScope>();
    auto lifetime = std::make_shared<int>(0);
    assert(server->register_route(
        "fake.Service", "Async", TREVRPC_RPC_KIND_UNARY, [](std::shared_ptr<ServerCallState>) {},
        lifetime, child_scope));
    assert(server->start());

    trevrpc::ShutdownOptions options;
    options.graceful_timeout = std::chrono::seconds(2);
    options.cancellation_timeout = std::chrono::seconds(2);
    auto shutdown = std::async(std::launch::async, [&] { return server->shutdown(options); });
    child_scope->wait_for_drain();
    assert(shutdown.wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
    child_scope->release();
    auto report = shutdown.get();
    assert(report);
    assert(report.value().outcome == trevrpc::ShutdownOutcome::Graceful);
    assert(report.value().released);
  }

  {
    auto test = make_runtime();
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
    assert(test.runtime->register_endpoint(endpoint));
    auto core = RpcServerCore::create(test.runtime, endpoint, 1, 4);
    assert(core);
    auto server = std::move(core).value();
    auto child_scope = std::make_shared<BlockingServerScope>();
    auto lifetime = std::make_shared<int>(0);
    assert(server->register_route(
        "fake.Service", "Async", TREVRPC_RPC_KIND_UNARY, [](std::shared_ptr<ServerCallState>) {},
        lifetime, child_scope));
    assert(server->start());

    trevrpc::ShutdownOptions options;
    options.graceful_timeout = std::chrono::nanoseconds::zero();
    options.cancellation_timeout = std::chrono::seconds(2);
    auto report = server->shutdown(options);
    assert(report);
    assert(report.value().outcome == trevrpc::ShutdownOutcome::Cancelled);
    assert(report.value().released);
    assert(child_scope->cancelled());
  }
}

void test_server_core_abandonment_drops_dispatcher_thread_owner() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  assert(test.runtime->register_endpoint(endpoint));
  auto core = RpcServerCore::create(test.runtime, endpoint, 1, 4);
  assert(core);
  auto server = std::move(core).value();
  auto destroyed = std::make_shared<std::promise<void>>();
  auto destroyed_future = destroyed->get_future();
  std::shared_ptr<void> lifetime(destroyed.get(), [destroyed](void*) { destroyed->set_value(); });
  assert(server->register_route(
      "fake.Service", "Abandon", TREVRPC_RPC_KIND_UNARY, [](std::shared_ptr<ServerCallState>) {},
      std::move(lifetime)));
  assert(server->start());

  // The dispatcher retains the last server owner. Stopping the runtime makes
  // that dispatcher return, so its self-destruction runs on the dispatcher
  // thread and exercises abandonment of State::dispatcher from that thread.
  server.reset();
  assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, 0) == 0);
  RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);
  assert(destroyed_future.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
}

void test_server_core_retains_returned_handlers_until_terminal_settlement() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  assert(test.runtime->register_endpoint(endpoint));
  auto core = RpcServerCore::create(test.runtime, endpoint, 1, 4);
  assert(core);
  auto server = std::move(core).value();

  std::mutex mutex;
  std::condition_variable condition;
  std::vector<std::shared_ptr<ServerCallState>> calls;
  bool second_entered = false;
  bool release_second = false;
  bool second_returned = false;
  auto lifetime = std::make_shared<int>(0);
  assert(server->register_route(
      "fake.Service", "Retained", TREVRPC_RPC_KIND_UNARY,
      [&](std::shared_ptr<ServerCallState> call) {
        std::unique_lock lock(mutex);
        calls.push_back(std::move(call));
        if (calls.size() != 2) {
          return;
        }
        second_entered = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release_second; });
        second_returned = true;
        condition.notify_all();
      },
      lifetime));
  assert(server->start());

  assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake, "fake.Service", "Retained",
                                            TREVRPC_RPC_KIND_UNARY, nullptr, 0) == 0);
  assert(trevrpc_cpp_rpc_fake_push_second_incoming(test.fake, "fake.Service", "Retained",
                                                   TREVRPC_RPC_KIND_UNARY, nullptr, 0) == 0);
  std::vector<std::shared_ptr<ServerCallState>> retained;
  {
    std::unique_lock lock(mutex);
    const bool entered =
        condition.wait_for(lock, std::chrono::seconds(2), [&] { return second_entered; });
    assert(entered);
    retained = calls;
  }
  assert(retained.size() == 2);
  assert(server->active() == 2);

  for (std::size_t index = 0; index < retained.size(); ++index) {
    auto settling = std::async(std::launch::async, [call = retained[index]] {
      return trevrpc::sync_wait(call->respond({}, trevrpc::Status::ok()));
    });
    trevrpc_cpp_rpc_fake_wait_stream_send_calls(test.fake, static_cast<unsigned int>(index + 1));
    assert(trevrpc_cpp_rpc_fake_push_last_server_send_complete(test.fake, index != 0) == 0);
    assert(settling.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    (void)settling.get();
    const std::size_t expected_active = retained.size() - index - 1;
    for (unsigned int attempt = 0; attempt != 2000 && server->active() != expected_active;
         ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert(server->active() == expected_active);
  }
  {
    std::unique_lock lock(mutex);
    release_second = true;
    condition.notify_all();
    const bool returned =
        condition.wait_for(lock, std::chrono::seconds(2), [&] { return second_returned; });
    assert(returned);
  }
  retained.clear();
  calls.clear();

  assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, 0) == 0);
  RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);
  server->cancel_for_abandonment();
  server.reset();
}

void test_callbacks_and_unbounded_operations() {
  auto test = make_runtime();
  std::array<std::uint64_t, 32> operation_ids{};
  for (auto& operation_id : operation_ids) {
    auto operation = test.runtime->reserve_operation();
    assert(operation);
    operation_id = operation.value();
  }

  trevrpc_rpc_endpoint_v1 endpoint{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake, test.runtime->native_handle(),
                                             TREVRPC_RPC_ENDPOINT_CLIENT, operation_ids.front(),
                                             &endpoint) == 0);
  assert(test.runtime->register_endpoint(endpoint));

  std::promise<trevrpc::Result<RpcEvent>> operation_result;
  assert(test.runtime->subscribe_operation(
      operation_ids.front(),
      [&](trevrpc::Result<RpcEvent> result) { operation_result.set_value(std::move(result)); }));
  auto duplicate_operation =
      test.runtime->subscribe_operation(operation_ids.front(), [](trevrpc::Result<RpcEvent>) {});
  assert(!duplicate_operation);
  assert(duplicate_operation.error().code() == -EBUSY);
  auto busy_operation = test.runtime->wait_operation(operation_ids.front());
  assert(!busy_operation);
  assert(busy_operation.error().code() == -EBUSY);

  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  auto ready = operation_result.get_future().get();
  assert(ready);
  assert(ready.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);
  for (std::size_t index = 1; index < operation_ids.size(); ++index) {
    test.runtime->reject_operation(operation_ids[index]);
  }

  std::promise<trevrpc::Result<RpcEvent>> endpoint_result;
  assert(test.runtime->subscribe_endpoint(endpoint, [&](trevrpc::Result<RpcEvent> result) {
    auto reentrant_shutdown = test.runtime->shutdown();
    assert(!reentrant_shutdown);
    assert(reentrant_shutdown.error().code() == -EDEADLK);
    endpoint_result.set_value(std::move(result));
  }));
  auto endpoint_ready = endpoint_result.get_future().get();
  assert(endpoint_ready);
  assert(endpoint_ready.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);

  auto close_operation = test.runtime->reserve_operation();
  assert(close_operation);
  assert(trevrpc_rpc_endpoint_close(test.runtime->native_handle(), endpoint,
                                    close_operation.value()) == 0);
  auto closed = test.runtime->wait_operation(close_operation.value());
  assert(closed);
  assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
  test.runtime->unregister_endpoint(endpoint);
  assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
  assert(test.runtime->shutdown());
}

void test_callback_blocking_waits_fail_without_deadlock() {
  auto test = make_runtime();

  auto callback_operation = test.runtime->reserve_operation();
  auto pending_operation = test.runtime->reserve_operation();
  assert(callback_operation);
  assert(pending_operation);
  std::promise<int> operation_wait_error;
  assert(test.runtime->subscribe_operation(
      callback_operation.value(), [&](trevrpc::Result<RpcEvent> result) {
        assert(result);
        auto waited = test.runtime->wait_operation(pending_operation.value());
        operation_wait_error.set_value(waited ? 0 : waited.error().code());
      }));
  RpcEvent operation_event;
  operation_event.kind = TREVRPC_RPC_EVENT_CALL_READY;
  operation_event.operation_id = callback_operation.value();
  RpcEventRuntimeTestPeer::dispatch(*test.runtime, std::move(operation_event));
  assert(operation_wait_error.get_future().get() == -EDEADLK);
  test.runtime->reject_operation(pending_operation.value());

  const trevrpc_rpc_stream_v1 callback_stream{43, 6, 2};
  assert(test.runtime->register_stream(callback_stream));
  std::promise<int> stream_wait_error;
  assert(test.runtime->subscribe_stream(callback_stream, [&](trevrpc::Result<RpcEvent> result) {
    assert(result);
    auto waited = test.runtime->wait_stream(callback_stream);
    stream_wait_error.set_value(waited ? 0 : waited.error().code());
  }));
  RpcEvent stream_event;
  stream_event.kind = TREVRPC_RPC_EVENT_STREAM_READABLE;
  stream_event.stream = callback_stream;
  RpcEventRuntimeTestPeer::dispatch(*test.runtime, std::move(stream_event));
  assert(stream_wait_error.get_future().get() == -EDEADLK);
  test.runtime->unregister_stream(callback_stream);

  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  std::promise<trevrpc::Result<RpcIncomingCall>> incoming_result;
  std::promise<int> incoming_wait_error;
  assert(test.runtime->subscribe_incoming([&](trevrpc::Result<RpcIncomingCall> result) noexcept {
    auto waited = test.runtime->wait_incoming();
    incoming_wait_error.set_value(waited ? 0 : waited.error().code());
    incoming_result.set_value(std::move(result));
  }));
  assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake, "fake.Service", "CallbackWait",
                                            TREVRPC_RPC_KIND_UNARY, nullptr, 0) == 0);
  assert(incoming_wait_error.get_future().get() == -EDEADLK);
  auto incoming = incoming_result.get_future().get();
  assert(incoming);

  assert(test.runtime->register_stream(incoming.value().stream));
  auto close_operation = test.runtime->reserve_operation();
  assert(close_operation);
  assert(trevrpc_rpc_call_close(test.runtime->native_handle(), incoming.value().call,
                                close_operation.value(), TREVRPC_RPC_CLOSE_FLAG_ABORT, 0) == 0);
  assert(test.runtime->wait_operation(close_operation.value()));
  while (true) {
    auto terminal = test.runtime->wait_stream(incoming.value().stream);
    assert(terminal);
    if (terminal.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
      break;
    }
  }
  test.runtime->unregister_stream(incoming.value().stream);
  assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), incoming.value().stream) == 0);
  assert(trevrpc_rpc_call_release(test.runtime->native_handle(), incoming.value().call) == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
}

void test_stream_and_incoming_subscriptions() {
  {
    auto test = make_runtime();
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);
    auto open_operation = test.runtime->reserve_operation();
    assert(open_operation);
    const auto [call, stream] = open_call(test, endpoint, open_operation.value());
    assert(test.runtime->register_stream(stream));
    assert(trevrpc_cpp_rpc_fake_push_stream_ready(test.fake) == 0);
    trevrpc_cpp_rpc_fake_wait_stream_send_calls(test.fake, 1);
    assert(trevrpc_cpp_rpc_fake_push_last_send_complete(test.fake) == 0);
    auto call_ready = test.runtime->wait_operation(open_operation.value());
    assert(call_ready);
    assert(call_ready.value().kind == TREVRPC_RPC_EVENT_CALL_READY);

    std::promise<trevrpc::Result<RpcEvent>> stream_result;
    assert(test.runtime->subscribe_stream(stream, [&](trevrpc::Result<RpcEvent> result) {
      stream_result.set_value(std::move(result));
    }));
    auto duplicate_stream =
        test.runtime->subscribe_stream(stream, [](trevrpc::Result<RpcEvent>) {});
    assert(!duplicate_stream);
    assert(duplicate_stream.error().code() == -EBUSY);
    auto busy_stream = test.runtime->wait_stream(stream);
    assert(!busy_stream);
    assert(busy_stream.error().code() == -EBUSY);
    assert(trevrpc_cpp_rpc_fake_push_stream_readable(test.fake) == 0);
    auto readable = stream_result.get_future().get();
    assert(readable);
    assert(readable.value().kind == TREVRPC_RPC_EVENT_STREAM_READABLE);

    auto close_operation = test.runtime->reserve_operation();
    assert(close_operation);
    assert(trevrpc_rpc_call_close(test.runtime->native_handle(), call, close_operation.value(),
                                  TREVRPC_RPC_CLOSE_FLAG_ABORT, 0) == 0);
    auto closed = test.runtime->wait_operation(close_operation.value());
    assert(closed);
    while (true) {
      auto terminal = test.runtime->wait_stream(stream);
      assert(terminal);
      if (terminal.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
        break;
      }
    }
    test.runtime->unregister_stream(stream);
    assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), stream) == 0);
    assert(trevrpc_rpc_call_release(test.runtime->native_handle(), call) == 0);
    close_endpoint(test, endpoint);
    assert(test.runtime->shutdown());
  }

  {
    auto test = make_runtime();
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
    std::promise<trevrpc::Result<RpcIncomingCall>> incoming_result;
    assert(test.runtime->subscribe_incoming([&](trevrpc::Result<RpcIncomingCall> result) noexcept {
      incoming_result.set_value(std::move(result));
    }));
    auto duplicate_incoming =
        test.runtime->subscribe_incoming([](trevrpc::Result<RpcIncomingCall>) noexcept {});
    assert(!duplicate_incoming);
    assert(duplicate_incoming.error().code() == -EBUSY);
    auto busy_incoming = test.runtime->wait_incoming();
    assert(!busy_incoming);
    assert(busy_incoming.error().code() == -EBUSY);

    constexpr std::string_view body = "callback incoming body";
    assert(trevrpc_cpp_rpc_fake_push_incoming(
               test.fake, "fake.Service", "Callback", TREVRPC_RPC_KIND_UNARY,
               reinterpret_cast<const std::uint8_t*>(body.data()), body.size()) == 0);
    auto incoming = incoming_result.get_future().get();
    assert(incoming);
    assert(incoming.value().initial_message.size() == body.size());

    assert(test.runtime->register_stream(incoming.value().stream));
    auto close_operation = test.runtime->reserve_operation();
    assert(close_operation);
    assert(trevrpc_rpc_call_close(test.runtime->native_handle(), incoming.value().call,
                                  close_operation.value(), TREVRPC_RPC_CLOSE_FLAG_ABORT, 0) == 0);
    auto closed = test.runtime->wait_operation(close_operation.value());
    assert(closed);
    while (true) {
      auto terminal = test.runtime->wait_stream(incoming.value().stream);
      assert(terminal);
      if (terminal.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
        break;
      }
    }
    test.runtime->unregister_stream(incoming.value().stream);
    assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), incoming.value().stream) == 0);
    assert(trevrpc_rpc_call_release(test.runtime->native_handle(), incoming.value().call) == 0);
    close_endpoint(test, endpoint);
    assert(test.runtime->shutdown());
  }
}

void test_callback_cancellation() {
  auto test = make_runtime();

  auto operation = test.runtime->reserve_operation();
  assert(operation);
  std::promise<trevrpc::Result<RpcEvent>> operation_result;
  assert(
      test.runtime->subscribe_operation(operation.value(), [&](trevrpc::Result<RpcEvent> result) {
        operation_result.set_value(std::move(result));
      }));
  test.runtime->reject_operation(operation.value());
  auto cancelled_operation = operation_result.get_future().get();
  assert(!cancelled_operation);
  assert(cancelled_operation.error().code() == -ECANCELED);

  const trevrpc_rpc_stream_v1 stream{41, 2, 3};
  assert(test.runtime->register_stream(stream));
  std::promise<trevrpc::Result<RpcEvent>> stream_result;
  assert(test.runtime->subscribe_stream(stream, [&](trevrpc::Result<RpcEvent> result) {
    stream_result.set_value(std::move(result));
  }));
  test.runtime->unregister_stream(stream);
  auto cancelled_stream = stream_result.get_future().get();
  assert(!cancelled_stream);
  assert(cancelled_stream.error().code() == -ECANCELED);

  const trevrpc_rpc_endpoint_v1 endpoint{51, 4, 5};
  assert(test.runtime->register_endpoint(endpoint));
  std::promise<trevrpc::Result<RpcEvent>> endpoint_result;
  assert(test.runtime->subscribe_endpoint(endpoint, [&](trevrpc::Result<RpcEvent> result) {
    endpoint_result.set_value(std::move(result));
  }));
  test.runtime->unregister_endpoint(endpoint);
  auto cancelled_endpoint = endpoint_result.get_future().get();
  assert(!cancelled_endpoint);
  assert(cancelled_endpoint.error().code() == -ECANCELED);

  std::promise<trevrpc::Result<RpcIncomingCall>> incoming_result;
  assert(test.runtime->subscribe_incoming([&](trevrpc::Result<RpcIncomingCall> result) noexcept {
    incoming_result.set_value(std::move(result));
  }));
  auto shutdown = test.runtime->shutdown();
  assert(shutdown);
  auto cancelled_incoming = incoming_result.get_future().get();
  assert(!cancelled_incoming);
  assert(cancelled_incoming.error().code() == -ESHUTDOWN);
}

void test_endpoint_capacity_recovery() {
  trevrpc_rpc_runtime_config_v1 config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
  config.event_capacity = 1;
  config.endpoint_capacity = 1;
  config.call_capacity = 1;
  config.stream_capacity = 1;
  auto test = make_runtime(&config);

  for (int iteration = 0; iteration != 3; ++iteration) {
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);
    assert(test.runtime->register_endpoint(endpoint));
    auto ready = test.runtime->wait_endpoint(endpoint);
    assert(ready);
    assert(ready.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);

    auto close_operation = test.runtime->reserve_operation();
    assert(close_operation);
    const int close_error = trevrpc_rpc_endpoint_close(test.runtime->native_handle(), endpoint,
                                                       close_operation.value());
    assert(close_error == 0);
    auto closed = test.runtime->wait_operation(close_operation.value());
    assert(closed);
    assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    auto subject_closed = test.runtime->wait_endpoint(endpoint);
    assert(subject_closed);
    assert(subject_closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    test.runtime->unregister_endpoint(endpoint);
    assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
  }
  assert(test.runtime->shutdown());
}

void test_split_close_and_spontaneous_stop() {
  {
    auto test = make_runtime();
    trevrpc_cpp_rpc_fake_block_close(test.fake);
    auto close = std::async(std::launch::async, [&] { return test.runtime->request_close(); });
    trevrpc_cpp_rpc_fake_wait_close_entered(test.fake);
    auto early_release = test.runtime->drain_and_release();
    assert(!early_release);
    assert(early_release.error().code() == -EAGAIN);
    trevrpc_cpp_rpc_fake_release_close(test.fake);
    auto close_operation = close.get();
    assert(close_operation);
    assert(close_operation.value() != 0);
    auto stopped = test.runtime->wait_operation(close_operation.value());
    assert(stopped);
    assert(stopped.value().kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(test.runtime->drain_and_release());
  }

  {
    auto test = make_runtime();
    assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, -EIO) == 0);
    RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);
    auto stopped = test.runtime->shutdown();
    assert(!stopped);
    assert(stopped.error().code() == -EIO);
    assert(test.runtime->native_handle() == nullptr);
    assert(test.runtime->shutdown());
  }

  {
    auto test = make_runtime();
    trevrpc_cpp_rpc_fake_block_close(test.fake);
    auto close = std::async(std::launch::async, [&] { return test.runtime->request_close(); });
    trevrpc_cpp_rpc_fake_wait_close_entered(test.fake);
    assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, 0) == 0);
    RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);
    trevrpc_cpp_rpc_fake_release_close(test.fake);

    auto close_operation = close.get();
    assert(close_operation);
    assert(close_operation.value() != 0);
    auto stopped = test.runtime->wait_operation(close_operation.value());
    assert(stopped);
    assert(stopped.value().kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(test.runtime->drain_and_release());
  }
}

void test_strict_shutdown_retries_joined_close_failure() {
  auto test = make_runtime();
  trevrpc_cpp_rpc_fake_set_close_result(test.fake, -EAGAIN);
  trevrpc_cpp_rpc_fake_block_close(test.fake);
  auto public_close = std::async(std::launch::async, [&] { return test.runtime->request_close(); });
  trevrpc_cpp_rpc_fake_wait_close_entered(test.fake);

  auto release_close = std::async(std::launch::async, [&] {
    const bool joined = RpcEventRuntimeTestPeer::wait_for_close_submission_waiters(
        *test.runtime, 1, std::chrono::seconds(2));
    trevrpc_cpp_rpc_fake_release_close(test.fake);
    return joined;
  });
  assert(test.runtime->shutdown());
  assert(release_close.get());

  auto public_result = public_close.get();
  assert(!public_result);
  assert(public_result.error().code() == -EAGAIN);
}

void test_shutdown_completion_single_consumer() {
  {
    auto test = make_runtime();
    auto close = test.runtime->request_close();
    assert(close);
    auto stopped = test.runtime->wait_operation(close.value());
    assert(stopped);
    assert(stopped.value().kind == TREVRPC_RPC_EVENT_STOPPED);

    auto repeated_wait = test.runtime->wait_operation(close.value());
    assert(!repeated_wait);
    assert(repeated_wait.error().code() == -EALREADY);
    auto repeated_close = test.runtime->request_close();
    assert(!repeated_close);
    assert(repeated_close.error().code() == -EALREADY);
    assert(test.runtime->drain_and_release());
  }

  {
    auto test = make_runtime();
    auto close = test.runtime->request_close();
    assert(close);
    std::promise<trevrpc::Result<RpcEvent>> completion;
    assert(test.runtime->subscribe_operation(close.value(), [&](trevrpc::Result<RpcEvent> result) {
      completion.set_value(std::move(result));
    }));
    auto stopped = completion.get_future().get();
    assert(stopped);
    assert(stopped.value().kind == TREVRPC_RPC_EVENT_STOPPED);

    auto repeated_wait = test.runtime->wait_operation(close.value());
    assert(!repeated_wait);
    assert(repeated_wait.error().code() == -EALREADY);
    auto repeated_subscription =
        test.runtime->subscribe_operation(close.value(), [](trevrpc::Result<RpcEvent>) {});
    assert(!repeated_subscription);
    assert(repeated_subscription.error().code() == -EALREADY);
    for (;;) {
      auto released = test.runtime->drain_and_release();
      if (released) {
        break;
      }
      assert(released.error().code() == -EBUSY);
      std::this_thread::yield();
    }
  }
}

void test_spontaneous_stop_drops_last_owner() {
  auto test = make_runtime();
  assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, 0) == 0);
  RpcEventRuntimeTestPeer::drop_last_owner_after_stopped(test.runtime);
}

void test_driver_failure_rejects_new_consumers() {
  auto test = make_runtime();
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  const trevrpc_rpc_stream_v1 stream{17, 3, 1};
  const trevrpc_rpc_endpoint_v1 endpoint{19, 5, 1};
  assert(test.runtime->register_stream(stream));
  assert(test.runtime->register_endpoint(endpoint));

  RpcEventRuntimeTestPeer::fail_driver(*test.runtime, -EIO);

  auto reserved = test.runtime->reserve_operation();
  assert(!reserved);
  assert(reserved.error().code() == -EIO);
  auto registered_stream = test.runtime->register_stream(trevrpc_rpc_stream_v1{23, 7, 1});
  assert(!registered_stream);
  assert(registered_stream.error().code() == -EIO);
  auto registered_endpoint = test.runtime->register_endpoint(trevrpc_rpc_endpoint_v1{29, 11, 1});
  assert(!registered_endpoint);
  assert(registered_endpoint.error().code() == -EIO);

  auto operation_subscription =
      test.runtime->subscribe_operation(operation.value(), [](trevrpc::Result<RpcEvent>) {});
  assert(!operation_subscription);
  assert(operation_subscription.error().code() == -EIO);
  auto stream_subscription =
      test.runtime->subscribe_stream(stream, [](trevrpc::Result<RpcEvent>) {});
  assert(!stream_subscription);
  assert(stream_subscription.error().code() == -EIO);
  auto endpoint_subscription =
      test.runtime->subscribe_endpoint(endpoint, [](trevrpc::Result<RpcEvent>) {});
  assert(!endpoint_subscription);
  assert(endpoint_subscription.error().code() == -EIO);
  auto incoming_subscription =
      test.runtime->subscribe_incoming([](trevrpc::Result<RpcIncomingCall>) noexcept {});
  assert(!incoming_subscription);
  assert(incoming_subscription.error().code() == -EIO);

  test.runtime->reject_operation(operation.value());
  test.runtime->unregister_stream(stream);
  test.runtime->unregister_endpoint(endpoint);
  assert(test.runtime->shutdown());
  assert(test.runtime->native_handle() == nullptr);
}

void test_buffered_completions_precede_driver_failure() {
  {
    auto test = make_runtime();
    auto operation = test.runtime->reserve_operation();
    assert(operation);
    RpcEvent event;
    event.kind = TREVRPC_RPC_EVENT_CALL_READY;
    event.operation_id = operation.value();
    RpcEventRuntimeTestPeer::dispatch(*test.runtime, std::move(event));
    RpcEventRuntimeTestPeer::fail_driver(*test.runtime, -EIO);
    RpcEventRuntimeTestPeer::wait_for_terminal_settlement(*test.runtime);
    auto busy = test.runtime->shutdown();
    assert(!busy);
    assert(busy.error().code() == -EBUSY);

    std::promise<trevrpc::Result<RpcEvent>> result;
    assert(test.runtime->subscribe_operation(
        operation.value(),
        [&](trevrpc::Result<RpcEvent> delivered) { result.set_value(std::move(delivered)); }));
    auto delivered = result.get_future().get();
    assert(delivered);
    assert(delivered.value().kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(test.runtime->shutdown());
  }

  {
    auto test = make_runtime();
    const trevrpc_rpc_stream_v1 stream{31, 8, 2};
    assert(test.runtime->register_stream(stream));
    RpcEvent event;
    event.kind = TREVRPC_RPC_EVENT_STREAM_READABLE;
    event.stream = stream;
    RpcEventRuntimeTestPeer::dispatch(*test.runtime, std::move(event));
    RpcEventRuntimeTestPeer::fail_driver(*test.runtime, -EIO);
    RpcEventRuntimeTestPeer::wait_for_terminal_settlement(*test.runtime);

    std::promise<trevrpc::Result<RpcEvent>> result;
    assert(test.runtime->subscribe_stream(stream, [&](trevrpc::Result<RpcEvent> delivered) {
      result.set_value(std::move(delivered));
    }));
    auto delivered = result.get_future().get();
    assert(delivered);
    assert(delivered.value().kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    test.runtime->unregister_stream(stream);
    assert(test.runtime->shutdown());
  }
}

void test_waiter_cancellation() {
  auto test = make_runtime();
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  auto operation_waiter = std::async(
      std::launch::async, [&] { return test.runtime->wait_operation(operation.value()); });
  RpcEventRuntimeTestPeer::wait_for_operation_waiters(*test.runtime, 1);
  test.runtime->reject_operation(operation.value());
  auto operation_result = operation_waiter.get();
  assert(!operation_result);
  assert(operation_result.error().code() == -ECANCELED);

  const trevrpc_rpc_stream_v1 stream{41, 2, 3};
  assert(test.runtime->register_stream(stream));
  auto stream_waiter =
      std::async(std::launch::async, [&] { return test.runtime->wait_stream(stream); });
  RpcEventRuntimeTestPeer::wait_for_stream_waiters(*test.runtime, 1);
  test.runtime->unregister_stream(stream);
  auto stream_result = stream_waiter.get();
  assert(!stream_result);
  assert(stream_result.error().code() == -ECANCELED);

  auto incoming_waiter =
      std::async(std::launch::async, [&] { return test.runtime->wait_incoming(); });
  RpcEventRuntimeTestPeer::wait_for_incoming_waiter(*test.runtime);
  assert(test.runtime->shutdown());
  auto incoming_result = incoming_waiter.get();
  assert(!incoming_result);
  assert(incoming_result.error().code() == -ESHUTDOWN);
}

void test_destructor_after_close_rejection() {
  auto test = make_runtime();
  trevrpc_cpp_rpc_fake_set_close_result(test.fake, -EAGAIN);
  test.runtime.reset();
}

void test_control_wake_failures_make_progress() {
  {
    auto test = make_runtime();
    RpcEventRuntimeTestPeer::fail_next_control_write(*test.runtime, EINTR);
    auto shutdown = std::async(std::launch::async, [&] { return test.runtime->shutdown(); });
    assert(shutdown.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    assert(shutdown.get());
  }

  {
    auto test = make_runtime();
    RpcEventRuntimeTestPeer::fail_next_control_write(*test.runtime, EIO);
    auto shutdown = std::async(std::launch::async, [&] { return test.runtime->shutdown(); });
    assert(shutdown.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    assert(shutdown.get());
  }
}

void test_shutdown_callback_fails_once_without_stopped_event() {
  auto test = make_runtime();
  RpcEventRuntimeTestPeer::pause_driver(*test.runtime);
  auto close = test.runtime->request_close();
  assert(close);

  std::promise<trevrpc::Result<RpcEvent>> completion;
  assert(test.runtime->subscribe_operation(close.value(), [&](trevrpc::Result<RpcEvent> result) {
    completion.set_value(std::move(result));
  }));
  RpcEventRuntimeTestPeer::settle_driver_failure(*test.runtime, -EIO);

  auto failed = completion.get_future().get();
  assert(!failed);
  assert(failed.error().code() == -EIO);
  auto repeated =
      test.runtime->subscribe_operation(close.value(), [](trevrpc::Result<RpcEvent>) {});
  assert(!repeated);
  assert(repeated.error().code() == -EALREADY);

  RpcEventRuntimeTestPeer::resume_driver(*test.runtime);
  RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);
  assert(test.runtime->shutdown());
}

void test_driver_failure_close_retries() {
  {
    auto test = make_runtime();
    trevrpc_cpp_rpc_fake_set_close_result(test.fake, -EAGAIN);
    RpcEventRuntimeTestPeer::fail_driver(*test.runtime, -EIO);
    RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);
    assert(test.runtime->shutdown());
  }

  {
    auto test = make_runtime();
    trevrpc_cpp_rpc_fake_set_persistent_close_result(test.fake, -EAGAIN);
    RpcEventRuntimeTestPeer::fail_driver(*test.runtime, -EIO);
    auto rejected = std::async(std::launch::async, [&] { return test.runtime->shutdown(); });
    assert(rejected.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    auto rejected_result = rejected.get();
    assert(!rejected_result);
    assert(rejected_result.error().code() == -EAGAIN);

    trevrpc_cpp_rpc_fake_set_persistent_close_result(test.fake, 0);
    RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);
    assert(test.runtime->shutdown());
  }
}

void test_concurrent_nonclean_shutdown() {
  auto test = make_runtime();
  trevrpc_cpp_rpc_fake_set_close_status(test.fake, -EIO);
  trevrpc_cpp_rpc_fake_block_close(test.fake);

  auto first = std::async(std::launch::async, [&] { return test.runtime->shutdown(); });
  trevrpc_cpp_rpc_fake_wait_close_entered(test.fake);

  constexpr std::size_t waiter_count = 7;
  std::array<std::future<trevrpc::Result<void>>, waiter_count> waiters;
  for (auto& waiter : waiters) {
    waiter = std::async(std::launch::async, [&] { return test.runtime->shutdown(); });
  }
  RpcEventRuntimeTestPeer::wait_for_shutdown_waiters(*test.runtime, waiter_count);
  trevrpc_cpp_rpc_fake_release_close(test.fake);

  auto first_result = first.get();
  assert(!first_result);
  assert(first_result.error().code() == -EIO);
  for (auto& waiter : waiters) {
    auto result = waiter.get();
    assert(!result);
    assert(result.error().code() == -EIO);
  }
  assert(test.runtime->shutdown());
}

void test_shutdown_busy_then_retry() {
  auto test = make_runtime();
  const trevrpc_rpc_stream_v1 binding_stream{71, 9, 3};
  assert(test.runtime->register_stream(binding_stream));

  auto busy = test.runtime->shutdown();
  assert(!busy);
  assert(busy.error().code() == -EBUSY);
  assert(test.runtime->native_handle() != nullptr);

  test.runtime->unregister_stream(binding_stream);
  assert(test.runtime->shutdown());
  assert(test.runtime->native_handle() == nullptr);
}

void test_strict_shutdown_generation_results() {
  auto test = make_runtime();
  const trevrpc_rpc_stream_v1 binding_stream{73, 10, 4};
  assert(test.runtime->register_stream(binding_stream));
  RpcEventRuntimeTestPeer::block_next_completed_strict_waiter(*test.runtime);

  auto first = std::async(std::launch::async, [&] { return test.runtime->shutdown(); });
  RpcEventRuntimeTestPeer::wait_for_blocked_strict_waiter(*test.runtime);
  test.runtime->unregister_stream(binding_stream);

  auto second = std::async(std::launch::async, [&] { return test.runtime->shutdown(); });
  auto second_result = second.get();
  assert(second_result);
  RpcEventRuntimeTestPeer::release_blocked_strict_waiter(*test.runtime);

  auto first_result = first.get();
  assert(!first_result);
  assert(first_result.error().code() == -EBUSY);
}

void test_native_runtime_lease_pins_release() {
  auto test = make_runtime();
  auto lease = test.runtime->native_handle();
  assert(lease != nullptr);
  assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, 0) == 0);
  RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);

  auto busy = test.runtime->drain_and_release();
  assert(!busy);
  assert(busy.error().code() == -EBUSY);
  lease = {};
  assert(test.runtime->drain_and_release());
}

void test_last_owner_dropped_from_callback() {
  auto test = make_runtime();
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  trevrpc_rpc_endpoint_v1 endpoint{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake, test.runtime->native_handle(),
                                             TREVRPC_RPC_ENDPOINT_CLIENT, operation.value(),
                                             &endpoint) == 0);

  std::weak_ptr<RpcEventRuntime> observer = test.runtime;
  std::weak_ptr<void> state_observer = RpcEventRuntimeTestPeer::state_observer(*test.runtime);
  std::promise<void> callback_done;
  auto callback_owner = test.runtime;
  assert(test.runtime->subscribe_operation(
      operation.value(), [owner = std::move(callback_owner),
                          &callback_done](trevrpc::Result<RpcEvent> result) mutable {
        assert(result);
        assert(result.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);
        owner.reset();
        callback_done.set_value();
      }));
  test.runtime.reset();
  assert(trevrpc_cpp_rpc_fake_push_connection_ready(test.fake) == 0);
  callback_done.get_future().get();
  while (!observer.expired()) {
    std::this_thread::yield();
  }
  while (!state_observer.expired()) {
    std::this_thread::yield();
  }
}

void test_pending_stream_sequence_at_tiny_cap() {
  trevrpc_rpc_runtime_config_v1 config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
  config.event_capacity = 1;
  config.endpoint_capacity = 1;
  config.call_capacity = 1;
  config.stream_capacity = 1;
  auto test = make_runtime(&config);
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  const auto [call, stream] = open_call(test, endpoint, operation.value());

  RpcEvent readable;
  readable.kind = TREVRPC_RPC_EVENT_STREAM_READABLE;
  readable.stream = stream;
  RpcEventRuntimeTestPeer::dispatch(*test.runtime, std::move(readable));
  RpcEventRuntimeTestPeer::wait_for_pending_stream_events(*test.runtime, stream, 1);

  assert(trevrpc_cpp_rpc_fake_push_stream_closed(test.fake, -ECONNRESET) == 0);
  RpcEventRuntimeTestPeer::wait_for_pending_stream_events(*test.runtime, stream, 4);
  auto failed = test.runtime->wait_operation(operation.value());
  assert(failed);
  assert(failed.value().kind == TREVRPC_RPC_EVENT_CALL_FAILED);
  assert(test.runtime->register_stream(stream));
  constexpr std::array expected{
      TREVRPC_RPC_EVENT_STREAM_READABLE,
      TREVRPC_RPC_EVENT_CALL_FAILED,
      TREVRPC_RPC_EVENT_STREAM_CLOSED,
      TREVRPC_RPC_EVENT_CALL_CLOSED,
  };
  for (const std::uint32_t kind : expected) {
    auto event = test.runtime->wait_stream(stream);
    assert(event);
    assert(event.value().kind == kind);
  }
  test.runtime->unregister_stream(stream);
  assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), stream) == 0);
  assert(trevrpc_rpc_call_release(test.runtime->native_handle(), call) == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
}

void test_malformed_incoming_pointers_are_rejected() {
  const std::array<std::uint32_t, 4> malformed_kinds{
      TREVRPC_CPP_RPC_FAKE_MALFORMED_SERVICE,
      TREVRPC_CPP_RPC_FAKE_MALFORMED_METHOD,
      TREVRPC_CPP_RPC_FAKE_MALFORMED_DATA,
      TREVRPC_CPP_RPC_FAKE_MALFORMED_METADATA,
  };
  for (const auto malformed_kind : malformed_kinds) {
    auto test = make_runtime();
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
    trevrpc_cpp_rpc_fake_malformed_next_incoming(test.fake, test.runtime->native_handle(),
                                                 malformed_kind);
    constexpr std::string_view body = "malformed incoming";
    assert(trevrpc_cpp_rpc_fake_push_incoming(
               test.fake, "fake.Service", "Malformed", TREVRPC_RPC_KIND_UNARY,
               reinterpret_cast<const std::uint8_t*>(body.data()), body.size()) == 0);

    auto incoming = test.runtime->wait_incoming();
    if (malformed_kind == TREVRPC_CPP_RPC_FAKE_MALFORMED_SERVICE ||
        malformed_kind == TREVRPC_CPP_RPC_FAKE_MALFORMED_METHOD) {
      assert(!incoming);
      assert(incoming.error().code() == -EINVAL);
    } else {
      assert(incoming);
      assert(incoming.value().preparation_status == -EINVAL);
      assert(test.runtime->register_stream(incoming.value().stream));
      auto close_operation = test.runtime->reserve_operation();
      assert(close_operation);
      assert(trevrpc_rpc_call_close(test.runtime->native_handle(), incoming.value().call,
                                    close_operation.value(), TREVRPC_RPC_CLOSE_FLAG_ABORT, 0) == 0);
      assert(test.runtime->wait_operation(close_operation.value()));
      for (;;) {
        auto terminal = test.runtime->wait_stream(incoming.value().stream);
        assert(terminal);
        if (terminal.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
          break;
        }
      }
      test.runtime->unregister_stream(incoming.value().stream);
      assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), incoming.value().stream) ==
             0);
      assert(trevrpc_rpc_call_release(test.runtime->native_handle(), incoming.value().call) == 0);
    }
    close_endpoint(test, endpoint);
    assert(test.runtime->shutdown());
  }
}

void test_incoming_body_pressure_and_overload_followups() {
  trevrpc_rpc_runtime_config_v1 config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
  config.event_capacity = 1;
  config.endpoint_capacity = 1;
  config.call_capacity = 2;
  config.stream_capacity = 2;
  std::vector<std::uint8_t> body(256 * 1024, 0x5a);

  {
    auto test = make_runtime(&config);
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
    assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake, "fake.Service", "Pressure",
                                              TREVRPC_RPC_KIND_UNARY, body.data(),
                                              body.size()) == 0);
    RpcEventRuntimeTestPeer::wait_for_incoming_backlog(*test.runtime, 1);

    auto incoming = test.runtime->wait_incoming();
    assert(incoming);
    assert(incoming.value().method == "Pressure");
    assert(incoming.value().initial_message.size() == body.size());
    assert(std::equal(incoming.value().initial_message.begin(),
                      incoming.value().initial_message.end(),
                      reinterpret_cast<const std::byte*>(body.data())));

    assert(test.runtime->register_stream(incoming.value().stream));
    auto close_operation = test.runtime->reserve_operation();
    assert(close_operation);
    assert(trevrpc_rpc_call_close(test.runtime->native_handle(), incoming.value().call,
                                  close_operation.value(), TREVRPC_RPC_CLOSE_FLAG_ABORT, 0) == 0);
    assert(test.runtime->wait_operation(close_operation.value()));
    while (true) {
      auto terminal = test.runtime->wait_stream(incoming.value().stream);
      assert(terminal);
      if (terminal.value().kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
        break;
      }
    }
    test.runtime->unregister_stream(incoming.value().stream);
    assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), incoming.value().stream) == 0);
    assert(trevrpc_rpc_call_release(test.runtime->native_handle(), incoming.value().call) == 0);
    close_endpoint(test, endpoint);
    assert(test.runtime->shutdown());
  }

  {
    auto test = make_runtime(&config);
    const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
    assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake, "fake.Service", "Queued",
                                              TREVRPC_RPC_KIND_UNARY, body.data(),
                                              body.size()) == 0);
    RpcEventRuntimeTestPeer::wait_for_incoming_backlog(*test.runtime, 1);
    assert(trevrpc_cpp_rpc_fake_push_second_incoming(test.fake, "fake.Service", "Overload",
                                                     TREVRPC_RPC_KIND_CLIENT_STREAMING, body.data(),
                                                     body.size()) == 0);
    RpcEventRuntimeTestPeer::wait_for_rejected_streams(*test.runtime, 1);
    RpcEventRuntimeTestPeer::wait_for_rejected_streams(*test.runtime, 0);
    close_endpoint(test, endpoint);
    auto shutdown = test.runtime->shutdown();
    assert(shutdown);
    assert(test.runtime->native_handle() == nullptr);
  }
}

void test_shutdown_with_unclaimed_incoming() {
  auto test = make_runtime();
  (void)start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  constexpr std::string_view body = "unclaimed incoming body";
  assert(trevrpc_cpp_rpc_fake_push_incoming(
             test.fake, "fake.Service", "Unclaimed", TREVRPC_RPC_KIND_UNARY,
             reinterpret_cast<const std::uint8_t*>(body.data()), body.size()) == 0);
  RpcEventRuntimeTestPeer::wait_for_incoming_backlog(*test.runtime, 1);
  auto shutdown = test.runtime->shutdown();
  if (!shutdown) {
    assert(shutdown.error().code() == -EIO);
  }
  assert(test.runtime->native_handle() == nullptr);
}

void test_runtime_cleanup_final_owner_drops_on_driver() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto owner = open_client_stream(test, channel);
  std::weak_ptr<RpcClientStream> observer = owner;
  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, 0, false);
  RpcClientStreamTestPeer::schedule_cleanup(std::move(owner));
  for (unsigned int attempt = 0; attempt != 2000 && !observer.expired(); ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  assert(observer.expired());
  assert(channel->request_close());
}

void test_cleanup_admission_races_finalization() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto stream = open_client_stream(test, channel);
  RpcEventRuntimeTestPeer::pause_finalization(*test.runtime);
  test.runtime->request_abandon();
  RpcEventRuntimeTestPeer::wait_for_finalization_pause(*test.runtime);
  trevrpc_cpp_rpc_fake_set_stream_release_result(test.fake, -EBUSY, false);
  auto closing = std::async(std::launch::async, [stream] { stream->close(); });
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  RpcEventRuntimeTestPeer::release_finalization(*test.runtime);
  closing.get();
  assert(channel->request_close());
}

void test_request_close_from_callback_does_not_wait() {
  auto test = make_runtime();
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  std::promise<int> request_result;
  assert(test.runtime->subscribe_operation(operation.value(), [&](Result<RpcEvent> result) {
    assert(result);
    auto requested = test.runtime->request_close();
    request_result.set_value(requested ? 0 : requested.error().code());
  }));
  RpcEvent event;
  event.kind = TREVRPC_RPC_EVENT_CALL_READY;
  event.operation_id = operation.value();
  RpcEventRuntimeTestPeer::dispatch(*test.runtime, std::move(event));
  assert(request_result.get_future().get() == 0);
  assert(test.runtime->shutdown());
}

void test_client_open_from_callback_defers_cleanup() {
  auto test = make_runtime();
  auto channel = make_client_channel(test);
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  std::promise<int> callback_result;
  assert(test.runtime->subscribe_operation(operation.value(), [&](Result<RpcEvent> result) {
    assert(result);
    CallOptions options;
    const std::array<std::byte, 1> initial{std::byte{0x01}};
    auto opened = RpcClientStream::open(channel, "fake.Service", "CallbackOpen",
                                        TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, initial, options);
    callback_result.set_value(opened ? 0 : opened.error().code());
  }));
  RpcEvent event;
  event.kind = TREVRPC_RPC_EVENT_CALL_READY;
  event.operation_id = operation.value();
  RpcEventRuntimeTestPeer::dispatch(*test.runtime, std::move(event));
  assert(callback_result.get_future().get() == -EDEADLK);

  trevrpc_cpp_rpc_fake_wait_stream_open_calls(test.fake, 1);
  assert(trevrpc_cpp_rpc_fake_push_stream_ready(test.fake) == 0);
  trevrpc_cpp_rpc_fake_wait_stream_send_calls(test.fake, 1);
  assert(trevrpc_cpp_rpc_fake_push_last_send_complete(test.fake) == 0);
  auto closed = channel->close();
  // The callback-started call is deliberately rejected with -EDEADLK. Its
  // deferred cleanup must complete before channel close returns, without
  // orphaning the operation, native call, or runtime handle.
  assert(closed);
  assert(test.runtime->native_handle() == nullptr);
}

void test_rejected_incoming_dispatch_cleanup_releases_native_handles() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake, "fake.Service", "Rejected",
                                            TREVRPC_RPC_KIND_UNARY, nullptr, 0) == 0);
  auto incoming = test.runtime->wait_incoming();
  assert(incoming);
  assert(test.runtime->reject_incoming(incoming.value()));

  trevrpc_rpc_diagnostics_v1 diagnostics{};
  assert(trevrpc_rpc_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
  assert(trevrpc_rpc_runtime_get_diagnostics_v1(test.runtime->native_handle(), &diagnostics) == 0);
  assert(diagnostics.live_calls == 0);
  assert(diagnostics.live_streams == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
}

void test_rejected_incoming_cleanup_failure_abandons_runtime() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake, "fake.Service", "RejectedFailure",
                                            TREVRPC_RPC_KIND_UNARY, nullptr, 0) == 0);
  auto incoming = test.runtime->wait_incoming();
  assert(incoming);

  RpcEventRuntimeTestPeer::fail_next_operation_reservation(*test.runtime, -ENOBUFS);
  auto rejected = test.runtime->reject_incoming(incoming.value());
  assert(!rejected);
  assert(rejected.error().code() == -ENOBUFS);
  for (unsigned int attempt = 0;
       attempt != 2000 && test.runtime->native_handle() != nullptr;
       ++attempt) {
    std::this_thread::yield();
  }
  assert(test.runtime->native_handle() == nullptr);
  (void)endpoint;
}

void test_server_call_allocation_failure_rejects_native_handles() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  assert(test.runtime->register_endpoint(endpoint));
  auto core = RpcServerCore::create(test.runtime, endpoint, 1, 4);
  assert(core);
  auto server = std::move(core).value();
  auto lifetime = std::make_shared<int>(0);
  std::atomic_bool handler_called{false};
  assert(server->register_route(
      "fake.Service", "AllocationFailure", TREVRPC_RPC_KIND_UNARY,
      [&](std::shared_ptr<ServerCallState>) { handler_called.store(true, std::memory_order_release); },
      lifetime));
  assert(server->start());

  const std::size_t releases_before = trevrpc_cpp_rpc_fake_release_handle_count(test.fake);
  ServerCallStateTestPeer::fail_next_rpc_allocation();
  assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake, "fake.Service", "AllocationFailure",
                                            TREVRPC_RPC_KIND_UNARY, nullptr, 0) == 0);
  for (unsigned int attempt = 0;
       attempt != 2000 &&
       trevrpc_cpp_rpc_fake_release_handle_count(test.fake) < releases_before + 1;
       ++attempt) {
    std::this_thread::yield();
  }
  assert(trevrpc_cpp_rpc_fake_release_handle_count(test.fake) >= releases_before + 1);
  assert(!handler_called.load(std::memory_order_acquire));

  trevrpc_rpc_diagnostics_v1 diagnostics{};
  assert(trevrpc_rpc_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
  assert(trevrpc_rpc_runtime_get_diagnostics_v1(test.runtime->native_handle(), &diagnostics) == 0);
  assert(diagnostics.live_calls == 0);
  assert(diagnostics.live_streams == 0);

  trevrpc::ShutdownOptions options;
  auto shutdown = server->shutdown(options);
  assert(shutdown);
  assert(shutdown.value().released);
}

void test_server_shutdown_after_runtime_stop_preserves_retryability() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  assert(test.runtime->register_endpoint(endpoint));
  auto core = RpcServerCore::create(test.runtime, endpoint, 1, 4);
  assert(core);
  auto server = std::move(core).value();

  assert(trevrpc_cpp_rpc_fake_force_transport_stop(test.fake, -EIO) == 0);
  RpcEventRuntimeTestPeer::wait_for_stopped(*test.runtime);

  trevrpc::ShutdownOptions options;
  auto first = server->shutdown(options);
  assert(!first);
  assert(first.error().code() == -EIO);
  assert(test.runtime->native_handle() == nullptr);

  auto second = server->shutdown(options);
  assert(second);
  assert(second.value().released);
}

void test_deferred_endpoint_close_survives_deadlock_error() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);
  test.runtime->unregister_endpoint(endpoint);

  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, -EDEADLK, false);
  auto deferred = RpcEventRuntimeTestPeer::settle_unregistered_endpoint(*test.runtime, endpoint);
  assert(!deferred);
  assert(deferred.error().code() == -EDEADLK);

  trevrpc_cpp_rpc_fake_set_connection_close_result(test.fake, 0, false);
  assert(RpcEventRuntimeTestPeer::settle_unregistered_endpoint(*test.runtime, endpoint));
  assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
  assert(test.runtime->shutdown());
}

void test_cleanup_shutdown_reentrant_stream_callback() {
  auto target = make_runtime();
  auto target_channel = make_client_channel(target);
  auto target_stream = open_client_stream(target, target_channel);

  auto cleanup = make_runtime();
  auto cleanup_channel = make_client_channel(cleanup);
  auto cleanup_owner = open_client_stream(cleanup, cleanup_channel);
  auto cleanup_stream = RpcClientStreamTestPeer::wrap(std::move(cleanup_owner));
  std::thread::id target_driver;
  for (unsigned int attempt = 0; attempt != 2000 && target_driver == std::thread::id{}; ++attempt) {
    target_driver = RpcEventRuntimeTestPeer::driver_thread_id(*target.runtime);
    if (target_driver == std::thread::id{}) {
      std::this_thread::yield();
    }
  }
  assert(target_driver != std::thread::id{});
  trevrpc_cpp_rpc_fake_set_stream_release_result(cleanup.fake, -EBUSY, true);
  cleanup_stream.close();
  for (unsigned int attempt = 0;
       attempt != 2000 && trevrpc_cpp_rpc_fake_release_handle_count(cleanup.fake) == 0; ++attempt) {
    std::this_thread::yield();
  }
  assert(trevrpc_cpp_rpc_fake_release_handle_count(cleanup.fake) != 0);

  RpcEventRuntimeTestPeer::pause_driver(*target.runtime);
  RpcClientStreamTestPeer::drain_pending(target_stream);
  std::promise<void> callback_entered;
  auto callback_entered_future = callback_entered.get_future();
  std::promise<void> callback_done;
  auto callback_done_future = callback_done.get_future();
  std::promise<void> allow_callback;
  auto callback_release = allow_callback.get_future().share();
  auto callback = std::make_shared<RpcEventRuntime::EventCallback>();
  const std::weak_ptr<RpcEventRuntime::EventCallback> callback_weak = callback;
  *callback = [&, callback_weak](Result<RpcEvent> result) {
    auto callback_owner = callback_weak.lock();
    if (!result || result.value().kind != TREVRPC_RPC_EVENT_STREAM_CLOSED) {
      if (result && callback_owner) {
        (void)RpcClientStreamTestPeer::subscribe(target_stream, *callback_owner);
      }
      return;
    }
    RpcClientStreamTestPeer::note(target_stream, result.value());
    callback_entered.set_value();
    callback_release.wait();
    (void)target_stream->close_result();
    callback_done.set_value();
  };
  assert(RpcClientStreamTestPeer::subscribe(target_stream, *callback));

  auto closing = std::async(std::launch::async, [&] { target_stream->close(); });
  RpcEventRuntimeTestPeer::resume_driver(*target.runtime);
  assert(callback_entered_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  std::atomic<unsigned int> shutdown_ready{0};
  std::promise<void> allow_shutdown;
  auto shutdown_release = allow_shutdown.get_future().share();
  auto stopping = std::async(std::launch::async, [&] {
    shutdown_ready.fetch_add(1, std::memory_order_release);
    shutdown_release.wait();
    RpcClientStreamTestPeer::shutdown_cleanup_service();
  });
  auto stopping_again = std::async(std::launch::async, [&] {
    shutdown_ready.fetch_add(1, std::memory_order_release);
    shutdown_release.wait();
    RpcClientStreamTestPeer::shutdown_cleanup_service();
  });
  for (unsigned int attempt = 0;
       attempt != 2000 && shutdown_ready.load(std::memory_order_acquire) != 2; ++attempt) {
    std::this_thread::yield();
  }
  assert(shutdown_ready.load(std::memory_order_acquire) == 2);
  allow_shutdown.set_value();
  allow_callback.set_value();
  assert(callback_done_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  assert(stopping.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  assert(stopping_again.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  assert(closing.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  closing.get();
  stopping.get();
  stopping_again.get();
  assert(RpcEventRuntimeTestPeer::driver_thread_id(*target.runtime) == target_driver);
}

} // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string_view(argv[1]) == "server-core-active") {
    test_stop_incoming_unblocks_waiter_without_stopping_runtime();
    test_server_core_abandonment_drops_dispatcher_thread_owner();
    test_server_core_drains_and_cancels_child_scopes_before_runtime_shutdown();
    test_server_core_retains_returned_handlers_until_terminal_settlement();
    test_server_shutdown_after_runtime_stop_preserves_retryability();
    test_rejected_incoming_dispatch_cleanup_releases_native_handles();
    test_rejected_incoming_cleanup_failure_abandons_runtime();
    test_server_call_allocation_failure_rejects_native_handles();
    return 0;
  }
  test_timer_runs_on_event_driver_at_deadline();
  test_failed_adopt_retains_runtime_ownership();
  test_endpoint_registration_failure_releases_endpoint_capacity();
  test_unregistered_endpoint_settlement_precedes_cleanup_reservation();
  test_endpoint_registration_commit_failure_preserves_pending_events();
  test_endpoint_preregistration();
  test_client_registration_failure_closes_unregistered_call();
  test_client_partial_stream_without_call_does_not_wait_for_close_operation();
  test_client_open_owner_allocation_failure_precedes_admission();
  test_cleanup_task_allocation_failure_abandons_owner();
  test_cleanup_requeue_allocation_failure_abandons_owner();
  test_client_stream_release_retries_independently();
  test_client_call_release_persistent_error_retries();
  test_public_client_stream_close_retains_stream_release();
  test_public_client_stream_close_retains_call_release();
  test_persistent_cleanup_error_abandons_on_shutdown();
  test_client_receive_allocation_failure_releases_and_closes();
  test_client_close_after_runtime_stop_releases_without_terminal_wait();
  test_client_concurrent_close_waits_for_one_cleanup();
  test_client_status_fin_waits_for_paused_send();
  test_client_receive_returns_terminal_before_cleanup_retry();
  test_client_drains_queued_receives_after_terminal_event();
  test_client_close_retry_exhaustion_releases_reservations();
  test_stream_preregistration_and_terminal_order();
  test_operation_and_stream_dual_routing();
  test_incoming_copy();
  test_stop_incoming_unblocks_waiter_without_stopping_runtime();
  test_server_core_abandonment_drops_dispatcher_thread_owner();
  test_server_core_drains_and_cancels_child_scopes_before_runtime_shutdown();
  test_callbacks_and_unbounded_operations();
  test_callback_blocking_waits_fail_without_deadlock();
  test_stream_and_incoming_subscriptions();
  test_callback_cancellation();
  test_endpoint_capacity_recovery();
  test_split_close_and_spontaneous_stop();
  test_strict_shutdown_retries_joined_close_failure();
  test_shutdown_completion_single_consumer();
  test_spontaneous_stop_drops_last_owner();
  test_driver_failure_rejects_new_consumers();
  test_buffered_completions_precede_driver_failure();
  test_waiter_cancellation();
  test_destructor_after_close_rejection();
  test_control_wake_failures_make_progress();
  test_shutdown_callback_fails_once_without_stopped_event();
  test_driver_failure_close_retries();
  test_concurrent_nonclean_shutdown();
  test_shutdown_busy_then_retry();
  test_strict_shutdown_generation_results();
  test_native_runtime_lease_pins_release();
  test_last_owner_dropped_from_callback();
  test_pending_stream_sequence_at_tiny_cap();
  test_malformed_incoming_pointers_are_rejected();
  test_incoming_body_pressure_and_overload_followups();
  test_shutdown_with_unclaimed_incoming();
  test_runtime_cleanup_final_owner_drops_on_driver();
  test_cleanup_admission_races_finalization();
  test_request_close_from_callback_does_not_wait();
  test_client_open_from_callback_defers_cleanup();
  test_cleanup_shutdown_reentrant_stream_callback();
  test_deferred_endpoint_close_survives_deadlock_error();
  test_server_shutdown_after_runtime_stop_preserves_retryability();
  test_rejected_incoming_dispatch_cleanup_releases_native_handles();
  test_rejected_incoming_cleanup_failure_abandons_runtime();
  test_server_call_allocation_failure_rejects_native_handles();
  return 0;
}
