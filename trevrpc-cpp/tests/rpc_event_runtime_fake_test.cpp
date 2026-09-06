#include <trevrpc/trevrpc.hpp>
#include <trevrpc_rpc.h>

#include "detail/rpc_client.hpp"
#include "detail/rpc_event_runtime.hpp"
#include "rpc_event_runtime_fake_fixture.h"

#include <array>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <utility>

namespace trevrpc::detail {

class RpcSyncClientTestPeer {
public:
  static std::unique_ptr<RpcSyncClient> create(
      std::shared_ptr<RpcEventRuntime> runtime, trevrpc_rpc_endpoint_v1 endpoint) {
    return std::unique_ptr<RpcSyncClient>(new RpcSyncClient(std::move(runtime), endpoint));
  }
};

class RpcEventRuntimeTestPeer {
public:
  static void wait_for_operation_waiters(RpcEventRuntime& runtime, std::size_t count) {
    std::unique_lock lock(runtime.mutex_);
    runtime.condition_.wait(lock, [&] { return runtime.operation_waiters_ >= count; });
  }

  static void wait_for_stream_waiters(RpcEventRuntime& runtime, std::size_t count) {
    std::unique_lock lock(runtime.mutex_);
    runtime.condition_.wait(lock, [&] { return runtime.stream_waiters_ >= count; });
  }

  static void wait_for_endpoint_waiters(RpcEventRuntime& runtime, std::size_t count) {
    std::unique_lock lock(runtime.mutex_);
    runtime.condition_.wait(lock, [&] { return runtime.endpoint_waiters_ >= count; });
  }

  static void wait_for_shutdown_waiters(RpcEventRuntime& runtime, std::size_t count) {
    std::unique_lock lock(runtime.mutex_);
    runtime.condition_.wait(lock, [&] { return runtime.shutdown_waiters_ >= count; });
  }
};

} // namespace trevrpc::detail

namespace {

using trevrpc::detail::RpcEventRuntime;
using trevrpc::detail::RpcEventRuntimeTestPeer;
using trevrpc::detail::RpcSyncClientTestPeer;

struct TestRuntime {
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  std::shared_ptr<RpcEventRuntime> runtime;
};

TestRuntime make_runtime() {
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  trevrpc_rpc_runtime* raw_runtime = nullptr;
  assert(trevrpc_cpp_rpc_fake_fixture_create(&fake, &raw_runtime) == 0);
  auto adopted = RpcEventRuntime::adopt(raw_runtime);
  assert(adopted);
  return TestRuntime{fake, std::move(adopted).value()};
}

trevrpc_rpc_endpoint_v1 start_endpoint(TestRuntime& test, std::uint32_t mode) {
  auto operation = test.runtime->reserve_operation();
  assert(operation);
  trevrpc_rpc_endpoint_v1 endpoint{};
  assert(trevrpc_cpp_rpc_fake_start_endpoint(test.fake,
             test.runtime->native_handle(),
             mode,
             operation.value(),
             &endpoint) == 0);
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
  assert(trevrpc_rpc_endpoint_close(
             test.runtime->native_handle(), endpoint, operation.value()) == 0);
  auto closed = test.runtime->wait_operation(operation.value());
  assert(closed);
  assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
  assert(trevrpc_rpc_endpoint_release(test.runtime->native_handle(), endpoint) == 0);
}

std::pair<trevrpc_rpc_call_v1, trevrpc_rpc_stream_v1>
open_call(TestRuntime& test,
          trevrpc_rpc_endpoint_v1 endpoint,
          std::uint64_t operation_id,
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
  assert(trevrpc_rpc_call_open_v1(test.runtime->native_handle(),
             endpoint,
             &config,
             operation_id,
             &call,
             &stream) == 0);
  return {call, stream};
}

void test_endpoint_preregistration() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);

  assert(trevrpc_cpp_rpc_fake_push_connection_closed(test.fake, -ECONNRESET) == 0);
  const trevrpc_rpc_endpoint_v1 wrong_generation{
      endpoint.owner, endpoint.slot, endpoint.generation + 1};
  assert(test.runtime->register_endpoint(wrong_generation));
  assert(test.runtime->register_endpoint(endpoint));

  auto wrong_waiter = std::async(
      std::launch::async, [&] { return test.runtime->wait_endpoint(wrong_generation); });
  RpcEventRuntimeTestPeer::wait_for_endpoint_waiters(*test.runtime, 1);
  auto closed = test.runtime->wait_endpoint(endpoint);
  assert(closed);
  assert(closed.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
  assert(closed.value().operation_id == 0);
  assert(closed.value().status == -ECONNRESET);
  test.runtime->unregister_endpoint(wrong_generation);
  auto wrong_result = wrong_waiter.get();
  assert(!wrong_result);
  assert(wrong_result.error().code() == -ECANCELED);

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

  const trevrpc_rpc_stream_v1 wrong_generation{
      stream.owner, stream.slot, stream.generation + 1};
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
  assert(trevrpc_rpc_call_close(test.runtime->native_handle(),
             call,
             close_operation.value(),
             TREVRPC_RPC_CLOSE_FLAG_ABORT,
             17) == 0);
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

  assert(trevrpc_rpc_stream_release(test.runtime->native_handle(), stream) == 0);
  assert(trevrpc_rpc_call_release(test.runtime->native_handle(), call) == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
}

void test_incoming_copy() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_SERVER);
  constexpr std::string_view body = "owned incoming body";
  assert(trevrpc_cpp_rpc_fake_push_incoming(test.fake,
             "fake.Service",
             "Incoming",
             TREVRPC_RPC_KIND_UNARY,
             reinterpret_cast<const std::uint8_t*>(body.data()),
             body.size()) == 0);
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
  assert(trevrpc_rpc_call_accept(test.runtime->native_handle(),
             incoming.value().call,
             accept_operation.value()) == 0);
  auto accepted = test.runtime->wait_operation(accept_operation.value());
  assert(accepted);
  assert(accepted.value().kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);

  auto close_operation = test.runtime->reserve_operation();
  assert(close_operation);
  assert(trevrpc_rpc_call_close(test.runtime->native_handle(),
             incoming.value().call,
             close_operation.value(),
             TREVRPC_RPC_CLOSE_FLAG_ABORT,
             0) == 0);
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
  assert(trevrpc_rpc_stream_release(
             test.runtime->native_handle(), incoming.value().stream) == 0);
  assert(trevrpc_rpc_call_release(test.runtime->native_handle(), incoming.value().call) == 0);
  close_endpoint(test, endpoint);
  assert(test.runtime->shutdown());
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
  assert(test.runtime->shutdown());
}

void test_sync_client_peer_close_before_response() {
  auto test = make_runtime();
  const auto endpoint = start_endpoint(test, TREVRPC_RPC_ENDPOINT_CLIENT);
  auto client = RpcSyncClientTestPeer::create(test.runtime, endpoint);

  auto call = std::async(std::launch::async, [&] {
    return client->call_unary("fake.Service", "PeerClose", {});
  });
  trevrpc_cpp_rpc_fake_wait_stream_open_calls(test.fake, 1);
  assert(trevrpc_cpp_rpc_fake_push_stream_ready(test.fake) == 0);
  trevrpc_cpp_rpc_fake_wait_stream_send_calls(test.fake, 1);
  assert(trevrpc_cpp_rpc_fake_push_last_send_complete(test.fake) == 0);
  assert(trevrpc_cpp_rpc_fake_push_stream_closed(test.fake, -ECONNRESET) == 0);

  auto result = call.get();
  assert(!result);
  assert(result.error().code() == -ECONNRESET);
  assert(client->close());
}

void test_destructor_after_close_rejection() {
  auto test = make_runtime();
  trevrpc_cpp_rpc_fake_set_close_result(test.fake, -EAGAIN);
  test.runtime.reset();
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

} // namespace

int main() {
  test_endpoint_preregistration();
  test_stream_preregistration_and_terminal_order();
  test_operation_and_stream_dual_routing();
  test_incoming_copy();
  test_waiter_cancellation();
  test_sync_client_peer_close_before_response();
  test_destructor_after_close_rejection();
  test_concurrent_nonclean_shutdown();
  return 0;
}
