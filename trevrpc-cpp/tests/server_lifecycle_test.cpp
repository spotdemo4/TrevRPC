#include <trevrpc/async.hpp>
#include <trevrpc/trevrpc.hpp>

#include "detail/lifecycle.hpp"

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>

namespace {

using namespace std::chrono_literals;

trevrpc::Server make_server() {
  trevrpc::ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.cert_file = TREVRPC_CPP_TEST_CERT;
  config.key_file = TREVRPC_CPP_TEST_KEY;
  auto server = trevrpc::Server::listen(config);
  assert(server);
  return std::move(server).value();
}

class FakeAsyncScope final : public trevrpc::detail::AsyncServerScopeControl {
public:
  void cancel() noexcept override { ++cancel_count; }

  trevrpc::Result<void> drain_until(std::chrono::steady_clock::time_point) noexcept override {
    ++drain_count;
    if (!drain_enabled.load()) {
      return trevrpc::Error::runtime(-ETIMEDOUT, "fake async scope remains active");
    }
    return {};
  }

  std::atomic<bool> drain_enabled{false};
  std::atomic<int> cancel_count{0};
  std::atomic<int> drain_count{0};
};

int unused_route(void*, trevrpc_call*) { return 0; }

void require_same_options(const trevrpc_server_options_v1& left,
                          const trevrpc_server_options_v1& right) {
  assert(left.max_concurrent_connections == right.max_concurrent_connections);
  assert(left.max_concurrent_streams_per_connection == right.max_concurrent_streams_per_connection);
  assert(left.max_concurrent_requests == right.max_concurrent_requests);
  assert(left.worker_count == right.worker_count);
  assert(left.worker_queue_capacity == right.worker_queue_capacity);
  assert(left.graceful_shutdown_timeout_nanos == right.graceful_shutdown_timeout_nanos);
  assert(left.initial_request_timeout_nanos == right.initial_request_timeout_nanos);
  assert(left.max_stream_messages == right.max_stream_messages);
  assert(left.max_stream_body_size == right.max_stream_body_size);
  assert(left.stream_idle_timeout_nanos == right.stream_idle_timeout_nanos);
}

void test_shutdown_retains_and_resumes_async_scope() {
  auto server = make_server();
  auto route = std::make_shared<int>(1);
  std::weak_ptr<int> weak_route = route;
  auto scope = std::make_shared<FakeAsyncScope>();
  auto registered = trevrpc::detail::AsyncRegistrationAccess::register_route(
      server, "test.AsyncLifecycle", "Blocked", TREVRPC_RPC_KIND_UNARY, unused_route, route,
      route.get(), scope);
  assert(registered);
  route.reset();

  trevrpc::ShutdownOptions immediate;
  immediate.graceful_timeout = 0ns;
  immediate.cancellation_timeout = 0ns;
  auto timed_out = server.shutdown(immediate);
  assert(timed_out);
  assert(timed_out.value().outcome == trevrpc::ShutdownOutcome::TimedOut);
  assert(!timed_out.value().released);
  assert(server.native_handle() != nullptr);
  assert(!weak_route.expired());
  assert(scope->cancel_count.load() == 1);
  assert(scope->drain_count.load() == 2);

  scope->drain_enabled = true;
  trevrpc::ShutdownOptions resumed;
  resumed.graceful_timeout = 2s;
  resumed.cancellation_timeout = 2s;
  auto released = server.shutdown(resumed);
  assert(released);
  assert(released.value().outcome == trevrpc::ShutdownOutcome::Cancelled);
  assert(released.value().final_phase == trevrpc::ServerPhase::Released);
  assert(released.value().released);
  assert(server.native_handle() == nullptr);
  assert(weak_route.expired());
  assert(scope->cancel_count.load() == 1);
  assert(scope->drain_count.load() == 3);

  auto repeated = server.shutdown(resumed);
  assert(repeated);
  assert(repeated.value().released);
  assert(scope->drain_count.load() == 3);
}

} // namespace

int main() {
  trevrpc_server_options_v1 defaults{};
  assert(trevrpc_server_options_v1_init(&defaults, sizeof(defaults)) == 0);

  auto server = make_server();
  assert(server.set_options({}));
  trevrpc_server_options_v1 unchanged{};
  assert(trevrpc_server_options_v1_init(&unchanged, sizeof(unchanged)) == 0);
  assert(trevrpc_server_get_options_v1(server.native_handle(), &unchanged) == 0);
  require_same_options(defaults, unchanged);

  trevrpc::ServerOptions partial;
  partial.worker_count = 3;
  partial.max_stream_messages = 0;
  partial.stream_idle_timeout = 25ms;
  assert(server.set_options(partial));
  trevrpc_server_options_v1 updated{};
  assert(trevrpc_server_options_v1_init(&updated, sizeof(updated)) == 0);
  assert(trevrpc_server_get_options_v1(server.native_handle(), &updated) == 0);
  assert(updated.worker_count == 3);
  assert(updated.max_stream_messages == 0);
  assert(updated.stream_idle_timeout_nanos == static_cast<std::uint64_t>(25ms / 1ns));
  assert(updated.max_concurrent_connections == defaults.max_concurrent_connections);
  assert(updated.worker_queue_capacity == defaults.worker_queue_capacity);

  trevrpc::ShutdownOptions shutdown_options;
  shutdown_options.graceful_timeout = 2s;
  shutdown_options.cancellation_timeout = 2s;
  auto report = server.shutdown(shutdown_options);
  assert(report);
  assert(report.value().outcome == trevrpc::ShutdownOutcome::Graceful);
  assert(report.value().final_phase == trevrpc::ServerPhase::Released);
  assert(report.value().released);
  assert(server.native_handle() == nullptr);

  auto repeated = server.shutdown(shutdown_options);
  assert(repeated);
  assert(repeated.value().released);

  test_shutdown_retains_and_resumes_async_scope();
  return 0;
}
