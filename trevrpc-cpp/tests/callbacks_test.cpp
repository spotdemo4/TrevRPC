#include <trevrpc/callbacks.hpp>
#include <trevrpc/trevrpc.hpp>

#include "detail/callbacks.hpp"
#include "detail/lifecycle.hpp"

#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

struct EmptyMessage {
  [[nodiscard]] std::size_t ByteSizeLong() const noexcept { return 0; }
  [[nodiscard]] bool SerializeToArray(void*, int size) const noexcept { return size == 0; }
  [[nodiscard]] bool ParseFromArray(const void*, int size) noexcept { return size == 0; }
};

class RecordingSink final : public trevrpc::CallbackExceptionSink {
public:
  void callback_exception(std::string_view callback, std::exception_ptr exception) override {
    assert(exception != nullptr);
    {
      std::lock_guard lock(mutex_);
      callbacks_.emplace_back(callback);
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool wait_for_count(std::size_t count) {
    std::unique_lock lock(mutex_);
    return condition_.wait_for(lock, 5s, [this, count] { return callbacks_.size() >= count; });
  }

  [[nodiscard]] bool contains(std::string_view callback) const {
    std::lock_guard lock(mutex_);
    for (const auto& value : callbacks_) {
      if (value == callback) {
        return true;
      }
    }
    return false;
  }

private:
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<std::string> callbacks_;
};

class ThrowingAuthorizer final : public trevrpc::Authorizer {
public:
  trevrpc::Status authorize(const trevrpc::CallContext&,
                            const trevrpc::AuthorizationRequest&) override {
    throw std::runtime_error("authorizer threw");
  }
};

class ThrowingMetrics final : public trevrpc::MetricsObserver {
public:
  void rpc_started(const trevrpc::RpcStartedEvent&) override {
    throw std::runtime_error("metrics started threw");
  }

  void rpc_finished(const trevrpc::RpcFinishedEvent&) override {
    throw std::runtime_error("metrics finished threw");
  }
};

class ThrowingLogger final : public trevrpc::Logger {
public:
  void log(const trevrpc::LogEvent&) override { throw std::runtime_error("logger threw"); }
};

class NoopAuthorizer final : public trevrpc::Authorizer {
public:
  trevrpc::Status authorize(const trevrpc::CallContext&,
                            const trevrpc::AuthorizationRequest&) override {
    return trevrpc::Status::ok();
  }
};

class NoopMetrics final : public trevrpc::MetricsObserver {
public:
  void rpc_started(const trevrpc::RpcStartedEvent&) override {}
  void rpc_finished(const trevrpc::RpcFinishedEvent&) override {}
};

class NoopLogger final : public trevrpc::Logger {
public:
  void log(const trevrpc::LogEvent&) override {}
};

class DropChannelOwner final : public trevrpc::ChannelLifecycleObserver {
public:
  void arm(std::shared_ptr<trevrpc::Channel> channel) {
    std::lock_guard lock(mutex_);
    owner_ = std::move(channel);
    armed_ = true;
  }

  void channel_event(const trevrpc::ChannelLifecycleEvent&) override {
    std::shared_ptr<trevrpc::Channel> owner;
    {
      std::lock_guard lock(mutex_);
      if (!armed_ || !owner_) {
        return;
      }
      owner = std::move(owner_);
      dropped_ = true;
    }
    owner.reset();
    condition_.notify_all();
  }

  [[nodiscard]] bool wait_for_drop() {
    std::unique_lock lock(mutex_);
    return condition_.wait_for(lock, 5s, [this] { return dropped_; });
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::shared_ptr<trevrpc::Channel> owner_;
  bool armed_ = false;
  bool dropped_ = false;
};

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

void wait_until_serving(trevrpc::Server& server) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  for (;;) {
    auto phase = server.phase();
    assert(phase);
    if (phase.value() == trevrpc::ServerPhase::Serving) {
      return;
    }
    assert(phase.value() != trevrpc::ServerPhase::Released);
    assert(std::chrono::steady_clock::now() < deadline);
    std::this_thread::sleep_for(1ms);
  }
}

void test_callback_factories() {
  auto authorizer = std::make_shared<NoopAuthorizer>();
  auto metrics = std::make_shared<NoopMetrics>();
  auto logger = std::make_shared<NoopLogger>();
  auto observer = std::make_shared<DropChannelOwner>();
  auto sink = std::make_shared<RecordingSink>();

  auto authorizer_state =
      trevrpc::detail::make_authorizer_state(authorizer, sink);
  auto metrics_state = trevrpc::detail::make_metrics_state(metrics, sink);
  auto logger_state = trevrpc::detail::make_logger_state(logger, sink);
  auto lifecycle_state = trevrpc::detail::make_channel_lifecycle_state(observer, sink);

  assert(authorizer_state->callback == authorizer);
  assert(metrics_state->callback == metrics);
  assert(logger_state->callback == logger);
  assert(lifecycle_state->callback == observer);
  assert(authorizer_state->sink == sink);
  assert(metrics_state->sink == sink);
  assert(logger_state->sink == sink);
  assert(lifecycle_state->sink == sink);
}

void test_direct_dispatch_exception_containment() {
  auto server = make_server();
  assert((server.register_unary<EmptyMessage, EmptyMessage>(
      "example.Service", "Method",
      [](const trevrpc::CallContext&, const EmptyMessage&) {
        return trevrpc::Result<EmptyMessage>(EmptyMessage{});
      })));

  auto sink = std::make_shared<RecordingSink>();
  assert(server.set_authorizer(std::make_shared<ThrowingAuthorizer>(), sink));
  assert(server.set_metrics(std::make_shared<ThrowingMetrics>(), sink));
  assert(server.set_logger(std::make_shared<ThrowingLogger>(), sink));

  trevrpc::Result<void> serve_result;
  std::thread server_thread([&] { serve_result = server.serve(); });
  wait_until_serving(server);

  trevrpc::ChannelConfig channel_config;
  channel_config.skip_certificate_validation = true;
  auto connected = trevrpc::Channel::connect("127.0.0.1", server.port().value(), channel_config, 5s);
  assert(connected);
  auto channel = std::move(connected).value();
  auto response = channel->call_unary("example.Service", "Method", std::span<const std::byte>{}, {});
  assert(response);
  assert(response.value().status.code() == trevrpc::StatusCode::Internal);
  assert(sink->wait_for_count(4));
  assert(sink->contains("authorizer"));
  assert(sink->contains("metrics.rpc_started"));
  assert(sink->contains("metrics.rpc_finished"));
  assert(sink->contains("logger"));

  channel->close();
  trevrpc::ShutdownOptions options;
  options.graceful_timeout = 2s;
  options.cancellation_timeout = 2s;
  const auto report = server.shutdown(options);
  assert(report);
  assert(report.value().released);
  server_thread.join();
  assert(serve_result);
}

void test_server_callback_configuration() {
  auto server = make_server();
  assert(server.set_authorizer(std::make_shared<NoopAuthorizer>()));
  assert(server.set_metrics(std::make_shared<NoopMetrics>()));
  assert(server.set_logger(std::make_shared<NoopLogger>()));
  assert(server.clear_authorizer());
  assert(server.clear_metrics());
  assert(server.clear_logger());

  assert(server.set_authorizer(std::make_shared<NoopAuthorizer>()));
  assert(server.set_metrics(std::make_shared<NoopMetrics>()));
  assert(server.set_logger(std::make_shared<NoopLogger>()));
  trevrpc::Result<void> serve_result;
  std::thread server_thread([&] { serve_result = server.serve(); });
  wait_until_serving(server);
  const auto frozen = server.clear_authorizer();
  assert(!frozen);
  assert(frozen.error().code() == -EALREADY);

  trevrpc::ShutdownOptions options;
  options.graceful_timeout = 2s;
  options.cancellation_timeout = 2s;
  const auto report = server.shutdown(options);
  assert(report);
  assert(report.value().released);
  server_thread.join();
  assert(serve_result);
  assert(trevrpc::detail::drain_lifecycle_reaper_until(std::chrono::steady_clock::now() + 5s));
}

void test_server_final_owner_drop() {
  trevrpc::Result<void> serve_result;
  {
    auto server = make_server();
    std::thread server_thread([&] { serve_result = server.serve(); });
    wait_until_serving(server);
    server_thread.join();
    assert(serve_result);
  }
  assert(trevrpc::detail::drain_lifecycle_reaper_until(
      std::chrono::steady_clock::now() + 5s));
}

void test_channel_final_owner_drop_from_callback() {
  auto server = make_server();
  trevrpc::Result<void> serve_result;
  std::thread server_thread([&] { serve_result = server.serve(); });
  wait_until_serving(server);
  const auto port = server.port();
  assert(port);

  auto observer = std::make_shared<DropChannelOwner>();
  trevrpc::ChannelConfig config;
  config.skip_certificate_validation = true;
  config.lifecycle_observer = observer;
  auto channel_result = trevrpc::Channel::connect("127.0.0.1", port.value(), config, 5s);
  assert(channel_result);
  auto channel = std::move(channel_result).value();
  const auto ready = channel->wait_ready(5s);
  assert(ready);
  std::weak_ptr<trevrpc::Channel> weak = channel;
  observer->arm(std::move(channel));

  trevrpc::ShutdownOptions options;
  options.graceful_timeout = 0ns;
  options.cancellation_timeout = 2s;
  const auto report = server.shutdown(options);
  assert(report);
  assert(report.value().released);
  server_thread.join();
  assert(serve_result);
  assert(observer->wait_for_drop());
  assert(weak.expired());
  assert(trevrpc::detail::drain_lifecycle_reaper_until(std::chrono::steady_clock::now() + 5s));
}

} // namespace

int main() {
  test_callback_factories();
  test_direct_dispatch_exception_containment();
  test_server_callback_configuration();
  test_server_final_owner_drop();
  test_channel_final_owner_drop_from_callback();
  assert(trevrpc::detail::drain_lifecycle_reaper_until(std::chrono::steady_clock::now() + 5s));
  return 0;
}
