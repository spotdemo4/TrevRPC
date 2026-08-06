#include <trevrpc/callbacks.hpp>

#include "detail/callbacks.hpp"
#include "detail/lifecycle.hpp"

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace {

using namespace std::chrono_literals;

class RecordingSink final : public trevrpc::CallbackExceptionSink {
public:
  void callback_exception(std::string_view callback, std::exception_ptr exception) override {
    assert(exception != nullptr);
    last_callback = callback;
    ++count;
  }

  std::string last_callback;
  int count = 0;
};

class ThrowingSink final : public trevrpc::CallbackExceptionSink {
public:
  void callback_exception(std::string_view, std::exception_ptr) override {
    throw std::runtime_error("exception sink threw");
  }
};

class DenyingAuthorizer final : public trevrpc::Authorizer {
public:
  trevrpc::Status authorize(const trevrpc::CallContext&,
                            const trevrpc::AuthorizationRequest& request) override {
    assert(request.service == "example.Service");
    assert(request.method == "Method");
    assert(request.body_size == 17);
    assert(request.kind == TREVRPC_RPC_KIND_UNARY);
    const auto token = request.metadata.get("token");
    assert(token.has_value());
    const std::string_view value(reinterpret_cast<const char*>(token->data()), token->size());
    assert(value == "secret");
    return trevrpc::Status(trevrpc::StatusCode::PermissionDenied, "denied");
  }
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

class ThrowingTransportObserver final : public trevrpc::TransportObserver {
public:
  void transport_event(const trevrpc::TransportEvent&) override {
    throw std::runtime_error("transport observer threw");
  }
};

class ThrowingWebTransportAdmission final : public trevrpc::WebTransportAdmission {
public:
  bool admit(const trevrpc::WebTransportAdmissionRequest&) override {
    throw std::runtime_error("WebTransport admission threw");
  }
};

class ThrowingHttp3Admission final : public trevrpc::Http3Admission {
public:
  bool admit(const trevrpc::Http3AdmissionRequest&) override {
    throw std::runtime_error("HTTP/3 admission threw");
  }
};

class ThrowingChannelObserver final : public trevrpc::ChannelLifecycleObserver {
public:
  void channel_event(const trevrpc::ChannelLifecycleEvent&) override {
    assert(trevrpc::detail::running_in_channel_callback());
    throw std::runtime_error("channel observer threw");
  }
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

class NoopTransportObserver final : public trevrpc::TransportObserver {
public:
  void transport_event(const trevrpc::TransportEvent&) override {}
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

  bool wait_for_drop() {
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
    std::uint32_t phase = 0;
    const int error = trevrpc_server_get_phase(server.native_handle(), &phase);
    assert(error == 0);
    if (phase == TREVRPC_SERVER_PHASE_SERVING) {
      return;
    }
    assert(phase < TREVRPC_SERVER_PHASE_SERVING);
    assert(std::chrono::steady_clock::now() < deadline);
    std::this_thread::sleep_for(1ms);
  }
}

void test_authorizer_callbacks() {
  std::string service = "example.Service";
  std::string method = "Method";
  std::string key = "token";
  std::string value = "secret";
  trevrpc_metadata_entry entry{key.data(), key.size(),
                               reinterpret_cast<std::uint8_t*>(value.data()), value.size()};
  trevrpc_request request{};
  request.service = service.data();
  request.service_len = service.size();
  request.method = method.data();
  request.method_len = method.size();
  request.body_len = 17;
  request.metadata = {&entry, 1};
  request.kind = TREVRPC_RPC_KIND_UNARY;

  auto denying = trevrpc::detail::make_authorizer_state(std::make_shared<DenyingAuthorizer>(), {});
  trevrpc_status status{};
  const int denied =
      trevrpc::detail::authorizer_trampoline(denying.get(), nullptr, &request, &status);
  assert(denied == 0);
  assert(status.code == TREVRPC_STATUS_PERMISSION_DENIED);
  assert(std::string_view(status.message, status.message_len) == "denied");

  auto sink = std::make_shared<RecordingSink>();
  auto throwing =
      trevrpc::detail::make_authorizer_state(std::make_shared<ThrowingAuthorizer>(), sink);
  const int failed =
      trevrpc::detail::authorizer_trampoline(throwing.get(), nullptr, &request, &status);
  assert(failed == 0);
  assert(status.code == TREVRPC_STATUS_INTERNAL);
  assert(std::string_view(status.message, status.message_len) == "authorizer callback threw");
  assert(sink->count == 1);
  assert(sink->last_callback == "authorizer");
}

void test_observer_exception_containment() {
  auto sink = std::make_shared<RecordingSink>();
  auto metrics = trevrpc::detail::make_metrics_state(std::make_shared<ThrowingMetrics>(), sink);
  const trevrpc_rpc_started_event started{"Service", 7, "Method", 6, 11};
  metrics->native.rpc_started(metrics->native.user_data, &started);
  assert(sink->count == 1);
  assert(sink->last_callback == "metrics.rpc_started");
  const trevrpc_rpc_finished_event finished{
      "Service", 7, "Method", 6, 11, 13, TREVRPC_STATUS_UNAVAILABLE, 19};
  metrics->native.rpc_finished(metrics->native.user_data, &finished);
  assert(sink->count == 2);
  assert(sink->last_callback == "metrics.rpc_finished");

  auto logger = trevrpc::detail::make_logger_state(std::make_shared<ThrowingLogger>(), sink);
  const trevrpc_log_event log{
      TREVRPC_LOG_LEVEL_ERROR, "event", 5, "message", 7, "Service", 7, "Method", 6, -5};
  logger->native.log(logger->native.user_data, &log);
  assert(sink->count == 3);
  assert(sink->last_callback == "logger");

  auto transport =
      trevrpc::detail::make_transport_state(std::make_shared<ThrowingTransportObserver>(), sink);
  const trevrpc_transport_event event{TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, 1, -9, "failed", 6};
  transport->native.transport_event(transport->native.user_data, &event);
  assert(sink->count == 4);
  assert(sink->last_callback == "transport_observer");
}

void test_admission_and_recursive_exception_containment() {
  auto sink = std::make_shared<RecordingSink>();
  auto webtransport = trevrpc::detail::make_webtransport_admission_state(
      std::make_shared<ThrowingWebTransportAdmission>(), sink);
  const trevrpc_webtransport_admission_request webtransport_request{"/rpc",   4, "host", 4,
                                                                    "origin", 6, 1};
  const int webtransport_result =
      trevrpc::detail::webtransport_admission_trampoline(webtransport.get(), &webtransport_request);
  assert(webtransport_result != 0);
  assert(sink->last_callback == "webtransport_admission");

  auto http3 =
      trevrpc::detail::make_http3_admission_state(std::make_shared<ThrowingHttp3Admission>(), sink);
  const trevrpc_http3_admission_request http3_request{"/rpc", 4, "host", 4, 1};
  const int http3_result = trevrpc::detail::http3_admission_trampoline(http3.get(), &http3_request);
  assert(http3_result != 0);
  assert(sink->last_callback == "http3_admission");

  auto channel = trevrpc::detail::make_channel_lifecycle_state(
      std::make_shared<ThrowingChannelObserver>(), std::make_shared<ThrowingSink>());
  const trevrpc_channel_event channel_event{TREVRPC_CHANNEL_EVENT_CONNECT_FAILED,
                                            TREVRPC_CHANNEL_RECONNECTING, 3, -7};
  trevrpc::detail::channel_lifecycle_trampoline(channel.get(), &channel_event);
  assert(!trevrpc::detail::running_in_channel_callback());
}

void test_server_callback_configuration() {
  auto server = make_server();
  assert(server.set_authorizer(std::make_shared<NoopAuthorizer>()));
  assert(server.set_metrics(std::make_shared<NoopMetrics>()));
  assert(server.set_logger(std::make_shared<NoopLogger>()));
  assert(server.set_transport_observer(std::make_shared<NoopTransportObserver>()));
  assert(server.clear_authorizer());
  assert(server.clear_metrics());
  assert(server.clear_logger());
  assert(server.clear_transport_observer());

  assert(server.set_authorizer(std::make_shared<NoopAuthorizer>()));
  assert(server.set_metrics(std::make_shared<NoopMetrics>()));
  assert(server.set_logger(std::make_shared<NoopLogger>()));
  assert(server.set_transport_observer(std::make_shared<NoopTransportObserver>()));
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
  test_authorizer_callbacks();
  test_observer_exception_containment();
  test_admission_and_recursive_exception_containment();
  test_server_callback_configuration();
  test_channel_final_owner_drop_from_callback();
  assert(trevrpc::detail::drain_lifecycle_reaper_until(std::chrono::steady_clock::now() + 5s));
  return 0;
}
