#include <trevrpc/callbacks.hpp>
#include <trevrpc/trevrpc.hpp>

#include <cassert>
#include <chrono>
#include <memory>

namespace {

using namespace std::chrono_literals;

class NoopLogger final : public trevrpc::Logger {
public:
  void log(const trevrpc::LogEvent&) override {}
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

void test_shutdown_releases_abi1_server() {
  auto server = make_server();
  trevrpc::ShutdownOptions options;
  options.graceful_timeout = 2s;
  options.cancellation_timeout = 2s;
  auto released = server.shutdown(options);
  assert(released);
  assert(released.value().final_phase == trevrpc::ServerPhase::Released);
  assert(released.value().released);
  auto repeated = server.shutdown(options);
  assert(repeated);
  assert(repeated.value().released);
}

void test_large_frame_webtransport_server_starts() {
  trevrpc::ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.cert_file = TREVRPC_CPP_TEST_CERT;
  config.key_file = TREVRPC_CPP_TEST_KEY;
  config.enable_native = false;
  config.webtransport_path = "/trevrpc";
  config.webtransport_origin = "http://127.0.0.1:4443";
  config.max_frame_size = 64u * 1024u * 1024u + 1024u;
  auto server = trevrpc::Server::listen(config);
  assert(server);
  auto serving = server.value().serve();
  assert(serving);

  trevrpc::ShutdownOptions options;
  options.graceful_timeout = 2s;
  options.cancellation_timeout = 2s;
  auto released = server.value().shutdown(options);
  assert(released);
  assert(released.value().released);
}

void test_request_stop_reports_stopping() {
  auto server = make_server();
  auto configuring = server.phase();
  assert(configuring);
  assert(configuring.value() == trevrpc::ServerPhase::Configuring);

  assert(server.request_stop());
  auto stopping = server.phase();
  assert(stopping);
  assert(stopping.value() == trevrpc::ServerPhase::Stopping);

  auto configure_after_stop = server.set_logger(std::make_shared<NoopLogger>());
  assert(!configure_after_stop);
  assert(configure_after_stop.error().code() == -ESHUTDOWN);

  trevrpc::ShutdownOptions options;
  options.graceful_timeout = 2s;
  options.cancellation_timeout = 2s;
  auto released = server.shutdown(options);
  assert(released);
  assert(released.value().released);
}

} // namespace

int main() {
  test_shutdown_releases_abi1_server();
  test_large_frame_webtransport_server_starts();
  test_request_stop_reports_stopping();
  return 0;
}
