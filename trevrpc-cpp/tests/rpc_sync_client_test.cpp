#include "common.pb.h"
#include "generator.pb.h"

#include <trevrpc/trevrpc.hpp>

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace fixture = trevrpc::cpp::test::v1;
namespace common = trevrpc::cpp::test::common;

int main() {
  trevrpc::ServerConfig server_config;
  server_config.host = "127.0.0.1";
  server_config.cert_file = TREVRPC_CPP_TEST_CERT;
  server_config.key_file = TREVRPC_CPP_TEST_KEY;
  auto listening = trevrpc::Server::listen(server_config);
  assert(listening);
  trevrpc::Server server = std::move(listening).value();
  std::mutex handler_mutex;
  std::condition_variable handler_condition;
  bool handler_entered = false;
  bool handler_release = false;
  auto registered = server.register_unary<fixture::Outer::Request, common::ImportedReply>(
      "trevrpc.cpp.test.v1.Fixture", "Unary",
      [&](const trevrpc::CallContext& context, const fixture::Outer::Request& request) {
        const auto metadata = context.metadata().get("request-key");
        assert(metadata.has_value());
        const auto empty_metadata = context.metadata().get("empty-request");
        assert(empty_metadata.has_value());
        assert(empty_metadata->empty());
        if (request.name() == "blocked") {
          std::unique_lock lock(handler_mutex);
          handler_entered = true;
          handler_condition.notify_all();
          handler_condition.wait(lock, [&] { return handler_release; });
        }
        common::ImportedReply reply;
        reply.set_message("rpc1:" + request.name());
        trevrpc::Metadata response_metadata;
        response_metadata.set("response-key", "response-value");
        response_metadata.set("empty-response", std::span<const std::byte>{});
        return trevrpc::Result<trevrpc::Response<common::ImportedReply>>(
            trevrpc::Response<common::ImportedReply>{std::move(reply),
                                                     std::move(response_metadata)});
      });
  assert(registered);
  auto port = server.port();
  assert(port);

  trevrpc::Result<void> serve_result;
  std::thread server_thread([&] { serve_result = server.serve(); });

  trevrpc::ChannelConfig client_config;
  client_config.skip_certificate_validation = true;
  client_config.max_frame_size = 64u * 1024u * 1024u + 1024u;
  auto connected = trevrpc::Channel::connect("127.0.0.1", port.value(), client_config);
  assert(connected);
  auto client = std::move(connected).value();

  fixture::Outer::Request request;
  request.set_name("foundation");
  std::vector<std::byte> request_body(request.ByteSizeLong());
  assert(request.SerializeToArray(request_body.data(), static_cast<int>(request_body.size())));
  trevrpc::CallOptions options;
  options.metadata.set("request-key", "request-value");
  options.metadata.set("empty-request", std::span<const std::byte>{});
  auto response = client->call_unary("trevrpc.cpp.test.v1.Fixture", "Unary", request_body, options);
  assert(response);
  assert(response.value().status.is_ok());
  const auto response_metadata = response.value().metadata.get("response-key");
  assert(response_metadata.has_value());
  assert(std::string(reinterpret_cast<const char*>(response_metadata->data()),
                     response_metadata->size()) == "response-value");
  const auto empty_response_metadata = response.value().metadata.get("empty-response");
  assert(empty_response_metadata.has_value());
  assert(empty_response_metadata->empty());

  common::ImportedReply reply;
  assert(reply.ParseFromArray(response.value().body.data(),
                              static_cast<int>(response.value().body.size())));
  assert(reply.message() == "rpc1:foundation");

  fixture::Outer::Request blocked_request;
  blocked_request.set_name("blocked");
  std::vector<std::byte> blocked_body(blocked_request.ByteSizeLong());
  assert(
      blocked_request.SerializeToArray(blocked_body.data(), static_cast<int>(blocked_body.size())));
  auto blocked_call = std::async(std::launch::async, [&] {
    return client->call_unary("trevrpc.cpp.test.v1.Fixture", "Unary", blocked_body, options);
  });
  {
    std::unique_lock lock(handler_mutex);
    handler_condition.wait(lock, [&] { return handler_entered; });
  }
  std::promise<void> close_started;
  auto close_entered = close_started.get_future();
  auto concurrent_close = std::async(std::launch::async, [&] {
    close_started.set_value();
    client->close();
    return true;
  });
  close_entered.wait();
  {
    std::lock_guard lock(handler_mutex);
    handler_release = true;
  }
  handler_condition.notify_all();
  auto blocked_response = blocked_call.get();
  assert(blocked_response);
  assert(blocked_response.value().status.is_ok());
  assert(concurrent_close.get());
  client.reset();
  assert(server.request_stop());
  server_thread.join();
  assert(serve_result);
  return 0;
}
