#include "generator.trevrpc.hpp"

#include <trevrpc/trevrpc.hpp>

#include <cassert>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace fixture = trevrpc::cpp::test::v1;
namespace common = trevrpc::cpp::test::common;
using namespace std::chrono_literals;

class FixtureService final : public fixture::FixtureService {
public:
  trevrpc::Result<common::ImportedReply> Unary(const trevrpc::CallContext& context,
                                               const fixture::Outer::Request& request) override {
    if (request.name() == "throw") {
      throw std::runtime_error("fixture exception");
    }
    common::ImportedReply reply;
    if (request.name() == "metadata") {
      const auto value = context.metadata().get("test-key");
      assert(value.has_value());
      reply.set_message(std::string(reinterpret_cast<const char*>(value->data()), value->size()));
    } else {
      reply.set_message("hello, " + request.name());
    }
    return reply;
  }

  trevrpc::Status ServerStreaming(const trevrpc::CallContext&,
                                  const fixture::Outer::Request& request,
                                  trevrpc::ServerWriter<common::ImportedReply>& writer) override {
    if (request.name() == "denied") {
      trevrpc::Metadata metadata;
      metadata.set("test-trailer", "trailer-value");
      return trevrpc::Status(trevrpc::StatusCode::PermissionDenied, "denied by fixture",
                             std::move(metadata));
    }
    if (request.name() == "invalid-metadata") {
      trevrpc::Metadata metadata;
      metadata.set(std::string(TREVRPC_MAX_METADATA_KEY_LEN + 1, 'x'), "invalid");
      return trevrpc::Status(trevrpc::StatusCode::PermissionDenied, "invalid metadata",
                             std::move(metadata));
    }
    for (const char* prefix : {"hello, ", "goodbye, "}) {
      common::ImportedReply reply;
      reply.set_message(prefix + request.name());
      auto sent = writer.send(reply);
      if (!sent) {
        return trevrpc::Status::internal(sent.error().message());
      }
    }
    return trevrpc::Status::ok();
  }

  trevrpc::Result<common::ImportedReply>
  ClientStreaming(const trevrpc::CallContext&,
                  trevrpc::ServerReader<fixture::Outer::Request>& reader) override {
    std::string names;
    for (;;) {
      auto request = reader.receive();
      if (!request) {
        return request.error();
      }
      if (!request.value().has_value()) {
        break;
      }
      if (!names.empty()) {
        names += ',';
      }
      names += request.value().value().name();
    }
    common::ImportedReply reply;
    reply.set_message(names);
    return reply;
  }

  trevrpc::Status BidirectionalStreaming(
      const trevrpc::CallContext&,
      trevrpc::ServerReaderWriter<fixture::Outer::Request, common::ImportedReply>& stream)
      override {
    for (;;) {
      auto request = stream.receive();
      if (!request) {
        return trevrpc::Status::internal(request.error().message());
      }
      if (!request.value().has_value()) {
        return trevrpc::Status::ok();
      }
      common::ImportedReply reply;
      reply.set_message("echo, " + request.value().value().name());
      auto sent = stream.send(reply);
      if (!sent) {
        return trevrpc::Status::internal(sent.error().message());
      }
    }
  }
};

template <typename Call>
void expect_messages(Call& call, const std::vector<std::string>& expected) {
  for (const std::string& value : expected) {
    auto event = call.receive();
    assert(event);
    assert(event.value().is_message());
    assert(event.value().message().message() == value);
  }
  auto terminal = call.receive();
  assert(terminal);
  assert(!terminal.value().is_message());
  assert(terminal.value().status().is_ok());
}

int main() {
  trevrpc::ServerConfig server_config;
  server_config.host = "127.0.0.1";
  server_config.port = 0;
  server_config.cert_file = TREVRPC_CPP_TEST_CERT;
  server_config.key_file = TREVRPC_CPP_TEST_KEY;
  auto listening = trevrpc::Server::listen(server_config);
  assert(listening);
  trevrpc::Server server = std::move(listening).value();
  auto registered = fixture::RegisterFixture(server, std::make_shared<FixtureService>());
  assert(registered);
  auto port = server.port();
  assert(port);

  trevrpc::Result<void> serve_result;
  std::thread server_thread([&] { serve_result = server.serve(); });

  trevrpc::ChannelConfig channel_config;
  channel_config.skip_certificate_validation = true;
  auto connected = trevrpc::Channel::connect("127.0.0.1", port.value(), channel_config, 5s);
  assert(connected);
  auto channel = std::move(connected).value();
  fixture::FixtureClient client(channel);

  fixture::Outer::Request request;
  request.set_name("unary");
  auto unary = client.Unary(request);
  assert(unary);
  assert(unary.value().message.message() == "hello, unary");

  request.set_name("metadata");
  trevrpc::CallOptions metadata_options;
  metadata_options.metadata.set("test-key", "metadata-value");
  auto metadata_response = client.Unary(request, metadata_options);
  assert(metadata_response);
  assert(metadata_response.value().message.message() == "metadata-value");

  request.set_name("throw");
  auto thrown = client.Unary(request);
  assert(!thrown);
  assert(thrown.error().status().has_value());
  assert(thrown.error().status().value().code() == trevrpc::StatusCode::Internal);

  const std::uint8_t malformed_body[] = {0xff};
  trevrpc_response* malformed_response = nullptr;
  const int malformed_error =
      trevrpc_channel_call_unary(channel->native_handle(), "trevrpc.cpp.test.v1.Fixture", "Unary",
                                 malformed_body, sizeof(malformed_body), &malformed_response);
  assert(malformed_error == 0);
  assert(malformed_response != nullptr);
  assert(malformed_response->status == TREVRPC_STATUS_INVALID_ARGUMENT);
  trevrpc_response_free(malformed_response);

  request.set_name("server");
  auto server_stream = client.ServerStreaming(request);
  assert(server_stream);
  expect_messages(server_stream.value(), {"hello, server", "goodbye, server"});

  request.set_name("denied");
  auto denied = client.ServerStreaming(request);
  assert(denied);
  auto denied_event = denied.value().receive();
  assert(denied_event);
  assert(!denied_event.value().is_message());
  assert(denied_event.value().status().code() == trevrpc::StatusCode::PermissionDenied);
  assert(denied_event.value().status().message() == "denied by fixture");
  assert(denied_event.value().status().metadata().get("test-trailer").has_value());

  request.set_name("invalid-metadata");
  auto invalid_metadata = client.ServerStreaming(request);
  assert(invalid_metadata);
  auto invalid_metadata_event = invalid_metadata.value().receive();
  assert(invalid_metadata_event);
  assert(!invalid_metadata_event.value().is_message());
  assert(invalid_metadata_event.value().status().code() == trevrpc::StatusCode::Internal);

  trevrpc::CallOptions negative_options;
  negative_options.timeout = -1ns;
  request.set_name("negative-timeout");
  auto negative_timeout = client.Unary(request, negative_options);
  assert(!negative_timeout);
  assert(negative_timeout.error().kind() == trevrpc::Error::Kind::Runtime);
  assert(!channel->wait_ready(-1ns));

  trevrpc::ServerOptions negative_server_options;
  negative_server_options.stream_idle_timeout = -1ns;
  assert(!server.set_options(negative_server_options));

  auto client_stream = client.ClientStreaming();
  assert(client_stream);
  for (const char* name : {"left", "right"}) {
    fixture::Outer::Request streamed;
    streamed.set_name(name);
    assert(client_stream.value().send(streamed));
  }
  assert(client_stream.value().finish_send());
  expect_messages(client_stream.value(), {"left,right"});

  auto bidi = client.BidirectionalStreaming();
  assert(bidi);
  for (const char* name : {"one", "two"}) {
    fixture::Outer::Request streamed;
    streamed.set_name(name);
    assert(bidi.value().send(streamed));
  }
  assert(bidi.value().finish_send());
  expect_messages(bidi.value(), {"echo, one", "echo, two"});

  channel->close();
  channel.reset();
  server.shutdown();
  server_thread.join();
  assert(serve_result);
  return 0;
}
