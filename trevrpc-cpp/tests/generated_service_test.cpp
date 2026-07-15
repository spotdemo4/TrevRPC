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

constexpr std::size_t kStreamMessageCount = 128;

fixture::Outer::Request make_request(const std::string& name) {
  fixture::Outer::Request request;
  request.set_name(name);
  return request;
}

common::ImportedReply make_reply(const std::string& message) {
  common::ImportedReply reply;
  reply.set_message(message);
  return reply;
}

std::vector<std::string> indexed_messages(const std::string& prefix) {
  std::vector<std::string> messages;
  messages.reserve(kStreamMessageCount);
  for (std::size_t index = 0; index < kStreamMessageCount; ++index) {
    messages.push_back(prefix + std::to_string(index));
  }
  return messages;
}

std::string join_messages(const std::vector<std::string>& messages) {
  std::string joined;
  for (const std::string& message : messages) {
    if (!joined.empty()) {
      joined += ',';
    }
    joined += message;
  }
  return joined;
}

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
    if (request.name() == "stress") {
      for (std::size_t index = 0; index < kStreamMessageCount; ++index) {
        auto sent = writer.send(make_reply("server-" + std::to_string(index)));
        if (!sent) {
          return trevrpc::Status::internal(sent.error().message());
        }
      }
      return trevrpc::Status::ok();
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
      auto sent = stream.send(make_reply("echo, " + request.value().value().name()));
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
  server_config.max_pending_send_bytes = std::size_t{64} * 1024;
  server_config.max_pending_send_count = 1;
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
  channel_config.max_pending_send_bytes = std::size_t{64} * 1024;
  channel_config.max_pending_send_count = 1;
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

  request.set_name("stress");
  auto server_stream_stress = client.ServerStreaming(request);
  assert(server_stream_stress);
  expect_messages(server_stream_stress.value(), indexed_messages("server-"));

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
  const auto client_messages = indexed_messages("client-");
  for (const std::string& message : client_messages) {
    assert(client_stream.value().send(make_request(message)));
  }
  assert(client_stream.value().finish_send());
  expect_messages(client_stream.value(), {join_messages(client_messages)});

  trevrpc_stream* status_terminated_stream = nullptr;
  assert(trevrpc_channel_start_stream(channel->native_handle(), "trevrpc.cpp.test.v1.Fixture",
                                      "ClientStreaming", TREVRPC_RPC_KIND_CLIENT_STREAMING, nullptr,
                                      0, &status_terminated_stream) == 0);
  const std::string status_terminated_body = make_request("status-terminated").SerializeAsString();
  assert(trevrpc_stream_send_message_copy_wait(
             status_terminated_stream,
             reinterpret_cast<const std::uint8_t*>(status_terminated_body.data()),
             status_terminated_body.size()) == 0);
  assert(trevrpc_stream_send_status(status_terminated_stream, TREVRPC_STATUS_OK, nullptr, 0) == 0);
  assert(trevrpc_stream_finish_send(status_terminated_stream) == 0);
  trevrpc_stream_frame* status_terminated_response = nullptr;
  assert(trevrpc_stream_recv(status_terminated_stream, &status_terminated_response) == 0);
  assert(status_terminated_response != nullptr);
  assert(status_terminated_response->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
  common::ImportedReply status_terminated_reply;
  assert(status_terminated_reply.ParseFromArray(
      status_terminated_response->body, static_cast<int>(status_terminated_response->body_len)));
  assert(status_terminated_reply.message() == "status-terminated");
  trevrpc_stream_frame_free(status_terminated_response);
  assert(trevrpc_stream_recv(status_terminated_stream, &status_terminated_response) == 0);
  assert(status_terminated_response != nullptr);
  assert(status_terminated_response->kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
  assert(status_terminated_response->status == TREVRPC_STATUS_OK);
  trevrpc_stream_frame_free(status_terminated_response);
  trevrpc_stream_close(status_terminated_stream);

  auto bidi = client.BidirectionalStreaming();
  assert(bidi);
  const auto bidi_messages = indexed_messages("bidi-");
  for (const std::string& message : bidi_messages) {
    assert(bidi.value().send(make_request(message)));
  }
  assert(bidi.value().finish_send());
  std::vector<std::string> bidi_responses;
  bidi_responses.reserve(kStreamMessageCount);
  for (const std::string& message : bidi_messages) {
    bidi_responses.push_back("echo, " + message);
  }
  expect_messages(bidi.value(), bidi_responses);

  channel->close();
  channel.reset();
  server.shutdown();
  server_thread.join();
  assert(serve_result);
  return 0;
}
