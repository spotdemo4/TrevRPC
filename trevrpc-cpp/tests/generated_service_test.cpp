#include "generator.trevrpc.hpp"

#include <trevrpc/callbacks.hpp>
#include <trevrpc/trevrpc.hpp>

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

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

class SelectiveAuthorizer final : public trevrpc::Authorizer {
public:
  trevrpc::Status authorize(const trevrpc::CallContext&,
                            const trevrpc::AuthorizationRequest& request) override {
    if (request.service == "denied.Service") {
      return trevrpc::Status(trevrpc::StatusCode::PermissionDenied, "denied by authorizer");
    }
    return trevrpc::Status::ok();
  }
};

class RecordingMetrics final : public trevrpc::MetricsObserver {
public:
  void rpc_started(const trevrpc::RpcStartedEvent& event) override {
    {
      std::lock_guard lock(mutex_);
      started_.push_back(event);
    }
    condition_.notify_all();
  }

  void rpc_finished(const trevrpc::RpcFinishedEvent& event) override {
    {
      std::lock_guard lock(mutex_);
      finished_.push_back(event);
    }
    condition_.notify_all();
  }

  [[nodiscard]] std::size_t started_count() const {
    std::lock_guard lock(mutex_);
    return started_.size();
  }

  [[nodiscard]] std::size_t finished_count() const {
    std::lock_guard lock(mutex_);
    return finished_.size();
  }

  trevrpc::RpcFinishedEvent wait_for_finished(std::size_t count) {
    std::unique_lock lock(mutex_);
    const bool completed = condition_.wait_for(lock, 5s, [&] { return finished_.size() >= count; });
    assert(completed);
    return finished_[count - 1];
  }

private:
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<trevrpc::RpcStartedEvent> started_;
  std::vector<trevrpc::RpcFinishedEvent> finished_;
};

class FixtureService final : public fixture::FixtureService {
public:
  trevrpc::Result<trevrpc::Response<common::ImportedReply>>
  Unary(const trevrpc::CallContext& context, const fixture::Outer::Request& request) override {
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
    trevrpc::Metadata metadata;
    metadata.set("response-key", "response-value");
    return trevrpc::Response<common::ImportedReply>{std::move(reply), std::move(metadata)};
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
      metadata.set(std::string(trevrpc::Metadata::max_key_size + 1, 'x'), "invalid");
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

  trevrpc::Result<trevrpc::Response<common::ImportedReply>>
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
    trevrpc::Metadata metadata;
    metadata.set("response-key", "response-value");
    return trevrpc::Response<common::ImportedReply>{std::move(reply), std::move(metadata)};
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
  auto metrics = std::make_shared<RecordingMetrics>();
  assert(server.set_authorizer(std::make_shared<SelectiveAuthorizer>()));
  assert(server.set_metrics(metrics));
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
  const std::size_t unary_started_before = metrics->started_count();
  const std::size_t unary_finished_before = metrics->finished_count();
  auto unary = client.Unary(request);
  assert(unary);
  assert(unary.value().message.message() == "hello, unary");
  assert(unary.value().metadata.get("response-key").has_value());
  const auto unary_metrics = metrics->wait_for_finished(unary_finished_before + 1);
  assert(metrics->started_count() == unary_started_before + 1);
  assert(unary_metrics.service == "trevrpc.cpp.test.v1.Fixture");
  assert(unary_metrics.method == "Unary");
  assert(unary_metrics.request_body_size == request.ByteSizeLong());
  assert(unary_metrics.response_body_size == unary.value().message.ByteSizeLong());
  assert(unary_metrics.status == trevrpc::StatusCode::Ok);
  assert(unary_metrics.elapsed >= 0ns);
  std::this_thread::sleep_for(10ms);
  assert(metrics->finished_count() == unary_finished_before + 1);

  const std::byte denied_body[] = {std::byte{1}, std::byte{2}, std::byte{3}};
  const std::size_t denied_started_before = metrics->started_count();
  const std::size_t denied_finished_before = metrics->finished_count();
  auto denied_response = channel->call_unary("denied.Service", "Method", denied_body, {});
  assert(denied_response);
  assert(denied_response.value().status.code() == trevrpc::StatusCode::PermissionDenied);
  const auto denied_metrics = metrics->wait_for_finished(denied_finished_before + 1);
  assert(metrics->started_count() == denied_started_before + 1);
  assert(denied_metrics.service == "denied.Service");
  assert(denied_metrics.method == "Method");
  assert(denied_metrics.request_body_size == std::size(denied_body));
  assert(denied_metrics.response_body_size == 0);
  assert(denied_metrics.status == trevrpc::StatusCode::PermissionDenied);
  assert(denied_metrics.elapsed >= 0ns);
  std::this_thread::sleep_for(10ms);
  assert(metrics->finished_count() == denied_finished_before + 1);

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

  const std::byte malformed_body[] = {std::byte{0xff}};
  auto malformed_response = channel->call_unary("trevrpc.cpp.test.v1.Fixture", "Unary",
                                                std::span<const std::byte>(malformed_body), {});
  assert(malformed_response);
  assert(malformed_response.value().status.code() == trevrpc::StatusCode::InvalidArgument);

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

  auto client_stream = client.ClientStreaming();
  assert(client_stream);
  const auto client_messages = indexed_messages("client-");
  for (const std::string& message : client_messages) {
    assert(client_stream.value().send(make_request(message)));
  }
  auto client_stream_response = client_stream.value().finish_and_receive();
  assert(client_stream_response);
  assert(client_stream_response.value().message.message() == join_messages(client_messages));
  assert(client_stream_response.value().metadata.get("response-key").has_value());

  auto status_terminated_stream = channel->start_stream(
      "trevrpc.cpp.test.v1.Fixture", "ClientStreaming", TREVRPC_RPC_KIND_CLIENT_STREAMING, {}, {});
  assert(status_terminated_stream);
  const auto status_terminated_body = trevrpc::detail::serialize(make_request("status-terminated"));
  assert(status_terminated_body);
  assert(status_terminated_stream.value().send(status_terminated_body.value()));
  assert(status_terminated_stream.value().finish_send());
  auto status_terminated_message = status_terminated_stream.value().receive();
  assert(status_terminated_message);
  assert(!status_terminated_message.value().terminal);
  assert(status_terminated_message.value().message);
  common::ImportedReply status_terminated_reply;
  assert(status_terminated_reply.ParseFromArray(
      status_terminated_message.value().body.data(),
      static_cast<int>(status_terminated_message.value().body.size())));
  assert(status_terminated_reply.message() == "status-terminated");
  auto status_terminated_response = status_terminated_stream.value().receive();
  assert(status_terminated_response);
  assert(status_terminated_response.value().terminal);
  assert(!status_terminated_response.value().message);
  assert(status_terminated_response.value().status.is_ok());

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

  request.set_name("stress");
  auto live_stream_result = client.ServerStreaming(request);
  assert(live_stream_result);
  auto live_stream = std::move(live_stream_result).value();
  auto close_future = std::async(std::launch::async, [&] {
    channel->close();
    return true;
  });
  assert(close_future.wait_for(2s) == std::future_status::ready);
  assert(close_future.get());
  auto rejected_call = client.Unary(request);
  assert(!rejected_call);
  live_stream.close();

  channel.reset();
  assert(server.request_stop());
  server_thread.join();
  assert(serve_result);
  trevrpc::ShutdownOptions shutdown_options;
  shutdown_options.graceful_timeout = 0ns;
  shutdown_options.cancellation_timeout = 5s;
  auto shutdown = server.shutdown(shutdown_options);
  assert(shutdown);
  assert(shutdown.value().released);
  assert(metrics->finished_count() == metrics->started_count());
  return 0;
}
