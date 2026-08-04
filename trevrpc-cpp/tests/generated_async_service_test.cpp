#include "generator.trevrpc.hpp"

#include <trevrpc/async.hpp>

#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace fixture = trevrpc::cpp::test::v1;
namespace common = trevrpc::cpp::test::common;
using namespace std::chrono_literals;

namespace {

fixture::Outer::Request request(std::string name) {
  fixture::Outer::Request value;
  value.set_name(std::move(name));
  return value;
}

common::ImportedReply reply(std::string message) {
  common::ImportedReply value;
  value.set_message(std::move(message));
  return value;
}

class AsyncFixtureService final : public fixture::FixtureAsyncService {
public:
  trevrpc::Task<trevrpc::Result<trevrpc::Response<common::ImportedReply>>>
  Unary(trevrpc::CallContext context, fixture::Outer::Request value) override {
    auto request_metadata = context.metadata().get("request-key");
    assert(request_metadata.has_value());
    trevrpc::Metadata metadata;
    metadata.set("response-key", "unary-metadata");
    co_return trevrpc::Response<common::ImportedReply>{reply("hello, " + value.name()),
                                                       std::move(metadata)};
  }

  trevrpc::Task<trevrpc::Status>
  ServerStreaming(trevrpc::CallContext, fixture::Outer::Request value,
                  trevrpc::AsyncServerWriter<common::ImportedReply> writer) override {
    auto first = co_await writer.send(reply("first, " + value.name()));
    if (!first) {
      co_return trevrpc::Status::internal(first.error().message());
    }
    auto second = co_await writer.send(reply("second, " + value.name()));
    if (!second) {
      co_return trevrpc::Status::internal(second.error().message());
    }
    trevrpc::Metadata metadata;
    metadata.set("response-key", "server-stream-metadata");
    co_return trevrpc::Status(trevrpc::StatusCode::Ok, {}, std::move(metadata));
  }

  trevrpc::Task<trevrpc::Result<trevrpc::Response<common::ImportedReply>>>
  ClientStreaming(trevrpc::CallContext,
                  trevrpc::AsyncServerReader<fixture::Outer::Request> reader) override {
    std::string names;
    for (;;) {
      auto next = co_await reader.receive();
      if (!next) {
        co_return next.error();
      }
      if (!next.value().has_value()) {
        break;
      }
      if (!names.empty()) {
        names.push_back(',');
      }
      names += next.value()->name();
    }
    trevrpc::Metadata metadata;
    metadata.set("response-key", "client-stream-metadata");
    co_return trevrpc::Response<common::ImportedReply>{reply(std::move(names)),
                                                       std::move(metadata)};
  }

  trevrpc::Task<trevrpc::Status>
  BidirectionalStreaming(trevrpc::CallContext,
                         trevrpc::AsyncServerReader<fixture::Outer::Request> reader,
                         trevrpc::AsyncServerWriter<common::ImportedReply> writer) override {
    for (;;) {
      auto next = co_await reader.receive();
      if (!next) {
        co_return trevrpc::Status::internal(next.error().message());
      }
      if (!next.value().has_value()) {
        trevrpc::Metadata metadata;
        metadata.set("response-key", "bidi-metadata");
        co_return trevrpc::Status(trevrpc::StatusCode::Ok, {}, std::move(metadata));
      }
      auto sent = co_await writer.send(reply("echo, " + next.value()->name()));
      if (!sent) {
        co_return trevrpc::Status::internal(sent.error().message());
      }
    }
  }
};

void expect_null_clients(const std::shared_ptr<trevrpc::AsyncRuntime>& runtime) {
  fixture::FixtureAsyncClient null_channel(nullptr, runtime);
  auto unary = trevrpc::sync_wait(null_channel.Unary(request("null")));
  assert(!unary);
  assert(unary.error().kind() == trevrpc::Error::Kind::Runtime);
  auto server_stream = trevrpc::sync_wait(null_channel.ServerStreaming(request("null")));
  assert(!server_stream);
  auto client_stream = trevrpc::sync_wait(null_channel.ClientStreaming());
  assert(!client_stream);
  auto bidi = trevrpc::sync_wait(null_channel.BidirectionalStreaming());
  assert(!bidi);

  fixture::FixtureAsyncClient null_runtime(nullptr, nullptr);
  auto no_runtime = trevrpc::sync_wait(null_runtime.Unary(request("null")));
  assert(!no_runtime);
  assert(no_runtime.error().code() == -EINVAL);
}

} // namespace

int main() {
  auto executor_result = trevrpc::ThreadPoolExecutor::create(
      trevrpc::ThreadPoolExecutorOptions{.worker_count = 4, .queue_capacity = 64});
  assert(executor_result);
  auto executor = std::move(executor_result).value();
  auto runtime_result = trevrpc::AsyncRuntime::create(executor);
  assert(runtime_result);
  auto runtime = std::move(runtime_result).value();

  expect_null_clients(runtime);

  trevrpc::ServerConfig server_config;
  server_config.host = "127.0.0.1";
  server_config.port = 0;
  server_config.cert_file = TREVRPC_CPP_TEST_CERT;
  server_config.key_file = TREVRPC_CPP_TEST_KEY;
  auto listening = trevrpc::Server::listen(server_config);
  assert(listening);
  trevrpc::Server server = std::move(listening).value();
  auto registered =
      fixture::RegisterFixtureAsync(server, std::make_shared<AsyncFixtureService>(), runtime);
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
  fixture::FixtureAsyncClient client(channel, runtime);

  trevrpc::AsyncCallOptions call_options;
  call_options.call_options.metadata.set("request-key", "request-value");
  call_options.deadline = trevrpc::Deadline::clock::now() + 30s;

  auto unary = trevrpc::sync_wait(client.Unary(request("async"), call_options));
  assert(unary);
  assert(unary.value().message.message() == "hello, async");
  assert(unary.value().metadata.get("response-key").has_value());

  auto server_stream = trevrpc::sync_wait(client.ServerStreaming(request("stream"), call_options));
  assert(server_stream);
  auto first = trevrpc::sync_wait(server_stream.value().responses.receive());
  assert(first && first.value().is_message());
  assert(first.value().message().message() == "first, stream");
  auto second = trevrpc::sync_wait(server_stream.value().responses.receive());
  assert(second && second.value().is_message());
  assert(second.value().message().message() == "second, stream");
  auto server_terminal = trevrpc::sync_wait(server_stream.value().responses.receive());
  assert(server_terminal && !server_terminal.value().is_message());
  assert(server_terminal.value().status().is_ok());
  assert(server_terminal.value().status().metadata().get("response-key").has_value());

  auto client_stream = trevrpc::sync_wait(client.ClientStreaming(call_options));
  assert(client_stream);
  assert(trevrpc::sync_wait(client_stream.value().requests.send(request("one"))));
  assert(trevrpc::sync_wait(client_stream.value().requests.send(request("two"))));
  auto client_response = trevrpc::sync_wait(client_stream.value().finish_and_receive());
  if (!client_response) {
    std::cerr << "client-streaming failure: " << client_response.error().code() << ": "
              << client_response.error().message() << '\n';
  }
  assert(client_response);
  assert(client_response.value().message.message() == "one,two");
  assert(client_response.value().metadata.get("response-key").has_value());

  auto bidi = trevrpc::sync_wait(client.BidirectionalStreaming(call_options));
  assert(bidi);
  auto sender = std::move(bidi.value().requests);
  auto receiver = std::move(bidi.value().responses);
  std::thread sender_thread([sender = std::move(sender)]() mutable {
    assert(trevrpc::sync_wait(sender.send(request("moved"))));
    assert(trevrpc::sync_wait(sender.finish_send()));
  });
  auto echoed = trevrpc::sync_wait(receiver.receive());
  assert(echoed && echoed.value().is_message());
  assert(echoed.value().message().message() == "echo, moved");
  auto bidi_terminal = trevrpc::sync_wait(receiver.receive());
  assert(bidi_terminal && !bidi_terminal.value().is_message());
  assert(bidi_terminal.value().status().is_ok());
  assert(bidi_terminal.value().status().metadata().get("response-key").has_value());
  sender_thread.join();

  trevrpc::CancellationSource cancellation;
  cancellation.cancel();
  trevrpc::AsyncCallOptions cancelled_options;
  cancelled_options.cancellation = cancellation.token();
  auto cancelled = trevrpc::sync_wait(client.Unary(request("cancelled"), cancelled_options));
  assert(!cancelled);
  assert(cancelled.error().kind() == trevrpc::Error::Kind::Runtime);

  trevrpc::Cancellation native_cancellation;
  native_cancellation.cancel();
  trevrpc::CancellationSource cpp_cancellation;
  trevrpc::AsyncCallOptions combined_cancellation_options;
  combined_cancellation_options.call_options.cancellation = &native_cancellation;
  combined_cancellation_options.cancellation = cpp_cancellation.token();
  auto native_cancelled =
      trevrpc::sync_wait(client.Unary(request("native-cancelled"), combined_cancellation_options));
  assert(!native_cancelled);
  assert(native_cancelled.error().kind() == trevrpc::Error::Kind::Runtime);

  channel->close();
  channel.reset();
  trevrpc::ShutdownOptions shutdown_options;
  shutdown_options.graceful_timeout = 5s;
  shutdown_options.cancellation_timeout = 5s;
  auto shutdown = server.shutdown(shutdown_options);
  assert(shutdown);
  assert(shutdown.value().released);
  server_thread.join();
  assert(serve_result);

  executor->request_stop();
  assert(executor->drain_until(trevrpc::Deadline::clock::now() + 5s));
  return 0;
}
