#include "nested/service.trevrpc.hpp"

#include <trevrpc/async.hpp>
#include <trevrpc/callbacks.hpp>

#include <cassert>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

namespace consumer = trevrpc::cpp::consumer;
using namespace std::chrono_literals;

namespace {

consumer::Response response(std::string value) {
  consumer::Response result;
  result.set_value(std::move(value));
  return result;
}

class SyncService final : public consumer::ConsumerServiceService {
public:
  trevrpc::Result<trevrpc::Response<consumer::Response>>
  Call(const trevrpc::CallContext&, const consumer::Request& request) override {
    trevrpc::Metadata metadata;
    metadata.set("consumer", "sync");
    return trevrpc::Response<consumer::Response>{response(request.value()), std::move(metadata)};
  }

  trevrpc::Status ServerStreaming(const trevrpc::CallContext&, const consumer::Request& request,
                                  trevrpc::ServerWriter<consumer::Response>& writer) override {
    auto sent = writer.send(response(request.value()));
    return sent ? trevrpc::Status::ok() : trevrpc::Status::internal(sent.error().message());
  }

  trevrpc::Result<trevrpc::Response<consumer::Response>>
  ClientStreaming(const trevrpc::CallContext&,
                  trevrpc::ServerReader<consumer::Request>& reader) override {
    auto request = reader.receive();
    if (!request) {
      return request.error();
    }
    return trevrpc::Response<consumer::Response>{
        response(request.value() ? request.value()->value() : std::string{}), {}};
  }

  trevrpc::Status BidirectionalStreaming(
      const trevrpc::CallContext&,
      trevrpc::ServerReaderWriter<consumer::Request, consumer::Response>& stream) override {
    auto request = stream.receive();
    if (!request) {
      return trevrpc::Status::internal(request.error().message());
    }
    if (request.value()) {
      auto sent = stream.send(response(request.value()->value()));
      if (!sent) {
        return trevrpc::Status::internal(sent.error().message());
      }
    }
    return trevrpc::Status::ok();
  }
};

class AsyncService final : public consumer::ConsumerServiceAsyncService {
public:
  trevrpc::Task<trevrpc::Result<trevrpc::Response<consumer::Response>>>
  Call(trevrpc::CallContext, consumer::Request request) override {
    co_return trevrpc::Response<consumer::Response>{response(request.value()), {}};
  }

  trevrpc::Task<trevrpc::Status>
  ServerStreaming(trevrpc::CallContext, consumer::Request request,
                  trevrpc::AsyncServerWriter<consumer::Response> writer) override {
    auto sent = co_await writer.send(response(request.value()));
    co_return sent ? trevrpc::Status::ok() : trevrpc::Status::internal(sent.error().message());
  }

  trevrpc::Task<trevrpc::Result<trevrpc::Response<consumer::Response>>>
  ClientStreaming(trevrpc::CallContext,
                  trevrpc::AsyncServerReader<consumer::Request> reader) override {
    auto request = co_await reader.receive();
    if (!request) {
      co_return request.error();
    }
    co_return trevrpc::Response<consumer::Response>{
        response(request.value() ? request.value()->value() : std::string{}), {}};
  }

  trevrpc::Task<trevrpc::Status>
  BidirectionalStreaming(trevrpc::CallContext, trevrpc::AsyncServerReader<consumer::Request> reader,
                         trevrpc::AsyncServerWriter<consumer::Response> writer) override {
    auto request = co_await reader.receive();
    if (!request) {
      co_return trevrpc::Status::internal(request.error().message());
    }
    if (request.value()) {
      auto sent = co_await writer.send(response(request.value()->value()));
      if (!sent) {
        co_return trevrpc::Status::internal(sent.error().message());
      }
    }
    co_return trevrpc::Status::ok();
  }
};

trevrpc::Task<int> async_value() { co_return 7; }

} // namespace

int main() {
  static_assert(std::is_copy_constructible_v<trevrpc::Cancellation>);
  static_assert(std::is_copy_assignable_v<trevrpc::Cancellation>);
  auto async_service = std::make_shared<AsyncService>();

  auto executor_result = trevrpc::ThreadPoolExecutor::create();
  assert(executor_result);
  auto executor = std::move(executor_result).value();
  auto runtime_result = trevrpc::AsyncRuntime::create(executor);
  assert(runtime_result);
  auto runtime = std::move(runtime_result).value();

  consumer::ConsumerServiceClient null_sync(nullptr);
  consumer::Request request;
  request.set_value("generated");
  assert(!null_sync.Call(request));
  consumer::ConsumerServiceAsyncClient null_async(nullptr, runtime);
  assert(!trevrpc::sync_wait(null_async.Call(request)));

  bool spawned = false;
  auto started = trevrpc::spawn(async_value(), [&](trevrpc::TaskCompletion<int> completion) {
    assert(!completion.exception);
    assert(completion.value == 7);
    spawned = true;
  });
  assert(started && spawned);
  assert(trevrpc::sync_wait(async_value()) == 7);

  trevrpc::ServerConfig server_config;
  server_config.host = "127.0.0.1";
  server_config.port = 0;
  server_config.cert_file = TREVRPC_CPP_CONSUMER_CERT;
  server_config.key_file = TREVRPC_CPP_CONSUMER_KEY;
  auto listening = trevrpc::Server::listen(server_config);
  assert(listening);
  trevrpc::Server server = std::move(listening).value();
  assert(consumer::RegisterConsumerService(server, std::make_shared<SyncService>()));
  auto port = server.port();
  assert(port);
  trevrpc::Result<void> serve_result;
  std::thread server_thread([&] { serve_result = server.serve(); });

  trevrpc::ChannelConfig channel_config;
  channel_config.skip_certificate_validation = true;
  auto connected = trevrpc::Channel::connect("127.0.0.1", port.value(), channel_config, 5s);
  assert(connected);
  auto channel = std::move(connected).value();
  consumer::ConsumerServiceClient client(channel);
  auto result = client.Call(request);
  assert(result && result.value().message.value() == "generated");
  channel->close();
  channel.reset();

  trevrpc::ShutdownOptions shutdown_options;
  shutdown_options.graceful_timeout = 5s;
  auto shutdown = server.shutdown(shutdown_options);
  assert(shutdown && shutdown.value().released);
  server_thread.join();
  assert(serve_result);

  auto async_listening = trevrpc::Server::listen(server_config);
  assert(async_listening);
  trevrpc::Server async_server = std::move(async_listening).value();
  assert(consumer::RegisterConsumerServiceAsync(async_server, async_service, runtime));
  auto async_port = async_server.port();
  assert(async_port);
  trevrpc::Result<void> async_serve_result;
  std::thread async_server_thread([&] { async_serve_result = async_server.serve(); });

  auto async_connected =
      trevrpc::Channel::connect("127.0.0.1", async_port.value(), channel_config, 5s);
  assert(async_connected);
  auto async_channel = std::move(async_connected).value();
  consumer::ConsumerServiceAsyncClient async_client(async_channel, runtime);

  auto async_unary = trevrpc::sync_wait(async_client.Call(request));
  assert(async_unary && async_unary.value().message.value() == "generated");

  auto async_server_stream = trevrpc::sync_wait(async_client.ServerStreaming(request));
  assert(async_server_stream);
  auto async_server_message = trevrpc::sync_wait(async_server_stream.value().responses.receive());
  assert(async_server_message && async_server_message.value().is_message());
  assert(async_server_message.value().message().value() == "generated");
  auto async_server_terminal = trevrpc::sync_wait(async_server_stream.value().responses.receive());
  assert(async_server_terminal && !async_server_terminal.value().is_message());
  assert(async_server_terminal.value().status().is_ok());

  auto async_client_stream = trevrpc::sync_wait(async_client.ClientStreaming());
  assert(async_client_stream);
  assert(trevrpc::sync_wait(async_client_stream.value().requests.send(request)));
  auto async_client_response = trevrpc::sync_wait(async_client_stream.value().finish_and_receive());
  assert(async_client_response && async_client_response.value().message.value() == "generated");

  auto async_bidi = trevrpc::sync_wait(async_client.BidirectionalStreaming());
  assert(async_bidi);
  assert(trevrpc::sync_wait(async_bidi.value().requests.send(request)));
  assert(trevrpc::sync_wait(async_bidi.value().requests.finish_send()));
  auto async_bidi_message = trevrpc::sync_wait(async_bidi.value().responses.receive());
  assert(async_bidi_message && async_bidi_message.value().is_message());
  assert(async_bidi_message.value().message().value() == "generated");
  auto async_bidi_terminal = trevrpc::sync_wait(async_bidi.value().responses.receive());
  assert(async_bidi_terminal && !async_bidi_terminal.value().is_message());
  assert(async_bidi_terminal.value().status().is_ok());

  async_channel->close();
  async_channel.reset();
  auto async_shutdown = async_server.shutdown(shutdown_options);
  assert(async_shutdown && async_shutdown.value().released);
  async_server_thread.join();
  assert(async_serve_result);

  executor->request_stop();
  assert(executor->drain_until(trevrpc::Deadline::clock::now() + 5s));
  return 0;
}
