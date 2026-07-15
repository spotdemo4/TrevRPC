#include "benchmark_peer.hpp"

#include "benchmark.grpc.pb.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace trevrpc_bench;

namespace benchmark = trevrpc::benchmark::v1;

namespace {

void require(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

[[nodiscard]] std::string read_file(const char* path) {
  std::ifstream input(path, std::ios::binary);
  require(static_cast<bool>(input), "failed to read test certificate");
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void require_status(const grpc::Status& status, grpc::StatusCode code, const char* message) {
  require(status.error_code() == code, "unexpected gRPC status code");
  require(status.error_message() == message, "unexpected gRPC status message");
}

void test_callback_client(const std::shared_ptr<ClientFactory>& factory, ClientConfig& config) {
  std::unique_ptr<BenchmarkClient> client = factory->create();
  for (RpcKind kind :
       {RpcKind::Unary, RpcKind::ClientStream, RpcKind::ServerStream, RpcKind::Bidi}) {
    config.rpc_kind = kind;
    require(!client->run(config, 42).has_value(), "callback RPC failed");
  }
}

void test_concurrent_bidi(const std::shared_ptr<ClientFactory>& factory,
                          const ClientConfig& base_config) {
  constexpr std::size_t lane_count = 8;
  std::vector<std::string> errors(lane_count);
  std::vector<std::thread> lanes;
  lanes.reserve(lane_count);
  for (std::size_t lane = 0; lane < lane_count; ++lane) {
    lanes.emplace_back([&, lane] {
      ClientConfig config = base_config;
      config.rpc_kind = RpcKind::Bidi;
      config.messages_per_stream = 32;
      try {
        std::unique_ptr<BenchmarkClient> client = factory->create();
        if (const std::optional<std::string> error = client->run(config, lane); error.has_value()) {
          errors[lane] = *error;
        }
      } catch (const std::exception& error) {
        errors[lane] = error.what();
      }
    });
  }
  for (std::thread& lane : lanes) {
    lane.join();
  }
  for (const std::string& error : errors) {
    require(error.empty(), "concurrent callback bidi RPC failed");
  }
}

void test_unary(benchmark::BenchmarkService::Stub& stub) {
  benchmark::BenchmarkRequest request;
  request.set_sequence(17);
  request.set_payload(make_payload(257, 17));
  request.set_response_bytes(513);
  benchmark::BenchmarkResponse response;
  grpc::ClientContext context;
  const grpc::Status status = stub.Unary(&context, request, &response);
  require(status.ok(), "unary status was not OK");
  require(response.sequence() == 17, "unary response sequence mismatch");
  require(valid_payload(response.payload(), 513, 17), "unary response payload mismatch");

  request.set_response_bytes(static_cast<std::uint32_t>(kMaximumPayloadBytes + 1));
  grpc::ClientContext invalid_context;
  require_status(stub.Unary(&invalid_context, request, &response),
                 grpc::StatusCode::INVALID_ARGUMENT,
                 "response_bytes exceeds the benchmark peer limit");

  request.set_response_bytes(0);
  request.set_payload(std::string(kMaximumPayloadBytes + 1, '\0'));
  grpc::ClientContext oversized_context;
  require_status(stub.Unary(&oversized_context, request, &response),
                 grpc::StatusCode::INVALID_ARGUMENT,
                 "request payload exceeds the benchmark peer limit");
}

void test_client_stream(benchmark::BenchmarkService::Stub& stub) {
  benchmark::BenchmarkSummary summary;
  grpc::ClientContext context;
  std::unique_ptr<grpc::ClientWriter<benchmark::BenchmarkRequest>> stream =
      stub.ClientStream(&context, &summary);
  for (std::uint64_t sequence = 0; sequence < 4; ++sequence) {
    benchmark::BenchmarkRequest request;
    request.set_sequence(sequence);
    request.set_payload(make_payload(257, sequence));
    request.set_response_bytes(513);
    require(stream->Write(request), "client-stream write failed");
  }
  require(stream->WritesDone(), "client-stream half-close failed");
  require(stream->Finish().ok(), "client-stream status was not OK");
  require(summary.message_count() == 4, "client-stream message count mismatch");
  require(summary.payload_bytes() == std::uint64_t{4} * 257, "client-stream byte count mismatch");
}

void test_server_stream(benchmark::BenchmarkService::Stub& stub) {
  benchmark::StreamRequest request;
  request.set_message_count(4);
  request.set_payload(make_payload(257, 0));
  request.set_response_bytes(513);
  grpc::ClientContext context;
  std::unique_ptr<grpc::ClientReader<benchmark::BenchmarkResponse>> stream =
      stub.ServerStream(&context, request);
  benchmark::BenchmarkResponse response;
  std::uint64_t count = 0;
  while (stream->Read(&response)) {
    require(response.sequence() == count, "server-stream response sequence mismatch");
    require(valid_payload(response.payload(), 513, count),
            "server-stream response payload mismatch");
    ++count;
  }
  require(stream->Finish().ok(), "server-stream status was not OK");
  require(count == 4, "server-stream response count mismatch");

  request.set_message_count(0);
  grpc::ClientContext invalid_context;
  stream = stub.ServerStream(&invalid_context, request);
  require(!stream->Read(&response), "invalid server stream returned a response");
  require_status(stream->Finish(), grpc::StatusCode::INVALID_ARGUMENT,
                 "stream response settings exceed the benchmark peer limits");
}

void test_bidi(benchmark::BenchmarkService::Stub& stub) {
  grpc::ClientContext context;
  std::unique_ptr<
      grpc::ClientReaderWriter<benchmark::BenchmarkRequest, benchmark::BenchmarkResponse>>
      stream = stub.Bidi(&context);
  std::string write_error;
  std::thread writer([&] {
    for (std::uint64_t sequence = 0; sequence < 32; ++sequence) {
      benchmark::BenchmarkRequest request;
      request.set_sequence(sequence);
      request.set_payload(make_payload(257, sequence));
      request.set_response_bytes(513);
      if (!stream->Write(request)) {
        write_error = "bidi request write failed";
        return;
      }
    }
    if (!stream->WritesDone()) {
      write_error = "bidi half-close failed";
    }
  });

  benchmark::BenchmarkResponse response;
  std::uint64_t count = 0;
  std::string read_error;
  while (stream->Read(&response)) {
    if (response.sequence() != count) {
      read_error = "bidi response sequence mismatch";
      context.TryCancel();
      break;
    }
    if (!valid_payload(response.payload(), 513, count)) {
      read_error = "bidi response payload mismatch";
      context.TryCancel();
      break;
    }
    ++count;
  }
  writer.join();
  require(read_error.empty(), "bidi reader failed");
  require(write_error.empty(), "bidi writer failed");
  require(stream->Finish().ok(), "bidi status was not OK");
  require(count == 32, "bidi response count mismatch");

  grpc::ClientContext invalid_context;
  stream = stub.Bidi(&invalid_context);
  benchmark::BenchmarkRequest invalid_request;
  invalid_request.set_response_bytes(static_cast<std::uint32_t>(kMaximumPayloadBytes + 1));
  require(stream->Write(invalid_request), "invalid bidi request write failed unexpectedly");
  static_cast<void>(stream->WritesDone());
  require(!stream->Read(&response), "invalid bidi stream returned a response");
  require_status(stream->Finish(), grpc::StatusCode::INVALID_ARGUMENT,
                 "response_bytes exceeds the benchmark peer limit");
}

} // namespace

int main() {
  ServerConfig server_config;
  server_config.stack = Stack::GrpcHttp2;
  server_config.endpoint = Endpoint{"127.0.0.1", 0};
  server_config.certificate = TREVRPC_CPP_BENCH_TEST_CERT;
  server_config.private_key = TREVRPC_CPP_BENCH_TEST_KEY;
  std::unique_ptr<BenchmarkServer> server = start_grpc_server(server_config);
  require(server->port() != 0, "gRPC port 0 was not resolved");

  ClientConfig client_config;
  client_config.stack = Stack::GrpcHttp2;
  client_config.endpoint = Endpoint{"127.0.0.1", server->port()};
  client_config.certificate = TREVRPC_CPP_BENCH_TEST_CERT;
  client_config.concurrency = 1;
  client_config.measurement_ms = 1;
  client_config.request_bytes = 257;
  client_config.response_bytes = 513;
  client_config.messages_per_stream = 4;
  std::shared_ptr<ClientFactory> factory = connect_grpc_client(client_config);
  test_callback_client(factory, client_config);
  test_concurrent_bidi(factory, client_config);

  grpc::SslCredentialsOptions tls;
  tls.pem_root_certs = read_file(TREVRPC_CPP_BENCH_TEST_CERT);
  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
      client_config.endpoint.address(client_config.endpoint.port), grpc::SslCredentials(tls));
  require(channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(5)),
          "secure gRPC channel did not connect");
  std::unique_ptr<benchmark::BenchmarkService::Stub> stub =
      benchmark::BenchmarkService::NewStub(channel);
  test_unary(*stub);
  test_client_stream(*stub);
  test_server_stream(*stub);
  test_bidi(*stub);

  std::shared_ptr<grpc::Channel> insecure_channel =
      grpc::CreateChannel(client_config.endpoint.address(client_config.endpoint.port),
                          grpc::InsecureChannelCredentials());
  std::unique_ptr<benchmark::BenchmarkService::Stub> insecure_stub =
      benchmark::BenchmarkService::NewStub(insecure_channel);
  benchmark::BenchmarkRequest insecure_request;
  benchmark::BenchmarkResponse insecure_response;
  grpc::ClientContext insecure_context;
  insecure_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
  require(!insecure_stub->Unary(&insecure_context, insecure_request, &insecure_response).ok(),
          "TLS server accepted an insecure RPC");

  stub.reset();
  channel.reset();
  factory->close();
  server->shutdown();
  server->shutdown();
  require(server->stopped(), "gRPC server did not stop");
  require(!server->finish_error().has_value(), "gRPC server shutdown failed");
}
