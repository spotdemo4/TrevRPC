#include "benchmark_peer.hpp"

#include "benchmark.grpc.pb.h"

#include <grpc/compression.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/support/server_callback.h>

#include <condition_variable>
#include <fstream>
#include <limits>
#include <mutex>
#include <queue>
#include <utility>

namespace benchmark = trevrpc::benchmark::v1;

namespace trevrpc_bench {
namespace {

constexpr std::size_t kMaximumQueuedBidiResponses = 1;

[[nodiscard]] std::string read_pem(const std::string& path, std::string_view description) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw PeerError("setup", "tls_file_read_failed",
                    "failed to open " + std::string(description) + ": " + path);
  }
  std::string contents((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
  if (contents.empty()) {
    throw PeerError("setup", "tls_file_read_failed",
                    std::string(description) + " is empty: " + path);
  }
  return contents;
}

[[nodiscard]] grpc::Status invalid_argument(std::string message) {
  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, std::move(message));
}

[[nodiscard]] grpc::Status resource_exhausted(std::string message) {
  return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, std::move(message));
}

[[nodiscard]] std::string describe(const grpc::Status& status) {
  return "gRPC status " + std::to_string(static_cast<int>(status.error_code())) + ": " +
         status.error_message();
}

class GrpcBenchmarkService final : public benchmark::BenchmarkService::CallbackService {
public:
  grpc::ServerUnaryReactor* Unary(grpc::CallbackServerContext* context,
                                  const benchmark::BenchmarkRequest* request,
                                  benchmark::BenchmarkResponse* response) override {
    auto* reactor = context->DefaultReactor();
    if (request->payload().size() > kMaximumPayloadBytes) {
      reactor->Finish(invalid_argument("request payload exceeds the benchmark peer limit"));
      return reactor;
    }
    if (request->response_bytes() > kMaximumPayloadBytes) {
      reactor->Finish(invalid_argument("response_bytes exceeds the benchmark peer limit"));
      return reactor;
    }
    response->set_sequence(request->sequence());
    response->set_payload(make_payload(request->response_bytes(), request->sequence()));
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  grpc::ServerReadReactor<benchmark::BenchmarkRequest>*
  ClientStream(grpc::CallbackServerContext*, benchmark::BenchmarkSummary* summary) override {
    class Reactor final : public grpc::ServerReadReactor<benchmark::BenchmarkRequest> {
    public:
      explicit Reactor(benchmark::BenchmarkSummary* summary) : summary_(summary) {
        StartRead(&request_);
      }

      void OnReadDone(bool ok) override {
        if (!ok) {
          summary_->set_message_count(message_count_);
          summary_->set_payload_bytes(payload_bytes_);
          Finish(grpc::Status::OK);
          return;
        }
        if (message_count_ >= kMaximumMessagesPerStream) {
          Finish(resource_exhausted("message count exceeds the benchmark peer limit"));
          return;
        }
        const std::size_t size = request_.payload().size();
        if (size > kMaximumPayloadBytes) {
          Finish(invalid_argument("request payload exceeds the benchmark peer limit"));
          return;
        }
        if (size > std::numeric_limits<std::uint64_t>::max() - payload_bytes_) {
          Finish(grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "stream summary overflow"));
          return;
        }
        ++message_count_;
        payload_bytes_ += static_cast<std::uint64_t>(size);
        StartRead(&request_);
      }

      void OnDone() override { delete this; }

    private:
      benchmark::BenchmarkSummary* summary_;
      benchmark::BenchmarkRequest request_;
      std::uint64_t message_count_ = 0;
      std::uint64_t payload_bytes_ = 0;
    };
    return new Reactor(summary);
  }

  grpc::ServerWriteReactor<benchmark::BenchmarkResponse>*
  ServerStream(grpc::CallbackServerContext*, const benchmark::StreamRequest* request) override {
    class Reactor final : public grpc::ServerWriteReactor<benchmark::BenchmarkResponse> {
    public:
      explicit Reactor(const benchmark::StreamRequest& request)
          : message_count_(request.message_count()), response_bytes_(request.response_bytes()) {
        if (request.payload().size() > kMaximumPayloadBytes || message_count_ == 0 ||
            message_count_ > kMaximumMessagesPerStream || response_bytes_ > kMaximumPayloadBytes) {
          Finish(invalid_argument("stream response settings exceed the benchmark peer limits"));
          return;
        }
        write_next();
      }

      void OnWriteDone(bool ok) override {
        if (!ok) {
          Finish(grpc::Status(grpc::StatusCode::CANCELLED, "stream write failed"));
          return;
        }
        write_next();
      }

      void OnDone() override { delete this; }

    private:
      void write_next() {
        if (sequence_ == message_count_) {
          Finish(grpc::Status::OK);
          return;
        }
        response_.set_sequence(sequence_);
        response_.set_payload(make_payload(response_bytes_, sequence_));
        ++sequence_;
        StartWrite(&response_);
      }

      const std::uint32_t message_count_;
      const std::uint32_t response_bytes_;
      std::uint32_t sequence_ = 0;
      benchmark::BenchmarkResponse response_;
    };
    return new Reactor(*request);
  }

  grpc::ServerBidiReactor<benchmark::BenchmarkRequest, benchmark::BenchmarkResponse>*
  Bidi(grpc::CallbackServerContext*) override {
    class Reactor final : public grpc::ServerBidiReactor<benchmark::BenchmarkRequest,
                                                         benchmark::BenchmarkResponse> {
    public:
      Reactor() {
        read_in_flight_ = true;
        StartRead(&request_);
      }

      void OnReadDone(bool ok) override {
        bool start_write = false;
        bool finish = false;
        grpc::Status finish_status = grpc::Status::OK;
        bool start_read = false;
        {
          std::lock_guard lock(mutex_);
          if (finishing_) {
            return;
          }
          read_in_flight_ = false;
          if (!ok) {
            reads_done_ = true;
            if (!write_in_flight_ && responses_.empty()) {
              finishing_ = true;
              finish = true;
              finish_status = terminal_status_;
            }
          } else if (message_count_ >= kMaximumMessagesPerStream) {
            reads_done_ = true;
            terminal_status_ = resource_exhausted("message count exceeds the benchmark peer limit");
            if (!write_in_flight_ && responses_.empty()) {
              finishing_ = true;
              finish = true;
              finish_status = terminal_status_;
            }
          } else if (request_.response_bytes() > kMaximumPayloadBytes) {
            reads_done_ = true;
            terminal_status_ = invalid_argument("response_bytes exceeds the benchmark peer limit");
            if (!write_in_flight_ && responses_.empty()) {
              finishing_ = true;
              finish = true;
              finish_status = terminal_status_;
            }
          } else if (request_.payload().size() > kMaximumPayloadBytes) {
            reads_done_ = true;
            terminal_status_ = invalid_argument("request payload exceeds the benchmark peer limit");
            if (!write_in_flight_ && responses_.empty()) {
              finishing_ = true;
              finish = true;
              finish_status = terminal_status_;
            }
          } else {
            ++message_count_;
            benchmark::BenchmarkResponse response;
            response.set_sequence(request_.sequence());
            response.set_payload(make_payload(request_.response_bytes(), request_.sequence()));
            responses_.push(std::move(response));
            if (!write_in_flight_) {
              prepare_write();
              start_write = true;
            }
            if (responses_.size() < kMaximumQueuedBidiResponses) {
              read_in_flight_ = true;
              start_read = true;
            }
          }
        }
        if (start_read) {
          StartRead(&request_);
        }
        if (start_write) {
          StartWrite(&response_);
        }
        if (finish) {
          Finish(finish_status);
        }
      }

      void OnWriteDone(bool ok) override {
        bool start_write = false;
        bool start_read = false;
        bool finish = false;
        grpc::Status finish_status = grpc::Status::OK;
        {
          std::lock_guard lock(mutex_);
          if (finishing_) {
            return;
          }
          write_in_flight_ = false;
          if (ok && !responses_.empty()) {
            prepare_write();
            start_write = true;
          } else if (!ok || reads_done_) {
            finishing_ = true;
            finish = true;
            finish_status = ok ? terminal_status_
                               : grpc::Status(grpc::StatusCode::CANCELLED, "stream write failed");
          }
          if (!finishing_ && !reads_done_ && !read_in_flight_ &&
              responses_.size() < kMaximumQueuedBidiResponses) {
            read_in_flight_ = true;
            start_read = true;
          }
        }
        if (start_read) {
          StartRead(&request_);
        }
        if (start_write) {
          StartWrite(&response_);
        }
        if (finish) {
          Finish(finish_status);
        }
      }

      void OnDone() override { delete this; }

    private:
      void prepare_write() {
        response_ = std::move(responses_.front());
        responses_.pop();
        write_in_flight_ = true;
      }

      std::mutex mutex_;
      benchmark::BenchmarkRequest request_;
      benchmark::BenchmarkResponse response_;
      std::queue<benchmark::BenchmarkResponse> responses_;
      std::uint32_t message_count_ = 0;
      bool write_in_flight_ = false;
      bool read_in_flight_ = false;
      bool reads_done_ = false;
      bool finishing_ = false;
      grpc::Status terminal_status_;
    };
    return new Reactor;
  }
};

class CompletionState {
public:
  [[nodiscard]] grpc::Status await() {
    std::unique_lock lock(mutex_);
    condition_.wait(lock, [this] { return done_; });
    return std::move(status_);
  }

protected:
  void complete(const grpc::Status& status) {
    std::lock_guard lock(mutex_);
    status_ = status;
    done_ = true;
    condition_.notify_one();
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  grpc::Status status_;
  bool done_ = false;
};

class UnaryCall final : public grpc::ClientUnaryReactor, public CompletionState {
public:
  UnaryCall(benchmark::BenchmarkService::Stub* stub, const ClientConfig& config,
            std::uint64_t sequence)
      : expected_sequence_(sequence), expected_response_bytes_(config.response_bytes) {
    context_.set_compression_algorithm(GRPC_COMPRESS_NONE);
    request_.set_sequence(sequence);
    request_.set_payload(make_payload(config.request_bytes, sequence));
    request_.set_response_bytes(config.response_bytes);
    stub->async()->Unary(&context_, &request_, &response_, this);
    StartCall();
  }

  void OnDone(const grpc::Status& status) override { complete(status); }

  [[nodiscard]] std::optional<std::string> result() {
    const grpc::Status status = await();
    if (!status.ok()) {
      return describe(status);
    }
    if (response_.sequence() != expected_sequence_ ||
        !valid_payload(response_.payload(), expected_response_bytes_, expected_sequence_)) {
      return "unary response validation failed";
    }
    return std::nullopt;
  }

private:
  grpc::ClientContext context_;
  benchmark::BenchmarkRequest request_;
  benchmark::BenchmarkResponse response_;
  const std::uint64_t expected_sequence_;
  const std::uint32_t expected_response_bytes_;
};

class ClientStreamCall final : public grpc::ClientWriteReactor<benchmark::BenchmarkRequest>,
                               public CompletionState {
public:
  ClientStreamCall(benchmark::BenchmarkService::Stub* stub, const ClientConfig& config)
      : config_(config) {
    context_.set_compression_algorithm(GRPC_COMPRESS_NONE);
    stub->async()->ClientStream(&context_, &summary_, this);
    prepare_request();
    StartWrite(&request_);
    StartCall();
  }

  void OnWriteDone(bool ok) override {
    if (!ok) {
      return;
    }
    ++writes_completed_;
    if (writes_completed_ == config_.messages_per_stream) {
      StartWritesDone();
      return;
    }
    prepare_request();
    StartWrite(&request_);
  }

  void OnDone(const grpc::Status& status) override { complete(status); }

  [[nodiscard]] std::optional<std::string> result() {
    const grpc::Status status = await();
    if (!status.ok()) {
      return describe(status);
    }
    const std::uint64_t expected_bytes =
        static_cast<std::uint64_t>(config_.request_bytes) * config_.messages_per_stream;
    if (writes_completed_ != config_.messages_per_stream ||
        summary_.message_count() != config_.messages_per_stream ||
        summary_.payload_bytes() != expected_bytes) {
      return "client-stream summary validation failed";
    }
    return std::nullopt;
  }

private:
  void prepare_request() {
    request_.set_sequence(writes_completed_);
    request_.set_payload(make_payload(config_.request_bytes, writes_completed_));
    request_.set_response_bytes(config_.response_bytes);
  }

  const ClientConfig& config_;
  grpc::ClientContext context_;
  benchmark::BenchmarkRequest request_;
  benchmark::BenchmarkSummary summary_;
  std::uint32_t writes_completed_ = 0;
};

class ServerStreamCall final : public grpc::ClientReadReactor<benchmark::BenchmarkResponse>,
                               public CompletionState {
public:
  ServerStreamCall(benchmark::BenchmarkService::Stub* stub, const ClientConfig& config)
      : config_(config) {
    context_.set_compression_algorithm(GRPC_COMPRESS_NONE);
    request_.set_message_count(config.messages_per_stream);
    request_.set_payload(make_payload(config.request_bytes, 0));
    request_.set_response_bytes(config.response_bytes);
    stub->async()->ServerStream(&context_, &request_, this);
    StartRead(&response_);
    StartCall();
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      read_closed_ = true;
      return;
    }
    if (response_.sequence() != reads_completed_ ||
        !valid_payload(response_.payload(), config_.response_bytes, reads_completed_)) {
      validation_error_ = "server-stream response validation failed";
      context_.TryCancel();
      return;
    }
    ++reads_completed_;
    StartRead(&response_);
  }

  void OnDone(const grpc::Status& status) override { complete(status); }

  [[nodiscard]] std::optional<std::string> result() {
    const grpc::Status status = await();
    if (!validation_error_.empty()) {
      return validation_error_;
    }
    if (!status.ok()) {
      return describe(status);
    }
    if (!read_closed_ || reads_completed_ != config_.messages_per_stream) {
      return "server stream ended before all responses arrived";
    }
    return std::nullopt;
  }

private:
  const ClientConfig& config_;
  grpc::ClientContext context_;
  benchmark::StreamRequest request_;
  benchmark::BenchmarkResponse response_;
  std::string validation_error_;
  std::uint32_t reads_completed_ = 0;
  bool read_closed_ = false;
};

class BidiCall final
    : public grpc::ClientBidiReactor<benchmark::BenchmarkRequest, benchmark::BenchmarkResponse>,
      public CompletionState {
public:
  BidiCall(benchmark::BenchmarkService::Stub* stub, const ClientConfig& config) : config_(config) {
    context_.set_compression_algorithm(GRPC_COMPRESS_NONE);
    stub->async()->Bidi(&context_, this);
    prepare_request();
    StartWrite(&request_);
    StartRead(&response_);
    StartCall();
  }

  void OnWriteDone(bool ok) override {
    if (!ok) {
      set_validation_error("bidi request write failed");
      return;
    }
    ++writes_completed_;
    if (writes_completed_ == config_.messages_per_stream) {
      StartWritesDone();
      return;
    }
    prepare_request();
    StartWrite(&request_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      read_closed_ = true;
      return;
    }
    if (response_.sequence() != reads_completed_ ||
        !valid_payload(response_.payload(), config_.response_bytes, reads_completed_)) {
      set_validation_error("bidi response validation failed");
      return;
    }
    ++reads_completed_;
    StartRead(&response_);
  }

  void OnDone(const grpc::Status& status) override { complete(status); }

  [[nodiscard]] std::optional<std::string> result() {
    const grpc::Status status = await();
    {
      std::lock_guard lock(error_mutex_);
      if (!validation_error_.empty()) {
        return validation_error_;
      }
    }
    if (!status.ok()) {
      return describe(status);
    }
    if (!read_closed_ || writes_completed_ != config_.messages_per_stream ||
        reads_completed_ != config_.messages_per_stream) {
      return "bidi stream ended before all messages completed";
    }
    return std::nullopt;
  }

private:
  void prepare_request() {
    request_.set_sequence(writes_completed_);
    request_.set_payload(make_payload(config_.request_bytes, writes_completed_));
    request_.set_response_bytes(config_.response_bytes);
  }

  void set_validation_error(std::string error) {
    {
      std::lock_guard lock(error_mutex_);
      if (validation_error_.empty()) {
        validation_error_ = std::move(error);
      }
    }
    context_.TryCancel();
  }

  const ClientConfig& config_;
  grpc::ClientContext context_;
  benchmark::BenchmarkRequest request_;
  benchmark::BenchmarkResponse response_;
  std::mutex error_mutex_;
  std::string validation_error_;
  std::uint32_t writes_completed_ = 0;
  std::uint32_t reads_completed_ = 0;
  bool read_closed_ = false;
};

class GrpcBenchmarkClient final : public BenchmarkClient {
public:
  explicit GrpcBenchmarkClient(const std::shared_ptr<grpc::Channel>& channel)
      : stub_(benchmark::BenchmarkService::NewStub(channel)) {}

  std::optional<std::string> run(const ClientConfig& config, std::uint64_t sequence) override {
    switch (config.rpc_kind) {
    case RpcKind::Unary: {
      UnaryCall call(stub_.get(), config, sequence);
      return call.result();
    }
    case RpcKind::ClientStream: {
      ClientStreamCall call(stub_.get(), config);
      return call.result();
    }
    case RpcKind::ServerStream: {
      ServerStreamCall call(stub_.get(), config);
      return call.result();
    }
    case RpcKind::Bidi: {
      BidiCall call(stub_.get(), config);
      return call.result();
    }
    }
    return "unknown RPC kind";
  }

private:
  std::unique_ptr<benchmark::BenchmarkService::Stub> stub_;
};

class GrpcClientFactory final : public ClientFactory {
public:
  explicit GrpcClientFactory(std::shared_ptr<grpc::Channel> channel)
      : channel_(std::move(channel)) {}

  ~GrpcClientFactory() override { close(); }

  std::unique_ptr<BenchmarkClient> create() override {
    std::lock_guard lock(mutex_);
    if (channel_ == nullptr) {
      throw std::runtime_error("gRPC channel is closed");
    }
    return std::make_unique<GrpcBenchmarkClient>(channel_);
  }

  void close() override {
    std::lock_guard lock(mutex_);
    channel_.reset();
  }

private:
  std::mutex mutex_;
  std::shared_ptr<grpc::Channel> channel_;
};

class GrpcBenchmarkServer final : public BenchmarkServer {
public:
  GrpcBenchmarkServer(std::unique_ptr<grpc::Server> server, std::uint16_t port)
      : server_(std::move(server)), port_(port) {}

  ~GrpcBenchmarkServer() override { shutdown(); }

  std::uint16_t port() const override { return port_; }

  bool stopped() const override { return stopped_; }

  void shutdown() override {
    if (server_ == nullptr || stopped_) {
      return;
    }
    server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
    server_->Wait();
    stopped_ = true;
  }

  std::optional<std::string> finish_error() override { return std::nullopt; }

private:
  std::unique_ptr<grpc::Server> server_;
  const std::uint16_t port_;
  bool stopped_ = false;
};

} // namespace

std::unique_ptr<BenchmarkServer> start_grpc_server(const ServerConfig& config) {
  grpc::SslServerCredentialsOptions tls;
  tls.pem_key_cert_pairs.push_back(
      {read_pem(config.private_key, "private key"), read_pem(config.certificate, "certificate")});

  grpc::ServerBuilder builder;
  builder.SetMaxReceiveMessageSize(static_cast<int>(kMaximumFrameSize));
  builder.SetMaxSendMessageSize(static_cast<int>(kMaximumFrameSize));
  builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_NONE);
  int selected_port = 0;
  builder.AddListeningPort(config.endpoint.address(config.endpoint.port),
                           grpc::SslServerCredentials(tls), &selected_port);
  auto service = std::make_unique<GrpcBenchmarkService>();
  builder.RegisterService(service.get());
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (server == nullptr || selected_port <= 0 ||
      selected_port > std::numeric_limits<std::uint16_t>::max()) {
    throw PeerError("setup", "listen_failed", "gRPC failed to start the benchmark server");
  }

  class OwningServer final : public BenchmarkServer {
  public:
    OwningServer(std::unique_ptr<GrpcBenchmarkService> service,
                 std::unique_ptr<BenchmarkServer> server)
        : service_(std::move(service)), server_(std::move(server)) {}

    std::uint16_t port() const override { return server_->port(); }
    bool stopped() const override { return server_->stopped(); }
    void shutdown() override { server_->shutdown(); }
    std::optional<std::string> finish_error() override { return server_->finish_error(); }

  private:
    std::unique_ptr<GrpcBenchmarkService> service_;
    std::unique_ptr<BenchmarkServer> server_;
  };

  return std::make_unique<OwningServer>(
      std::move(service), std::make_unique<GrpcBenchmarkServer>(std::move(server), selected_port));
}

std::shared_ptr<ClientFactory> connect_grpc_client(const ClientConfig& config) {
  grpc::SslCredentialsOptions tls;
  tls.pem_root_certs = read_pem(config.certificate, "CA certificate");
  grpc::ChannelArguments arguments;
  arguments.SetCompressionAlgorithm(GRPC_COMPRESS_NONE);
  arguments.SetMaxReceiveMessageSize(static_cast<int>(kMaximumFrameSize));
  arguments.SetMaxSendMessageSize(static_cast<int>(kMaximumFrameSize));
  std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
      config.endpoint.address(config.endpoint.port), grpc::SslCredentials(tls), arguments);
  if (!channel->WaitForConnected(std::chrono::system_clock::now() + kConnectTimeout)) {
    throw PeerError("setup", "connect_failed", "timed out connecting to the gRPC server");
  }
  return std::make_shared<GrpcClientFactory>(std::move(channel));
}

} // namespace trevrpc_bench
