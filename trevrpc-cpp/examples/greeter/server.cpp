#include "greeter.trevrpc.hpp"

#include <trevrpc/trevrpc.hpp>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

class Greeter final : public hello::v1::GreeterService {
public:
  trevrpc::Result<hello::v1::HelloReply> SayHello(const trevrpc::CallContext&,
                                                  const hello::v1::HelloRequest& request) override {
    hello::v1::HelloReply reply;
    reply.set_message("Hello, " + request.name());
    return reply;
  }

  trevrpc::Status LotsOfReplies(const trevrpc::CallContext&, const hello::v1::HelloRequest& request,
                                trevrpc::ServerWriter<hello::v1::HelloReply>& writer) override {
    for (const char* prefix : {"Hello, ", "Goodbye, "}) {
      hello::v1::HelloReply reply;
      reply.set_message(prefix + request.name());
      auto sent = writer.send(reply);
      if (!sent) {
        return trevrpc::Status::internal(sent.error().message());
      }
    }
    return trevrpc::Status::ok();
  }

  trevrpc::Result<hello::v1::HelloReply>
  LotsOfGreetings(const trevrpc::CallContext&,
                  trevrpc::ServerReader<hello::v1::HelloRequest>& reader) override {
    std::string names;
    for (;;) {
      auto request = reader.receive();
      if (!request) {
        return request.error();
      }
      if (!request.value()) {
        break;
      }
      if (!names.empty()) {
        names += ", ";
      }
      names += request.value().value().name();
    }
    hello::v1::HelloReply reply;
    reply.set_message("Hello, " + names);
    return reply;
  }

  trevrpc::Status
  BidiHello(const trevrpc::CallContext&,
            trevrpc::ServerReaderWriter<hello::v1::HelloRequest, hello::v1::HelloReply>& stream)
      override {
    for (;;) {
      auto request = stream.receive();
      if (!request) {
        return trevrpc::Status::internal(request.error().message());
      }
      if (!request.value()) {
        return trevrpc::Status::ok();
      }
      hello::v1::HelloReply reply;
      reply.set_message("Hello from bidi, " + request.value().value().name());
      auto sent = stream.send(reply);
      if (!sent) {
        return trevrpc::Status::internal(sent.error().message());
      }
    }
  }
};

int main(int argc, char** argv) {
  trevrpc::ServerConfig config;
  config.host = argc > 1 ? argv[1] : "127.0.0.1";
  config.port = argc > 2 ? static_cast<std::uint16_t>(std::atoi(argv[2])) : 50051;
  if (argc > 3) {
    config.cert_file = argv[3];
  }
  if (argc > 4) {
    config.key_file = argv[4];
  }

  auto listening = trevrpc::Server::listen(config);
  if (!listening) {
    std::cerr << "listen failed: " << listening.error().message() << '\n';
    return 1;
  }
  trevrpc::Server server = std::move(listening).value();
  auto registered = hello::v1::RegisterGreeter(server, std::make_shared<Greeter>());
  if (!registered) {
    std::cerr << "registration failed: " << registered.error().message() << '\n';
    return 1;
  }
  std::cout << "serving on " << config.host << ':' << config.port << '\n';
  auto served = server.serve();
  if (!served) {
    std::cerr << "serve failed: " << served.error().message() << '\n';
    return 1;
  }
  return 0;
}
