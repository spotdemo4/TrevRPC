# trevrpc-cpp

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Protobuf

```proto
syntax = "proto3";

package hello.v1;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
  rpc LotsOfReplies(HelloRequest) returns (stream HelloReply);
  rpc LotsOfGreetings(stream HelloRequest) returns (HelloReply);
  rpc BidiHello(stream HelloRequest) returns (stream HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Generate with `protoc --cpp_out` + `protoc --trevrpc-cpp_out`.

## Client

```cpp
#include <trevrpc/trevrpc.hpp>
#include "greeter.trevrpc.hpp"

trevrpc::ChannelConfig cfg;
cfg.skip_certificate_validation = true;
auto ch = trevrpc::Channel::connect("127.0.0.1", 50051, cfg, 5s).value();
hello::v1::GreeterClient client(ch);

hello::v1::HelloRequest req;
req.set_name("TrevRPC");

// Unary
auto reply = client.SayHello(req);
std::cout << reply.value().message.message() << "\n";

// Server streaming
auto stream = client.LotsOfReplies(req).value();
for (;;) {
  auto ev = stream.receive();
  if (!ev || !ev.value().is_message()) break;
  std::cout << ev.value().message().message() << "\n";
}

// Client streaming
auto cs = client.LotsOfGreetings().value();
cs.send(req);
cs.finish_send();
auto summary = cs.receive(); // terminal status or message

// Bidi
auto bidi = client.BidiHello().value();
bidi.send(req);
auto ev = bidi.receive();
bidi.finish_send();
```

## Server

```cpp
class Greeter final : public hello::v1::GreeterService {
 public:
  trevrpc::Result<trevrpc::Response<hello::v1::HelloReply>>
  SayHello(const trevrpc::CallContext&, const hello::v1::HelloRequest& req) override {
    hello::v1::HelloReply r; r.set_message("hello, " + req.name());
    return trevrpc::Response<hello::v1::HelloReply>{std::move(r), {}};
  }
  trevrpc::Status LotsOfReplies(const trevrpc::CallContext&, const hello::v1::HelloRequest& req,
                                trevrpc::ServerWriter<hello::v1::HelloReply>& w) override {
    hello::v1::HelloReply r; r.set_message("hello, " + req.name()); w.send(r);
    r.set_message("goodbye, " + req.name()); w.send(r);
    return trevrpc::Status::ok();
  }
  trevrpc::Result<trevrpc::Response<hello::v1::HelloReply>>
  LotsOfGreetings(const trevrpc::CallContext&, trevrpc::ServerReader<hello::v1::HelloRequest>& r) override {
    std::string names;
    while (auto req = r.receive()) { if (!req.value()) break; names += req.value().value().name() + ", "; }
    hello::v1::HelloReply reply; reply.set_message("hello, " + names);
    return trevrpc::Response<hello::v1::HelloReply>{std::move(reply), {}};
  }
  trevrpc::Status BidiHello(const trevrpc::CallContext&,
                            trevrpc::ServerReaderWriter<hello::v1::HelloRequest, hello::v1::HelloReply>& s) override {
    while (auto req = s.receive()) {
      if (!req.value()) return trevrpc::Status::ok();
      hello::v1::HelloReply r; r.set_message("hello, " + req.value().value().name()); s.send(r);
    }
    return trevrpc::Status::ok();
  }
};

auto srv = trevrpc::Server::listen({.host="127.0.0.1", .port=50051}).value();
hello::v1::RegisterGreeter(srv, std::make_shared<Greeter>());
srv.serve();
```
