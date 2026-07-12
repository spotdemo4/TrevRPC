#include "greeter.trevrpc.hpp"

#include <trevrpc/trevrpc.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

using namespace std::chrono_literals;

template <typename Call> bool print_stream(Call& call) {
  for (;;) {
    auto event = call.receive();
    if (!event) {
      std::cerr << event.error().message() << '\n';
      return false;
    }
    if (!event.value().is_message()) {
      if (!event.value().status().is_ok()) {
        std::cerr << event.value().status().message() << '\n';
        return false;
      }
      return true;
    }
    std::cout << event.value().message().message() << '\n';
  }
}

int main(int argc, char** argv) {
  const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
  const auto port = argc > 2 ? static_cast<std::uint16_t>(std::atoi(argv[2])) : 50051;
  const std::string name = argc > 3 ? argv[3] : "TrevRPC";
  trevrpc::ChannelConfig config;
  config.skip_certificate_validation = true;
  auto connected = trevrpc::Channel::connect(host, port, config, 5s);
  if (!connected) {
    std::cerr << "connect failed: " << connected.error().message() << '\n';
    return 1;
  }
  hello::v1::GreeterClient client(std::move(connected).value());

  hello::v1::HelloRequest request;
  request.set_name(name);
  auto unary = client.SayHello(request);
  if (!unary) {
    std::cerr << unary.error().message() << '\n';
    return 1;
  }
  std::cout << unary.value().message.message() << '\n';

  auto replies = client.LotsOfReplies(request);
  if (!replies || !print_stream(replies.value())) {
    return 1;
  }

  auto greetings = client.LotsOfGreetings();
  if (!greetings) {
    return 1;
  }
  for (const char* streamed_name : {"Alice", "Bob"}) {
    hello::v1::HelloRequest streamed;
    streamed.set_name(streamed_name);
    if (!greetings.value().send(streamed)) {
      return 1;
    }
  }
  if (!greetings.value().finish_send() || !print_stream(greetings.value())) {
    return 1;
  }

  auto bidi = client.BidiHello();
  if (!bidi) {
    return 1;
  }
  for (const char* streamed_name : {"Carol", "Dave"}) {
    hello::v1::HelloRequest streamed;
    streamed.set_name(streamed_name);
    if (!bidi.value().send(streamed)) {
      return 1;
    }
  }
  if (!bidi.value().finish_send() || !print_stream(bidi.value())) {
    return 1;
  }
  return 0;
}
