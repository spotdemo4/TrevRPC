#include "cpp_rpc_harness.hpp"
#include "state_payload.pb.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <initializer_list>
#include <utility>
#include <vector>

namespace {

#define REQUIRE(condition)                                                     \
  do {                                                                         \
    if (!(condition)) {                                                        \
      std::abort();                                                            \
    }                                                                          \
  } while (false)

using StatePayload = trevrpc::conformance::c_family::StatePayload;
using ClientCall = trevrpc::ClientStreamingCall<StatePayload, StatePayload>;

class ScriptedStream {
public:
  explicit ScriptedStream(
      std::initializer_list<std::vector<std::uint8_t>> frames,
      bool finish_response = true) {
    auto stream = harness_.start(TREVRPC_RPC_KIND_CLIENT_STREAMING);
    REQUIRE(stream);
    stream_ = std::move(stream).value();
    for (const auto &frame : frames) {
      REQUIRE(harness_.push_frame(frame) == 0);
    }
    if (finish_response) {
      REQUIRE(harness_.finish_response() == 0);
    }
  }

  ScriptedStream(const ScriptedStream &) = delete;
  ScriptedStream &operator=(const ScriptedStream &) = delete;

  trevrpc::detail::ClientStream take() { return std::move(stream_); }

  [[nodiscard]] bool wait_terminal_status_seen() const {
    return harness_.wait_terminal_status_seen();
  }

  [[nodiscard]] bool terminal_status_seen() const noexcept {
    return harness_.terminal_status_seen();
  }

  [[nodiscard]] int fail_receive(int error) {
    return harness_.fail_receive(error);
  }

  [[nodiscard]] std::size_t close_count() const {
    return harness_.close_count();
  }

private:
  cf::RpcStreamHarness harness_;
  trevrpc::detail::ClientStream stream_;
};

template <typename Call>
void close_once(Call &call, const ScriptedStream &scripted) {
  call.close();
  call.close();
  REQUIRE(scripted.close_count() == 1);
}

void test_runtime_error() {
  ScriptedStream scripted({{0x80}});
  ClientCall call(scripted.take());
  auto result = call.receive();
  REQUIRE(!result);
  REQUIRE(result.error().kind() == trevrpc::Error::Kind::Runtime);
  close_once(call, scripted);
}

void test_protobuf_error() {
  ScriptedStream scripted({{0x22, 0x01, 0x1a}, {0x08, 0x01}});
  ClientCall call(scripted.take());
  auto result = call.receive();
  REQUIRE(!result);
  REQUIRE(result.error().kind() == trevrpc::Error::Kind::Protobuf);
  close_once(call, scripted);
}

void test_rpc_error() {
  ScriptedStream scripted({{0x08, 0x01, 0x10, 0x03}});
  ClientCall call(scripted.take());
  auto result = call.receive();
  REQUIRE(!result);
  REQUIRE(result.error().kind() == trevrpc::Error::Kind::Rpc);
  REQUIRE(result.error().status()->code() ==
          trevrpc::StatusCode::InvalidArgument);
  close_once(call, scripted);
}

void test_terminal_trailing_precedence() {
  {
    ScriptedStream scripted({{0x08, 0x01}, {0x80}});
    ClientCall call(scripted.take());
    auto result = call.receive();
    REQUIRE(!result);
    REQUIRE(result.error().kind() == trevrpc::Error::Kind::Runtime);
    REQUIRE(scripted.terminal_status_seen());
    close_once(call, scripted);
  }
  {
    ScriptedStream scripted({{0x08, 0x01}}, false);
    ClientCall call(scripted.take());
    auto receiving =
        std::async(std::launch::async, [&call] { return call.receive(); });
    REQUIRE(scripted.wait_terminal_status_seen());
    REQUIRE(scripted.fail_receive(-EIO) == 0);
    auto result = receiving.get();
    REQUIRE(!result);
    REQUIRE(result.error().kind() == trevrpc::Error::Kind::Runtime);
    REQUIRE(scripted.terminal_status_seen());
    close_once(call, scripted);
  }
}

void test_remote_status_precedes_cardinality() {
  ScriptedStream scripted({{0x22, 0x03, 0x1a, 0x01, 0x61},
                           {0x22, 0x03, 0x1a, 0x01, 0x62},
                           {0x08, 0x01, 0x10, 0x0e}});
  ClientCall call(scripted.take());
  REQUIRE(call.receive());
  REQUIRE(call.receive());
  auto terminal = call.receive();
  REQUIRE(!terminal);
  REQUIRE(terminal.error().kind() == trevrpc::Error::Kind::Rpc);
  REQUIRE(terminal.error().status()->code() ==
          trevrpc::StatusCode::Unavailable);
  close_once(call, scripted);
}

void test_missing_terminal_precedes_cardinality() {
  ScriptedStream scripted(
      {{0x22, 0x03, 0x1a, 0x01, 0x61}, {0x22, 0x03, 0x1a, 0x01, 0x62}});
  ClientCall call(scripted.take());
  REQUIRE(call.receive());
  REQUIRE(call.receive());
  auto terminal = call.receive();
  REQUIRE(!terminal);
  REQUIRE(terminal.error().kind() == trevrpc::Error::Kind::Protobuf);
  REQUIRE(terminal.error().message() ==
          "response stream ended before terminal status");
  close_once(call, scripted);
}

void test_cardinality_after_clean_ok() {
  {
    ScriptedStream scripted({{0x08, 0x01}});
    ClientCall call(scripted.take());
    auto terminal = call.receive();
    REQUIRE(!terminal);
    REQUIRE(terminal.error().kind() == trevrpc::Error::Kind::Protobuf);
    REQUIRE(terminal.error().message() ==
            "client-streaming RPC did not return exactly one response message");
    close_once(call, scripted);
  }
  {
    ScriptedStream scripted({{0x22, 0x03, 0x1a, 0x01, 0x61},
                             {0x22, 0x03, 0x1a, 0x01, 0x62},
                             {0x08, 0x01}});
    ClientCall call(scripted.take());
    REQUIRE(call.receive());
    REQUIRE(call.receive());
    auto terminal = call.receive();
    REQUIRE(!terminal);
    REQUIRE(terminal.error().kind() == trevrpc::Error::Kind::Protobuf);
    REQUIRE(terminal.error().message() ==
            "client-streaming RPC did not return exactly one response message");
    close_once(call, scripted);
  }
  {
    ScriptedStream scripted({{0x22, 0x03, 0x1a, 0x01, 0x61}, {0x08, 0x01}});
    ClientCall call(scripted.take());
    auto message = call.receive();
    REQUIRE(message);
    REQUIRE(message.value().is_message());
    REQUIRE(message.value().message().body() == "a");
    auto terminal = call.receive();
    REQUIRE(terminal);
    REQUIRE(!terminal.value().is_message());
    REQUIRE(terminal.value().status().is_ok());
    close_once(call, scripted);
  }
}

} // namespace

int main() {
  test_runtime_error();
  test_protobuf_error();
  test_rpc_error();
  test_terminal_trailing_precedence();
  test_remote_status_precedes_cardinality();
  test_missing_terminal_precedes_cardinality();
  test_cardinality_after_clean_ok();
  return 0;
}
