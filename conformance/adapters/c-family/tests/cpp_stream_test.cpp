#include "state_payload.pb.h"
#include "trevrpc_runtime_internal.h"

#include <trevrpc/trevrpc.hpp>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
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
      int source_error = 0)
      : owned_frames_(frames) {
    std::vector<trevrpc_scripted_frame_body> views;
    views.reserve(owned_frames_.size());
    for (const auto &frame : owned_frames_) {
      views.push_back({frame.data(), frame.size()});
    }
    REQUIRE(trevrpc_scripted_stream_new(
                views.data(), views.size(), source_error,
                TREVRPC_DEFAULT_MAX_FRAME_SIZE, &stream_, &source_) == 0);
  }

  ~ScriptedStream() {
    if (stream_ != nullptr) {
      trevrpc_stream_close(stream_);
    }
    trevrpc_scripted_stream_source_free(source_);
  }

  ScriptedStream(const ScriptedStream &) = delete;
  ScriptedStream &operator=(const ScriptedStream &) = delete;

  trevrpc::detail::ClientStream take() {
    return trevrpc::detail::ClientStream(std::exchange(stream_, nullptr));
  }

  [[nodiscard]] std::size_t close_count() const {
    return trevrpc_scripted_stream_close_count(source_);
  }

private:
  std::vector<std::vector<std::uint8_t>> owned_frames_;
  trevrpc_stream *stream_ = nullptr;
  trevrpc_scripted_stream_source *source_ = nullptr;
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
  for (int source_error : {0, -EIO}) {
    ScriptedStream scripted(
        source_error == 0
            ? std::initializer_list<std::vector<std::uint8_t>>{{0x08, 0x01},
                                                               {0x80}}
            : std::initializer_list<std::vector<std::uint8_t>>{{0x08, 0x01}},
        source_error);
    ClientCall call(scripted.take());
    auto result = call.receive();
    REQUIRE(!result);
    REQUIRE(result.error().kind() == trevrpc::Error::Kind::Protobuf);
    REQUIRE(result.error().message() ==
            "response stream contained trailing data after terminal status");
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
          "stream ended without a terminal status");
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
