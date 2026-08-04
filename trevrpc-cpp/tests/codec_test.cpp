#include "common.pb.h"
#include "full.pb.h"

#include <trevrpc/trevrpc.hpp>

#include <array>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <thread>
#include <type_traits>

namespace {

#define REQUIRE(condition)                                                                         \
  do {                                                                                             \
    if (!(condition)) {                                                                            \
      std::abort();                                                                                \
    }                                                                                              \
  } while (false)

void test_error_categories() {
  const auto runtime = trevrpc::Error::runtime(-EINVAL, "runtime");
  REQUIRE(runtime.kind() == trevrpc::Error::Kind::Runtime);
  REQUIRE(runtime.code() == -EINVAL);
  REQUIRE(!runtime.status().has_value());

  const auto rpc =
      trevrpc::Error::rpc(trevrpc::Status(trevrpc::StatusCode::InvalidArgument, "rpc"));
  REQUIRE(rpc.kind() == trevrpc::Error::Kind::Rpc);
  REQUIRE(rpc.status().has_value());
  REQUIRE(rpc.status()->code() == trevrpc::StatusCode::InvalidArgument);

  const auto protobuf = trevrpc::Error::protobuf("protobuf");
  REQUIRE(protobuf.kind() == trevrpc::Error::Kind::Protobuf);
  REQUIRE(!protobuf.status().has_value());

  trevrpc::Result<int> ok_status_error(trevrpc::Status::ok());
  REQUIRE(!ok_status_error);
  REQUIRE(ok_status_error.error().kind() == trevrpc::Error::Kind::Rpc);
  REQUIRE(ok_status_error.error().status()->code() == trevrpc::StatusCode::Internal);

  trevrpc::Result<int> explicit_ok_error(trevrpc::Error::rpc(trevrpc::Status::ok()));
  REQUIRE(!explicit_ok_error);
  REQUIRE(explicit_ok_error.error().status()->code() == trevrpc::StatusCode::Internal);

  trevrpc::Result<void> void_ok(trevrpc::Status::ok());
  REQUIRE(void_ok);
}

} // namespace

int main() {
  trevrpc::cpp::test::common::ImportedReply lite;
  lite.set_message("lite");
  auto lite_body = trevrpc::detail::serialize(lite);
  assert(lite_body);
  auto decoded_lite = trevrpc::detail::parse<trevrpc::cpp::test::common::ImportedReply>(
      lite_body.value(), "lite decode failed");
  assert(decoded_lite);
  assert(decoded_lite.value().message() == "lite");

  trevrpc::cpp::test::common::ImportedReply empty;
  auto empty_body = trevrpc::detail::serialize(empty);
  assert(empty_body);
  assert(empty_body.value().empty());

  trevrpc::cpp::test::full::FullMessage full;
  full.set_value("full");
  auto full_body = trevrpc::detail::serialize(full);
  assert(full_body);
  auto decoded_full = trevrpc::detail::parse<trevrpc::cpp::test::full::FullMessage>(
      full_body.value(), "full decode failed");
  assert(decoded_full);
  assert(decoded_full.value().value() == "full");

  const std::array<std::byte, 2> wrong_known_wire = {std::byte{0x08}, std::byte{0x01}};
  auto wrong_known = trevrpc::detail::parse<trevrpc::cpp::test::full::FullMessage>(
      wrong_known_wire, "full decode failed");
  assert(!wrong_known);

  const std::array<std::byte, 2> valid_unknown = {std::byte{0x10}, std::byte{0x01}};
  auto unknown = trevrpc::detail::parse<trevrpc::cpp::test::full::FullMessage>(
      valid_unknown, "full decode failed");
  assert(unknown);

  trevrpc::Metadata metadata;
  metadata.set("test-key", "test-value");
  const auto value = metadata.get("test-key");
  assert(value.has_value());
  assert(value->size() == std::string("test-value").size());

  static_assert(std::is_copy_constructible_v<trevrpc::Cancellation>);
  static_assert(std::is_copy_assignable_v<trevrpc::Cancellation>);
  trevrpc::Cancellation cancellation;
  trevrpc::Cancellation copy = cancellation;
  trevrpc::Cancellation assigned;
  assigned = copy;
  assert(!cancellation.cancelled());
  std::thread cancel_thread([copy]() mutable { copy.cancel(); });
  cancel_thread.join();
  assert(cancellation.cancelled());
  assert(assigned.cancelled());

  trevrpc::ChannelConfig invalid_channel_config;
  invalid_channel_config.max_idle_timeout = std::chrono::milliseconds(-1);
  auto invalid_channel = trevrpc::Channel::connect("127.0.0.1", 1, invalid_channel_config);
  assert(!invalid_channel);
  invalid_channel_config.max_idle_timeout = std::chrono::milliseconds(0);
  invalid_channel_config.keep_alive =
      std::chrono::milliseconds(static_cast<std::uint64_t>(UINT32_MAX) + 1);
  invalid_channel = trevrpc::Channel::connect("127.0.0.1", 1, invalid_channel_config);
  assert(!invalid_channel);

  trevrpc::ServerConfig invalid_server_config;
  invalid_server_config.max_idle_timeout = std::chrono::milliseconds(-1);
  auto invalid_server = trevrpc::Server::listen(invalid_server_config);
  assert(!invalid_server);
  invalid_server_config.max_idle_timeout = std::chrono::milliseconds(0);
  invalid_server_config.keep_alive =
      std::chrono::milliseconds(static_cast<std::uint64_t>(UINT32_MAX) + 1);
  invalid_server = trevrpc::Server::listen(invalid_server_config);
  assert(!invalid_server);

  test_error_categories();
  return 0;
}
