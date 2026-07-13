#include "common.pb.h"
#include "full.pb.h"

#include <trevrpc/trevrpc.hpp>

#include <cassert>
#include <string>

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

  trevrpc::Metadata metadata;
  metadata.set("test-key", "test-value");
  const auto value = metadata.get("test-key");
  assert(value.has_value());
  assert(value->size() == std::string("test-value").size());

  trevrpc::Cancellation cancellation;
  assert(!cancellation.cancelled());
  cancellation.cancel();
  assert(cancellation.cancelled());

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
  return 0;
}
