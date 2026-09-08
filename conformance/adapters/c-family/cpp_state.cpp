#include "cpp_rpc_harness.hpp"
#include "peer.h"

#include "state_payload.pb.h"

#include <trevrpc/trevrpc.hpp>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using StatePayload = trevrpc::conformance::c_family::StatePayload;

using ServerCall = trevrpc::ServerStreamingCall<StatePayload>;
using ClientCall = trevrpc::ClientStreamingCall<StatePayload, StatePayload>;

void classify_error(const trevrpc::Error &native,
                    const cf::RpcStreamHarness *harness, cf_error *error) {
  switch (native.kind()) {
  case trevrpc::Error::Kind::Rpc:
    cf_error_set(error, "remote_status",
                 static_cast<std::uint32_t>(native.status()->code()));
    return;
  case trevrpc::Error::Kind::Protobuf:
    if (native.message() == "stream ended without a terminal status" ||
        native.message() == "response stream ended before terminal status") {
      cf_error_set(error, "missing_terminal_status",
                   TREVRPC_RPC_STATUS_INTERNAL);
    } else if (native.message() == "response stream contained trailing data "
                                   "after terminal status") {
      cf_error_set(error, "trailing_frame", TREVRPC_RPC_STATUS_INTERNAL);
    } else if (native.message() == "client-streaming RPC did not return "
                                   "exactly one response message") {
      cf_error_set(error, "response_cardinality", TREVRPC_RPC_STATUS_INTERNAL);
    } else {
      cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
    }
    return;
  case trevrpc::Error::Kind::Runtime:
    break;
  }

  if (harness != nullptr && harness->terminal_status_seen()) {
    cf_error_set(error, "trailing_frame", TREVRPC_RPC_STATUS_INTERNAL);
    return;
  }
  const auto diagnostic = harness == nullptr ? TREVRPC_WIRE_DIAGNOSTIC_NONE
                                             : harness->receive_diagnostic();
  if (native.code() == CF_WIRE_ERR_FRAME_TOO_LARGE ||
      native.code() == -EMSGSIZE) {
    cf_error_set(error, "frame_too_large",
                 TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED);
  } else if (diagnostic == TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND) {
    cf_error_set(error, "unsupported_frame_kind",
                 TREVRPC_RPC_STATUS_INVALID_ARGUMENT);
  } else if (diagnostic == TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA) {
    cf_error_set(error, "invalid_metadata", TREVRPC_RPC_STATUS_INTERNAL);
  } else {
    cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
  }
}

std::optional<std::vector<std::byte>>
serialize_payload(const StatePayload &payload, cf_error *error) {
  StatePayload canonical = payload;
  canonical.DiscardUnknownFields();
  auto encoded = trevrpc::detail::serialize(canonical);
  if (!encoded) {
    classify_error(encoded.error(), nullptr, error);
    return std::nullopt;
  }
  return std::move(encoded).value();
}

void append_close_count(cf_json *payload, std::size_t close_count) {
  cf_json_append(payload, ",\"transport_close_count\":");
  cf_json_append_size_string(payload, close_count);
}

[[nodiscard]] trevrpc::Result<trevrpc::detail::ClientStream>
start_scripted_stream(cf::RpcStreamHarness &harness, const cf_command *command,
                      std::uint32_t kind) {
  auto stream = harness.start(kind);
  if (!stream) {
    return stream.error();
  }
  for (std::size_t i = 0; i < command->frame_count; ++i) {
    const int native_error = harness.push_frame(
        std::span(command->frames[i].data, command->frames[i].len));
    if (native_error != 0) {
      return trevrpc::Error::runtime(native_error);
    }
  }
  const int native_error = harness.finish_response();
  if (native_error != 0) {
    return trevrpc::Error::runtime(native_error);
  }
  return std::move(stream).value();
}

int run_server(const cf_command *command, cf_json *payload, cf_error *error) {
  cf::RpcStreamHarness harness;
  auto stream = start_scripted_stream(harness, command,
                                      TREVRPC_RPC_KIND_SERVER_STREAMING);
  if (!stream) {
    classify_error(stream.error(), &harness, error);
    return -1;
  }
  ServerCall call{std::move(stream).value()};

  std::vector<std::vector<std::byte>> messages;
  int result = -1;
  for (;;) {
    auto event = call.receive();
    if (!event) {
      classify_error(event.error(), &harness, error);
      break;
    }
    if (event.value().is_message()) {
      auto encoded = serialize_payload(event.value().message(), error);
      if (!encoded.has_value()) {
        break;
      }
      messages.push_back(std::move(encoded).value());
      continue;
    }

    const trevrpc::Status terminal = event.value().status();
    if (!terminal.is_ok()) {
      classify_error(trevrpc::Error::rpc(terminal), &harness, error);
      break;
    }

    cf_json_append(payload, ",\"events\":[");
    for (std::size_t i = 0; i < messages.size(); ++i) {
      if (i > 0) {
        cf_json_append_char(payload, ',');
      }
      cf_json_append(payload, "{\"event\":\"message\",\"body_hex\":");
      cf_json_append_hex(
          payload, reinterpret_cast<const std::uint8_t *>(messages[i].data()),
          messages[i].size());
      cf_json_append_char(payload, '}');
    }
    if (!messages.empty()) {
      cf_json_append_char(payload, ',');
    }
    cf_json_append(payload, "{\"event\":\"eof\"},{\"event\":\"eof\"}]");
    cf_json_append(payload, ",\"terminal_status\":{\"status_raw\":");
    cf_json_append_u64_string(payload,
                              static_cast<std::uint32_t>(terminal.code()));
    cf_json_append(payload, ",\"status_code\":");
    cf_json_append_u32(payload, static_cast<std::uint32_t>(terminal.code()));
    cf_json_append(payload, ",\"message_hex\":");
    cf_json_append_hex(
        payload,
        reinterpret_cast<const std::uint8_t *>(terminal.message().data()),
        terminal.message().size());
    cf_json_append(payload, ",\"metadata\":[");
    const auto &entries = terminal.metadata().entries();
    for (std::size_t i = 0; i < entries.size(); ++i) {
      if (i > 0) {
        cf_json_append_char(payload, ',');
      }
      cf_json_append(payload, "{\"key_hex\":");
      cf_json_append_hex(
          payload,
          reinterpret_cast<const std::uint8_t *>(entries[i].key.data()),
          entries[i].key.size());
      cf_json_append(payload, ",\"value_hex\":");
      cf_json_append_hex(
          payload,
          reinterpret_cast<const std::uint8_t *>(entries[i].value.data()),
          entries[i].value.size());
      cf_json_append_char(payload, '}');
    }
    cf_json_append(payload, "]}");
    result = payload->failed ? -1 : 0;
    break;
  }

  call.close();
  append_close_count(payload, harness.close_count());
  harness.shutdown();
  return result;
}

int run_client(const cf_command *command, cf_json *payload, cf_error *error) {
  cf::RpcStreamHarness harness;
  auto stream = start_scripted_stream(harness, command,
                                      TREVRPC_RPC_KIND_CLIENT_STREAMING);
  if (!stream) {
    classify_error(stream.error(), &harness, error);
    return -1;
  }
  ClientCall call{std::move(stream).value()};

  std::optional<std::vector<std::byte>> first_response;
  int result = -1;
  for (;;) {
    auto event = call.receive();
    if (!event) {
      classify_error(event.error(), &harness, error);
      break;
    }
    if (event.value().is_message()) {
      auto encoded = serialize_payload(event.value().message(), error);
      if (!encoded.has_value()) {
        break;
      }
      if (!first_response.has_value()) {
        first_response = std::move(encoded).value();
      }
      continue;
    }

    if (!first_response.has_value()) {
      cf_error_set(error, "response_cardinality", TREVRPC_RPC_STATUS_INTERNAL);
      break;
    }
    cf_json_append(payload, ",\"response_body_hex\":");
    cf_json_append_hex(
        payload, reinterpret_cast<const std::uint8_t *>(first_response->data()),
        first_response->size());
    result = payload->failed ? -1 : 0;
    break;
  }

  call.close();
  harness.shutdown();
  return result;
}

} // namespace

extern "C" int cf_cpp_state_dispatch(const cf_command *command,
                                     cf_json *payload, cf_error *error) {
  if (std::strcmp(command->operation, "state.server_stream") == 0) {
    return run_server(command, payload, error);
  }
  return run_client(command, payload, error);
}
