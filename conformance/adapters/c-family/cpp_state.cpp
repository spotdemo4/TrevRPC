#include "peer.h"

#include "state_payload.pb.h"
#include "trevrpc_runtime_internal.h"

#include <trevrpc/trevrpc.hpp>

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

struct ScriptedStream {
  trevrpc_stream *stream = nullptr;
  trevrpc_scripted_stream_source *source = nullptr;
  std::vector<trevrpc_scripted_frame_body> frames;

  ~ScriptedStream() {
    if (stream != nullptr) {
      trevrpc_stream_close(stream);
    }
    trevrpc_scripted_stream_source_free(source);
  }

  ScriptedStream(const ScriptedStream &) = delete;
  ScriptedStream &operator=(const ScriptedStream &) = delete;
  ScriptedStream() = default;

  bool initialize(const cf_command *command) {
    frames.reserve(command->frame_count);
    for (std::size_t i = 0; i < command->frame_count; ++i) {
      frames.push_back(trevrpc_scripted_frame_body{command->frames[i].data,
                                                   command->frames[i].len});
    }
    return trevrpc_scripted_stream_new(frames.data(), frames.size(), 0,
                                       TREVRPC_DEFAULT_MAX_FRAME_SIZE, &stream,
                                       &source) == 0;
  }
};

void classify_error(const trevrpc::Error &native, trevrpc_stream *stream,
                    cf_error *error) {
  switch (native.kind()) {
  case trevrpc::Error::Kind::Rpc:
    cf_error_set(error, "remote_status",
                 static_cast<std::uint32_t>(native.status()->code()));
    return;
  case trevrpc::Error::Kind::Protobuf:
    if (native.message() == "stream ended without a terminal status") {
      cf_error_set(error, "missing_terminal_status", TREVRPC_STATUS_INTERNAL);
    } else if (native.message() == "response stream contained trailing data "
                                   "after terminal status") {
      cf_error_set(error, "trailing_frame", TREVRPC_STATUS_INTERNAL);
    } else if (native.message() == "client-streaming RPC did not return "
                                   "exactly one response message") {
      cf_error_set(error, "response_cardinality", TREVRPC_STATUS_INTERNAL);
    } else {
      cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    }
    return;
  case trevrpc::Error::Kind::Runtime:
    break;
  }

  const auto diagnostic = trevrpc_stream_last_wire_diagnostic(stream);
  if (native.code() == TREVRPC_ERR_FRAME_TOO_LARGE) {
    cf_error_set(error, "frame_too_large", TREVRPC_STATUS_RESOURCE_EXHAUSTED);
  } else if (diagnostic == TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND) {
    cf_error_set(error, "unsupported_frame_kind",
                 TREVRPC_STATUS_INVALID_ARGUMENT);
  } else if (diagnostic == TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA) {
    cf_error_set(error, "invalid_metadata", TREVRPC_STATUS_INTERNAL);
  } else {
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
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

int run_server(const cf_command *command, cf_json *payload, cf_error *error) {
  ScriptedStream scripted;
  if (!scripted.initialize(command)) {
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    return -1;
  }
  trevrpc_stream *native_stream = scripted.stream;
  ServerCall call{trevrpc::detail::ClientStream(native_stream)};
  scripted.stream = nullptr;

  std::vector<std::vector<std::byte>> messages;
  int result = -1;
  for (;;) {
    auto event = call.receive();
    if (!event) {
      classify_error(event.error(), native_stream, error);
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
      classify_error(trevrpc::Error::rpc(terminal), native_stream, error);
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
  append_close_count(payload,
                     trevrpc_scripted_stream_close_count(scripted.source));
  return result;
}

int run_client(const cf_command *command, cf_json *payload, cf_error *error) {
  ScriptedStream scripted;
  if (!scripted.initialize(command)) {
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    return -1;
  }
  trevrpc_stream *native_stream = scripted.stream;
  ClientCall call{trevrpc::detail::ClientStream(native_stream)};
  scripted.stream = nullptr;

  std::optional<std::vector<std::byte>> first_response;
  int result = -1;
  for (;;) {
    auto event = call.receive();
    if (!event) {
      classify_error(event.error(), native_stream, error);
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
      cf_error_set(error, "response_cardinality", TREVRPC_STATUS_INTERNAL);
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
