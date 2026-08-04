#include "abi6_bridge.hpp"

#include <cerrno>
#include <cstring>

namespace trevrpc::detail {
namespace {

template <typename T, std::size_t (*Count)(const T*),
          int (*At)(const T*, std::size_t, trevrpc_bytes_view*, trevrpc_bytes_view*)>
[[nodiscard]] Result<Metadata> copy_inbound_metadata(const T* value) {
  Metadata result;
  for (std::size_t index = 0; index < Count(value); ++index) {
    trevrpc_bytes_view key{};
    trevrpc_bytes_view entry_value{};
    const int error = At(value, index, &key, &entry_value);
    if (error != 0) {
      return Error::runtime(error);
    }
    if ((key.data == nullptr && key.len > 0) ||
        (entry_value.data == nullptr && entry_value.len > 0)) {
      return Error::runtime(-EINVAL, "runtime returned an invalid metadata view");
    }
    const char* key_data = key.data == nullptr ? "" : reinterpret_cast<const char*>(key.data);
    const auto bytes =
        entry_value.data == nullptr
            ? std::span<const std::byte>{}
            : std::span(reinterpret_cast<const std::byte*>(entry_value.data), entry_value.len);
    result.set(std::string(key_data, key.len), bytes);
  }
  return result;
}

} // namespace

Result<Metadata> copy_metadata(const trevrpc_inbound_response* response) {
  return copy_inbound_metadata<trevrpc_inbound_response, trevrpc_inbound_response_metadata_count,
                               trevrpc_inbound_response_metadata_at>(response);
}

Result<Metadata> copy_metadata(const trevrpc_inbound_stream_frame* frame) {
  return copy_inbound_metadata<trevrpc_inbound_stream_frame,
                               trevrpc_inbound_stream_frame_metadata_count,
                               trevrpc_inbound_stream_frame_metadata_at>(frame);
}

Metadata copy_metadata(const trevrpc_metadata& metadata) {
  Metadata result;
  for (std::size_t index = 0; index < metadata.entries_len; ++index) {
    const trevrpc_metadata_entry& entry = metadata.entries[index];
    result.set(std::string(entry.key, entry.key_len),
               std::span(reinterpret_cast<const std::byte*>(entry.value), entry.value_len));
  }
  return result;
}

NativeMetadata::~NativeMetadata() { trevrpc_metadata_reset(&metadata_); }

NativeMetadata::NativeMetadata(NativeMetadata&& other) noexcept
    : metadata_(std::exchange(other.metadata_, {})) {}

NativeMetadata& NativeMetadata::operator=(NativeMetadata&& other) noexcept {
  if (this != &other) {
    trevrpc_metadata_reset(&metadata_);
    metadata_ = std::exchange(other.metadata_, {});
  }
  return *this;
}

int NativeMetadata::assign(const Metadata& metadata) {
  for (const Metadata::Entry& entry : metadata.entries()) {
    const int error = trevrpc_metadata_set(
        &metadata_, entry.key.data(), entry.key.size(),
        reinterpret_cast<const std::uint8_t*>(entry.value.data()), entry.value.size());
    if (error != 0) {
      return error;
    }
  }
  return 0;
}

NativeCallOptions::NativeCallOptions(NativeCallOptions&& other) noexcept
    : options(other.options), metadata(std::move(other.metadata)),
      retained_cancellation(std::move(other.retained_cancellation)) {
  if (options.metadata != nullptr) {
    options.metadata = metadata.get();
  }
  if (retained_cancellation.has_value()) {
    options.cancellation = retained_cancellation->native_handle();
  }
}

NativeCallOptions& NativeCallOptions::operator=(NativeCallOptions&& other) noexcept {
  if (this != &other) {
    options = other.options;
    metadata = std::move(other.metadata);
    retained_cancellation = std::move(other.retained_cancellation);
    if (options.metadata != nullptr) {
      options.metadata = metadata.get();
    }
    if (retained_cancellation.has_value()) {
      options.cancellation = retained_cancellation->native_handle();
    }
  }
  return *this;
}

Result<NativeCallOptions> normalize_call_options(const CallOptions& options,
                                                 bool retain_cancellation) {
  if (options.timeout.count() < 0 || options.response_idle_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "call durations must not be negative");
  }
  NativeCallOptions result;
  int error = trevrpc_call_options_v1_init(&result.options, sizeof(result.options));
  if (error != 0) {
    return Error::runtime(error);
  }
  error = result.metadata.assign(options.metadata);
  if (error != 0) {
    return Error::runtime(error);
  }
  result.options.metadata = options.metadata.empty() ? nullptr : result.metadata.get();
  result.options.timeout_nanos = static_cast<std::uint64_t>(options.timeout.count());
  if (options.cancellation != nullptr && retain_cancellation) {
    result.retained_cancellation.emplace(*options.cancellation);
    result.options.cancellation = result.retained_cancellation->native_handle();
  } else {
    result.options.cancellation =
        options.cancellation == nullptr ? nullptr : options.cancellation->native_handle();
  }
  result.options.max_response_body_size = options.max_response_body_size;
  result.options.max_response_messages = options.max_response_messages;
  result.options.max_response_stream_body_size = options.max_response_stream_body_size;
  result.options.response_idle_timeout_nanos =
      static_cast<std::uint64_t>(options.response_idle_timeout.count());
  result.options.request_body_lifetime = TREVRPC_REQUEST_BODY_BORROW_UNTIL_RETURN;
  return result;
}

trevrpc_request native_request(std::string_view service, std::string_view method,
                               std::uint32_t kind, std::span<const std::byte> body) noexcept {
  trevrpc_request request{};
  request.service = service.data();
  request.service_len = service.size();
  request.method = method.data();
  request.method_len = method.size();
  request.body = reinterpret_cast<const std::uint8_t*>(body.data());
  request.body_len = body.size();
  request.kind = kind;
  request.version = TREVRPC_WIRE_VERSION;
  return request;
}

int status_from(const trevrpc_inbound_response* response, const Metadata& metadata, Status* out) {
  std::uint32_t code = TREVRPC_STATUS_UNKNOWN;
  trevrpc_bytes_view message{};
  int error = trevrpc_inbound_response_get_status(response, &code);
  if (error == 0) {
    error = trevrpc_inbound_response_get_message(response, &message);
  }
  if (error != 0) {
    return error;
  }
  if (message.data == nullptr && message.len > 0) {
    return -EINVAL;
  }
  *out = Status(static_cast<StatusCode>(trevrpc_status_code_from_uint32(code)),
                message.data == nullptr
                    ? std::string{}
                    : std::string(reinterpret_cast<const char*>(message.data), message.len),
                metadata);
  return 0;
}

int status_from(const trevrpc_inbound_stream_frame* frame, Status* out) {
  std::uint32_t code = TREVRPC_STATUS_UNKNOWN;
  trevrpc_bytes_view message{};
  int error = trevrpc_inbound_stream_frame_get_status(frame, &code);
  if (error == 0) {
    error = trevrpc_inbound_stream_frame_get_message(frame, &message);
  }
  if (error != 0) {
    return error;
  }
  if (message.data == nullptr && message.len > 0) {
    return -EINVAL;
  }
  auto metadata = copy_metadata(frame);
  if (!metadata) {
    return metadata.error().code();
  }
  *out = Status(static_cast<StatusCode>(trevrpc_status_code_from_uint32(code)),
                message.data == nullptr
                    ? std::string{}
                    : std::string(reinterpret_cast<const char*>(message.data), message.len),
                std::move(metadata).value());
  return 0;
}

Result<StreamFrame> decode_inbound_frame(trevrpc_stream* stream,
                                         trevrpc_inbound_stream_frame* raw_frame) {
  NativeInboundFrame frame(raw_frame);
  if (frame.get() == nullptr) {
    return Error::protobuf("stream ended without a terminal status");
  }
  std::uint32_t kind = 0;
  const int kind_error = trevrpc_inbound_stream_frame_get_kind(frame.get(), &kind);
  if (kind_error != 0) {
    return Error::runtime(kind_error);
  }
  StreamFrame result;
  result.terminal = kind == TREVRPC_STREAM_FRAME_KIND_STATUS;
  if (result.terminal) {
    const int status_error = status_from(frame.get(), &result.status);
    if (status_error != 0) {
      return Error::runtime(status_error);
    }
    frame = NativeInboundFrame{};
    trevrpc_inbound_stream_frame* raw_trailing = nullptr;
    const int trailing_error = trevrpc_stream_recv_inbound(stream, &raw_trailing);
    NativeInboundFrame trailing(raw_trailing);
    if (trailing_error != 0 || trailing.get() != nullptr) {
      return Error::protobuf("response stream contained trailing data after terminal status");
    }
    return result;
  }
  if (kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
    return Error::protobuf("response stream contained an unknown frame kind");
  }
  trevrpc_bytes_view body{};
  const int body_error = trevrpc_inbound_stream_frame_get_body(frame.get(), &body);
  if (body_error != 0) {
    return Error::runtime(body_error);
  }
  if (body.data == nullptr && body.len > 0) {
    return Error::runtime(-EINVAL, "runtime returned an invalid stream body view");
  }
  result.body.resize(body.len);
  if (body.len > 0) {
    std::memcpy(result.body.data(), body.data, body.len);
  }
  return result;
}

Result<StreamFrame> receive_inbound_frame(trevrpc_stream* stream) {
  trevrpc_inbound_stream_frame* raw_frame = nullptr;
  const int error = trevrpc_stream_recv_inbound(stream, &raw_frame);
  if (error != 0) {
    trevrpc_inbound_stream_frame_release(raw_frame);
    return Error::runtime(error);
  }
  return decode_inbound_frame(stream, raw_frame);
}

int respond_borrowed(trevrpc_call* call, std::uint32_t status, std::string_view message,
                     std::span<const std::byte> body, const trevrpc_metadata* metadata) {
  trevrpc_response_view_v1 response{};
  const int error = trevrpc_response_view_v1_init(&response, sizeof(response));
  if (error != 0) {
    return error;
  }
  response.status = status;
  response.message = message.empty() ? nullptr : message.data();
  response.message_len = message.size();
  response.body = body.empty() ? nullptr : reinterpret_cast<const std::uint8_t*>(body.data());
  response.body_len = body.size();
  response.metadata = metadata;
  return trevrpc_call_respond_borrowed_v1(call, &response);
}

int finish_borrowed(trevrpc_call* call, std::uint32_t status, std::string_view message,
                    const trevrpc_metadata* metadata) {
  trevrpc_status_view_v1 view{};
  const int error = trevrpc_status_view_v1_init(&view, sizeof(view));
  if (error != 0) {
    return error;
  }
  view.status = status;
  view.message = message.empty() ? nullptr : message.data();
  view.message_len = message.size();
  view.metadata = metadata;
  return trevrpc_call_finish_stream_borrowed_v1(call, &view);
}

} // namespace trevrpc::detail
