#pragma once

#include <trevrpc/trevrpc.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

namespace trevrpc::detail {

template <typename T, void (*Release)(T*)> class NativeOwned {
public:
  NativeOwned() = default;
  explicit NativeOwned(T* value) noexcept : value_(value) {}
  ~NativeOwned() { Release(value_); }
  NativeOwned(const NativeOwned&) = delete;
  NativeOwned& operator=(const NativeOwned&) = delete;
  NativeOwned(NativeOwned&& other) noexcept : value_(std::exchange(other.value_, nullptr)) {}
  NativeOwned& operator=(NativeOwned&& other) noexcept {
    if (this != &other) {
      Release(value_);
      value_ = std::exchange(other.value_, nullptr);
    }
    return *this;
  }

  [[nodiscard]] T* get() const noexcept { return value_; }

private:
  T* value_ = nullptr;
};

using NativeInboundResponse =
    NativeOwned<trevrpc_inbound_response, trevrpc_inbound_response_release>;
using NativeInboundFrame =
    NativeOwned<trevrpc_inbound_stream_frame, trevrpc_inbound_stream_frame_release>;

class NativeMetadata final {
public:
  NativeMetadata() = default;
  ~NativeMetadata();
  NativeMetadata(const NativeMetadata&) = delete;
  NativeMetadata& operator=(const NativeMetadata&) = delete;
  NativeMetadata(NativeMetadata&& other) noexcept;
  NativeMetadata& operator=(NativeMetadata&& other) noexcept;

  [[nodiscard]] int assign(const Metadata& metadata);
  [[nodiscard]] trevrpc_metadata* get() noexcept { return &metadata_; }
  [[nodiscard]] const trevrpc_metadata* get() const noexcept { return &metadata_; }

private:
  trevrpc_metadata metadata_{};
};

struct NativeCallOptions {
  trevrpc_call_options_v1 options{};
  NativeMetadata metadata;
  std::optional<Cancellation> retained_cancellation;

  NativeCallOptions() = default;
  NativeCallOptions(const NativeCallOptions&) = delete;
  NativeCallOptions& operator=(const NativeCallOptions&) = delete;
  NativeCallOptions(NativeCallOptions&& other) noexcept;
  NativeCallOptions& operator=(NativeCallOptions&& other) noexcept;
};

[[nodiscard]] Result<NativeCallOptions> normalize_call_options(const CallOptions& options,
                                                               bool retain_cancellation = false);
[[nodiscard]] trevrpc_request native_request(std::string_view service, std::string_view method,
                                             std::uint32_t kind,
                                             std::span<const std::byte> body) noexcept;
[[nodiscard]] Result<Metadata> copy_metadata(const trevrpc_inbound_response* response);
[[nodiscard]] Result<Metadata> copy_metadata(const trevrpc_inbound_stream_frame* frame);
[[nodiscard]] Metadata copy_metadata(const trevrpc_metadata& metadata);
[[nodiscard]] int status_from(const trevrpc_inbound_response* response, const Metadata& metadata,
                              Status* out);
[[nodiscard]] int status_from(const trevrpc_inbound_stream_frame* frame, Status* out);
[[nodiscard]] Result<StreamFrame> decode_inbound_frame(trevrpc_stream* stream,
                                                       trevrpc_inbound_stream_frame* frame);
[[nodiscard]] Result<StreamFrame> receive_inbound_frame(trevrpc_stream* stream);
[[nodiscard]] int respond_borrowed(trevrpc_call* call, std::uint32_t status,
                                   std::string_view message, std::span<const std::byte> body,
                                   const trevrpc_metadata* metadata);
[[nodiscard]] int finish_borrowed(trevrpc_call* call, std::uint32_t status,
                                  std::string_view message, const trevrpc_metadata* metadata);

} // namespace trevrpc::detail
