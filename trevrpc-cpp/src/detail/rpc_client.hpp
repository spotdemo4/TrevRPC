#pragma once

#include "rpc_event_runtime.hpp"

#include <trevrpc_rpc_msquic.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>

namespace trevrpc::detail {

class RpcSyncClient final {
public:
  [[nodiscard]] static Result<std::unique_ptr<RpcSyncClient>>
  connect(std::string_view host, std::uint16_t port, const ChannelConfig& config = {});

  ~RpcSyncClient();
  RpcSyncClient(const RpcSyncClient&) = delete;
  RpcSyncClient& operator=(const RpcSyncClient&) = delete;

  [[nodiscard]] Result<ByteResponse> call_unary(std::string_view service,
                                                std::string_view method,
                                                std::span<const std::byte> body,
                                                const CallOptions& options = {});
  [[nodiscard]] Result<void> close();

private:
  friend class RpcSyncClientTestPeer;

  RpcSyncClient(std::shared_ptr<RpcEventRuntime> runtime,
                trevrpc_rpc_endpoint_v1 endpoint) noexcept
      : runtime_(std::move(runtime)), endpoint_(endpoint) {}

  [[nodiscard]] Result<trevrpc_rpc_receive*> receive(trevrpc_rpc_stream_v1 stream,
                                                       bool& stream_closed,
                                                       bool& call_closed);
  [[nodiscard]] Result<void> close_call(trevrpc_rpc_call_v1 call,
                                        trevrpc_rpc_stream_v1 stream,
                                        bool stream_registered = true,
                                        bool stream_closed = false,
                                        bool call_closed = false);

  std::mutex lifecycle_mutex_;
  std::shared_ptr<RpcEventRuntime> runtime_;
  trevrpc_rpc_endpoint_v1 endpoint_{};
  bool closed_ = false;
};

} // namespace trevrpc::detail
