#pragma once

#include <trevrpc/trevrpc.hpp>

#include "trevrpc_wire_internal.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace cf {

class RpcStreamHarness final {
public:
  RpcStreamHarness();
  ~RpcStreamHarness();
  RpcStreamHarness(const RpcStreamHarness &) = delete;
  RpcStreamHarness &operator=(const RpcStreamHarness &) = delete;

  [[nodiscard]] trevrpc::Result<trevrpc::detail::ClientStream>
  start(std::uint32_t kind);
  [[nodiscard]] int push_frame(std::span<const std::uint8_t> frame);
  [[nodiscard]] int finish_response();
  [[nodiscard]] int fail_receive(int error);
  [[nodiscard]] bool wait_terminal_status_seen(
      std::chrono::milliseconds timeout = std::chrono::seconds(1)) const;
  [[nodiscard]] bool terminal_status_seen() const noexcept;
  [[nodiscard]] trevrpc_wire_diagnostic_reason
  receive_diagnostic() const noexcept;
  [[nodiscard]] std::size_t close_count() const noexcept;
  void shutdown() noexcept;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace cf
