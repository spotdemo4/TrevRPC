#include "benchmark_peer.hpp"

#include <algorithm>
#include <utility>

namespace trevrpc_bench {

PeerError::PeerError(std::string phase, std::string code, std::string_view message)
    : std::runtime_error(std::string(message)), phase_(std::move(phase)), code_(std::move(code)) {}

const std::string& PeerError::phase() const noexcept { return phase_; }

const std::string& PeerError::code() const noexcept { return code_; }

std::string Endpoint::address(std::uint16_t actual_port) const {
  if (host.find(':') != std::string::npos) {
    return '[' + host + "]:" + std::to_string(actual_port);
  }
  return host + ':' + std::to_string(actual_port);
}

std::string make_payload(std::size_t size, std::uint64_t) { return std::string(size, '\0'); }

bool valid_payload(std::string_view payload, std::size_t size, std::uint64_t) {
  return payload.size() == size &&
         std::all_of(payload.begin(), payload.end(), [](char character) { return character == 0; });
}

} // namespace trevrpc_bench
