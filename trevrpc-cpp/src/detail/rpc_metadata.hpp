#pragma once

#include <trevrpc/trevrpc.hpp>

#include <trevrpc_rpc.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <utility>

namespace trevrpc::detail {

[[nodiscard]] inline bool valid_rpc_metadata(const Metadata& metadata) noexcept {
  if (metadata.entries().size() > std::numeric_limits<std::uint32_t>::max()) {
    return false;
  }
  for (const auto& entry : metadata.entries()) {
    if (entry.key.size() > Metadata::max_key_size ||
        entry.value.size() > Metadata::max_value_size) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] inline Metadata copy_rpc_metadata(const trevrpc_rpc_metadata_entry_v1* entries,
                                                std::uint32_t count) {
  Metadata metadata;
  for (std::uint32_t index = 0; index < count; ++index) {
    const auto& entry = entries[index];
    std::string key;
    if (entry.key_len != 0) {
      key.assign(entry.key, entry.key_len);
    }
    std::span<const std::byte> value;
    if (entry.value_len != 0) {
      value = std::span(reinterpret_cast<const std::byte*>(entry.value), entry.value_len);
    }
    metadata.set(std::move(key), value);
  }
  return metadata;
}

} // namespace trevrpc::detail
