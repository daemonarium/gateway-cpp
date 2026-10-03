#pragma once

#define XXH_INLINE_ALL
#include "xxhash.h"

#include "gateway/types.hpp"

#include <cstddef>

namespace waos {
  namespace gateway {

    inline std::uint64_t xxh3_64(const void* data, std::size_t size) {
      return static_cast<std::uint64_t>(XXH3_64bits(data, size));
    }

    inline std::uint64_t xxh3_64(const binary& bytes) {
      return xxh3_64(bytes.data(), bytes.size());
    }

    inline binary xxh3_64_le(const void* data, std::size_t size) {
      return hash_to_le_bytes(xxh3_64(data, size));
    }

    inline binary xxh3_64_le(const binary& bytes) {
      return xxh3_64_le(bytes.data(), bytes.size());
    }

  }
}
