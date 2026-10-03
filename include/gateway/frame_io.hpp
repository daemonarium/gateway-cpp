#pragma once

#include "gateway/types.hpp"

#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

extern "C" {
#include <io/circular_buffer.h>
}

namespace waos {
  namespace gateway {

    inline void write_all(struct circular_buffer* buf, const std::uint8_t* data, std::size_t n) {
      if(!buf || n == 0) {
        return;
      }
      circular_buffer_reserve_for_writing(buf, n);
      std::size_t off = 0;
      while(off < n) {
        std::size_t m = circular_buffer_write(buf, data + off, n - off);
        if(m == 0) {
          break;
        }
        off += m;
      }
    }

    inline binary drain_one_frame(struct circular_buffer* buf) {
      if(!buf || circular_buffer_readable_bytes(buf) < 4) {
        return {};
      }
      std::uint32_t len = 0;
      circular_buffer_peek(buf, 0, &len, sizeof(len));
      if(circular_buffer_readable_bytes(buf) < 4u + len) {
        return {};
      }
      binary out(4u + len);
      circular_buffer_read(buf, out.data(), out.size());
      return out;
    }

    inline std::vector<binary> drain_frames(struct circular_buffer* buf) {
      std::vector<binary> frames;
      for(;;) {
        binary frame = drain_one_frame(buf);
        if(frame.empty()) {
          break;
        }
        frames.push_back(std::move(frame));
      }
      return frames;
    }

    inline std::optional<std::uint64_t> peek_inner_rpc_request_id(const binary& payload) {
      if(payload.size() < 20) {
        return std::nullopt;
      }
      std::uint32_t type_index = 0;
      std::memcpy(&type_index, payload.data() + 4, sizeof(type_index));
      if(type_index != 0) {
        return std::nullopt;
      }
      std::uint64_t request_id = 0;
      std::memcpy(&request_id, payload.data() + 12, sizeof(request_id));
      return request_id;
    }

  }
}
