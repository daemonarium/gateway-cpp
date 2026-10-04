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

    // Inner channel indexes after register_basic_types + register_inner_stream_messages.
    constexpr std::uint32_t inner_rpc_request = 0;
    constexpr std::uint32_t inner_rpc_response = 1;
    constexpr std::uint32_t inner_rpc_error = 2;
    constexpr std::uint32_t inner_stream_item = 4;
    constexpr std::uint32_t inner_stream_key = 5;
    constexpr std::uint32_t inner_stream_done = 6;
    constexpr std::uint32_t inner_stream_error = 7;
    constexpr std::uint32_t inner_stream_reset = 8;
    constexpr std::uint32_t inner_stream_cancel = 9;
    constexpr std::uint32_t inner_stream_opened = 10;

    inline std::optional<std::uint32_t> read_inner_type_index(const binary& payload) {
      if(payload.size() < 8) {
        return std::nullopt;
      }
      std::uint32_t type_index = 0;
      std::memcpy(&type_index, payload.data() + 4, sizeof(type_index));
      return type_index;
    }

    inline std::optional<std::uint64_t> peek_inner_rpc_request_id(const binary& payload) {
      if(payload.size() < 20) {
        return std::nullopt;
      }
      auto type_index = read_inner_type_index(payload);
      if(!type_index || *type_index != inner_rpc_request) {
        return std::nullopt;
      }
      std::uint64_t request_id = 0;
      std::memcpy(&request_id, payload.data() + 12, sizeof(request_id));
      return request_id;
    }

    // Client → gateway. rpc_request (one-shot) and stream_cancel (ref_id).
    inline bool is_forwarded_inner_frame(const binary& payload) {
      auto type_index = read_inner_type_index(payload);
      if(!type_index) {
        return false;
      }
      if(*type_index == inner_rpc_request) {
        return payload.size() >= 20;
      }
      if(*type_index == inner_stream_cancel) {
        return payload.size() >= 8;
      }
      return false;
    }

    inline bool is_inner_stream_event(std::uint32_t type_index) {
      return type_index == inner_stream_item
        || type_index == inner_stream_key
        || type_index == inner_stream_done
        || type_index == inner_stream_error
        || type_index == inner_stream_reset
        || type_index == inner_stream_opened;
    }

    inline bool is_inner_rpc_result(std::uint32_t type_index) {
      return type_index == inner_rpc_response || type_index == inner_rpc_error;
    }

    // stream_id is the first field, at byte 12 (after len, type, encoded_size).
    inline std::optional<std::uint64_t> read_inner_stream_id(const binary& payload) {
      if(payload.size() < 20) {
        return std::nullopt;
      }
      std::uint64_t stream_id = 0;
      std::memcpy(&stream_id, payload.data() + 12, sizeof(stream_id));
      return stream_id;
    }

    inline binary rewrite_inner_stream_id(binary payload, std::uint64_t stream_id) {
      if(payload.size() >= 20) {
        std::memcpy(payload.data() + 12, &stream_id, sizeof(stream_id));
      }
      return payload;
    }

  }
}
