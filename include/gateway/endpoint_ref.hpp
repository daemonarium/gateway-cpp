#pragma once

#include "gateway/expr.hpp"
#include "gateway/types.hpp"

#include "examples/structures/stream_protocol.hpp"

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <vector>

namespace waos {
  namespace gateway {

    struct endpoint_ref {
      virtual ~endpoint_ref() = default;
      virtual void send_message(binary payload) = 0;
      virtual void on_message(std::function<void(binary payload)> handler) = 0;

      // Gateway-owned stream. Default rejects so a callback echo endpoint
      // answers set_reference with unknown_api.
      virtual void start_stream(
        std::uint64_t /*stream_id*/,
        std::uint64_t /*function_id*/,
        const std::vector<literal_expr>& /*literals*/,
        stream_flags /*flags*/ = stream_flags::none,
        std::uint64_t /*since_timestamp*/ = 0
      ) {
        throw std::runtime_error(error_unknown_api);
      }

      virtual void cancel_stream(std::uint64_t /*stream_id*/) {}
    };

  }
}
