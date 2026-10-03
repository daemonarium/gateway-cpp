#pragma once

#include "gateway/frame_io.hpp"
#include "gateway/types.hpp"

#include "serialization/socket_message_channel.hpp"
#include "serialization/socket_transport.hpp"
#include "serialization/rpc.hpp"

#include <stdexcept>

extern "C" {
#include <io/circular_buffer.h>
}

namespace waos {
  namespace gateway {

    inline binary encode_inner_rpc_error(std::uint64_t request_id, const char* message) {
      struct circular_buffer in {};
      struct circular_buffer out {};
      circular_buffer_init(&in, 4096, nullptr);
      circular_buffer_init(&out, 4096, nullptr);
      serialization::flat_buffer_socket_message_channel ch(
        serialization::buffer_transport(&in, &out)
      );
      ch.register_message_type<serialization::rpc_request>();
      ch.register_message_type<serialization::rpc_response>();
      ch.register_message_type<serialization::rpc_error>();
      ch.register_exception_type<std::runtime_error>();
      auto& writer = ch.begin_send(serialization::rpc_error{request_id});
      writer.write_exception(std::runtime_error(message ? message : error_unknown_api));
      writer.write_rest();
      ch.commit_send();
      binary frame = drain_one_frame(&out);
      circular_buffer_deinit(&in);
      circular_buffer_deinit(&out);
      return frame;
    }

  }
}
