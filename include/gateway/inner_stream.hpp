#pragma once

#include "gateway/frame_io.hpp"

#include "examples/structures/stream_protocol.hpp"
#include "serialization/rpc.hpp"
#include "serialization/socket_message_channel.hpp"

#include <cstdint>

extern "C" {
#include <io/circular_buffer.h>
}

namespace waos {
  namespace gateway {

    // Register after register_basic_types so indexes match the TS inner channel.
    // stream_key is registered so later indexes stay aligned. Do not register
    // stream_observe_begin / stream_retransmit.
    template <typename Channel>
    void register_inner_stream_messages(Channel& channel) {
      channel.template register_message_type<socket_opened>();
      channel.template register_message_type<stream_item>();
      channel.template register_message_type<stream_key>();
      channel.template register_message_type<stream_done>();
      channel.template register_message_type<stream_error>();
      channel.template register_message_type<stream_reset>();
      channel.template register_message_type<stream_cancel>();
      channel.template register_message_type<stream_opened>();
    }

    inline binary encode_stream_cancel(std::uint64_t stream_id) {
      struct circular_buffer in {};
      struct circular_buffer out {};
      circular_buffer_init(&in, 4096, nullptr);
      circular_buffer_init(&out, 4096, nullptr);
      serialization::flat_buffer_socket_message_channel channel(
        serialization::buffer_transport(&in, &out)
      );
      channel.template register_message_type<serialization::rpc_request>();
      channel.template register_message_type<serialization::rpc_response>();
      channel.template register_message_type<serialization::rpc_error>();
      register_inner_stream_messages(channel);
      channel.send_message(stream_cancel{stream_id});
      binary frame = drain_one_frame(&out);
      circular_buffer_deinit(&in);
      circular_buffer_deinit(&out);
      return frame;
    }

  }
}
