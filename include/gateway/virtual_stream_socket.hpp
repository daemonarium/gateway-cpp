#pragma once

#include "gateway/api_connection.hpp"
#include "gateway/inner_stream.hpp"

#include "observable/socket_mux.hpp"
#include "observable/stream_multiplexer.hpp"
#include "serialization/socket_rpc_client.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <utility>

extern "C" {
#include <io/circular_buffer.h>
}

namespace waos {
  namespace gateway {

    // Client-side virtual socket: bounded input buffer feeding StreamMultiplexer.
    class virtual_stream_socket {
      public:
        using rpc_type = serialization::socket_rpc_client<serialization::flat_materialized_codec, mux_transport>;
        using mux_type = waos::observable::StreamMultiplexer<rpc_type, rpc_type>;

        explicit virtual_stream_socket(
          std::function<void(std::size_t)> on_consumed,
          std::size_t buffer_size = waos::observable::default_mux_window * 4
        )
          : on_consumed_(std::move(on_consumed)) {
          circular_buffer_init(&in_, buffer_size, nullptr);
          circular_buffer_init(&out_, buffer_size, nullptr);
          inner_.emplace(mux_transport{&in_, &out_, &on_flush_});
          inner_->register_basic_types();
          inner_->channel().template register_exception_type<std::runtime_error>();
          register_inner_stream_messages(*inner_);
          mux_.emplace(*inner_, *inner_, std::uint64_t{0}, std::uint32_t{0});
        }

        virtual_stream_socket(const virtual_stream_socket&) = delete;
        virtual_stream_socket& operator=(const virtual_stream_socket&) = delete;

        ~virtual_stream_socket() {
          mux_.reset();
          inner_.reset();
          circular_buffer_deinit(&in_);
          circular_buffer_deinit(&out_);
        }

        mux_type& mux() { return *mux_; }

        void deliver(const binary& payload) {
          const std::size_t before = circular_buffer_readable_bytes(&in_);
          if(!payload.empty()) {
            write_all(&in_, payload.data(), payload.size());
          }
          inner_->handle_data();
          const std::size_t after = circular_buffer_readable_bytes(&in_);
          const std::size_t consumed = before + payload.size() - after;
          if(consumed > 0 && on_consumed_) {
            on_consumed_(consumed);
          }
        }

      private:
        std::function<void(std::size_t)> on_consumed_;
        std::function<void()> on_flush_;
        struct circular_buffer in_ {};
        struct circular_buffer out_ {};
        std::optional<rpc_type> inner_;
        std::optional<mux_type> mux_;
    };

  }
}
