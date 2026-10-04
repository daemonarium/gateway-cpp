#pragma once

#include "gateway/frame_io.hpp"
#include "gateway/inner_stream.hpp"
#include "gateway/types.hpp"

#include "observable/stream_multiplexer.hpp"
#include "serialization/socket_rpc_client.hpp"
#include "serialization/socket_transport.hpp"

#include <functional>
#include <memory>
#include <optional>
#include <utility>

extern "C" {
#include <io/circular_buffer.h>
}

namespace waos {
  namespace gateway {

    struct mux_transport {
      struct circular_buffer* in = nullptr;
      struct circular_buffer* out = nullptr;
      std::function<void()>* on_flush = nullptr;

      struct circular_buffer& input_buffer() { return *in; }
      struct circular_buffer& output_buffer() { return *out; }
      void flush() {
        if(on_flush && *on_flush) {
          (*on_flush)();
        }
      }
      void drain() {}
    };

    class api_connection {
      public:
        using rpc_type = serialization::socket_rpc_client<serialization::flat_materialized_codec, mux_transport>;
        using mux_type = waos::observable::StreamMultiplexer<rpc_type, rpc_type>;

        api_connection(std::uint32_t api_id, std::function<void(binary)> send_outer)
          : api_id_(api_id), send_outer_(std::move(send_outer)) {
          circular_buffer_init(&in_, 4096, nullptr);
          circular_buffer_init(&out_, 4096, nullptr);
          on_flush_ = [this] { flush_out(); };
          inner_.emplace(mux_transport{&in_, &out_, &on_flush_});
          inner_->register_basic_types();
          inner_->channel().template register_exception_type<std::runtime_error>();
          register_inner_stream_messages(*inner_);
          mux_.emplace(*inner_, *inner_, std::uint64_t{0}, std::uint32_t{0});
        }

        api_connection(const api_connection&) = delete;
        api_connection& operator=(const api_connection&) = delete;

        ~api_connection() {
          mux_.reset();
          inner_.reset();
          circular_buffer_deinit(&in_);
          circular_buffer_deinit(&out_);
        }

        std::uint32_t api_id() const { return api_id_; }

        rpc_type& rpc() {
          return *inner_;
        }

        mux_type& mux() {
          return *mux_;
        }

        void deliver(const binary& inner_frame) {
          write_all(&in_, inner_frame.data(), inner_frame.size());
          inner_->handle_data();
        }

      private:
        std::uint32_t api_id_ = 0;
        std::function<void(binary)> send_outer_;
        struct circular_buffer in_ {};
        struct circular_buffer out_ {};
        std::function<void()> on_flush_;
        std::optional<rpc_type> inner_;
        std::optional<mux_type> mux_;

        void flush_out() {
          for(auto& frame : drain_frames(&out_)) {
            send_outer_(std::move(frame));
          }
        }
    };

  }
}
