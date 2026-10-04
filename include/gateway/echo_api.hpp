#pragma once

#include "gateway/callback_endpoint.hpp"
#include "gateway/frame_io.hpp"
#include "gateway/types.hpp"

#include "examples/structures/stream_protocol.hpp"
#include "serialization/socket_rpc_server.hpp"
#include "serialization/socket_transport.hpp"
#include "serialization/type_description.hpp"
#include "serialization/type_registry.hpp"
#include "serialization/type_registry_blob.hpp"

#include <memory>
#include <stdexcept>
#include <string>

extern "C" {
#include <io/circular_buffer.h>
}

namespace waos {
  namespace gateway {

    struct echo_service {
      static std::uint64_t echo(std::uint64_t x) {
        return x;
      }
    };

    inline binary make_echo_blob() {
      serialization::type_registry registry(true);
      serialization::type_description::create(echo_service{}, registry);
      const std::string bytes = serialization::serialize_type_registry_blob(registry);
      return binary(bytes.begin(), bytes.end());
    }

    class stream_endpoint;

    // function_id by declaration order: 0 echo_stream, 1 echo_fail, 2 echo_reset, 3 echo_mark.
    struct echo_stream_service {
      [[=serialization::policy::ignore]]
      stream_endpoint* owner = nullptr;

      void echo_stream(stream_handle handle, std::uint64_t key, std::uint64_t count);
      void echo_fail(stream_handle handle);
      void echo_reset(stream_handle handle);
      void echo_mark(stream_handle handle, std::uint64_t key);
    };

    inline binary make_stream_blob() {
      serialization::type_registry registry(true);
      serialization::type_description::create(echo_stream_service{}, registry);
      const std::string bytes = serialization::serialize_type_registry_blob(registry);
      return binary(bytes.begin(), bytes.end());
    }

    struct echo_buffers {
      circular_buffer a_to_b{};
      circular_buffer b_to_a{};

      explicit echo_buffers(std::size_t buf_size = 4096) {
        circular_buffer_init(&a_to_b, buf_size, nullptr);
        circular_buffer_init(&b_to_a, buf_size, nullptr);
      }

      ~echo_buffers() {
        circular_buffer_deinit(&a_to_b);
        circular_buffer_deinit(&b_to_a);
      }

      echo_buffers(const echo_buffers&) = delete;
      echo_buffers& operator=(const echo_buffers&) = delete;
    };

    struct echo_endpoint {
      echo_buffers buffers;
      echo_service svc;
      serialization::flat_buffer_socket_rpc_server server;
      std::shared_ptr<callback_endpoint_ref> ep;
      bool called = false;

      echo_endpoint()
        : server(serialization::buffer_transport(&buffers.a_to_b, &buffers.b_to_a)),
          ep(std::make_shared<callback_endpoint_ref>()) {
        server.register_basic_types();
        server.register_recursive(svc);
        server.channel().register_exception_type<std::runtime_error>();
        ep->set_sink([this](binary payload) {
          called = true;
          write_all(&buffers.a_to_b, payload.data(), payload.size());
          server.handle_data();
          for(auto& frame : drain_frames(&buffers.b_to_a)) {
            ep->emit(std::move(frame));
          }
        });
      }
    };

  }
}
