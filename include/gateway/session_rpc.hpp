#pragma once

#include "gateway/session.hpp"
#include "observable/socket_mux.hpp"

#include "serialization/rpc.hpp"

#include <stdexcept>
#include <vector>

namespace waos {
  namespace gateway {

    // Pointer targets written by flat write_rest. Order is the wire type index
    // and must match the DynamicObject order in make_session_description_blob.
    template <typename Rpc>
    void register_session_pointer_types(Rpc& rpc) {
      rpc.channel().template register_dynamic_type<ref_expr_node>();
      rpc.channel().template register_dynamic_type<literal_expr_node>();
      rpc.channel().template register_dynamic_type<member_expr_node>();
      rpc.channel().template register_dynamic_type<call_expr_node>();
      rpc.channel().template register_dynamic_type<switch_expr_node>();
      rpc.channel().template register_dynamic_type<non_empty_expr_node>();
    }

    template <typename Rpc>
    void register_session_channel(Rpc& rpc) {
      rpc.register_basic_types();
      rpc.template register_message_type<api_message>();
      waos::observable::register_mux_socket_messages(rpc);
      rpc.channel().template register_exception_type<std::runtime_error>();
      register_session_pointer_types(rpc);
    }

    template <typename Server>
    void register_session_methods(Server& server, session_api& sess) {
      server.register_recursive(client_protocol{});
      server.register_recursive(server_protocol{});
      server.register_recursive(std::vector<api_info>{});
      server.template register_object_methods<session_api>(sess);
    }

    template <typename Server>
    void attach_session(Server& server, session_api& sess) {
      register_session_channel(server);
      register_session_methods(server, sess);
    }

  }
}
