#pragma once

#include "gateway/session.hpp"

#include "serialization/rpc.hpp"

#include <stdexcept>
#include <vector>

namespace waos {
  namespace gateway {

    template <typename Rpc>
    void register_session_channel(Rpc& rpc) {
      rpc.register_basic_types();
      rpc.template register_message_type<api_message>();
      rpc.channel().template register_exception_type<std::runtime_error>();
    }

    template <typename Server>
    void register_session_methods(Server& server, session& sess) {
      server.register_recursive(client_protocol{});
      server.register_recursive(server_protocol{});
      server.register_recursive(std::vector<api_info>{});
      server.register_object_method(sess, &session::hello);
      server.register_object_method(sess, &session::list_apis);
      server.register_object_method(sess, &session::get_rpc_description);
      server.register_object_method(sess, &session::get_session_description);
      server.register_object_method(sess, &session::set_reference);
      server.register_object_method(sess, &session::observe_reference);
      server.register_object_method(sess, &session::unobserve_reference);
      server.register_object_method(sess, &session::dispose_reference);
    }

    template <typename Server>
    void attach_session(Server& server, session& sess) {
      register_session_channel(server);
      register_session_methods(server, sess);
    }

  }
}
