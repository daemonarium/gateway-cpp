#pragma once

#include "gateway/session.hpp"
#include "observable/socket_mux.hpp"

#include "serialization/type_description.hpp"
#include "serialization/type_registry.hpp"
#include "serialization/type_registry_blob.hpp"

#include <memory>
#include <stdexcept>

namespace waos {
  namespace gateway {

    // Per-connection API object. Methods delegate to session_state.
    // Reflected for the description blob and registered with register_object_methods
    // on the live instance, so each socket keeps its own state.
    class session_api {
      public:
        session_api() = default;

        explicit session_api(std::shared_ptr<session_state> state)
          : state_(std::move(state)) {}

        // Present so type_description lists the instance methods. Not dispatched.
        [[=serialization::policy::ignore]]
        static session_api& get_rpc_instance() {
          static session_api dummy;
          return dummy;
        }

        [[=serialization::policy::ignore]]
        virtual ~session_api() = default;

        virtual server_protocol hello(client_protocol client) {
          return state_->hello(std::move(client));
        }

        virtual std::vector<api_info> list_apis() const {
          return state_->list_apis();
        }

        virtual binary get_rpc_description(std::uint32_t api_id) const {
          return state_->get_rpc_description(api_id);
        }

        virtual binary get_session_description() const {
          return state_->get_session_description();
        }

        virtual void set_reference(std::uint32_t ref_id, expr value) {
          state_->set_reference(ref_id, std::move(value));
        }

        virtual std::uint32_t observe_reference(std::uint32_t ref_id) {
          return state_->observe_reference(ref_id);
        }

        virtual void unobserve_reference(std::uint32_t ref_id) {
          state_->unobserve_reference(ref_id);
        }

        virtual void dispose_reference(std::uint32_t ref_id) {
          state_->dispose_reference(ref_id);
        }

        virtual std::uint32_t fetch_reference(std::uint32_t ref_id) {
          return state_->fetch_reference(ref_id);
        }

        virtual std::uint32_t open_pipe(std::uint32_t cookie) {
          return state_->open_pipe(cookie);
        }

        virtual void close_pipe(std::uint32_t socket_id) {
          state_->close_pipe(socket_id);
        }

        [[=serialization::policy::ignore]]
        void set_observation_hooks(
          std::function<void(std::uint32_t, const expr&)> on_set,
          std::function<std::uint32_t(std::uint32_t)> on_observe,
          std::function<void(std::uint32_t)> on_unobserve,
          std::function<void(std::uint32_t)> on_dispose,
          std::function<std::uint32_t(std::uint32_t)> on_fetch,
          std::function<std::uint32_t(std::uint32_t)> on_open_pipe = {},
          std::function<void(std::uint32_t)> on_close_pipe = {}
        ) {
          state_->set_observation_hooks(
            std::move(on_set),
            std::move(on_observe),
            std::move(on_unobserve),
            std::move(on_dispose),
            std::move(on_fetch),
            std::move(on_open_pipe),
            std::move(on_close_pipe)
          );
        }

        [[=serialization::policy::ignore]]
        bool ready() const { return state_->ready(); }

        [[=serialization::policy::ignore]]
        bool failed() const { return state_->failed(); }

        [[=serialization::policy::ignore]]
        bool is_visible(std::uint32_t api_id) const { return state_->is_visible(api_id); }

        [[=serialization::policy::ignore]]
        std::shared_ptr<session_state> state_;
    };

    // Dynamic node order is the flat type index. Keep it aligned with
    // register_session_pointer_types.
    inline void describe_expr_nodes(serialization::type_registry& registry) {
      serialization::type_description::create(ref_expr_node{}, registry);
      serialization::type_description::create(literal_expr_node{}, registry);
      serialization::type_description::create(member_expr_node{}, registry);
      serialization::type_description::create(call_expr_node{}, registry);
      serialization::type_description::create(switch_expr_node{}, registry);
      serialization::type_description::create(non_empty_expr_node{}, registry);
    }

    inline binary make_session_description_blob() {
      serialization::type_registry registry(true);
      serialization::type_description::create(std::runtime_error("runtime_error"), registry);
      serialization::type_description::create(api_message{}, registry);
      serialization::type_description::create(session_api{}, registry);
      describe_expr_nodes(registry);
      serialization::type_description::create(waos::mux_socket_open{}, registry);
      serialization::type_description::create(waos::mux_socket_data{}, registry);
      serialization::type_description::create(waos::mux_socket_close{}, registry);
      serialization::type_description::create(waos::mux_socket_window{}, registry);
      const std::string bytes = serialization::serialize_type_registry_blob(registry);
      return binary(bytes.begin(), bytes.end());
    }

  }
}
