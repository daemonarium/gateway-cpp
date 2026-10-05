#pragma once

#include "gateway/session.hpp"

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

        virtual void set_reference(std::uint64_t ref_id, expr value) {
          state_->set_reference(ref_id, std::move(value));
        }

        virtual void observe_reference(std::uint64_t ref_id) {
          state_->observe_reference(ref_id);
        }

        virtual void unobserve_reference(std::uint64_t ref_id) {
          state_->unobserve_reference(ref_id);
        }

        virtual void dispose_reference(std::uint64_t ref_id) {
          state_->dispose_reference(ref_id);
        }

        virtual void fetch_reference(std::uint64_t ref_id) {
          state_->fetch_reference(ref_id);
        }

        [[=serialization::policy::ignore]]
        void set_observation_hooks(
          std::function<void(std::uint64_t, const expr&)> on_set,
          std::function<void(std::uint64_t)> on_observe,
          std::function<void(std::uint64_t)> on_unobserve,
          std::function<void(std::uint64_t)> on_dispose,
          std::function<void(std::uint64_t)> on_fetch
        ) {
          state_->set_observation_hooks(
            std::move(on_set),
            std::move(on_observe),
            std::move(on_unobserve),
            std::move(on_dispose),
            std::move(on_fetch)
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
      const std::string bytes = serialization::serialize_type_registry_blob(registry);
      return binary(bytes.begin(), bytes.end());
    }

  }
}
