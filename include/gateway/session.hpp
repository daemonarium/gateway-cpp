#pragma once

#include "gateway/expr.hpp"
#include "gateway/hash.hpp"
#include "gateway/types.hpp"

#include "serialization/type_description.hpp"
#include "serialization/type_registry.hpp"
#include "serialization/type_registry_blob.hpp"

#include <functional>
#include <optional>
#include <stdexcept>
#include <utility>

namespace waos {
  namespace gateway {

    binary make_session_description_blob();

    class session_state {
      public:
        session_state() = default;

        explicit session_state(
          std::vector<api_publication> publications,
          visibility_filter filter = default_visibility,
          std::optional<handshake_data> handshake = std::nullopt
        )
          : publications_(std::move(publications)),
            filter_(filter ? std::move(filter) : visibility_filter(default_visibility)) {
          for(const auto& pub : publications_) {
            if(pub.api_id == session_api_id) {
              throw std::invalid_argument("api_id 0 is reserved for the session namespace");
            }
            hashes_[pub.api_id] = xxh3_64_le(pub.description_blob);
          }
          if(handshake) {
            apply_handshake(std::move(*handshake));
          }
        }

        server_protocol hello(client_protocol client) {
          if(state_ == state::failed) {
            throw std::runtime_error(error_unsupported_protocol_version);
          }
          apply_handshake(std::move(client));
          return server_protocol{protocol_version_v1};
        }

        std::vector<api_info> list_apis() const {
          require_ready();
          std::vector<api_info> out;
          const auth_map& auth = handshake_ ? handshake_->auth : auth_map{};
          for(const auto& pub : publications_) {
            if(!filter_(pub.api_id, auth)) {
              continue;
            }
            api_info info;
            info.api_id = pub.api_id;
            info.name = pub.name;
            info.description_hash = hashes_.at(pub.api_id);
            out.push_back(std::move(info));
          }
          return out;
        }

        binary get_rpc_description(std::uint32_t api_id) const {
          require_ready();
          const api_publication* pub = find_visible(api_id);
          if(!pub) {
            throw std::runtime_error(error_unknown_api);
          }
          return pub->description_blob;
        }

        binary get_session_description() const {
          require_ready();
          return make_session_description_blob();
        }

        void set_reference(std::uint64_t ref_id, expr value) {
          require_ready();
          if(!on_set_reference_) {
            throw std::runtime_error(error_unknown_api);
          }
          on_set_reference_(ref_id, value);
        }

        void observe_reference(std::uint64_t ref_id) {
          require_ready();
          if(on_observe_reference_) {
            on_observe_reference_(ref_id);
          }
        }

        void unobserve_reference(std::uint64_t ref_id) {
          require_ready();
          if(on_unobserve_reference_) {
            on_unobserve_reference_(ref_id);
          }
        }

        void dispose_reference(std::uint64_t ref_id) {
          require_ready();
          if(on_dispose_reference_) {
            on_dispose_reference_(ref_id);
          }
        }

        void fetch_reference(std::uint64_t ref_id) {
          require_ready();
          if(on_fetch_reference_) {
            on_fetch_reference_(ref_id);
          }
        }

        [[=serialization::policy::ignore]]
        void set_observation_hooks(
          std::function<void(std::uint64_t, const expr&)> on_set,
          std::function<void(std::uint64_t)> on_observe,
          std::function<void(std::uint64_t)> on_unobserve,
          std::function<void(std::uint64_t)> on_dispose,
          std::function<void(std::uint64_t)> on_fetch
        ) {
          on_set_reference_ = std::move(on_set);
          on_observe_reference_ = std::move(on_observe);
          on_unobserve_reference_ = std::move(on_unobserve);
          on_dispose_reference_ = std::move(on_dispose);
          on_fetch_reference_ = std::move(on_fetch);
        }

        [[=serialization::policy::ignore]]
        bool ready() const { return state_ == state::ready; }

        [[=serialization::policy::ignore]]
        bool failed() const { return state_ == state::failed; }

        [[=serialization::policy::ignore]]
        bool is_visible(std::uint32_t api_id) const { return find_visible(api_id) != nullptr; }

      private:
        enum class state {
          pre_handshake,
          ready,
          failed
        };

        [[=serialization::policy::ignore]]
        std::vector<api_publication> publications_;

        [[=serialization::policy::ignore]]
        visibility_filter filter_ = default_visibility;

        [[=serialization::policy::ignore]]
        std::map<std::uint32_t, binary> hashes_;

        [[=serialization::policy::ignore]]
        std::optional<handshake_data> handshake_;

        [[=serialization::policy::ignore]]
        state state_ = state::pre_handshake;

        [[=serialization::policy::ignore]]
        std::function<void(std::uint64_t, const expr&)> on_set_reference_;

        [[=serialization::policy::ignore]]
        std::function<void(std::uint64_t)> on_observe_reference_;

        [[=serialization::policy::ignore]]
        std::function<void(std::uint64_t)> on_unobserve_reference_;

        [[=serialization::policy::ignore]]
        std::function<void(std::uint64_t)> on_dispose_reference_;

        [[=serialization::policy::ignore]]
        std::function<void(std::uint64_t)> on_fetch_reference_;

        void apply_handshake(handshake_data data) {
          if(data.protocol_version != protocol_version_v1) {
            state_ = state::failed;
            throw std::runtime_error(error_unsupported_protocol_version);
          }
          handshake_ = std::move(data);
          state_ = state::ready;
        }

        void require_ready() const {
          if(state_ == state::failed) {
            throw std::runtime_error(error_unsupported_protocol_version);
          }
          if(state_ != state::ready) {
            throw std::runtime_error(error_handshake_required);
          }
        }

        const api_publication* find_visible(std::uint32_t api_id) const {
          const auth_map& auth = handshake_ ? handshake_->auth : auth_map{};
          for(const auto& pub : publications_) {
            if(pub.api_id != api_id) {
              continue;
            }
            if(!filter_(pub.api_id, auth)) {
              return nullptr;
            }
            return &pub;
          }
          return nullptr;
        }
    };

  }
}

#include "gateway/session_api.hpp"
