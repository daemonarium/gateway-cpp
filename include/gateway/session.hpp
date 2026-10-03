#pragma once

#include "gateway/hash.hpp"
#include "gateway/types.hpp"

#include "serialization/type_description.hpp"
#include "serialization/type_registry.hpp"
#include "serialization/type_registry_blob.hpp"

#include <optional>
#include <stdexcept>
#include <utility>

namespace waos {
  namespace gateway {

    binary make_session_description_blob();

    class session {
      public:
        session() = default;

        explicit session(
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

        // Reflection-only. Dispatch uses register_object_method on the connection instance.
        [[=serialization::policy::ignore]]
        static session& get_rpc_instance() {
          static session dummy;
          return dummy;
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

    inline binary make_session_description_blob() {
      serialization::type_registry registry(true);
      serialization::type_description::create(api_message{}, registry);
      serialization::type_description::create(session{}, registry);
      const std::string bytes = serialization::serialize_type_registry_blob(registry);
      return binary(bytes.begin(), bytes.end());
    }

  }
}
