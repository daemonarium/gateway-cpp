#pragma once

#include "serialization/annotations.hpp"
#include "serialization/rpc.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace waos {
  namespace gateway {

    constexpr std::uint64_t protocol_version_v1 = 1;
    constexpr std::uint32_t session_api_id = 0;

    constexpr std::uint64_t fn_hello = 0;
    constexpr std::uint64_t fn_list_apis = 1;
    constexpr std::uint64_t fn_get_rpc_description = 2;
    constexpr std::uint64_t fn_get_session_description = 3;
    constexpr std::uint64_t fn_set_reference = 4;
    constexpr std::uint64_t fn_observe_reference = 5;
    constexpr std::uint64_t fn_unobserve_reference = 6;
    constexpr std::uint64_t fn_dispose_reference = 7;

    constexpr const char* error_handshake_required = "handshake_required";
    constexpr const char* error_unsupported_protocol_version = "unsupported_protocol_version";
    constexpr const char* error_unknown_api = "unknown_api";
    constexpr const char* error_dependent_ref_not_supported = "dependent_ref_not_supported";
    constexpr const char* error_unknown_expr = "unknown_expr";

    using binary = serialization::binary;
    using auth_map = std::map<std::string, binary>;

    struct [[=serialization::message()]] api_message {
      std::uint32_t api_id = 0;
    };

    struct client_protocol {
      std::uint64_t protocol_version = 0;
      auth_map auth;
    };

    using handshake_data = client_protocol;

    struct server_protocol {
      std::uint64_t protocol_version = 0;
    };

    struct api_info {
      std::uint32_t api_id = 0;
      std::string name;
      binary description_hash;
    };

    struct api_publication {
      std::uint32_t api_id = 0;
      std::string name;
      binary description_blob;
    };

    using visibility_filter = std::function<bool(std::uint32_t api_id, const auth_map& auth)>;

    // Empty auth sees every publication. Auth entry "hidden" is packed uint32 LE ids to hide.
    inline bool default_visibility(std::uint32_t api_id, const auth_map& auth) {
      auto it = auth.find("hidden");
      if(it == auth.end()) {
        return true;
      }
      const binary& raw = it->second;
      for(std::size_t i = 0; i + 4 <= raw.size(); i += 4) {
        std::uint32_t hid =
          static_cast<std::uint32_t>(raw[i]) |
          (static_cast<std::uint32_t>(raw[i + 1]) << 8) |
          (static_cast<std::uint32_t>(raw[i + 2]) << 16) |
          (static_cast<std::uint32_t>(raw[i + 3]) << 24);
        if(hid == api_id) {
          return false;
        }
      }
      return true;
    }

    inline binary hash_to_le_bytes(std::uint64_t hash) {
      binary out(8);
      for(int i = 0; i < 8; ++i) {
        out[static_cast<std::size_t>(i)] = static_cast<std::uint8_t>(hash >> (8 * i));
      }
      return out;
    }

  }
}
