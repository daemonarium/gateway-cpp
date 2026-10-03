#pragma once

#include "gateway/endpoint_ref.hpp"
#include "gateway/frame_io.hpp"
#include "gateway/inner_rpc_error.hpp"
#include "gateway/session.hpp"
#include "gateway/session_rpc.hpp"

#include "serialization/socket_rpc_server.hpp"

#include <map>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace waos {
  namespace gateway {

    struct bound_publication {
      std::uint32_t api_id = 0;
      std::string name;
      binary description_blob;
      std::shared_ptr<endpoint_ref> endpoint;
    };

    inline std::vector<api_publication> catalog_from(const std::vector<bound_publication>& pubs) {
      std::vector<api_publication> out;
      out.reserve(pubs.size());
      for(const auto& p : pubs) {
        out.push_back(api_publication{p.api_id, p.name, p.description_blob});
      }
      return out;
    }

    template <typename Codec = serialization::flat_materialized_codec,
              typename Transport = serialization::socket_c_transport>
    class session_server {
      public:
        using rpc_type = serialization::socket_rpc_server<Codec, Transport>;

        session_server(
          Transport transport,
          std::vector<bound_publication> pubs,
          visibility_filter filter = default_visibility,
          std::optional<handshake_data> handshake = std::nullopt
        )
          : sess_(catalog_from(pubs), std::move(filter), std::move(handshake)),
            rpc_(std::move(transport)) {
          for(auto& p : pubs) {
            if(p.endpoint) {
              endpoints_[p.api_id] = std::move(p.endpoint);
            }
          }
          attach_session(rpc_, sess_);
          rpc_.template on_message<api_message>(
            std::function<void(api_message&, size_t)>([this](api_message& msg, size_t payload_size) {
              on_api_message(msg, payload_size);
            })
          );
          bind_endpoint_handlers();
        }

        explicit session_server(
          std::uint32_t socket_id,
          std::vector<bound_publication> pubs,
          visibility_filter filter = default_visibility,
          std::optional<handshake_data> handshake = std::nullopt
        )
          requires std::is_same_v<Transport, serialization::socket_c_transport>
          : session_server(serialization::socket_c_transport(socket_id), std::move(pubs), std::move(filter), std::move(handshake)) {}

        void handle_data() {
          rpc_.handle_data();
        }

        rpc_type& rpc() { return rpc_; }
        session& sess() { return sess_; }

      private:
        session sess_;
        std::map<std::uint32_t, std::shared_ptr<endpoint_ref>> endpoints_;
        rpc_type rpc_;

        void bind_endpoint_handlers() {
          for(auto& [id, ep] : endpoints_) {
            const std::uint32_t api_id = id;
            ep->on_message([this, api_id](binary payload) {
              if(!sess_.ready()) {
                return;
              }
              auto& writer = rpc_.channel().begin_send(api_message{api_id});
              if(!payload.empty()) {
                writer.write_raw(payload.data(), payload.size());
              }
              rpc_.channel().commit_send();
            });
          }
        }

        void on_api_message(api_message& msg, size_t payload_size) {
          binary payload(payload_size);
          if(payload_size > 0) {
            rpc_.channel().reader().read_raw(payload.data(), payload_size);
          }
          if(!peek_inner_rpc_request_id(payload)) {
            return;
          }
          const char* err = nullptr;
          if(sess_.failed()) {
            err = error_unsupported_protocol_version;
          } else if(!sess_.ready()) {
            err = error_handshake_required;
          } else if(msg.api_id == session_api_id || !sess_.is_visible(msg.api_id)) {
            err = error_unknown_api;
          } else {
            auto it = endpoints_.find(msg.api_id);
            if(it == endpoints_.end() || !it->second) {
              err = error_unknown_api;
            } else {
              it->second->send_message(std::move(payload));
              return;
            }
          }
          reply_inner_error(msg.api_id, payload, err);
        }

        void reply_inner_error(std::uint32_t api_id, const binary& inner_request, const char* message) {
          auto request_id = peek_inner_rpc_request_id(inner_request);
          if(!request_id) {
            return;
          }
          binary err_frame = encode_inner_rpc_error(*request_id, message);
          auto& writer = rpc_.channel().begin_send(api_message{api_id});
          if(!err_frame.empty()) {
            writer.write_raw(err_frame.data(), err_frame.size());
          }
          rpc_.channel().commit_send();
        }
    };

  }
}
