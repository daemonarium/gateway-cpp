#pragma once

#include "gateway/api_connection.hpp"
#include "gateway/session_rpc.hpp"
#include "gateway/types.hpp"

#include "serialization/promise.hpp"
#include "serialization/socket_rpc_client.hpp"

#include <map>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace waos {
  namespace gateway {

    template <typename Codec = serialization::flat_materialized_codec,
              typename Transport = serialization::socket_c_transport>
    class session_client {
      public:
        using rpc_type = serialization::socket_rpc_client<Codec, Transport>;

        explicit session_client(
          Transport transport,
          std::optional<handshake_data> handshake = std::nullopt
        )
          : rpc_(std::move(transport)), handshake_(std::move(handshake)) {
          register_session_channel(rpc_);
          rpc_.template on_message<api_message>(
            std::function<void(api_message&, size_t)>([this](api_message& msg, size_t payload_size) {
              binary payload(payload_size);
              if(payload_size > 0) {
                rpc_.channel().reader().read_raw(payload.data(), payload_size);
              }
              auto it = connections_.find(msg.api_id);
              if(it != connections_.end()) {
                it->second->deliver(payload);
              }
            })
          );
        }

        explicit session_client(
          std::uint32_t socket_id,
          std::optional<handshake_data> handshake = std::nullopt
        )
          requires std::is_same_v<Transport, serialization::socket_c_transport>
          : session_client(serialization::socket_c_transport(socket_id), std::move(handshake)) {}

        void handle_data() {
          rpc_.handle_data();
        }

        rpc_type& rpc() { return rpc_; }

        std::shared_ptr<serialization::promise<server_protocol>> hello(client_protocol client) {
          return rpc_.template call<server_protocol>(0, std::move(client));
        }

        std::shared_ptr<serialization::promise<std::vector<api_info>>> list_apis() {
          auto p = rpc_.template call<std::vector<api_info>>(1);
          p->on_resolved([this](std::vector<api_info>& infos) {
            hashes_.clear();
            for(const auto& info : infos) {
              hashes_[info.api_id] = info.description_hash;
            }
          });
          return p;
        }

        std::shared_ptr<serialization::promise<binary>> get_rpc_description(std::uint32_t api_id) {
          auto cached = blobs_.find(api_id);
          auto hash_it = hashes_.find(api_id);
          if(cached != blobs_.end() && hash_it != hashes_.end() && cached->second.first == hash_it->second) {
            auto hit = std::make_shared<serialization::promise<binary>>();
            hit->resolve(cached->second.second);
            return hit;
          }
          auto p = rpc_.template call<binary>(2, api_id);
          p->on_resolved([this, api_id](binary& blob) {
            binary hash;
            auto h = hashes_.find(api_id);
            if(h != hashes_.end()) {
              hash = h->second;
            }
            blobs_[api_id] = {std::move(hash), blob};
          });
          return p;
        }

        std::shared_ptr<serialization::promise<binary>> get_session_description() {
          return rpc_.template call<binary>(3);
        }

        std::shared_ptr<serialization::promise<api_connection*>> open_api(
          std::uint32_t api_id,
          std::optional<binary> blob = std::nullopt
        ) {
          auto out = std::make_shared<serialization::promise<api_connection*>>();
          auto existing = connections_.find(api_id);
          if(existing != connections_.end()) {
            out->resolve(existing->second.get());
            return out;
          }
          if(blob) {
            blobs_[api_id] = {binary{}, *blob};
            out->resolve(&make_connection(api_id));
            return out;
          }
          auto desc = get_rpc_description(api_id);
          desc->on_resolved([this, api_id, out](binary&) {
            out->resolve(&make_connection(api_id));
          });
          desc->on_rejected([out](std::exception_ptr ep) {
            out->reject(ep);
          });
          return out;
        }

        bool skip_hello() const {
          return handshake_.has_value();
        }

      private:
        rpc_type rpc_;
        std::optional<handshake_data> handshake_;
        std::map<std::uint32_t, binary> hashes_;
        std::map<std::uint32_t, std::pair<binary, binary>> blobs_;
        std::map<std::uint32_t, std::unique_ptr<api_connection>> connections_;

        api_connection& make_connection(std::uint32_t api_id) {
          auto conn = std::make_unique<api_connection>(api_id, [this, api_id](binary frame) {
            auto& writer = rpc_.channel().begin_send(api_message{api_id});
            if(!frame.empty()) {
              writer.write_raw(frame.data(), frame.size());
            }
            rpc_.channel().commit_send();
          });
          api_connection* ptr = conn.get();
          connections_[api_id] = std::move(conn);
          return *ptr;
        }
    };

  }
}
