#pragma once

#include "gateway/api_connection.hpp"
#include "gateway/expr.hpp"
#include "gateway/generated/session_rpc_client.hpp"
#include "gateway/gateway_reference_ops.hpp"
#include "gateway/reference.hpp"
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
          : rpc_(std::make_shared<rpc_type>(std::move(transport))),
            api_(rpc_),
            handshake_(std::move(handshake)) {
          register_session_channel(*rpc_);
          rpc_->template on_message<api_message>(
            std::function<void(api_message&, size_t)>([this](api_message& msg, size_t payload_size) {
              binary payload(payload_size);
              if(payload_size > 0) {
                rpc_->channel().reader().read_raw(payload.data(), payload_size);
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
          rpc_->handle_data();
        }

        rpc_type& rpc() { return *rpc_; }

        std::shared_ptr<serialization::promise<server_protocol>> hello(client_protocol client) {
          return api_.hello(std::move(client));
        }

        std::shared_ptr<serialization::promise<std::vector<api_info>>> list_apis() {
          auto p = api_.list_apis();
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
          auto p = api_.get_rpc_description(api_id);
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
          return api_.get_session_description();
        }

        std::shared_ptr<reference> set_reference(
          std::uint64_t ref_id,
          expr value
        ) {
          direct_call call = validate_direct_expr(value);
          auto it = connections_.find(call.api_id);
          if(it == connections_.end()) {
            throw std::runtime_error("api not open");
          }
          return adopt_reference(ref_id, call, value, it->second.get());
        }

        std::shared_ptr<serialization::promise<void>> send_set_reference(
          std::uint64_t ref_id,
          expr value
        ) {
          return api_.set_reference(ref_id, value);
        }

        std::shared_ptr<serialization::promise<void>> observe_reference(std::uint64_t ref_id) {
          return api_.observe_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> unobserve_reference(std::uint64_t ref_id) {
          return api_.unobserve_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> fetch_reference(std::uint64_t ref_id) {
          return api_.fetch_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> dispose_reference(std::uint64_t ref_id) {
          refs_.erase(ref_id);
          return api_.dispose_reference(ref_id);
        }

        std::shared_ptr<reference> find_reference(std::uint64_t ref_id) const {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return nullptr;
          }
          return it->second;
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
        std::shared_ptr<rpc_type> rpc_;
        session_rpc<rpc_type> api_;
        std::optional<handshake_data> handshake_;
        std::map<std::uint32_t, binary> hashes_;
        std::map<std::uint32_t, std::pair<binary, binary>> blobs_;
        std::map<std::uint32_t, std::unique_ptr<api_connection>> connections_;
        std::map<std::uint64_t, std::shared_ptr<reference>> refs_;

        std::shared_ptr<reference> adopt_reference(
          std::uint64_t ref_id,
          const direct_call& call,
          expr value,
          api_connection* conn
        ) {
          std::vector<binary> args;
          args.reserve(call.literals.size());
          for(const literal_expr& lit : call.literals) {
            args.push_back(lit.value);
          }
          auto ops = std::make_shared<GatewayReferenceOps>(
            [this, ref_id, value]() { api_.set_reference(ref_id, value); },
            [this, ref_id]() { return observe_reference(ref_id); },
            [this, ref_id]() { return fetch_reference(ref_id); },
            [this, ref_id]() { return unobserve_reference(ref_id); },
            [this, ref_id]() { return api_.dispose_reference(ref_id); }
          );
          auto ref = std::make_shared<reference>(
            ref_id,
            path{call.api_id, call.function_id, std::move(args)},
            [conn, ref_id](waos::observable::StreamReceiver* receiver) {
              conn->mux().attachReceiver(ref_id, receiver);
            },
            std::move(ops)
          );
          refs_[ref_id] = ref;
          return ref;
        }

        api_connection& make_connection(std::uint32_t api_id) {
          auto conn = std::make_unique<api_connection>(api_id, [this, api_id](binary frame) {
            auto& writer = rpc_->channel().begin_send(api_message{api_id});
            if(!frame.empty()) {
              writer.write_raw(frame.data(), frame.size());
            }
            rpc_->channel().commit_send();
          });
          api_connection* ptr = conn.get();
          connections_[api_id] = std::move(conn);
          return *ptr;
        }
    };

  }
}
