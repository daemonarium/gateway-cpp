#pragma once

#include "gateway/api_connection.hpp"
#include "gateway/expr.hpp"
#include "gateway/generated/session_rpc_client.hpp"
#include "gateway/gateway_reference_ops.hpp"
#include "gateway/reference.hpp"
#include "gateway/session_rpc.hpp"
#include "gateway/types.hpp"
#include "gateway/virtual_stream_socket.hpp"

#include "observable/socket_mux.hpp"
#include "serialization/promise.hpp"
#include "serialization/socket_rpc_client.hpp"

#include <map>
#include <memory>
#include <optional>
#include <set>
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
        using mux_type = waos::observable::SocketMux<typename rpc_type::channel_type>;
        using stream_mux_type = virtual_stream_socket::mux_type;

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
          typename mux_type::Options opt;
          opt.on_data = [this](std::uint64_t id, const binary& payload) {
            ensure_virtual(id).deliver(payload);
          };
          opt.on_open = [this](std::uint64_t id) { ensure_virtual(id); };
          opt.on_close = [this](std::uint64_t id) { on_mux_close(id); };
          socket_mux_.emplace(rpc_->channel(), std::move(opt));
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

        void handle_drain() {
          if(socket_mux_) socket_mux_->on_writable();
        }

        rpc_type& rpc() { return *rpc_; }
        mux_type& socket_mux() { return *socket_mux_; }

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
          return adopt_reference(ref_id, call, value);
        }

        std::shared_ptr<serialization::promise<void>> send_set_reference(
          std::uint64_t ref_id,
          expr value
        ) {
          return api_.set_reference(ref_id, value);
        }

        std::shared_ptr<serialization::promise<void>> observe_reference(std::uint64_t ref_id) {
          ensure_mux_socket(ref_id);
          return api_.observe_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> unobserve_reference(std::uint64_t ref_id) {
          return api_.unobserve_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> fetch_reference(std::uint64_t ref_id) {
          ensure_mux_socket(ref_id);
          return api_.fetch_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> dispose_reference(std::uint64_t ref_id) {
          if(socket_mux_) socket_mux_->close_socket(ref_id);
          on_mux_close(ref_id);
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

        stream_mux_type& get_stream_multiplexer(std::uint64_t ref_id) {
          ensure_mux_socket(ref_id);
          return virtuals_.at(ref_id)->mux();
        }

        void set_mux_paused(std::uint64_t ref_id, bool paused) {
          if(socket_mux_) socket_mux_->set_paused(ref_id, paused);
        }

        void detach_mux_receiver(std::uint64_t ref_id) {
          auto it = virtuals_.find(ref_id);
          if(it != virtuals_.end()) {
            it->second->mux().attachReceiver(ref_id, nullptr);
          }
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
        std::optional<mux_type> socket_mux_;
        std::map<std::uint64_t, std::unique_ptr<virtual_stream_socket>> virtuals_;
        std::set<std::uint64_t> granted_;

        virtual_stream_socket& ensure_virtual(std::uint64_t id) {
          auto it = virtuals_.find(id);
          if(it != virtuals_.end()) return *it->second;
          auto slot = std::make_unique<virtual_stream_socket>([this, id](std::size_t n) {
            if(socket_mux_) socket_mux_->grant_window(id, n);
          });
          virtual_stream_socket& ref = *slot;
          virtuals_[id] = std::move(slot);
          return ref;
        }

        void ensure_mux_socket(std::uint64_t ref_id) {
          ensure_virtual(ref_id);
          if(socket_mux_ && !socket_mux_->has_socket(ref_id)) {
            socket_mux_->open_socket(ref_id);
          }
          if(granted_.insert(ref_id).second && socket_mux_) {
            socket_mux_->grant_window(ref_id, socket_mux_->default_window_bytes());
          }
        }

        void on_mux_close(std::uint64_t id) {
          virtuals_.erase(id);
          granted_.erase(id);
        }

        std::shared_ptr<reference> adopt_reference(
          std::uint64_t ref_id,
          const direct_call& call,
          expr value
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
            [this, ref_id](waos::observable::StreamReceiver* receiver) {
              get_stream_multiplexer(ref_id).attachReceiver(ref_id, receiver);
            },
            std::move(ops),
            [this, ref_id](bool paused) { set_mux_paused(ref_id, paused); },
            [this, ref_id]() { detach_mux_receiver(ref_id); }
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
