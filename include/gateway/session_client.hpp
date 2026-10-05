#pragma once

#include "gateway/api_connection.hpp"
#include "gateway/expr.hpp"
#include "gateway/generated/session_rpc_client.hpp"
#include "gateway/gateway_observable.hpp"
#include "gateway/gateway_reference_ops.hpp"
#include "gateway/reference.hpp"
#include "gateway/session_rpc.hpp"
#include "gateway/types.hpp"
#include "gateway/virtual_stream_socket.hpp"

#include "observable/socket_mux.hpp"
#include "serialization/compressed_codec.hpp"
#include "serialization/promise.hpp"
#include "serialization/socket_rpc_client.hpp"

#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <type_traits>
#include <utility>
#include <vector>

namespace waos {
  namespace gateway {

    template <typename Codec = serialization::compressed_codec,
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
          opt.on_data = [this](std::uint32_t id, const binary& payload) {
            on_mux_data(id, payload);
          };
          opt.on_open = [this](std::uint32_t id, std::uint8_t kind, std::uint32_t ref_id, std::uint32_t cookie) {
            on_mux_open(id, kind, ref_id, cookie);
          };
          opt.on_close = [this](std::uint32_t id) { on_mux_close(id); };
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
          std::uint32_t ref_id,
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
          std::uint32_t ref_id,
          expr value
        ) {
          return api_.set_reference(ref_id, value);
        }

        std::shared_ptr<serialization::promise<std::uint32_t>> observe_reference(
          std::uint32_t ref_id,
          waos::observable::StreamReceiver* receiver = nullptr
        ) {
          if(receiver) pending_observe_[ref_id] = receiver;
          return api_.observe_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> unobserve_reference(std::uint32_t ref_id) {
          pending_observe_.erase(ref_id);
          return api_.unobserve_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<std::uint32_t>> fetch_reference(
          std::uint32_t ref_id,
          waos::observable::StreamReceiver* receiver = nullptr
        ) {
          if(receiver) pending_fetch_[ref_id].push_back(receiver);
          return api_.fetch_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<void>> dispose_reference(std::uint32_t ref_id) {
          pending_observe_.erase(ref_id);
          pending_fetch_.erase(ref_id);
          refs_.erase(ref_id);
          return api_.dispose_reference(ref_id);
        }

        std::shared_ptr<serialization::promise<std::uint32_t>> open_pipe(std::uint32_t cookie) {
          return api_.open_pipe(cookie);
        }

        std::shared_ptr<serialization::promise<void>> close_pipe(std::uint32_t socket_id) {
          if(socket_mux_) socket_mux_->close_socket(socket_id);
          on_mux_close(socket_id);
          return api_.close_pipe(socket_id);
        }

        void send_pipe(std::uint32_t socket_id, binary payload) {
          if(socket_mux_) socket_mux_->send_data(socket_id, std::move(payload));
        }

        void on_pipe(std::function<void(std::uint32_t, std::uint32_t)> handler) {
          on_pipe_ = std::move(handler);
        }

        void on_pipe_data(std::function<void(std::uint32_t, const binary&)> handler) {
          on_pipe_data_ = std::move(handler);
        }

        std::shared_ptr<reference> find_reference(std::uint32_t ref_id) const {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return nullptr;
          }
          return it->second;
        }

        stream_mux_type& get_stream_multiplexer(std::uint32_t socket_id) {
          return ensure_virtual(socket_id).mux();
        }

        void set_mux_paused(std::uint32_t socket_id, bool paused) {
          if(socket_mux_) socket_mux_->set_paused(socket_id, paused);
        }

        void detach_mux_receiver(std::uint32_t socket_id) {
          auto it = virtuals_.find(socket_id);
          if(it != virtuals_.end()) {
            it->second->mux().attachReceiver(socket_id, nullptr);
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
        struct SocketEntry {
          std::uint8_t kind = 0;
          std::uint32_t ref_id = 0;
          std::uint32_t cookie = 0;
        };

        std::shared_ptr<rpc_type> rpc_;
        session_rpc<rpc_type> api_;
        std::optional<handshake_data> handshake_;
        std::map<std::uint32_t, binary> hashes_;
        std::map<std::uint32_t, std::pair<binary, binary>> blobs_;
        std::map<std::uint32_t, std::unique_ptr<api_connection>> connections_;
        std::map<std::uint32_t, std::shared_ptr<reference>> refs_;
        std::optional<mux_type> socket_mux_;
        std::map<std::uint32_t, std::unique_ptr<virtual_stream_socket>> virtuals_;
        std::set<std::uint32_t> granted_;
        std::map<std::uint32_t, SocketEntry> entries_;
        std::map<std::uint32_t, waos::observable::StreamReceiver*> pending_observe_;
        std::map<std::uint32_t, std::deque<waos::observable::StreamReceiver*>> pending_fetch_;
        std::function<void(std::uint32_t, std::uint32_t)> on_pipe_;
        std::function<void(std::uint32_t, const binary&)> on_pipe_data_;

        virtual_stream_socket& ensure_virtual(std::uint32_t id) {
          auto it = virtuals_.find(id);
          if(it != virtuals_.end()) return *it->second;
          auto slot = std::make_unique<virtual_stream_socket>([this, id](std::size_t n) {
            if(socket_mux_) socket_mux_->grant_window(id, static_cast<std::uint32_t>(n));
          });
          virtual_stream_socket& ref = *slot;
          virtuals_[id] = std::move(slot);
          return ref;
        }

        void grant_if_needed(std::uint32_t id) {
          if(granted_.insert(id).second && socket_mux_) {
            socket_mux_->grant_window(id, static_cast<std::uint32_t>(socket_mux_->default_window_bytes()));
          }
        }

        void bind_receiver(std::uint32_t socket_id, waos::observable::StreamReceiver* receiver) {
          if(!receiver) return;
          ensure_virtual(socket_id).mux().attachReceiver(socket_id, receiver);
          if(auto* go = dynamic_cast<GatewayObservable*>(receiver)) {
            go->set_mux_socket_id(socket_id);
          }
        }

        void on_mux_open(
          std::uint32_t id,
          std::uint8_t kind,
          std::uint32_t ref_id,
          std::uint32_t cookie
        ) {
          entries_[id] = SocketEntry{kind, ref_id, cookie};
          grant_if_needed(id);
          if(kind == mux_kind_pipe) {
            if(on_pipe_) on_pipe_(id, cookie);
            return;
          }
          if(kind == mux_kind_observe) {
            auto it = pending_observe_.find(ref_id);
            if(it != pending_observe_.end()) {
              bind_receiver(id, it->second);
              pending_observe_.erase(it);
            }
            return;
          }
          if(kind == mux_kind_fetch) {
            auto it = pending_fetch_.find(ref_id);
            if(it != pending_fetch_.end() && !it->second.empty()) {
              bind_receiver(id, it->second.front());
              it->second.pop_front();
              if(it->second.empty()) pending_fetch_.erase(it);
            }
          }
        }

        void on_mux_data(std::uint32_t id, const binary& payload) {
          auto entry = entries_.find(id);
          if(entry != entries_.end() && entry->second.kind == mux_kind_pipe) {
            if(on_pipe_data_) on_pipe_data_(id, payload);
            if(socket_mux_ && !payload.empty()) {
              socket_mux_->grant_window(id, static_cast<std::uint32_t>(payload.size()));
            }
            return;
          }
          ensure_virtual(id).deliver(payload);
        }

        void on_mux_close(std::uint32_t id) {
          virtuals_.erase(id);
          granted_.erase(id);
          entries_.erase(id);
        }

        std::shared_ptr<reference> adopt_reference(
          std::uint32_t ref_id,
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
            [this, ref_id](waos::observable::StreamReceiver* receiver) {
              observe_reference(ref_id, receiver);
            },
            [this, ref_id](waos::observable::StreamReceiver* receiver) {
              fetch_reference(ref_id, receiver);
            },
            [this, ref_id]() { unobserve_reference(ref_id); },
            [this, ref_id]() { api_.dispose_reference(ref_id); }
          );
          auto ref = std::make_shared<reference>(
            ref_id,
            path{call.api_id, call.function_id, std::move(args)},
            std::move(ops),
            [this](std::uint32_t socket, bool paused) { set_mux_paused(socket, paused); },
            [this](std::uint32_t socket) { detach_mux_receiver(socket); }
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
