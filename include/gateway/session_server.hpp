#pragma once

#include "gateway/endpoint_ref.hpp"
#include "gateway/expr.hpp"
#include "gateway/frame_io.hpp"
#include "gateway/inner_rpc_error.hpp"
#include "gateway/session.hpp"
#include "gateway/session_rpc.hpp"

#include "examples/structures/stream_protocol.hpp"
#include "observable/socket_mux.hpp"
#include "serialization/compressed_codec.hpp"
#include "serialization/socket_rpc_server.hpp"

#include <map>
#include <memory>
#include <optional>
#include <set>
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

    template <typename Codec = serialization::compressed_codec,
              typename Transport = serialization::socket_c_transport>
    class session_server {
      public:
        using rpc_type = serialization::socket_rpc_server<Codec, Transport>;
        using mux_type = waos::observable::SocketMux<typename rpc_type::channel_type>;

        session_server(
          Transport transport,
          std::vector<bound_publication> pubs,
          visibility_filter filter = default_visibility,
          std::optional<handshake_data> handshake = std::nullopt
        )
          : state_(std::make_shared<session_state>(catalog_from(pubs), std::move(filter), std::move(handshake))),
            sess_(state_),
            rpc_(std::move(transport)) {
          for(auto& p : pubs) {
            if(p.endpoint) {
              endpoints_[p.api_id] = std::move(p.endpoint);
            }
          }
          sess_.set_observation_hooks(
            [this](std::uint32_t ref_id, const expr& value) { on_set_reference(ref_id, value); },
            [this](std::uint32_t ref_id) { return on_observe_reference(ref_id); },
            [this](std::uint32_t ref_id) { on_unobserve_reference(ref_id); },
            [this](std::uint32_t ref_id) { on_dispose_reference(ref_id); },
            [this](std::uint32_t ref_id) { return on_fetch_reference(ref_id); },
            [this](std::uint32_t cookie) { return open_pipe(cookie); },
            [this](std::uint32_t socket_id) { close_pipe(socket_id); }
          );
          attach_session(rpc_, sess_);
          rpc_.template on_message<api_message>(
            std::function<void(api_message&, size_t)>([this](api_message& msg, size_t payload_size) {
              on_api_message(msg, payload_size);
            })
          );
          typename mux_type::Options opt;
          opt.on_data = [this](std::uint32_t id, const binary& payload) {
            on_mux_data(id, payload);
          };
          opt.on_close = [this](std::uint32_t id) { on_mux_close(id); };
          socket_mux_.emplace(rpc_.channel(), std::move(opt));
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

        void handle_drain() {
          if(socket_mux_) {
            socket_mux_->on_writable();
          }
        }

        rpc_type& rpc() { return rpc_; }
        mux_type& socket_mux() { return *socket_mux_; }
        session_api& sess() { return sess_; }
        session_state& state() { return *state_; }

        std::uint32_t open_pipe(std::uint32_t cookie) {
          const std::uint32_t socket = alloc_socket();
          pipes_.insert(socket);
          pipe_cookies_[socket] = cookie;
          if(socket_mux_) {
            socket_mux_->open_socket(socket, mux_kind_pipe, 0, cookie);
            socket_mux_->grant_window(socket, static_cast<std::uint32_t>(socket_mux_->default_window_bytes()));
          }
          return socket;
        }

        void close_pipe(std::uint32_t socket_id) {
          if(socket_mux_) socket_mux_->close_socket(socket_id);
          forget_socket(socket_id);
        }

        void send_pipe(std::uint32_t socket_id, binary payload) {
          if(socket_mux_) socket_mux_->send_data(socket_id, std::move(payload));
        }

        void on_pipe_data(std::function<void(std::uint32_t, const binary&)> handler) {
          on_pipe_data_ = std::move(handler);
        }

      private:
        struct ref_slot {
          std::uint32_t api_id = 0;
          std::uint64_t function_id = 0;
          std::vector<literal_expr> literals;
          std::uint32_t observe_socket = 0;
          std::set<std::uint32_t> fetch_sockets;
        };

        std::shared_ptr<session_state> state_;
        session_api sess_;
        std::map<std::uint32_t, std::shared_ptr<endpoint_ref>> endpoints_;
        std::map<std::uint32_t, ref_slot> refs_;
        std::map<std::uint64_t, std::uint32_t> stream_to_socket_;
        std::map<std::uint32_t, std::uint64_t> socket_to_stream_;
        std::map<std::uint32_t, std::uint32_t> socket_to_ref_;
        std::set<std::uint32_t> pipes_;
        std::map<std::uint32_t, std::uint32_t> pipe_cookies_;
        std::function<void(std::uint32_t, const binary&)> on_pipe_data_;
        std::uint32_t next_socket_id_ = 1;
        std::uint64_t next_stream_id_ = 1;
        rpc_type rpc_;
        std::optional<mux_type> socket_mux_;

        std::uint32_t alloc_socket() {
          std::uint32_t id = next_socket_id_++;
          if(id == 0) id = next_socket_id_++;
          return id;
        }

        void bind_endpoint_handlers() {
          for(auto& [id, ep] : endpoints_) {
            const std::uint32_t api_id = id;
            ep->on_message([this, api_id](binary payload) {
              if(!sess_.ready()) {
                return;
              }
              auto type_index = read_inner_type_index(payload);
              if(!type_index || !is_inner_stream_event(*type_index)) {
                send_api_message(api_id, payload);
                return;
              }
              auto stream_id = read_inner_stream_id(payload);
              if(!stream_id) {
                return;
              }
              auto sock_it = stream_to_socket_.find(*stream_id);
              if(sock_it == stream_to_socket_.end()) {
                return;
              }
              const std::uint32_t socket_id = sock_it->second;
              binary rewritten = rewrite_inner_stream_id(std::move(payload), socket_id);
              const bool complete_fetch =
                *type_index == inner_stream_done || *type_index == inner_stream_error;
              send_stream_frame(socket_id, std::move(rewritten), complete_fetch);
            });
          }
        }

        void on_api_message(api_message& msg, size_t payload_size) {
          binary payload(payload_size);
          if(payload_size > 0) {
            rpc_.channel().reader().read_raw(payload.data(), payload_size);
          }
          if(!is_forwarded_inner_frame(payload)) {
            return;
          }
          auto type_index = read_inner_type_index(payload);
          if(type_index && *type_index == inner_stream_cancel) {
            forward_cancel(msg.api_id, payload);
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

        void on_set_reference(std::uint32_t ref_id, const expr& value) {
          direct_call call = validate_direct_expr(value);
          drop_ref(ref_id);
          ref_slot slot;
          slot.api_id = call.api_id;
          slot.function_id = call.function_id;
          slot.literals = call.literals;
          refs_[ref_id] = std::move(slot);
        }

        std::uint32_t on_observe_reference(std::uint32_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return 0;
          }
          if(it->second.observe_socket != 0) {
            return it->second.observe_socket;
          }
          const std::uint32_t socket = alloc_socket();
          it->second.observe_socket = socket;
          socket_to_ref_[socket] = ref_id;
          if(socket_mux_) socket_mux_->open_socket(socket, mux_kind_observe, ref_id, 0);
          start_ref_stream(ref_id, it->second, socket, stream_flags::observe);
          return socket;
        }

        std::uint32_t on_fetch_reference(std::uint32_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return 0;
          }
          const std::uint32_t socket = alloc_socket();
          it->second.fetch_sockets.insert(socket);
          socket_to_ref_[socket] = ref_id;
          if(socket_mux_) socket_mux_->open_socket(socket, mux_kind_fetch, ref_id, 0);
          start_ref_stream(ref_id, it->second, socket, stream_flags::none);
          return socket;
        }

        void on_unobserve_reference(std::uint32_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return;
          }
          close_stream_socket(it->second.observe_socket, true);
          it->second.observe_socket = 0;
        }

        void on_dispose_reference(std::uint32_t ref_id) {
          drop_ref(ref_id);
        }

        void start_ref_stream(
          std::uint32_t ref_id,
          ref_slot& slot,
          std::uint32_t socket_id,
          stream_flags flags
        ) {
          auto it = endpoints_.find(slot.api_id);
          if(it == endpoints_.end() || !it->second) {
            throw std::runtime_error(error_unknown_api);
          }
          const std::uint64_t inner = next_stream_id_++;
          stream_to_socket_[inner] = socket_id;
          socket_to_stream_[socket_id] = inner;
          (void)ref_id;
          it->second->start_stream(inner, slot.function_id, slot.literals, flags);
        }

        void send_stream_frame(std::uint32_t socket_id, binary frame, bool complete_fetch) {
          if(!socket_mux_ || !socket_mux_->has_socket(socket_id)) {
            return;
          }
          auto sent = socket_mux_->send_data(socket_id, std::move(frame));
          if(complete_fetch) {
            sent->on_resolved([this, socket_id]() { complete_fetch_if_needed(socket_id); });
          }
        }

        void complete_fetch_if_needed(std::uint32_t socket_id) {
          auto ref_it = socket_to_ref_.find(socket_id);
          if(ref_it == socket_to_ref_.end()) return;
          auto slot_it = refs_.find(ref_it->second);
          if(slot_it == refs_.end()) return;
          if(slot_it->second.fetch_sockets.find(socket_id) == slot_it->second.fetch_sockets.end()) {
            return;
          }
          close_stream_socket(socket_id, false);
          slot_it->second.fetch_sockets.erase(socket_id);
        }

        void close_stream_socket(std::uint32_t socket_id, bool cancel_inner) {
          if(socket_id == 0) return;
          auto stream_it = socket_to_stream_.find(socket_id);
          std::uint64_t inner = 0;
          if(stream_it != socket_to_stream_.end()) {
            inner = stream_it->second;
            stream_to_socket_.erase(inner);
            socket_to_stream_.erase(stream_it);
          }
          auto ref_it = socket_to_ref_.find(socket_id);
          std::uint32_t api_id = 0;
          if(ref_it != socket_to_ref_.end()) {
            auto slot_it = refs_.find(ref_it->second);
            if(slot_it != refs_.end()) api_id = slot_it->second.api_id;
            socket_to_ref_.erase(ref_it);
          }
          if(socket_mux_) socket_mux_->close_socket(socket_id);
          if(cancel_inner && inner != 0) {
            auto ep = endpoints_.find(api_id);
            if(ep != endpoints_.end() && ep->second) {
              ep->second->cancel_stream(inner);
            }
          }
        }

        void drop_ref(std::uint32_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return;
          }
          ref_slot slot = std::move(it->second);
          refs_.erase(it);
          close_stream_socket(slot.observe_socket, true);
          for(std::uint32_t socket : slot.fetch_sockets) {
            close_stream_socket(socket, true);
          }
        }

        void forget_socket(std::uint32_t socket_id) {
          pipes_.erase(socket_id);
          pipe_cookies_.erase(socket_id);
        }

        void on_mux_data(std::uint32_t socket_id, const binary& payload) {
          if(pipes_.count(socket_id)) {
            if(on_pipe_data_) on_pipe_data_(socket_id, payload);
            if(socket_mux_ && !payload.empty()) {
              socket_mux_->grant_window(socket_id, static_cast<std::uint32_t>(payload.size()));
            }
          }
        }

        void on_mux_close(std::uint32_t socket_id) {
          forget_socket(socket_id);
        }

        void forward_cancel(std::uint32_t api_id, const binary& payload) {
          auto socket_id_opt = read_inner_stream_id(payload);
          if(!socket_id_opt) {
            return;
          }
          const std::uint32_t socket_id = static_cast<std::uint32_t>(*socket_id_opt);
          auto stream_it = socket_to_stream_.find(socket_id);
          if(stream_it == socket_to_stream_.end()) {
            return;
          }
          auto ref_it = socket_to_ref_.find(socket_id);
          if(ref_it == socket_to_ref_.end()) {
            return;
          }
          auto slot_it = refs_.find(ref_it->second);
          if(slot_it == refs_.end() || slot_it->second.api_id != api_id) {
            return;
          }
          auto ep = endpoints_.find(api_id);
          if(ep == endpoints_.end() || !ep->second) {
            return;
          }
          ep->second->send_message(rewrite_inner_stream_id(payload, stream_it->second));
        }

        void send_api_message(std::uint32_t api_id, const binary& payload) {
          auto& writer = rpc_.channel().begin_send(api_message{api_id});
          if(!payload.empty()) {
            writer.write_raw(payload.data(), payload.size());
          }
          rpc_.channel().commit_send();
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
