#pragma once

#include "gateway/endpoint_ref.hpp"
#include "gateway/expr.hpp"
#include "gateway/frame_io.hpp"
#include "gateway/inner_rpc_error.hpp"
#include "gateway/session.hpp"
#include "gateway/session_rpc.hpp"

#include "examples/structures/stream_protocol.hpp"
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
          : state_(std::make_shared<session_state>(catalog_from(pubs), std::move(filter), std::move(handshake))),
            sess_(state_),
            rpc_(std::move(transport)) {
          for(auto& p : pubs) {
            if(p.endpoint) {
              endpoints_[p.api_id] = std::move(p.endpoint);
            }
          }
          sess_.set_observation_hooks(
            [this](std::uint64_t ref_id, const expr& value) { on_set_reference(ref_id, value); },
            [this](std::uint64_t ref_id) { on_observe_reference(ref_id); },
            [this](std::uint64_t ref_id) { on_unobserve_reference(ref_id); },
            [this](std::uint64_t ref_id) { on_dispose_reference(ref_id); },
            [this](std::uint64_t ref_id) { on_fetch_reference(ref_id); }
          );
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
        session_api& sess() { return sess_; }
        session_state& state() { return *state_; }

      private:
        struct ref_slot {
          std::uint64_t stream_id = 0;
          std::uint32_t api_id = 0;
          std::uint64_t function_id = 0;
          std::vector<literal_expr> literals;
          bool observed = false;
          bool scan = false;
          std::vector<binary> pending;
        };

        std::shared_ptr<session_state> state_;
        session_api sess_;
        std::map<std::uint32_t, std::shared_ptr<endpoint_ref>> endpoints_;
        std::map<std::uint64_t, ref_slot> refs_;
        std::map<std::uint64_t, std::uint64_t> stream_to_ref_;
        std::uint64_t next_stream_id_ = 1;
        rpc_type rpc_;

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
              auto ref_it = stream_to_ref_.find(*stream_id);
              if(ref_it == stream_to_ref_.end()) {
                return;
              }
              auto slot_it = refs_.find(ref_it->second);
              if(slot_it == refs_.end()) {
                return;
              }
              binary rewritten = rewrite_inner_stream_id(std::move(payload), ref_it->second);
              if(!slot_it->second.observed) {
                slot_it->second.pending.push_back(rewritten);
              } else {
                send_api_message(api_id, rewritten);
              }
              if(slot_it->second.scan
                  && (*type_index == inner_stream_done || *type_index == inner_stream_error)) {
                complete_scan(slot_it->second);
              }
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

        void on_set_reference(std::uint64_t ref_id, const expr& value) {
          direct_call call = validate_direct_expr(value);
          drop_ref(ref_id);
          ref_slot slot;
          slot.stream_id = 0;
          slot.api_id = call.api_id;
          slot.function_id = call.function_id;
          slot.literals = call.literals;
          slot.observed = false;
          slot.scan = false;
          refs_[ref_id] = std::move(slot);
        }

        void on_observe_reference(std::uint64_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return;
          }
          start_ref_stream(ref_id, it->second, stream_flags::observe);
          it->second.scan = false;
          it->second.observed = true;
          flush_pending(it->second);
        }

        void on_fetch_reference(std::uint64_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return;
          }
          start_ref_stream(ref_id, it->second, stream_flags::none);
          it->second.observed = true;
          flush_pending(it->second);
        }

        void on_unobserve_reference(std::uint64_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return;
          }
          it->second.observed = false;
        }

        void on_dispose_reference(std::uint64_t ref_id) {
          drop_ref(ref_id);
        }

        void start_ref_stream(std::uint64_t ref_id, ref_slot& slot, stream_flags flags) {
          if(slot.stream_id != 0) {
            return;
          }
          auto it = endpoints_.find(slot.api_id);
          if(it == endpoints_.end() || !it->second) {
            throw std::runtime_error(error_unknown_api);
          }
          slot.stream_id = next_stream_id_++;
          slot.scan = flags == stream_flags::none;
          stream_to_ref_[slot.stream_id] = ref_id;
          it->second->start_stream(slot.stream_id, slot.function_id, slot.literals, flags);
        }

        void flush_pending(ref_slot& slot) {
          std::vector<binary> queued = std::move(slot.pending);
          slot.pending.clear();
          for(const binary& frame : queued) {
            send_api_message(slot.api_id, frame);
          }
        }

        void complete_scan(ref_slot& slot) {
          if(slot.stream_id == 0) {
            return;
          }
          stream_to_ref_.erase(slot.stream_id);
          slot.stream_id = 0;
          slot.scan = false;
        }

        void drop_ref(std::uint64_t ref_id) {
          auto it = refs_.find(ref_id);
          if(it == refs_.end()) {
            return;
          }
          ref_slot slot = std::move(it->second);
          refs_.erase(it);
          if(slot.stream_id != 0) {
            stream_to_ref_.erase(slot.stream_id);
            auto ep = endpoints_.find(slot.api_id);
            if(ep != endpoints_.end() && ep->second) {
              ep->second->cancel_stream(slot.stream_id);
            }
          }
        }

        void forward_cancel(std::uint32_t api_id, const binary& payload) {
          auto ref_id = read_inner_stream_id(payload);
          if(!ref_id) {
            return;
          }
          auto it = refs_.find(*ref_id);
          if(it == refs_.end() || it->second.api_id != api_id || it->second.stream_id == 0) {
            return;
          }
          auto ep = endpoints_.find(api_id);
          if(ep == endpoints_.end() || !ep->second) {
            return;
          }
          ep->second->send_message(rewrite_inner_stream_id(payload, it->second.stream_id));
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
