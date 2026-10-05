#pragma once

#include "gateway/echo_api.hpp"
#include "gateway/endpoint_ref.hpp"
#include "gateway/expr.hpp"
#include "gateway/frame_io.hpp"
#include "gateway/inner_stream.hpp"

#include "observable/observable.hpp"
#include "observable/stream_relay_consumer.hpp"
#include "serialization/promise.hpp"
#include "serialization/socket_rpc_client.hpp"
#include "serialization/socket_rpc_server.hpp"

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <stdexcept>
#include <utility>

extern "C" {
#include <io/circular_buffer.h>
}

namespace waos {
  namespace gateway {

    // Turns an inner echo_* RPC into stream_* frames on a private buffer.
    // The session server allocated the stream_id; this endpoint does not.
    class stream_endpoint : public endpoint_ref {
      public:
        using server_type = serialization::flat_buffer_socket_rpc_server;
        using channel_type = std::remove_reference_t<decltype(std::declval<server_type&>().channel())>;

        explicit stream_endpoint(std::size_t buffer_size = 4096)
          : buffers_(buffer_size),
            server_(serialization::buffer_transport(&buffers_.in, &buffers_.out)) {
          svc_.owner = this;
          server_.register_basic_types();
          server_.register_recursive(stream_handle{});
          server_.register_object_method(svc_, &echo_stream_service::echo_stream);
          server_.register_object_method(svc_, &echo_stream_service::echo_fail);
          server_.register_object_method(svc_, &echo_stream_service::echo_reset);
          server_.register_object_method(svc_, &echo_stream_service::echo_mark);
          server_.channel().template register_exception_type<std::runtime_error>();
          register_inner_stream_messages(server_);
          const std::size_t min_writable = buffer_size / 2 > 48 ? buffer_size / 2 : 48;
          server_.channel().set_min_writable_bytes(min_writable);
          server_.template on_message<stream_cancel>(
            std::function<void(stream_cancel&)>([this](stream_cancel& msg) {
              on_cancel(msg.stream_id);
            })
          );
        }

        stream_endpoint(const stream_endpoint&) = delete;
        stream_endpoint& operator=(const stream_endpoint&) = delete;

        binary description_blob() const {
          return make_stream_blob();
        }

        void send_message(binary payload) override {
          feed(payload, false);
        }

        void on_message(std::function<void(binary)> handler) override {
          handler_ = std::move(handler);
        }

        void start_stream(
          std::uint64_t stream_id,
          std::uint64_t function_id,
          const std::vector<literal_expr>& literals,
          stream_flags flags = stream_flags::none
        ) override {
          feed(encode_call(function_id, stream_id, literals, flags), true);
        }

        void cancel_stream(std::uint64_t stream_id) override {
          feed(encode_stream_cancel(stream_id), true);
        }

        // Extra change on the latest producer. Pending when the output buffer is tight.
        std::shared_ptr<serialization::promise<void>> push(const waos::observable::BinaryEntry& entry) {
          if(!last_) {
            throw std::runtime_error("no stream");
          }
          waos::observable::Change change;
          change.key = entry.key;
          change.value = entry.value;
          change.ts = entry.ts;
          return last_->relay->onChange(std::move(change));
        }

        void pump() {
          pump_frames(false);
        }

        void on_echo(stream_handle handle, std::uint64_t key, std::uint64_t count) {
          const std::uint64_t total = count == 0 ? 1 : count;
          producer& slot = open_producer(handle);
          const binary key_bytes = u64_to_le(key);
          for(std::uint64_t i = 0; i < total; ++i) {
            waos::observable::BinaryEntry entry;
            entry.key = key_bytes;
            entry.value = u64_to_le(i + 1);
            entry.ts = i + 1;
            slot.obs->onItem(std::move(entry));
          }
          slot.obs->onDone();
        }

        void on_fail(stream_handle handle) {
          producer& slot = open_producer(handle);
          slot.obs->onError(std::make_exception_ptr(std::runtime_error("stream_failed")));
        }

        void on_reset(stream_handle handle) {
          producer& slot = open_producer(handle);
          const binary key = u64_to_le(1);
          waos::observable::BinaryEntry first;
          first.key = key;
          first.value = u64_to_le(1);
          first.ts = 1;
          slot.obs->onItem(std::move(first));
          slot.obs->onReset();
          waos::observable::BinaryEntry second;
          second.key = key;
          second.value = u64_to_le(2);
          second.ts = 2;
          slot.obs->onItem(std::move(second));
          slot.obs->onDone();
        }

        void on_mark(stream_handle handle, std::uint64_t key) {
          producer& slot = open_producer(handle);
          waos::observable::BinaryEntry entry;
          entry.key = u64_to_le(key);
          entry.value = binary{0xab};
          entry.ts = 1;
          slot.obs->onItem(std::move(entry));
          slot.obs->onDone();
        }

      private:
        struct buffers {
          struct circular_buffer in {};
          struct circular_buffer out {};

          explicit buffers(std::size_t buffer_size) {
            circular_buffer_init(&in, buffer_size, nullptr);
            circular_buffer_init(&out, buffer_size, nullptr);
          }

          ~buffers() {
            circular_buffer_deinit(&in);
            circular_buffer_deinit(&out);
          }

          buffers(const buffers&) = delete;
          buffers& operator=(const buffers&) = delete;
        };

        struct producer {
          std::shared_ptr<waos::observable::Observable<waos::observable::BinaryEntry>> obs;
          std::shared_ptr<waos::observable::StreamRelayConsumer<channel_type>> relay;
          std::shared_ptr<waos::observable::ConsumerOutput> sub;
        };

        buffers buffers_;
        echo_stream_service svc_;
        server_type server_;
        std::map<std::uint64_t, producer> producers_;
        producer* last_ = nullptr;
        std::function<void(binary)> handler_;

        producer& open_producer(const stream_handle& handle) {
          auto& slot = producers_[handle.stream_id];
          slot.obs = std::make_shared<waos::observable::Observable<waos::observable::BinaryEntry>>();
          slot.relay = std::make_shared<waos::observable::StreamRelayConsumer<channel_type>>(
            server_.channel(),
            handle.stream_id,
            handle.since_timestamp
          );
          slot.sub = slot.obs->subscribe(slot.relay);
          last_ = &slot;
          server_.channel().template try_queue_message<stream_opened>(stream_opened{handle.stream_id});
          return slot;
        }

        void on_cancel(std::uint64_t stream_id) {
          auto it = producers_.find(stream_id);
          if(it == producers_.end()) {
            return;
          }
          if(it->second.sub && it->second.obs) {
            it->second.obs->unsubscribe(it->second.sub.get());
          }
          if(it->second.obs) {
            it->second.obs->kill();
          }
          if(last_ == &it->second) {
            last_ = nullptr;
          }
          producers_.erase(it);
        }

        binary encode_call(
          std::uint64_t function_id,
          std::uint64_t stream_id,
          const std::vector<literal_expr>& literals,
          stream_flags flags
        ) {
          struct circular_buffer in {};
          struct circular_buffer out {};
          circular_buffer_init(&in, 8192, nullptr);
          circular_buffer_init(&out, 8192, nullptr);
          serialization::flat_buffer_socket_rpc_client client(
            serialization::buffer_transport(&in, &out)
          );
          client.register_basic_types();
          stream_handle handle;
          handle.pid = 0;
          handle.socket = 0;
          handle.stream_id = stream_id;
          handle.since_timestamp = 0;
          handle.flags = static_cast<std::uint8_t>(flags);
          const std::uint64_t key = literals.empty() ? 0 : u64_from_le(literals[0].value);
          const std::uint64_t count = literals.size() < 2 ? 1 : u64_from_le(literals[1].value);
          if(function_id == 0) {
            client.template call<void>(0, handle, key, count);
          } else if(function_id == 1) {
            client.template call<void>(1, handle);
          } else if(function_id == 2) {
            client.template call<void>(2, handle);
          } else if(function_id == 3) {
            client.template call<void>(3, handle, key);
          } else {
            circular_buffer_deinit(&in);
            circular_buffer_deinit(&out);
            throw std::runtime_error(error_unknown_api);
          }
          binary frame = drain_one_frame(&out);
          circular_buffer_deinit(&in);
          circular_buffer_deinit(&out);
          return frame;
        }

        void feed(const binary& frame, bool internal) {
          write_all(&buffers_.in, frame.data(), frame.size());
          server_.handle_data();
          pump_frames(internal);
        }

        void pump_frames(bool internal) {
          for(int pass = 0; pass < 64; ++pass) {
            std::vector<binary> frames = drain_frames(&buffers_.out);
            for(binary& frame : frames) {
              auto type_index = read_inner_type_index(frame);
              if(internal && type_index && is_inner_rpc_result(*type_index)) {
                continue;
              }
              if(handler_) {
                handler_(std::move(frame));
              }
            }
            for(auto& [id, slot] : producers_) {
              (void)id;
              if(slot.relay) {
                slot.relay->onWritable();
              }
            }
            if(frames.empty() && circular_buffer_readable_bytes(&buffers_.out) == 0) {
              break;
            }
          }
        }
      };

    inline void echo_stream_service::echo_stream(stream_handle handle, std::uint64_t key, std::uint64_t count) {
      owner->on_echo(handle, key, count);
    }

    inline void echo_stream_service::echo_fail(stream_handle handle) {
      owner->on_fail(handle);
    }

    inline void echo_stream_service::echo_reset(stream_handle handle) {
      owner->on_reset(handle);
    }

    inline void echo_stream_service::echo_mark(stream_handle handle, std::uint64_t key) {
      owner->on_mark(handle, key);
    }

  }
}
