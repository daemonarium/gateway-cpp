#pragma once

#include "gateway/types.hpp"

#include "observable/observable.hpp"
#include "observable/stream_receiver.hpp"
#include "observable/types.hpp"
#include "serialization/promise.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace waos {
  namespace gateway {

    struct path {
      std::uint32_t api_id = 0;
      std::uint64_t function_id = 0;
      std::vector<binary> args;

      bool operator<(const path& other) const {
        if(api_id != other.api_id) {
          return api_id < other.api_id;
        }
        if(function_id != other.function_id) {
          return function_id < other.function_id;
        }
        return args < other.args;
      }
    };

    struct reference_ops {
      std::function<std::shared_ptr<serialization::promise<void>>(std::uint64_t)> observe;
      std::function<std::shared_ptr<serialization::promise<void>>(std::uint64_t)> unobserve;
      std::function<std::shared_ptr<serialization::promise<void>>(std::uint64_t)> dispose;
    };

    // One ref_id. v1.5 has a single path → one Observable.
    // Wire stream_id is the ref_id after the gateway rewrite.
    class reference
      : public waos::observable::StreamReceiver,
        public std::enable_shared_from_this<reference> {
      public:
        using entry_type = waos::observable::BinaryEntry;
        using observable_type = waos::observable::Observable<entry_type>;

        reference(
          std::uint64_t ref_id,
          path key,
          std::function<void(waos::observable::StreamReceiver*)> attach,
          reference_ops ops
        )
          : ref_id_(ref_id),
            path_(std::move(key)),
            attach_(std::move(attach)),
            ops_(std::move(ops)) {
          by_path_.emplace(path_, &obs_);
          waos::observable::DispatchCallbacks sink;
          sink.onChange = [](waos::observable::Change) {
            return serialization::promise<void>::resolved();
          };
          sink.onReady = [] {};
          sink.onReset = [] {};
          sink.onError = [](std::exception_ptr) {};
          sink.onClosed = [] {};
          // Keep a strong ref. ConsumerOutput only stores a weak_ptr, and a
          // dead subscriber drops the observable back to idle so onDone no-ops.
          obs_.subscribe(anchor_, std::move(sink), 0);
        }

        std::uint64_t ref_id() const { return ref_id_; }
        const path& key() const { return path_; }
        std::optional<std::uint64_t> last_wire_stream_id() const { return last_wire_stream_id_; }

        std::map<path, observable_type*>& observables() { return by_path_; }
        observable_type& sum() { return obs_; }

        std::shared_ptr<serialization::promise<void>> observe() {
          attach();
          return ops_.observe(ref_id_);
        }

        std::shared_ptr<serialization::promise<void>> unobserve() {
          return ops_.unobserve(ref_id_);
        }

        // Observe, then unobserve once the snapshot is ready.
        observable_type& fetch() {
          observe();
          auto self = weak_from_this();
          obs_.readyPromise()->on_resolved([self]() {
            if(auto locked = self.lock()) {
              locked->unobserve();
            }
          });
          return obs_;
        }

        std::shared_ptr<serialization::promise<void>> dispose() {
          detach();
          auto self = shared_from_this();
          auto pending = ops_.dispose(ref_id_);
          pending->on_resolved([self]() {
            if(self->obs_.state() != waos::observable::ObservableState::closed) {
              self->obs_.kill();
            }
          });
          return pending;
        }

        void onStreamItem(waos::stream_item& msg) override {
          last_wire_stream_id_ = msg.stream_id;
          entry_type entry;
          entry.key = std::move(msg.key);
          entry.value = std::move(msg.value);
          entry.ts = msg.timestamp;
          obs_.onItem(std::move(entry));
        }

        void onStreamDone(waos::stream_done&) override {
          obs_.onDone();
        }

        void onStreamReset(waos::stream_reset&) override {
          obs_.onReset();
        }

        void onStreamError(waos::stream_error& msg) override {
          obs_.onError(std::make_exception_ptr(std::runtime_error(msg.message)));
        }

      private:
        std::uint64_t ref_id_ = 0;
        path path_;
        std::function<void(waos::observable::StreamReceiver*)> attach_;
        reference_ops ops_;
        observable_type obs_;
        std::shared_ptr<int> anchor_ = std::make_shared<int>(0);
        std::map<path, observable_type*> by_path_;
        bool attached_ = false;
        std::optional<std::uint64_t> last_wire_stream_id_;

        void attach() {
          if(attached_) {
            return;
          }
          attached_ = true;
          if(attach_) {
            attach_(this);
          }
        }

        void detach() {
          if(!attached_) {
            return;
          }
          attached_ = false;
          if(attach_) {
            attach_(nullptr);
          }
        }
      };

  }
}
