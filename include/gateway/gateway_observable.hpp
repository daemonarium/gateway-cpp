#pragma once

#include "gateway/types.hpp"

#include "observable/observable.hpp"
#include "observable/remote_reference.hpp"
#include "observable/stream_receiver.hpp"
#include "observable/types.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace waos {
  namespace gateway {

    enum class gateway_observe_mode { observe, fetch };

    class GatewayObservable
      : public waos::observable::Observable<waos::observable::BinaryEntry>,
        public waos::observable::StreamReceiver {
      public:
        using entry_type = waos::observable::BinaryEntry;

        GatewayObservable(
          std::shared_ptr<waos::observable::ReferenceOps> ops,
          std::function<void(waos::observable::StreamReceiver*)> attach,
          gateway_observe_mode mode,
          std::function<void(bool)> set_paused = {},
          std::function<void()> detach = {}
        )
          : ops_(std::move(ops)),
            attach_(std::move(attach)),
            mode_(mode),
            set_paused_(std::move(set_paused)),
            detach_(std::move(detach)) {}

        std::optional<std::uint64_t> last_wire_stream_id() const {
          return last_wire_stream_id_;
        }

        void onStreamOpened(waos::stream_opened&) override {}

        void onStreamItem(waos::stream_item& msg) override {
          last_wire_stream_id_ = msg.stream_id;
          entry_type entry;
          entry.key = std::move(msg.key);
          entry.value = std::move(msg.value);
          entry.ts = msg.timestamp;
          onItem(std::move(entry));
        }

        void onStreamDone(waos::stream_done&) override {
          onDone();
        }

        void onStreamReset(waos::stream_reset&) override {
          onReset();
        }

        void onStreamError(waos::stream_error& msg) override {
          onError(std::make_exception_ptr(std::runtime_error(msg.message)));
        }

      protected:
        void onStart() override {
          if(attach_) attach_(this);
          attached_ = true;
          if(!ops_) return;
          if(mode_ == gateway_observe_mode::observe) ops_->observe(this);
          else ops_->fetch(this);
        }

        void onStop() override {
          if(ops_ && mode_ == gateway_observe_mode::observe) ops_->unobserve();
          if(attached_) {
            if(detach_) detach_();
            else if(attach_) attach_(nullptr);
            attached_ = false;
          }
        }

        void onPause() override {
          if(set_paused_) set_paused_(true);
        }

        void onResume() override {
          if(set_paused_) set_paused_(false);
        }

      private:
        std::shared_ptr<waos::observable::ReferenceOps> ops_;
        std::function<void(waos::observable::StreamReceiver*)> attach_;
        gateway_observe_mode mode_ = gateway_observe_mode::observe;
        std::function<void(bool)> set_paused_;
        std::function<void()> detach_;
        bool attached_ = false;
        std::optional<std::uint64_t> last_wire_stream_id_;
    };

  }
}
