#pragma once

#include "gateway/expr.hpp"
#include "observable/remote_reference.hpp"
#include "observable/stream_receiver.hpp"
#include "serialization/promise.hpp"

#include "examples/structures/stream_protocol.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

namespace waos {
  namespace gateway {

    class GatewayReferenceOps : public waos::observable::ReferenceOps {
      public:
        GatewayReferenceOps(
          std::function<void()> set,
          std::function<void(waos::observable::StreamReceiver*, std::uint64_t)> observe,
          std::function<void(waos::observable::StreamReceiver*, waos::stream_flags, std::uint64_t)> fetch,
          std::function<void()> unobserve,
          std::function<void()> dispose
        )
          : set_(std::move(set)),
            observe_(std::move(observe)),
            fetch_(std::move(fetch)),
            unobserve_(std::move(unobserve)),
            dispose_(std::move(dispose)) {}

        bool persistent() const override { return false; }

        void setReference() override {
          if(sent_ || !set_) return;
          sent_ = true;
          set_();
        }

        void observe(waos::observable::StreamReceiver* receiver, std::uint64_t since) override {
          if(observe_) observe_(receiver, since);
        }

        void fetch(
          waos::observable::StreamReceiver* receiver,
          waos::stream_flags flags,
          std::uint64_t since
        ) override {
          if(fetch_) fetch_(receiver, flags, since);
        }

        void unobserve() override {
          if(unobserve_) unobserve_();
        }

        void dispose() override {
          sent_ = false;
          if(dispose_) dispose_();
        }

      private:
        std::function<void()> set_;
        std::function<void(waos::observable::StreamReceiver*, std::uint64_t)> observe_;
        std::function<void(waos::observable::StreamReceiver*, waos::stream_flags, std::uint64_t)> fetch_;
        std::function<void()> unobserve_;
        std::function<void()> dispose_;
        bool sent_ = false;
    };

  }
}
