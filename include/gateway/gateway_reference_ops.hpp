#pragma once

#include "gateway/expr.hpp"
#include "observable/remote_reference.hpp"
#include "observable/stream_receiver.hpp"
#include "serialization/promise.hpp"

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
          std::function<std::shared_ptr<serialization::promise<void>>()> observe,
          std::function<std::shared_ptr<serialization::promise<void>>()> fetch,
          std::function<std::shared_ptr<serialization::promise<void>>()> unobserve,
          std::function<std::shared_ptr<serialization::promise<void>>()> dispose
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

        void observe(waos::observable::StreamReceiver*) override {
          if(observe_) observe_();
        }

        void fetch(waos::observable::StreamReceiver*) override {
          if(fetch_) fetch_();
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
        std::function<std::shared_ptr<serialization::promise<void>>()> observe_;
        std::function<std::shared_ptr<serialization::promise<void>>()> fetch_;
        std::function<std::shared_ptr<serialization::promise<void>>()> unobserve_;
        std::function<std::shared_ptr<serialization::promise<void>>()> dispose_;
        bool sent_ = false;
    };

  }
}
