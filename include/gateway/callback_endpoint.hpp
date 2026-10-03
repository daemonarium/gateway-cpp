#pragma once

#include "gateway/endpoint_ref.hpp"

namespace waos {
  namespace gateway {

    class callback_endpoint_ref : public endpoint_ref {
      public:
        void send_message(binary payload) override {
          if(sink_) {
            sink_(std::move(payload));
          }
        }

        void on_message(std::function<void(binary)> handler) override {
          handler_ = std::move(handler);
        }

        void set_sink(std::function<void(binary)> sink) {
          sink_ = std::move(sink);
        }

        void emit(binary payload) {
          if(handler_) {
            handler_(std::move(payload));
          }
        }

      private:
        std::function<void(binary)> sink_;
        std::function<void(binary)> handler_;
    };

  }
}
