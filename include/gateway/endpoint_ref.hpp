#pragma once

#include "gateway/types.hpp"

#include <functional>

namespace waos {
  namespace gateway {

    struct endpoint_ref {
      virtual ~endpoint_ref() = default;
      virtual void send_message(binary payload) = 0;
      virtual void on_message(std::function<void(binary payload)> handler) = 0;
    };

  }
}
