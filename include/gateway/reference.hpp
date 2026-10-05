#pragma once

#include "examples/structures/stream_protocol.hpp"
#include "gateway/expr.hpp"
#include "gateway/gateway_observable.hpp"
#include "gateway/gateway_reference_ops.hpp"
#include "gateway/types.hpp"

#include "observable/observable.hpp"
#include "observable/remote_reference.hpp"
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

    struct opened_stream {
      std::uint32_t socket_id = 0;
      std::shared_ptr<waos::observable::Disposable> handle;
    };

    // Thin handle. Creating a reference is local; observable() sends set_reference.
    class reference : public waos::observable::RemoteReference<waos::observable::BinaryEntry> {
      public:
        using entry_type = waos::observable::BinaryEntry;
        using observable_type = waos::observable::Observable<entry_type>;
        using open_stream_fn = std::function<
          std::shared_ptr<serialization::promise<opened_stream>>(
            waos::observable::StreamReceiver*,
            waos::stream_flags,
            std::uint64_t
          )>;

        reference(
          std::uint32_t ref_id,
          path key,
          std::shared_ptr<waos::observable::ReferenceOps> ops,
          std::function<void(std::uint32_t, bool)> set_paused = {},
          std::function<void(std::uint32_t)> detach = {},
          open_stream_fn open_stream = {}
        )
          : waos::observable::RemoteReference<entry_type>(
              ops,
              [ops, set_paused, detach]() -> std::shared_ptr<observable_type> {
                return std::make_shared<GatewayObservable>(
                  ops, gateway_observe_mode::observe, set_paused, detach);
              },
              [ops, set_paused, detach](std::uint64_t since) -> std::shared_ptr<observable_type> {
                return std::make_shared<GatewayObservable>(
                  ops, gateway_observe_mode::fetch, set_paused, detach, since);
              }
            ),
            ref_id_(ref_id),
            path_(std::move(key)),
            open_stream_(std::move(open_stream)) {}

        std::uint32_t ref_id() const { return ref_id_; }
        const path& key() const { return path_; }

        std::shared_ptr<serialization::promise<opened_stream>> open_stream(
          waos::observable::StreamReceiver* receiver,
          waos::stream_flags flags = waos::stream_flags::none,
          std::uint64_t since = 0
        ) {
          bind();
          if(!open_stream_) {
            auto p = std::make_shared<serialization::promise<opened_stream>>();
            p->reject(std::make_exception_ptr(std::runtime_error("open_stream unavailable")));
            return p;
          }
          return open_stream_(receiver, flags, since);
        }

        std::optional<std::uint64_t> last_wire_stream_id() const {
          auto live = peekLive();
          auto* go = dynamic_cast<GatewayObservable*>(live.get());
          if(!go) return std::nullopt;
          return go->last_wire_stream_id();
        }

        std::map<path, observable_type*> observables() {
          by_path_[path_] = &observable();
          return by_path_;
        }

        observable_type& sum() { return observable(); }

      private:
        std::uint32_t ref_id_ = 0;
        path path_;
        std::map<path, observable_type*> by_path_;
        open_stream_fn open_stream_;
    };

  }
}
