#pragma once

#include "gateway/expr.hpp"
#include "gateway/gateway_observable.hpp"
#include "gateway/gateway_reference_ops.hpp"
#include "gateway/types.hpp"

#include "observable/observable.hpp"
#include "observable/remote_reference.hpp"
#include "observable/stream_receiver.hpp"
#include "observable/types.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
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

    // Thin handle. Creating a reference is local; observable() sends set_reference.
    class reference : public waos::observable::RemoteReference<waos::observable::BinaryEntry> {
      public:
        using entry_type = waos::observable::BinaryEntry;
        using observable_type = waos::observable::Observable<entry_type>;

        reference(
          std::uint64_t ref_id,
          path key,
          std::function<void(waos::observable::StreamReceiver*)> attach,
          std::shared_ptr<waos::observable::ReferenceOps> ops
        )
          : waos::observable::RemoteReference<entry_type>(
              ops,
              [ops, attach]() -> std::shared_ptr<observable_type> {
                return std::make_shared<GatewayObservable>(
                  ops, attach, gateway_observe_mode::observe);
              },
              [ops, attach]() -> std::shared_ptr<observable_type> {
                return std::make_shared<GatewayObservable>(
                  ops, attach, gateway_observe_mode::fetch);
              }
            ),
            ref_id_(ref_id),
            path_(std::move(key)) {}

        std::uint64_t ref_id() const { return ref_id_; }
        const path& key() const { return path_; }

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
        std::uint64_t ref_id_ = 0;
        path path_;
        std::map<path, observable_type*> by_path_;
    };

  }
}
