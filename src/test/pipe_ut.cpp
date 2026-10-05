#include <boost/ut.hpp>

#include <cstdint>
#include <memory>
#include <vector>

#include "test_socket_utils.hpp"

#include "gateway/expr.hpp"
#include "gateway/session_client.hpp"
#include "gateway/session_server.hpp"
#include "gateway/stream_endpoint.hpp"

#include "observable/observable.hpp"
#include "observable/types.hpp"
#include "serialization/promise.hpp"

using namespace boost::ut;
using namespace waos::gateway;
using namespace waos::observable;

namespace {

  void mux_roundtrip(socket_loopback_pair& sockets, auto& client, auto& server) {
    for(int i = 0; i < 16; ++i) {
      sockets.transfer(sockets.s1, sockets.s0);
      server.handle_data();
      server.handle_drain();
      sockets.transfer(sockets.s0, sockets.s1);
      client.handle_data();
      client.handle_drain();
      if(i >= 1
          && socket_output_readable(sockets.s0) == 0
          && socket_output_readable(sockets.s1) == 0) {
        break;
      }
    }
  }

  struct pair {
    socket_loopback_pair sockets;
    std::shared_ptr<stream_endpoint> ep;
    std::unique_ptr<session_server<>> server;
    std::unique_ptr<session_client<>> client;

    pair() {
      ep = std::make_shared<stream_endpoint>();
      binary blob = make_stream_blob();
      server = std::make_unique<session_server<>>(
        sockets.s0,
        std::vector<bound_publication>{
          bound_publication{1, "stream", blob, ep}
        },
        default_visibility,
        client_protocol{protocol_version_v1, {}}
      );
      client = std::make_unique<session_client<>>(sockets.s1);
      client->open_api(1, blob);
    }
  };

}

int main() {
  "client open_pipe echoes both directions"_test = [] {
    pair sockets;
    sockets.server->on_pipe_data([&](std::uint32_t id, const binary& payload) {
      sockets.server->send_pipe(id, payload);
    });
    binary got;
    sockets.client->on_pipe_data([&](std::uint32_t, const binary& payload) {
      got = payload;
    });
    std::uint32_t socket_id = 0;
    sockets.client->open_pipe(42)->on_resolved([&](std::uint32_t id) { socket_id = id; });
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(socket_id != 0_u);
    binary ping{1, 2, 3};
    sockets.client->send_pipe(socket_id, ping);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(got.size() == 3_ul);
    expect(got[0] == 1);
  };

  "server-initiated openPipe notifies the client"_test = [] {
    pair sockets;
    std::uint32_t seen = 0;
    std::uint32_t cookie = 0;
    sockets.client->on_pipe([&](std::uint32_t id, std::uint32_t c) {
      seen = id;
      cookie = c;
    });
    const std::uint32_t socket_id = sockets.server->open_pipe(9);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(seen == socket_id);
    expect(cookie == 9_u);
    binary got;
    sockets.client->on_pipe_data([&](std::uint32_t, const binary& payload) {
      got = payload;
    });
    sockets.server->send_pipe(socket_id, binary{7});
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(got.size() == 1_ul);
    expect(got[0] == 7);
  };

  "close_pipe cleans up"_test = [] {
    pair sockets;
    std::uint32_t socket_id = 0;
    sockets.client->open_pipe(1)->on_resolved([&](std::uint32_t id) { socket_id = id; });
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(socket_id != 0_u);
    sockets.client->close_pipe(socket_id);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(sockets.client->socket_mux().has_socket(socket_id) == false);
  };

  "paused observe does not block a pipe or session RPC"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {}));
    struct SlowSink {
      std::shared_ptr<serialization::promise<void>> stall =
        std::make_shared<serialization::promise<void>>();
      std::uint64_t sinceTimestamp() const { return 0; }
      std::shared_ptr<serialization::promise<void>> onChange(Change) { return stall; }
      void onReady() {}
      void onReset() {}
      void onError(std::exception_ptr) {}
      void onClosed() {}
    };
    auto slow = std::make_shared<SlowSink>();
    ref->observable().subscribe(slow, 1);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    binary got;
    sockets.client->on_pipe_data([&](std::uint32_t, const binary& payload) { got = payload; });
    sockets.server->on_pipe_data([&](std::uint32_t id, const binary& payload) {
      sockets.server->send_pipe(id, payload);
    });
    std::uint32_t socket_id = 0;
    sockets.client->open_pipe(3)->on_resolved([&](std::uint32_t id) { socket_id = id; });
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(socket_id != 0_u);
    sockets.client->send_pipe(socket_id, binary{4, 5});
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(got.size() == 2_ul);
    std::vector<api_info> infos;
    sockets.client->list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(infos.size() == 1_ul);
    slow->stall->resolve();
  };

  return 0;
}
