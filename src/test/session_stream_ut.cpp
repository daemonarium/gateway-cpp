#include <boost/ut.hpp>

#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
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
    sockets.transfer(sockets.s1, sockets.s0);
    server.handle_data();
    sockets.transfer(sockets.s0, sockets.s1);
    client.handle_data();
  }

  std::string catch_rpc_error(std::exception_ptr ep) {
    try {
      std::rethrow_exception(ep);
    } catch(const std::exception& e) {
      return e.what();
    }
  }

  struct pair {
    socket_loopback_pair sockets;
    std::shared_ptr<stream_endpoint> ep;
    std::shared_ptr<stream_endpoint> ep2;
    std::unique_ptr<session_server<>> server;
    std::unique_ptr<session_client<>> client;

    explicit pair(std::size_t buffer_size = 4096) {
      ep = std::make_shared<stream_endpoint>(buffer_size);
      ep2 = std::make_shared<stream_endpoint>(buffer_size);
      binary blob = make_stream_blob();
      server = std::make_unique<session_server<>>(
        sockets.s0,
        std::vector<bound_publication>{
          bound_publication{1, "stream", blob, ep},
          bound_publication{2, "stream-b", blob, ep2}
        },
        default_visibility,
        client_protocol{protocol_version_v1, {}}
      );
      client = std::make_unique<session_client<>>(sockets.s1);
      client->open_api(1, blob);
      client->open_api(2, blob);
    }
  };

  struct Sink {
    bool* saw_reset = nullptr;
    std::uint64_t sinceTimestamp() const { return 0; }
    std::shared_ptr<serialization::promise<void>> onChange(Change) {
      return serialization::promise<void>::resolved();
    }
    void onReady() {}
    void onReset() { if(saw_reset) *saw_reset = true; }
    void onError(std::exception_ptr) {}
    void onClosed() {}
  };

  std::shared_ptr<Sink> start_live(reference& ref) {
    auto sink = std::make_shared<Sink>();
    ref.observable().subscribe(sink);
    return sink;
  }

}

int main() {
  "set_reference observe fills the observable with ref_id"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {}));
    auto sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    auto& obs = ref->sum();
    auto entries = obs.getEntries();
    expect(entries.size() == 1_ul);
    expect(u64_from_le(entries[0].key) == 0_ul);
    expect(u64_from_le(entries[0].value) == 1_ul);
    expect(obs.state() == ObservableState::ready);
    expect(ref->last_wire_stream_id().has_value());
    expect(*ref->last_wire_stream_id() == 7_ul);
    expect(ref->observables().size() == 1_ul);
  };

  "literal uint64 becomes the entry key"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(7)}));
    auto sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(u64_from_le(ref->sum().getEntries()[0].key) == 7_ul);
  };

  "fetch resolves entries then ends the scan stream"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(3)}));
    auto sink = std::make_shared<Sink>();
    auto obs = ref->stream();
    obs->subscribe(sink);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(obs->state() == ObservableState::ready);
    expect(u64_from_le(obs->getEntries()[0].key) == 3_ul);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    const std::size_t before = obs->getEntries().size();
    waos::observable::BinaryEntry extra;
    extra.key = u64_to_le(3);
    extra.value = u64_to_le(9);
    extra.ts = 9;
    sockets.ep->push(extra);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(obs->getEntries().size() == before);
  };

  "set_reference does not start a stream until subscribe"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {}));
    expect(ref->sum().getEntries().size() == 0_ul);
    expect(!ref->last_wire_stream_id().has_value());
    auto sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(ref->sum().getEntries().size() == 1_ul);
  };

  "observe after fetch starts a new observe stream"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(3)}));
    auto scan_sink = std::make_shared<Sink>();
    auto fetched = ref->stream();
    fetched->subscribe(scan_sink);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(fetched->state() == ObservableState::ready);
    const std::size_t before = fetched->getEntries().size();
    waos::observable::BinaryEntry extra;
    extra.key = u64_to_le(3);
    extra.value = u64_to_le(9);
    extra.ts = 9;
    sockets.ep->push(extra);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(fetched->getEntries().size() == before);
    auto live_sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    waos::observable::BinaryEntry live;
    live.key = u64_to_le(3);
    live.value = u64_to_le(11);
    live.ts = 11;
    sockets.ep->push(live);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(u64_from_le(ref->sum().getEntries()[0].value) == 11_ul);
  };

  "last unsubscribe drops and closes the observable"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {}));
    auto sink = std::make_shared<Sink>();
    auto out = ref->observable().subscribe(sink);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    out->unsubscribe();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(ref->peekLive() == nullptr || ref->peekLive()->state() == ObservableState::closed);
  };

  "rejects Ref arguments and non-call expressions"_test = [] {
    pair sockets;
    std::string dependent;
    try {
      sockets.client->set_reference(7, make_call(1, 0, {make_ref(3)}));
    } catch(...) {
      dependent = catch_rpc_error(std::current_exception());
    }
    expect(dependent == std::string(error_dependent_ref_not_supported));

    std::string member;
    try {
      sockets.client->set_reference(8, make_member(make_ref(1), 0));
    } catch(...) {
      member = catch_rpc_error(std::current_exception());
    }
    expect(member == std::string(error_unknown_expr));

    std::string switched;
    try {
      sockets.client->set_reference(9, make_switch(make_ref(1), {}));
    } catch(...) {
      switched = catch_rpc_error(std::current_exception());
    }
    expect(switched == std::string(error_unknown_expr));

    std::string leaf;
    try {
      sockets.client->set_reference(10, make_ref(1));
    } catch(...) {
      leaf = catch_rpc_error(std::current_exception());
    }
    expect(leaf == std::string(error_unknown_expr));
  };

  "stream_error and stream_reset reach the observable"_test = [] {
    pair sockets;
    auto failed = sockets.client->set_reference(7, make_call(1, 1, {}));
    auto fail_sink = start_live(*failed);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(failed->sum().state() == ObservableState::error);
    std::string message;
    try {
      std::rethrow_exception(failed->sum().lastError());
    } catch(const std::exception& e) {
      message = e.what();
    }
    expect(message.find("stream_failed") != std::string::npos);

    auto reset = sockets.client->set_reference(8, make_call(1, 2, {}));
    bool saw_reset = false;
    auto watcher = std::make_shared<Sink>();
    watcher->saw_reset = &saw_reset;
    reset->observable().subscribe(watcher);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(saw_reset == true);
    expect(reset->sum().state() == ObservableState::ready);
    expect(u64_from_le(reset->sum().getEntries()[0].value) == 2_ul);
  };

  "holds a change until the output buffer drains"_test = [] {
    pair sockets(1024);
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {}));
    auto sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    std::shared_ptr<serialization::promise<void>> blocked;
    for(int i = 0; i < 32 && !blocked; ++i) {
      BinaryEntry fat;
      fat.key = u64_to_le(static_cast<std::uint64_t>(i + 5));
      fat.value = binary(64, static_cast<std::uint8_t>(i));
      fat.ts = static_cast<std::uint64_t>(i + 5);
      auto pending = sockets.ep->push(fat);
      if(pending && pending->state == serialization::promise_state::pending) {
        blocked = pending;
      }
    }
    expect(blocked != nullptr);
    sockets.ep->pump();
    expect(blocked->state == serialization::promise_state::resolved);
  };

  "hold keeps the reference after last unsubscribe"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {}));
    auto holder = ref->hold();
    auto sink = std::make_shared<Sink>();
    auto out = ref->observable().subscribe(sink);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    out->unsubscribe();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(ref->peekLive() != nullptr);
    expect(ref->peekLive()->state() == ObservableState::idle);
    holder->dispose();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(ref->peekLive() == nullptr || ref->peekLive()->state() == ObservableState::closed);
  };

  "demuxes two refs and two api ids"_test = [] {
    pair sockets;
    auto a = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(1)}));
    auto b = sockets.client->set_reference(8, make_call(1, 3, {make_literal_u64(2)}));
    auto c = sockets.client->set_reference(9, make_call(2, 0, {make_literal_u64(3)}));
    auto sa = start_live(*a);
    auto sb = start_live(*b);
    auto sc = start_live(*c);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(u64_from_le(a->sum().getEntries()[0].key) == 1_ul);
    expect(b->sum().getEntries()[0].value.size() == 1_ul);
    expect(b->sum().getEntries()[0].value[0] == static_cast<std::uint8_t>(0xab));
    expect(u64_from_le(c->sum().getEntries()[0].key) == 3_ul);
  };
}
