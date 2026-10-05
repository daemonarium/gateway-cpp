#include <boost/ut.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
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
#include "observable/stream_receiver.hpp"
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

  constexpr std::uint32_t kFrameLenSize = 4;
  constexpr std::uint64_t kRpcRequestType = 0;
  constexpr std::uint64_t kObserveReferenceFn = 5;
  constexpr std::uint64_t kFetchReferenceFn = 8;

  binary peek_output(std::uint32_t sock) {
    struct socket_info* info = get_socket_info(static_cast<int>(sock));
    const std::size_t n = circular_buffer_readable_bytes(&info->output_buffer);
    binary out(n);
    if(n > 0) {
      circular_buffer_peek(&info->output_buffer, 0, out.data(), n);
    }
    return out;
  }

  std::size_t read_uvarint(const binary& bytes, std::size_t off, std::uint64_t& value) {
    if(off >= bytes.size()) return 0;
    const std::uint8_t n0 = bytes[off];
    auto need = [&](std::size_t n) { return off + n <= bytes.size(); };
    if((n0 & 0x80) == 0) {
      value = n0;
      return 1;
    }
    if((n0 & 0x40) == 0 && need(2)) {
      value = (static_cast<std::uint64_t>(n0 & 0x3F) << 8) | bytes[off + 1];
      return 2;
    }
    return 0;
  }

  std::uint64_t rpc_function_id(const binary& frame) {
    std::size_t off = kFrameLenSize;
    std::uint64_t type = 0, request_id = 0, function_id = 0;
    off += read_uvarint(frame, off, type);
    off += read_uvarint(frame, off, request_id);
    read_uvarint(frame, off, function_id);
    return function_id;
  }

  std::vector<std::uint64_t> rpc_function_ids(const binary& bytes) {
    std::vector<std::uint64_t> ids;
    std::size_t off = 0;
    while(off + kFrameLenSize <= bytes.size()) {
      std::uint32_t len = 0;
      std::memcpy(&len, bytes.data() + off, sizeof(len));
      if(off + kFrameLenSize + len > bytes.size()) break;
      binary frame(bytes.begin() + static_cast<std::ptrdiff_t>(off),
        bytes.begin() + static_cast<std::ptrdiff_t>(off + kFrameLenSize + len));
      std::uint64_t type = 0;
      read_uvarint(frame, kFrameLenSize, type);
      if(type == kRpcRequestType) {
        ids.push_back(rpc_function_id(frame));
      }
      off += kFrameLenSize + len;
    }
    return ids;
  }

  bool has_rpc_fn(const binary& bytes, std::uint64_t fn) {
    const auto ids = rpc_function_ids(bytes);
    return std::find(ids.begin(), ids.end(), fn) != ids.end();
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
    expect(*ref->last_wire_stream_id() != 0_ul);
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

  "slow consumer on ref A does not block ref B or session RPC"_test = [] {
    pair sockets;
    auto a = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(1)}));
    auto b = sockets.client->set_reference(8, make_call(2, 0, {make_literal_u64(2)}));
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
    a->observable().subscribe(slow, 1);
    auto sb = start_live(*b);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(b->sum().state() == ObservableState::ready);
    BinaryEntry fat;
    fat.key = u64_to_le(11);
    fat.value = binary(64, 1);
    fat.ts = 11;
    sockets.ep->push(fat);
    fat.key = u64_to_le(12);
    fat.ts = 12;
    sockets.ep->push(fat);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    BinaryEntry live;
    live.key = u64_to_le(9);
    live.value = u64_to_le(9);
    live.ts = 9;
    sockets.ep2->push(live);
    sockets.ep2->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    bool saw_b = false;
    for(const auto& entry : b->sum().getEntries()) {
      if(u64_from_le(entry.key) == 9) saw_b = true;
    }
    expect(saw_b == true);
    std::vector<api_info> infos;
    sockets.client->list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(infos.size() == 2_ul);
    slow->stall->resolve();
    BinaryEntry resume;
    resume.key = u64_to_le(13);
    resume.value = u64_to_le(13);
    resume.ts = 13;
    sockets.ep->push(resume);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    bool saw_a = false;
    for(const auto& entry : a->sum().getEntries()) {
      if(u64_from_le(entry.key) == 13) saw_a = true;
    }
    expect(saw_a == true);
  };

  "observe and fetch coexist on the same ref"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(3)}));
    auto live_sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(ref->sum().state() == ObservableState::ready);
    const auto live_id = *ref->last_wire_stream_id();
    struct Rec : StreamReceiver {
      std::size_t items = 0;
      void onStreamOpened(waos::stream_opened&) override {}
      void onStreamItem(waos::stream_item&) override { ++items; }
      void onStreamKey(waos::stream_key&) override {}
      void onStreamDone(waos::stream_done&) override {}
      void onStreamError(waos::stream_error&) override {}
      void onStreamReset(waos::stream_reset&) override {}
    };
    auto rec = std::make_shared<Rec>();
    std::uint32_t fetch_id = 0;
    sockets.client->fetch_reference(7, rec.get())->on_resolved([&](std::uint32_t id) {
      fetch_id = id;
    });
    expect(has_rpc_fn(peek_output(sockets.sockets.s1), kFetchReferenceFn) == true);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(fetch_id != 0_u);
    expect(static_cast<std::uint64_t>(fetch_id) != live_id);
    expect(rec->items > 0_ul);
    expect(sockets.client->socket_mux().has_socket(fetch_id) == false);
    expect(sockets.client->socket_mux().has_socket(static_cast<std::uint32_t>(live_id)) == true);
    BinaryEntry live_extra;
    live_extra.key = u64_to_le(11);
    live_extra.value = u64_to_le(11);
    live_extra.ts = 11;
    sockets.ep->push(live_extra);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    bool saw_live = false;
    for(const auto& entry : ref->sum().getEntries()) {
      if(u64_from_le(entry.key) == 11) saw_live = true;
    }
    expect(saw_live == true);
  };

  "two fetches coexist"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(3)}));
    auto a_sink = std::make_shared<Sink>();
    auto b_sink = std::make_shared<Sink>();
    auto a = ref->stream();
    auto b = ref->stream();
    a->subscribe(a_sink);
    b->subscribe(b_sink);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(a.get() != b.get());
    expect(a->state() == ObservableState::ready);
    expect(b->state() == ObservableState::ready);
    auto live_sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(ref->sum().state() == ObservableState::ready);
    BinaryEntry extra;
    extra.key = u64_to_le(12);
    extra.value = u64_to_le(12);
    extra.ts = 12;
    sockets.ep->push(extra);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    bool saw = false;
    for(const auto& entry : ref->sum().getEntries()) {
      if(u64_from_le(entry.key) == 12) saw = true;
    }
    expect(saw == true);
  };

  "stream during live observe does not fetch"_test = [] {
    pair sockets;
    auto ref = sockets.client->set_reference(7, make_call(1, 0, {make_literal_u64(3)}));
    auto live_sink = start_live(*ref);
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    expect(ref->sum().state() == ObservableState::ready);
    const auto live_id = *ref->last_wire_stream_id();
    const std::size_t pending_before = socket_output_readable(sockets.sockets.s1);
    auto scan = ref->stream();
    expect(scan.get() == ref->peekLive().get());
    expect(socket_output_readable(sockets.sockets.s1) == pending_before);
    expect(has_rpc_fn(peek_output(sockets.sockets.s1), kFetchReferenceFn) == false);
    expect(has_rpc_fn(peek_output(sockets.sockets.s1), kObserveReferenceFn) == false);
    auto replay = std::make_shared<Sink>();
    scan->subscribe(replay);
    expect(u64_from_le(scan->getEntries()[0].key) == 3_ul);
    expect(has_rpc_fn(peek_output(sockets.sockets.s1), kFetchReferenceFn) == false);
    expect(has_rpc_fn(peek_output(sockets.sockets.s1), kObserveReferenceFn) == false);
    expect(sockets.client->socket_mux().has_socket(static_cast<std::uint32_t>(live_id)) == true);
    BinaryEntry extra;
    extra.key = u64_to_le(11);
    extra.value = u64_to_le(11);
    extra.ts = 11;
    sockets.ep->push(extra);
    sockets.ep->pump();
    mux_roundtrip(sockets.sockets, *sockets.client, *sockets.server);
    bool saw = false;
    for(const auto& entry : scan->getEntries()) {
      if(u64_from_le(entry.key) == 11) saw = true;
    }
    expect(saw == true);
  };
}
