#include <boost/ut.hpp>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "test_socket_utils.hpp"

#include "gateway/hash.hpp"
#include "gateway/session.hpp"
#include "gateway/session_rpc.hpp"

#include "serialization/flat_materialized_codec.hpp"
#include "serialization/socket_message_channel.hpp"
#include "serialization/socket_rpc_client.hpp"
#include "serialization/socket_rpc_server.hpp"
#include "serialization/type_registry.hpp"
#include "serialization/type_registry_blob.hpp"
#include "serialization/type_description.hpp"

using namespace serialization;
using namespace waos::gateway;
using namespace boost::ut;

namespace {

  using flat_socket_rpc_server = socket_rpc_server<flat_materialized_codec>;
  using flat_socket_rpc_client = socket_rpc_client<flat_materialized_codec>;

  struct [[=serialization::message()]] ping {
    std::uint64_t seq = 0;
  };

  binary drain_output(uint32_t sock) {
    binary out;
    uint8_t tmp[4096];
    struct socket_info* src = get_socket_info(sock);
    for(;;) {
      size_t n = circular_buffer_read(&src->output_buffer, tmp, sizeof(tmp));
      if(n == 0) {
        break;
      }
      out.insert(out.end(), tmp, tmp + n);
    }
    return out;
  }

  void write_input(uint32_t sock, const binary& bytes) {
    if(bytes.empty()) {
      return;
    }
    struct socket_info* dst = get_socket_info(sock);
    size_t off = 0;
    while(off < bytes.size()) {
      size_t m = circular_buffer_write(&dst->input_buffer, bytes.data() + off, bytes.size() - off);
      off += m;
    }
  }

  std::string to_hex(const binary& bytes) {
    static const char* kDigits = "0123456789abcdef";
    std::string out;
    out.resize(bytes.size() * 2);
    for(size_t i = 0; i < bytes.size(); ++i) {
      out[i * 2] = kDigits[bytes[i] >> 4];
      out[i * 2 + 1] = kDigits[bytes[i] & 0x0f];
    }
    return out;
  }

  binary from_hex(const std::string& hex) {
    if(hex.size() % 2 != 0) {
      throw std::runtime_error("odd hex length");
    }
    binary out(hex.size() / 2);
    auto nibble = [](char c) -> std::uint8_t {
      if(c >= '0' && c <= '9') return static_cast<std::uint8_t>(c - '0');
      if(c >= 'a' && c <= 'f') return static_cast<std::uint8_t>(c - 'a' + 10);
      if(c >= 'A' && c <= 'F') return static_cast<std::uint8_t>(c - 'A' + 10);
      throw std::runtime_error("invalid hex");
    };
    for(size_t i = 0; i < out.size(); ++i) {
      out[i] = static_cast<std::uint8_t>((nibble(hex[i * 2]) << 4) | nibble(hex[i * 2 + 1]));
    }
    return out;
  }

  void write_testdata(const char* name, const std::string& hex) {
#ifdef GATEWAY_TESTDATA_DIR
    std::string path = std::string(GATEWAY_TESTDATA_DIR) + "/" + name;
    std::ofstream out(path, std::ios::trunc);
    out << hex;
#endif
    (void)name;
    (void)hex;
  }

  binary copy_payload(auto& reader, size_t payload_size) {
    binary out(payload_size);
    if(payload_size > 0) {
      reader.read_raw(out.data(), payload_size);
    }
    return out;
  }

  void rpc_roundtrip(socket_loopback_pair& sockets,
                     flat_socket_rpc_client& client,
                     flat_socket_rpc_server& server) {
    sockets.transfer(sockets.s1, sockets.s0);
    server.handle_data();
    sockets.transfer(sockets.s0, sockets.s1);
    client.handle_data();
  }

  binary sample_blob(const char* tag) {
    binary blob;
    const unsigned char* p = reinterpret_cast<const unsigned char*>(tag);
    blob.insert(blob.end(), p, p + std::strlen(tag));
    return blob;
  }

  binary pack_hidden_ids(std::initializer_list<std::uint32_t> ids) {
    binary out;
    out.reserve(ids.size() * 4);
    for(std::uint32_t id : ids) {
      out.push_back(static_cast<std::uint8_t>(id));
      out.push_back(static_cast<std::uint8_t>(id >> 8));
      out.push_back(static_cast<std::uint8_t>(id >> 16));
      out.push_back(static_cast<std::uint8_t>(id >> 24));
    }
    return out;
  }

  std::vector<api_publication> sample_publications() {
    return {
      api_publication{1, "alpha", sample_blob("blob-alpha")},
      api_publication{2, "beta", sample_blob("blob-beta")},
    };
  }

  std::string catch_rpc_error(std::exception_ptr ep) {
    try {
      std::rethrow_exception(ep);
    } catch(const std::runtime_error& e) {
      return e.what();
    } catch(const std::exception& e) {
      return e.what();
    }
  }

}

int main() {
  "api_message type index is 3"_test = [] {
    socket_loopback_pair sockets;
    flat_socket_message_channel ch0(sockets.s0);
    flat_socket_message_channel ch1(sockets.s1);

    ch0.register_message_type<rpc_request>();
    ch0.register_message_type<rpc_response>();
    ch0.register_message_type<rpc_error>();
    ch0.register_message_type<api_message>();

    ch1.register_message_type<rpc_request>();
    ch1.register_message_type<rpc_response>();
    ch1.register_message_type<rpc_error>();
    ch1.register_message_type<api_message>();

    std::uint32_t seen_id = 0;
    bool got = false;
    ch1.on_message<api_message>(std::function<void(api_message&)>([&](api_message& msg) {
      got = true;
      seen_id = msg.api_id;
    }));

    ch0.send_message(api_message{7});
    sockets.transfer(sockets.s0, sockets.s1);
    ch1.handle_data();

    expect(got == true);
    expect(seen_id == 7_u);
  };

  "api_message trailing ping payload copies to inner channel"_test = [] {
    binary inner_frame;
    {
      socket_loopback_pair inner_src;
      flat_socket_message_channel inner_send(inner_src.s0);
      inner_send.register_message_type<ping>();
      inner_send.send_message(ping{42});
      inner_frame = drain_output(inner_src.s0);
    }
    expect(inner_frame.size() >= 4_ul);

    std::uint32_t seen_id = 0;
    binary copied;
    {
      socket_loopback_pair session_sockets;
      flat_socket_message_channel outer_send(session_sockets.s0);
      flat_socket_message_channel outer_recv(session_sockets.s1);
    outer_send.register_message_type<rpc_request>();
    outer_send.register_message_type<rpc_response>();
    outer_send.register_message_type<rpc_error>();
    outer_send.register_message_type<api_message>();
    outer_recv.register_message_type<rpc_request>();
    outer_recv.register_message_type<rpc_response>();
    outer_recv.register_message_type<rpc_error>();
    outer_recv.register_message_type<api_message>();

      outer_recv.on_message<api_message>(std::function<void(api_message&, size_t)>(
        [&](api_message& msg, size_t payload_size) {
          seen_id = msg.api_id;
          copied = copy_payload(outer_recv.reader(), payload_size);
        }));

      auto& writer = outer_send.begin_send(api_message{1});
      writer.write_raw(inner_frame.data(), inner_frame.size());
      outer_send.commit_send();

      session_sockets.transfer(session_sockets.s0, session_sockets.s1);
      outer_recv.handle_data();
    }

    expect(seen_id == 1_u);
    expect(copied == inner_frame);

    {
      socket_loopback_pair inner_dst;
      write_input(inner_dst.s1, copied);
      flat_socket_message_channel inner_recv(inner_dst.s1);
      inner_recv.register_message_type<ping>();
      bool ping_got = false;
      std::uint64_t ping_seq = 0;
      inner_recv.on_message<ping>(std::function<void(ping&)>([&](ping& p) {
        ping_got = true;
        ping_seq = p.seq;
      }));
      inner_recv.handle_data();
      expect(ping_got == true);
      expect(ping_seq == 42_u);
    }
  };

  "two api_message frames stay framed"_test = [] {
    binary frame_a;
    {
      socket_loopback_pair inner_a;
      flat_socket_message_channel send_a(inner_a.s0);
      send_a.register_message_type<ping>();
      send_a.send_message(ping{1});
      frame_a = drain_output(inner_a.s0);
    }
    binary frame_b;
    {
      socket_loopback_pair inner_b;
      flat_socket_message_channel send_b(inner_b.s0);
      send_b.register_message_type<ping>();
      send_b.send_message(ping{2});
      frame_b = drain_output(inner_b.s0);
    }

    socket_loopback_pair session_sockets;
    flat_socket_message_channel outer_send(session_sockets.s0);
    flat_socket_message_channel outer_recv(session_sockets.s1);
    for(auto* ch : {&outer_send, &outer_recv}) {
      ch->register_message_type<rpc_request>();
      ch->register_message_type<rpc_response>();
      ch->register_message_type<rpc_error>();
      ch->register_message_type<api_message>();
    }

    std::vector<std::uint32_t> ids;
    std::vector<binary> payloads;
    outer_recv.on_message<api_message>(std::function<void(api_message&, size_t)>(
      [&](api_message& msg, size_t payload_size) {
        ids.push_back(msg.api_id);
        payloads.push_back(copy_payload(outer_recv.reader(), payload_size));
      }));

    {
      auto& w = outer_send.begin_send(api_message{1});
      w.write_raw(frame_a.data(), frame_a.size());
      outer_send.commit_send();
    }
    {
      auto& w = outer_send.begin_send(api_message{2});
      w.write_raw(frame_b.data(), frame_b.size());
      outer_send.commit_send();
    }

    session_sockets.transfer(session_sockets.s0, session_sockets.s1);
    outer_recv.handle_data();

    expect(ids.size() == 2_ul);
    expect(ids[0] == 1_u);
    expect(ids[1] == 2_u);
    expect(payloads[0] == frame_a);
    expect(payloads[1] == frame_b);
  };

  "hello version 1 returns server_protocol 1"_test = [] {
    socket_loopback_pair sockets;
    session_api sess(std::make_shared<session_state>(sample_publications()));
    flat_socket_rpc_server server(sockets.s0);
    flat_socket_rpc_client client(sockets.s1);
    attach_session(server, sess);
    register_session_channel(client);

    bool resolved = false;
    server_protocol result{};
    client.call<server_protocol>(0, client_protocol{protocol_version_v1, {}})->on_resolved([&](server_protocol r) {
      resolved = true;
      result = std::move(r);
    });
    rpc_roundtrip(sockets, client, server);
    expect(resolved == true);
    expect(result.protocol_version == protocol_version_v1);
  };

  "wrong hello version errors and later list_apis also errors"_test = [] {
    socket_loopback_pair sockets;
    session_api sess(std::make_shared<session_state>(sample_publications()));
    flat_socket_rpc_server server(sockets.s0);
    flat_socket_rpc_client client(sockets.s1);
    attach_session(server, sess);
    register_session_channel(client);

    std::string hello_err;
    client.call<server_protocol>(0, client_protocol{99, {}})->on_rejected([&](std::exception_ptr ep) {
      hello_err = catch_rpc_error(ep);
    });
    rpc_roundtrip(sockets, client, server);
    expect(hello_err == std::string(error_unsupported_protocol_version));

    std::string list_err;
    client.call<std::vector<api_info>>(1)->on_rejected([&](std::exception_ptr ep) {
      list_err = catch_rpc_error(ep);
    });
    rpc_roundtrip(sockets, client, server);
    expect(list_err == std::string(error_unsupported_protocol_version));
  };

  "list_apis before hello is handshake_required"_test = [] {
    socket_loopback_pair sockets;
    session_api sess(std::make_shared<session_state>(sample_publications()));
    flat_socket_rpc_server server(sockets.s0);
    flat_socket_rpc_client client(sockets.s1);
    attach_session(server, sess);
    register_session_channel(client);

    std::string err;
    client.call<std::vector<api_info>>(1)->on_rejected([&](std::exception_ptr ep) {
      err = catch_rpc_error(ep);
    });
    rpc_roundtrip(sockets, client, server);
    expect(err == std::string(error_handshake_required));
  };

  "constructor handshake_data skips hello"_test = [] {
    socket_loopback_pair sockets;
    session_api sess(std::make_shared<session_state>(sample_publications(), default_visibility, client_protocol{protocol_version_v1, {}}));
    flat_socket_rpc_server server(sockets.s0);
    flat_socket_rpc_client client(sockets.s1);
    attach_session(server, sess);
    register_session_channel(client);

    bool resolved = false;
    std::vector<api_info> apis;
    client.call<std::vector<api_info>>(1)->on_resolved([&](std::vector<api_info> r) {
      resolved = true;
      apis = std::move(r);
    });
    rpc_roundtrip(sockets, client, server);
    expect(resolved == true);
    expect(apis.size() == 2_ul);
    expect(apis[0].name == std::string("alpha"));
    expect(apis[1].name == std::string("beta"));
  };

  "list_apis hashes match xxh3 of blobs; get_rpc_description is stable"_test = [] {
    socket_loopback_pair sockets;
    auto pubs = sample_publications();
    binary hash_alpha = xxh3_64_le(pubs[0].description_blob);
    binary hash_beta = xxh3_64_le(pubs[1].description_blob);
    session_api sess(std::make_shared<session_state>(pubs, default_visibility, client_protocol{protocol_version_v1, {}}));
    flat_socket_rpc_server server(sockets.s0);
    flat_socket_rpc_client client(sockets.s1);
    attach_session(server, sess);
    register_session_channel(client);

    std::vector<api_info> apis;
    client.call<std::vector<api_info>>(1)->on_resolved([&](std::vector<api_info> r) {
      apis = std::move(r);
    });
    rpc_roundtrip(sockets, client, server);
    expect(apis.size() == 2_ul);
    expect(apis[0].description_hash == hash_alpha);
    expect(apis[1].description_hash == hash_beta);

    binary blob1;
    binary blob2;
    client.call<binary>(2, std::uint32_t{1})->on_resolved([&](binary r) { blob1 = std::move(r); });
    rpc_roundtrip(sockets, client, server);
    client.call<binary>(2, std::uint32_t{1})->on_resolved([&](binary r) { blob2 = std::move(r); });
    rpc_roundtrip(sockets, client, server);
    expect(blob1 == pubs[0].description_blob);
    expect(blob1 == blob2);
    expect(xxh3_64_le(blob1) == hash_alpha);
  };

  "xxh3 known vector"_test = [] {
    const char* input = "abc";
    binary got = xxh3_64_le(input, 3);
    expect(got.size() == 8_ul);
    write_testdata("xxh3_abc.hex", to_hex(got));
    // Official XXH3_64bits("abc") = 0x78af5f94892f3950, stored LE.
    binary expected = from_hex("50392f89945faf78");
    expect(got == expected);
  };

  "hidden and unknown api_id share unknown_api"_test = [] {
    socket_loopback_pair sockets;
    auth_map auth;
    auth.emplace("hidden", pack_hidden_ids({2}));
    session_api sess(std::make_shared<session_state>(sample_publications(), default_visibility, client_protocol{protocol_version_v1, auth}));
    flat_socket_rpc_server server(sockets.s0);
    flat_socket_rpc_client client(sockets.s1);
    attach_session(server, sess);
    register_session_channel(client);

    std::vector<api_info> apis;
    client.call<std::vector<api_info>>(1)->on_resolved([&](std::vector<api_info> r) {
      apis = std::move(r);
    });
    rpc_roundtrip(sockets, client, server);
    expect(apis.size() == 1_ul);
    expect(apis[0].api_id == 1_u);

    std::string hidden_err;
    std::string unknown_err;
    client.call<binary>(2, std::uint32_t{2})->on_rejected([&](std::exception_ptr ep) {
      hidden_err = catch_rpc_error(ep);
    });
    rpc_roundtrip(sockets, client, server);
    client.call<binary>(2, std::uint32_t{99})->on_rejected([&](std::exception_ptr ep) {
      unknown_err = catch_rpc_error(ep);
    });
    rpc_roundtrip(sockets, client, server);
    expect(hidden_err == std::string(error_unknown_api));
    expect(unknown_err == std::string(error_unknown_api));
    expect(hidden_err == unknown_err);
  };

  "get_session_description blob has methods 0-3, fetch_reference and api_message"_test = [] {
    socket_loopback_pair sockets;
    session_api sess(std::make_shared<session_state>(sample_publications(), default_visibility, client_protocol{protocol_version_v1, {}}));
    flat_socket_rpc_server server(sockets.s0);
    flat_socket_rpc_client client(sockets.s1);
    attach_session(server, sess);
    register_session_channel(client);

    binary blob;
    client.call<binary>(3)->on_resolved([&](binary r) { blob = std::move(r); });
    rpc_roundtrip(sockets, client, server);
    expect(blob.size() > 1_ul);

    type_registry reg(false);
    deserialize_type_registry_blob(blob, reg);
    expect(reg.get_type("api_message") != nullptr);

    std::vector<std::string> method_names;
    for(const auto& type : reg.get_types()) {
      if(type->getKind() != type_description::Kind::Object &&
         type->getKind() != type_description::Kind::DynamicObject) {
        continue;
      }
      for(const auto& member : type->getMembers()) {
        auto mt = member.type;
        if(!mt) {
          continue;
        }
        if(mt->getKind() == type_description::Kind::Function ||
           mt->getKind() == type_description::Kind::Method) {
          method_names.push_back(member.name);
        }
      }
    }
    expect(method_names.size() >= 9_ul);
    expect(method_names[0].find("hello") != std::string::npos);
    expect(method_names[1].find("list_apis") != std::string::npos);
    expect(method_names[2].find("get_rpc_description") != std::string::npos);
    expect(method_names[3].find("get_session_description") != std::string::npos);
    expect(method_names[8].find("fetch_reference") != std::string::npos);
  };

  "dump golden hello and api_message hex"_test = [] {
    binary hello_frame;
    {
      socket_loopback_pair hello_sockets;
      session_api sess(std::make_shared<session_state>(sample_publications()));
      flat_socket_rpc_server server(hello_sockets.s0);
      flat_socket_rpc_client client(hello_sockets.s1);
      attach_session(server, sess);
      register_session_channel(client);
      client.call<server_protocol>(0, client_protocol{protocol_version_v1, {}});
      hello_frame = drain_output(hello_sockets.s1);
    }
    write_testdata("hello_v1.hex", to_hex(hello_frame));
    expect(hello_frame.size() > 4_ul);

    binary inner_frame;
    {
      socket_loopback_pair inner;
      flat_socket_message_channel inner_send(inner.s0);
      inner_send.register_message_type<ping>();
      inner_send.send_message(ping{1});
      inner_frame = drain_output(inner.s0);
    }

    binary api_frame;
    {
      socket_loopback_pair outer;
      flat_socket_message_channel outer_send(outer.s0);
      outer_send.register_message_type<rpc_request>();
      outer_send.register_message_type<rpc_response>();
      outer_send.register_message_type<rpc_error>();
      outer_send.register_message_type<api_message>();
      auto& w = outer_send.begin_send(api_message{1});
      w.write_raw(inner_frame.data(), inner_frame.size());
      outer_send.commit_send();
      api_frame = drain_output(outer.s0);
    }
    write_testdata("api_message_ping.hex", to_hex(api_frame));
    expect(api_frame.size() > 4_ul);

    socket_loopback_pair verify;
    write_input(verify.s1, api_frame);
    flat_socket_message_channel outer_recv(verify.s1);
    outer_recv.register_message_type<rpc_request>();
    outer_recv.register_message_type<rpc_response>();
    outer_recv.register_message_type<rpc_error>();
    outer_recv.register_message_type<api_message>();
    std::uint32_t api_id = 0;
    binary payload;
    outer_recv.on_message<api_message>(std::function<void(api_message&, size_t)>(
      [&](api_message& msg, size_t payload_size) {
        api_id = msg.api_id;
        payload = copy_payload(outer_recv.reader(), payload_size);
      }));
    outer_recv.handle_data();
    expect(api_id == 1_u);
    expect(payload == inner_frame);
  };

  return 0;
}
