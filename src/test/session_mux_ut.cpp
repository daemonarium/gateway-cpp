#include <boost/ut.hpp>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "test_socket_utils.hpp"

#include "gateway/api_connection.hpp"
#include "gateway/callback_endpoint.hpp"
#include "gateway/echo_api.hpp"
#include "gateway/frame_io.hpp"
#include "gateway/hash.hpp"
#include "gateway/inner_rpc_error.hpp"
#include "gateway/session_client.hpp"
#include "gateway/session_server.hpp"

#include "serialization/rpc.hpp"
#include "serialization/socket_message_channel.hpp"
#include "serialization/socket_rpc_client.hpp"
#include "serialization/socket_rpc_server.hpp"
#include "serialization/socket_transport.hpp"
#include "serialization/type_description.hpp"
#include "serialization/type_registry.hpp"
#include "serialization/type_registry_blob.hpp"

using namespace serialization;
using namespace waos::gateway;
using namespace boost::ut;

namespace {

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

  std::string catch_rpc_error(std::exception_ptr ep) {
    try {
      std::rethrow_exception(ep);
    } catch(const std::runtime_error& e) {
      return e.what();
    } catch(const std::exception& e) {
      return e.what();
    }
  }

  void mux_roundtrip(socket_loopback_pair& sockets, auto& client, auto& server) {
    sockets.transfer(sockets.s1, sockets.s0);
    server.handle_data();
    sockets.transfer(sockets.s0, sockets.s1);
    client.handle_data();
  }

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

  std::size_t output_readable(uint32_t sock) {
    return circular_buffer_readable_bytes(&get_socket_info(sock)->output_buffer);
  }

  std::uint32_t read_u32_le(const binary& bytes, std::size_t off) {
    std::uint32_t v = 0;
    std::memcpy(&v, bytes.data() + off, sizeof(v));
    return v;
  }

  std::uint64_t read_u64_le(const binary& bytes, std::size_t off) {
    std::uint64_t v = 0;
    std::memcpy(&v, bytes.data() + off, sizeof(v));
    return v;
  }

  binary make_ascii_blob(const char* tag) {
    binary blob;
    const unsigned char* p = reinterpret_cast<const unsigned char*>(tag);
    blob.insert(blob.end(), p, p + std::strlen(tag));
    return blob;
  }

  binary encode_inner_rpc_response(std::uint64_t request_id) {
    circular_buffer in{};
    circular_buffer out{};
    circular_buffer_init(&in, 4096, nullptr);
    circular_buffer_init(&out, 4096, nullptr);
    flat_buffer_socket_message_channel ch(buffer_transport(&in, &out));
    ch.register_message_type<rpc_request>();
    ch.register_message_type<rpc_response>();
    ch.register_message_type<rpc_error>();
    ch.send_message(rpc_response{request_id});
    binary frame = drain_one_frame(&out);
    circular_buffer_deinit(&in);
    circular_buffer_deinit(&out);
    return frame;
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

  binary read_testdata(const char* name) {
#ifdef GATEWAY_TESTDATA_DIR
    std::string path = std::string(GATEWAY_TESTDATA_DIR) + "/" + name;
    std::ifstream in(path);
    std::string hex;
    in >> hex;
    return from_hex(hex);
#else
    (void)name;
    return {};
#endif
  }

  using flat_session_server = session_server<>;
  using flat_session_client = session_client<>;

}

int main() {
  "echo round-trip through callback endpoint"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    });
    flat_session_client client(sockets.s1);

    bool hello_ok = false;
    client.hello(client_protocol{protocol_version_v1, {}})->on_resolved([&](server_protocol r) {
      hello_ok = r.protocol_version == protocol_version_v1;
    });
    mux_roundtrip(sockets, client, server);
    expect(hello_ok == true);

    std::vector<api_info> infos;
    client.list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
    mux_roundtrip(sockets, client, server);
    expect(infos.size() == 1_ul);
    expect(infos[0].api_id == 1_u);

    api_connection* conn = nullptr;
    client.open_api(1)->on_resolved([&](api_connection* c) { conn = c; });
    mux_roundtrip(sockets, client, server);
    expect(conn != nullptr);

    std::uint64_t result = 0;
    bool resolved = false;
    conn->rpc().call<std::uint64_t>(0, std::uint64_t{7})->on_resolved([&](std::uint64_t r) {
      resolved = true;
      result = r;
    });
    mux_roundtrip(sockets, client, server);
    expect(resolved == true);
    expect(result == 7_ul);
    expect(ep1.called == true);
  };

  "two visible apis demux; hidden endpoint is not called"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    echo_endpoint ep2;
    echo_endpoint ep3;
    binary blob = make_echo_blob();
    auth_map auth;
    auth["hidden"] = pack_hidden_ids({3});
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep},
      bound_publication{2, "beta", blob, ep2.ep},
      bound_publication{3, "gamma", blob, ep3.ep}
    }, default_visibility, handshake_data{protocol_version_v1, auth});
    flat_session_client client(sockets.s1);

    std::vector<api_info> infos;
    client.list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
    mux_roundtrip(sockets, client, server);
    expect(infos.size() == 2_ul);

    api_connection* c1 = nullptr;
    api_connection* c2 = nullptr;
    client.open_api(1)->on_resolved([&](api_connection* c) { c1 = c; });
    mux_roundtrip(sockets, client, server);
    client.open_api(2)->on_resolved([&](api_connection* c) { c2 = c; });
    mux_roundtrip(sockets, client, server);
    expect(c1 != nullptr);
    expect(c2 != nullptr);

    std::uint64_t r1 = 0;
    std::uint64_t r2 = 0;
    c1->rpc().call<std::uint64_t>(0, std::uint64_t{7})->on_resolved([&](std::uint64_t r) { r1 = r; });
    c2->rpc().call<std::uint64_t>(0, std::uint64_t{11})->on_resolved([&](std::uint64_t r) { r2 = r; });
    mux_roundtrip(sockets, client, server);
    expect(r1 == 7_ul);
    expect(r2 == 11_ul);
    expect(ep3.called == false);
  };

  "hidden and unknown api_message share unknown_api"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    echo_endpoint ep3;
    binary blob = make_echo_blob();
    auth_map auth;
    auth["hidden"] = pack_hidden_ids({3});
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep},
      bound_publication{3, "gamma", blob, ep3.ep}
    }, default_visibility, handshake_data{protocol_version_v1, auth});
    flat_session_client client(sockets.s1);

    client.list_apis()->on_resolved([](std::vector<api_info>&) {});
    mux_roundtrip(sockets, client, server);

    std::string desc_err;
    client.get_rpc_description(3)->on_rejected([&](std::exception_ptr ep) {
      desc_err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);
    expect(desc_err == std::string(error_unknown_api));

    api_connection* hidden = nullptr;
    client.open_api(3, blob)->on_resolved([&](api_connection* c) { hidden = c; });
    expect(hidden != nullptr);
    std::string hidden_err;
    hidden->rpc().call<std::uint64_t>(0, std::uint64_t{1})->on_rejected([&](std::exception_ptr ep) {
      hidden_err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);

    api_connection* unknown = nullptr;
    client.open_api(99, blob)->on_resolved([&](api_connection* c) { unknown = c; });
    std::string unknown_err;
    unknown->rpc().call<std::uint64_t>(0, std::uint64_t{1})->on_rejected([&](std::exception_ptr ep) {
      unknown_err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);

    expect(hidden_err == std::string(error_unknown_api));
    expect(unknown_err == std::string(error_unknown_api));
    expect(ep3.called == false);
  };

  "api_id 0 is rejected and does not hit an endpoint"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    api_connection* conn = nullptr;
    client.open_api(0, blob)->on_resolved([&](api_connection* c) { conn = c; });
    std::string err;
    conn->rpc().call<std::uint64_t>(0, std::uint64_t{1})->on_rejected([&](std::exception_ptr ep) {
      err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);
    expect(err == std::string(error_unknown_api));
    expect(ep1.called == false);
  };

  "api_message before hello is handshake_required"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    });
    flat_session_client client(sockets.s1);

    api_connection* conn = nullptr;
    client.open_api(1, blob)->on_resolved([&](api_connection* c) { conn = c; });
    std::string err;
    conn->rpc().call<std::uint64_t>(0, std::uint64_t{1})->on_rejected([&](std::exception_ptr ep) {
      err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);
    expect(err == std::string(error_handshake_required));
    expect(ep1.called == false);
  };

  "handshake_data skips hello for echo"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    api_connection* conn = nullptr;
    client.open_api(1)->on_resolved([&](api_connection* c) { conn = c; });
    mux_roundtrip(sockets, client, server);
    expect(conn != nullptr);

    std::uint64_t result = 0;
    conn->rpc().call<std::uint64_t>(0, std::uint64_t{9})->on_resolved([&](std::uint64_t r) { result = r; });
    mux_roundtrip(sockets, client, server);
    expect(result == 9_ul);
  };

  "get_rpc_description cache hit skips second rpc"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    client.list_apis()->on_resolved([](std::vector<api_info>&) {});
    mux_roundtrip(sockets, client, server);

    binary first;
    client.get_rpc_description(1)->on_resolved([&](binary& b) { first = b; });
    mux_roundtrip(sockets, client, server);
    expect(first == blob);

    bool second_sync = false;
    binary second;
    client.get_rpc_description(1)->on_resolved([&](binary& b) {
      second_sync = true;
      second = b;
    });
    expect(second_sync == true);
    expect(second == first);
  };

  "empty publication list"_test = [] {
    socket_loopback_pair sockets;
    flat_session_server server(sockets.s0, {}, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    std::vector<api_info> infos;
    client.list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
    mux_roundtrip(sockets, client, server);
    expect(infos.size() == 0_ul);

    std::string err;
    client.get_rpc_description(1)->on_rejected([&](std::exception_ptr ep) {
      err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);
    expect(err == std::string(error_unknown_api));

    api_connection* conn = nullptr;
    client.open_api(1, make_echo_blob())->on_resolved([&](api_connection* c) { conn = c; });
    std::string echo_err;
    conn->rpc().call<std::uint64_t>(0, std::uint64_t{1})->on_rejected([&](std::exception_ptr ep) {
      echo_err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);
    expect(echo_err == std::string(error_unknown_api));
  };

  "peek inner rpc_request_id"_test = [] {
    binary a1(20, 0);
    std::uint32_t type0 = 0;
    std::memcpy(a1.data() + 4, &type0, 4);
    std::uint64_t rid = 0x0102030405060708ull;
    std::memcpy(a1.data() + 12, &rid, 8);
    auto got = peek_inner_rpc_request_id(a1);
    expect(got.has_value() == true);
    expect(*got == rid);

    binary a2 = a1;
    std::uint32_t type1 = 1;
    std::memcpy(a2.data() + 4, &type1, 4);
    expect(peek_inner_rpc_request_id(a2).has_value() == false);

    binary a3(19, 0);
    std::memcpy(a3.data() + 4, &type0, 4);
    expect(peek_inner_rpc_request_id(a3).has_value() == false);

    binary a4 = a1;
    std::uint32_t type2 = 2;
    std::memcpy(a4.data() + 4, &type2, 4);
    expect(peek_inner_rpc_request_id(a4).has_value() == false);
  };

  "failed session api_message is unsupported_protocol_version"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    });
    flat_session_client client(sockets.s1);

    std::string hello_err;
    client.hello(client_protocol{99, {}})->on_rejected([&](std::exception_ptr ep) {
      hello_err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);
    expect(hello_err == std::string(error_unsupported_protocol_version));
    expect(server.sess().failed() == true);

    api_connection* conn = nullptr;
    client.open_api(1, blob)->on_resolved([&](api_connection* c) { conn = c; });
    std::string inner_err;
    conn->rpc().call<std::uint64_t>(0, std::uint64_t{1})->on_rejected([&](std::exception_ptr ep) {
      inner_err = catch_rpc_error(ep);
    });
    mux_roundtrip(sockets, client, server);
    expect(inner_err == std::string(error_unsupported_protocol_version));
    expect(ep1.called == false);
  };

  "inner rpc_response is not forwarded"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    binary inner = encode_inner_rpc_response(1);
    auto& writer = client.rpc().channel().begin_send(api_message{1});
    writer.write_raw(inner.data(), inner.size());
    client.rpc().channel().commit_send();
    sockets.transfer(sockets.s1, sockets.s0);
    server.handle_data();
    expect(ep1.called == false);
    expect(output_readable(sockets.s0) == 0_ul);
  };

  "short inner trailing is not forwarded"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    binary inner{0x01, 0x02, 0x03};
    auto& writer = client.rpc().channel().begin_send(api_message{1});
    writer.write_raw(inner.data(), inner.size());
    client.rpc().channel().commit_send();
    sockets.transfer(sockets.s1, sockets.s0);
    server.handle_data();
    expect(ep1.called == false);
    expect(output_readable(sockets.s0) == 0_ul);
  };

  "empty auth shows every publication"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    echo_endpoint ep2;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep},
      bound_publication{2, "beta", blob, ep2.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    std::vector<api_info> infos;
    client.list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
    mux_roundtrip(sockets, client, server);
    expect(infos.size() == 2_ul);
    expect(infos[0].name == std::string("alpha"));
    expect(infos[1].name == std::string("beta"));

    api_connection* c1 = nullptr;
    api_connection* c2 = nullptr;
    client.open_api(1)->on_resolved([&](api_connection* c) { c1 = c; });
    mux_roundtrip(sockets, client, server);
    client.open_api(2)->on_resolved([&](api_connection* c) { c2 = c; });
    mux_roundtrip(sockets, client, server);

    std::uint64_t r1 = 0;
    std::uint64_t r2 = 0;
    c1->rpc().call<std::uint64_t>(0, std::uint64_t{7})->on_resolved([&](std::uint64_t r) { r1 = r; });
    c2->rpc().call<std::uint64_t>(0, std::uint64_t{11})->on_resolved([&](std::uint64_t r) { r2 = r; });
    mux_roundtrip(sockets, client, server);
    expect(r1 == 7_ul);
    expect(r2 == 11_ul);
  };

  "same description_hash caches per api_id"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    echo_endpoint ep2;
    binary blob = make_echo_blob();
    binary expected_hash = xxh3_64_le(blob);
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep},
      bound_publication{2, "beta", blob, ep2.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    std::vector<api_info> infos;
    client.list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
    mux_roundtrip(sockets, client, server);
    expect(infos.size() == 2_ul);
    expect(infos[0].description_hash == infos[1].description_hash);
    expect(infos[0].description_hash == expected_hash);

    binary first;
    client.get_rpc_description(1)->on_resolved([&](binary& b) { first = b; });
    mux_roundtrip(sockets, client, server);
    expect(first == blob);

    binary second;
    client.get_rpc_description(2)->on_resolved([&](binary& b) { second = b; });
    binary req2 = drain_output(sockets.s1);
    expect(req2.size() >= 28_ul);
    expect(read_u32_le(req2, 4) == 0_u);
    expect(read_u64_le(req2, 20) == 2_ul);
    write_input(sockets.s0, req2);
    server.handle_data();
    sockets.transfer(sockets.s0, sockets.s1);
    client.handle_data();
    expect(second == blob);

    bool hit1 = false;
    bool hit2 = false;
    client.get_rpc_description(1)->on_resolved([&](binary&) { hit1 = true; });
    client.get_rpc_description(2)->on_resolved([&](binary&) { hit2 = true; });
    expect(hit1 == true);
    expect(hit2 == true);
    expect(output_readable(sockets.s1) == 0_ul);

    api_connection* c1 = nullptr;
    api_connection* c2 = nullptr;
    client.open_api(1)->on_resolved([&](api_connection* c) { c1 = c; });
    client.open_api(2)->on_resolved([&](api_connection* c) { c2 = c; });
    std::uint64_t r1 = 0;
    std::uint64_t r2 = 0;
    c1->rpc().call<std::uint64_t>(0, std::uint64_t{7})->on_resolved([&](std::uint64_t r) { r1 = r; });
    c2->rpc().call<std::uint64_t>(0, std::uint64_t{11})->on_resolved([&](std::uint64_t r) { r2 = r; });
    mux_roundtrip(sockets, client, server);
    expect(r1 == 7_ul);
    expect(r2 == 11_ul);
  };

  "two api_message frames through ApiConnection stay framed"_test = [] {
    socket_loopback_pair sockets;
    echo_endpoint ep1;
    echo_endpoint ep2;
    binary blob = make_echo_blob();
    flat_session_server server(sockets.s0, {
      bound_publication{1, "alpha", blob, ep1.ep},
      bound_publication{2, "beta", blob, ep2.ep}
    }, default_visibility, handshake_data{protocol_version_v1, {}});
    flat_session_client client(sockets.s1);

    api_connection* c1 = nullptr;
    api_connection* c2 = nullptr;
    client.open_api(1)->on_resolved([&](api_connection* c) { c1 = c; });
    mux_roundtrip(sockets, client, server);
    client.open_api(2)->on_resolved([&](api_connection* c) { c2 = c; });
    mux_roundtrip(sockets, client, server);

    std::uint64_t r1 = 0;
    std::uint64_t r2 = 0;
    c1->rpc().call<std::uint64_t>(0, std::uint64_t{7})->on_resolved([&](std::uint64_t r) { r1 = r; });
    c2->rpc().call<std::uint64_t>(0, std::uint64_t{11})->on_resolved([&](std::uint64_t r) { r2 = r; });
    auto frames = drain_frames(&get_socket_info(sockets.s1)->output_buffer);
    expect(frames.size() == 2_ul);
    expect(read_u32_le(frames[0], 4) == 3_u);
    expect(read_u32_le(frames[0], 12) == 1_u);
    expect(read_u32_le(frames[1], 4) == 3_u);
    expect(read_u32_le(frames[1], 12) == 2_u);
    for(const auto& frame : frames) {
      write_input(sockets.s0, frame);
    }
    server.handle_data();
    sockets.transfer(sockets.s0, sockets.s1);
    client.handle_data();
    expect(r1 == 7_ul);
    expect(r2 == 11_ul);
  };

  "dump golden mux hex"_test = [] {
    binary list_req;
    binary list_res;
    {
      socket_loopback_pair sockets;
      binary alpha = make_ascii_blob("blob-alpha");
      binary beta = make_ascii_blob("blob-beta");
      flat_session_server server(sockets.s0, {
        bound_publication{1, "alpha", alpha, nullptr},
        bound_publication{2, "beta", beta, nullptr}
      }, default_visibility, handshake_data{protocol_version_v1, {}});
      flat_session_client client(sockets.s1);
      std::vector<api_info> infos;
      client.list_apis()->on_resolved([&](std::vector<api_info>& v) { infos = v; });
      list_req = drain_output(sockets.s1);
      write_input(sockets.s0, list_req);
      server.handle_data();
      list_res = drain_output(sockets.s0);
      write_input(sockets.s1, list_res);
      client.handle_data();
      expect(infos.size() == 2_ul);
      expect(infos[0].name == std::string("alpha"));
      expect(infos[1].name == std::string("beta"));
    }
    write_testdata("list_apis_req.hex", to_hex(list_req));
    write_testdata("list_apis_res.hex", to_hex(list_res));
    expect(read_u32_le(list_req, 4) == 0_u);
    expect(read_u64_le(list_req, 20) == 1_ul);

    binary desc_req;
    binary desc_res;
    {
      socket_loopback_pair sockets;
      binary alpha = make_ascii_blob("blob-alpha");
      binary beta = make_ascii_blob("blob-beta");
      flat_session_server server(sockets.s0, {
        bound_publication{1, "alpha", alpha, nullptr},
        bound_publication{2, "beta", beta, nullptr}
      }, default_visibility, handshake_data{protocol_version_v1, {}});
      flat_session_client client(sockets.s1);
      binary blob;
      client.get_rpc_description(1)->on_resolved([&](binary& b) { blob = b; });
      desc_req = drain_output(sockets.s1);
      write_input(sockets.s0, desc_req);
      server.handle_data();
      desc_res = drain_output(sockets.s0);
      write_input(sockets.s1, desc_res);
      client.handle_data();
      expect(blob == alpha);
    }
    write_testdata("get_rpc_description_req.hex", to_hex(desc_req));
    write_testdata("get_rpc_description_res.hex", to_hex(desc_res));
    expect(read_u32_le(desc_req, 4) == 0_u);
    expect(read_u64_le(desc_req, 20) == 2_ul);

    binary inner_err = encode_inner_rpc_error(1, error_unknown_api);
    write_testdata("inner_rpc_error_unknown_api.hex", to_hex(inner_err));
    expect(read_u32_le(inner_err, 4) == 2_u);

    binary echo_req;
    binary echo_res;
    {
      socket_loopback_pair sockets;
      echo_endpoint ep1;
      binary blob = make_echo_blob();
      flat_session_server server(sockets.s0, {
        bound_publication{1, "alpha", blob, ep1.ep}
      }, default_visibility, handshake_data{protocol_version_v1, {}});
      flat_session_client client(sockets.s1);
      api_connection* conn = nullptr;
      client.open_api(1, blob)->on_resolved([&](api_connection* c) { conn = c; });
      std::uint64_t result = 0;
      conn->rpc().call<std::uint64_t>(0, std::uint64_t{7})->on_resolved([&](std::uint64_t r) { result = r; });
      echo_req = drain_output(sockets.s1);
      write_input(sockets.s0, echo_req);
      server.handle_data();
      echo_res = drain_output(sockets.s0);
      write_input(sockets.s1, echo_res);
      client.handle_data();
      expect(result == 7_ul);
    }
    write_testdata("api_message_echo_req.hex", to_hex(echo_req));
    write_testdata("api_message_echo_res.hex", to_hex(echo_res));
    expect(read_u32_le(echo_req, 4) == 3_u);
    expect(read_u32_le(echo_req, 12) == 1_u);
    expect(read_u32_le(echo_res, 4) == 3_u);

#ifdef GATEWAY_TESTDATA_DIR
    expect(read_testdata("list_apis_req.hex") == list_req);
    expect(read_testdata("list_apis_res.hex") == list_res);
    expect(read_testdata("get_rpc_description_req.hex") == desc_req);
    expect(read_testdata("get_rpc_description_res.hex") == desc_res);
    expect(read_testdata("inner_rpc_error_unknown_api.hex") == inner_err);
    expect(read_testdata("api_message_echo_req.hex") == echo_req);
    expect(read_testdata("api_message_echo_res.hex") == echo_res);
#endif
  };
}
