#include "gateway/echo_api.hpp"
#include "gateway/session.hpp"
#include "gateway/session_server.hpp"
#include "gateway/stream_endpoint.hpp"

#include <utils/socket_handlers.hpp>

#include <socket/socket.h>

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <memory>
#include <vector>

using waos::gateway::bound_publication;
using waos::gateway::default_visibility;
using waos::gateway::echo_endpoint;
using waos::gateway::handshake_data;
using waos::gateway::make_echo_blob;
using waos::gateway::make_session_description_blob;
using waos::gateway::make_stream_blob;
using waos::gateway::stream_endpoint;
using waos::gateway::protocol_version_v1;
using waos::gateway::session_server;

namespace {

  std::unique_ptr<echo_endpoint> alpha;
  std::shared_ptr<stream_endpoint> stream_api;
  std::unique_ptr<echo_endpoint> gamma;
  std::unique_ptr<session_server<>> gateway_server;

}

extern "C" size_t rpc_description(std::uint32_t /*socket*/, char** buffer) {
  try {
    waos::gateway::binary blob = make_session_description_blob();
    *buffer = static_cast<char*>(std::malloc(blob.size()));
    if(!*buffer) {
      return 0;
    }
    std::memcpy(*buffer, blob.data(), blob.size());
    return blob.size();
  } catch(...) {
    return 0;
  }
}

extern "C" void waos_socket_data(std::uint32_t socket) {
  socket_handlers::handle_socket_data(socket);
}

extern "C" void waos_socket_output_drain(int socket) {
  socket_handlers::handle_socket_drain(static_cast<std::uint32_t>(socket));
}

extern "C" void start() {
  sockets_init(8);
  struct socket_options socket_options = {
    .input_buffer_size = 65536,
    .output_buffer_size = 65536,
    .flush_threshold = 4096,
    .data_event_threshold = 0,
    .drain_event_threshold = 4,
    .flags = 0,
  };
  socket_open(0, &socket_options);

  alpha = std::make_unique<echo_endpoint>();
  stream_api = std::make_shared<stream_endpoint>();
  gamma = std::make_unique<echo_endpoint>();
  waos::gateway::binary blob = make_echo_blob();
  handshake_data handshake;
  handshake.protocol_version = protocol_version_v1;
  std::vector<bound_publication> pubs;
  pubs.push_back({1, "alpha", blob, alpha->ep});
  pubs.push_back({2, "stream", make_stream_blob(), stream_api});
  pubs.push_back({3, "gamma", blob, gamma->ep});
  gateway_server = std::make_unique<session_server<>>(
    static_cast<std::uint32_t>(0),
    std::move(pubs),
    default_visibility,
    handshake
  );

  socket_handlers::register_socket_handler(0, []() {
    if(gateway_server) {
      gateway_server->handle_data();
    }
  }, []() {
    if(gateway_server) {
      gateway_server->handle_drain();
    }
  });
}
