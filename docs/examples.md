# Examples

These snippets use the headers in this repository. They are not built
by CMake. A real transport is any `socket_c_transport` or buffer pair
the rpc-cpp socket layer already accepts.

## Echo server

One publication. The endpoint echoes a `uint64`. This is the shape of
`src/module_main.cpp` with a single API.

```cpp
#include <gateway/echo_api.hpp>
#include <gateway/session_server.hpp>

#include <utils/socket_handlers.hpp>
#include <socket/socket.h>

waos::gateway::echo_endpoint alpha;
waos::gateway::handshake_data handshake;
handshake.protocol_version = waos::gateway::protocol_version_v1;

std::vector<waos::gateway::bound_publication> pubs;
pubs.push_back({1, "alpha", waos::gateway::make_echo_blob(), alpha.ep});

waos::gateway::session_server<> server(
  static_cast<std::uint32_t>(0),
  std::move(pubs),
  waos::gateway::default_visibility,
  handshake
);

socket_handlers::register_socket_handler(0, [&]() {
  server.handle_data();
}, [&]() {
  server.handle_drain();
});
```

`api_id` 0 is reserved for the session namespace. Publications start
at 1.

## Echo client

```cpp
#include <gateway/session_client.hpp>

waos::gateway::session_client<> client(transport);

waos::gateway::client_protocol hello;
hello.protocol_version = waos::gateway::protocol_version_v1;
client.hello(hello);
client.list_apis();

auto opened = client.open_api(1);
opened->on_resolved([&](waos::gateway::api_connection* api) {
  // api writes a complete flat inner frame as the trailing payload
  // of api_message { api_id = 1 }.
  (void)api;
});
```

`hello` is optional when the transport already carried the handshake.
`list_apis` returns names and an 8-byte XXH3-64 of each description
blob. `get_rpc_description` returns the type-registry blob for one
`api_id`.

## Stream API

Publish `stream_endpoint` the same way as the echo endpoint, with
`make_stream_blob()`. On the client:

```cpp
#include <gateway/session_client.hpp>

client.observe_reference(ref_id, nullptr, /*since*/ 0);
client.fetch_reference(ref_id, nullptr, waos::stream_flags::none, /*since*/ 0);
```

`observe_reference` is the one live socket for that reference.
`since` 0 is a full snapshot. A later subscribe in the same session
passes `maxTs` so the snapshot is not replayed. `fetch_reference`
always allocates a new socket. An empty `stream_item` value is a
tombstone. Apply last-timestamp-wins per key.

`unobserve_reference` closes only the live socket. Fetches stay until
`close_stream` or `dispose_reference`.

## Pipe

```cpp
auto opened = client.open_pipe(/*cookie*/ 0xA11CE);
opened->on_resolved([&](std::uint32_t pipe_id) {
  client.close_pipe(pipe_id);
});
```

The server answers with `mux_socket_open` whose `kind` is pipe.
`mux_socket_data` then carries raw application bytes in both
directions. Pipes do not go through the stream multiplexer. Closing
sends `mux_socket_close`.
