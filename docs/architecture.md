# Architecture

`SessionServer` is the session endpoint. It owns a compressed RPC
server, a `session_state` catalog, and a `SocketMux` for virtual
stream sockets. Each `bound_publication` maps an `api_id` to an
`endpoint_ref`. An incoming `api_message` is copied, trailing payload
and all, onto that endpoint. The server does not deserialize the inner
RPC.

`SessionClient` is the other end of the same channel. `hello`,
`list_apis`, and `get_rpc_description` are session methods (function
ids 0, 1, and 2). `open_api` fetches a publication's type-registry
blob and returns an `api_connection` that writes inner frames as the
trailing payload of `api_message`.

`virtual_stream_socket` is the client side of `mux_socket_*`. The
server allocates `socket_id`. `socket_id` 0 stays the session RPC.
Observe, fetch, and pipe each get their own socket. Byte credit is
`mux_socket_window`. One stalled socket does not block the others.

`echo_endpoint` and `stream_endpoint` are the reference publications
used by the wasm module and the tests. They are not required in a
consumer binary.

The checked-in client in `include/gateway/generated/session_rpc_client.hpp`
is the typed wrapper over those session function ids.
