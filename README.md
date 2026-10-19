# gateway-cpp

The C++ side of the Daemonarium gateway session protocol. One
`SessionServer` multiplexes many RPC APIs over a single bidirectional
message channel. The same sources compile to a WebAssembly module a
host loads, and to native test executables.

The CMake target is `gateway_cpp`. This repository is
`daemonarium/gateway-cpp`.

## What you need

Stock Clang cannot compile this library. Build the
[reflection-toolchain](https://github.com/daemonarium/reflection-toolchain)
sysroot and point `P2996_COMPILER_SYSROOT` at it.

This library links
[rpc-cpp](https://github.com/daemonarium/rpc-cpp):

```
git submodule add https://github.com/daemonarium/rpc-cpp.git extern/rpc-cpp
```

## Add it to a project

```
git submodule add https://github.com/daemonarium/gateway-cpp.git extern/gateway-cpp
```

```cmake
add_subdirectory(extern/rpc-cpp)
add_subdirectory(extern/gateway-cpp)
target_link_libraries(your_app PRIVATE gateway_cpp)
```

`rpc-cpp` must be added first so the `cpp_rpc` target exists. That
build does not fetch Boost.UT and does not build the tests.

## Build it standalone

```
cmake -B build -S . \
  -DCMAKE_TOOLCHAIN_FILE=$P2996_COMPILER_SYSROOT/toolchain/p2996-toolchain.cmake
cmake --build build
ctest --test-dir build --output-on-failure
```

The standalone configure expects `extern/rpc-cpp` in this tree.

## Wasm module

```
cmake -B build-web -S . \
  -DCMAKE_TOOLCHAIN_FILE=$P2996_COMPILER_SYSROOT/toolchain/emscripten-p2996-toolchain.cmake \
  -DWASM=ON
cmake --build build-web
```

`build-web/gateway-session.wasm` exports `_rpc_description`, `_start`,
`_waos_socket_data`, and `_waos_socket_output_drain`.

## One include

```cpp
#include <gateway/session_server.hpp>

waos::gateway::session_server<> server(
  transport,
  publications,
  waos::gateway::default_visibility,
  handshake
);
```

A publication is an `api_id`, a name, a type-registry blob, and an
endpoint. The server forwards `api_message` to that endpoint without
deserializing the inner RPC.

## Protocol

Byte layout and message indices are in
[docs/wire-format.md](docs/wire-format.md) and
[docs/session-api.md](docs/session-api.md). The channel codec is
compressed. Inner frames stay flat.

The base RPC those inner frames use is
[rpc-protocol](https://github.com/daemonarium/rpc-protocol) v0.1.0.
A separate gateway protocol repository is future work. These docs are
the C++ implementation's view of the session protocol.

## Examples

[docs/examples.md](docs/examples.md) shows an echo server, an echo
client, a stream API, and a duplex pipe.

## Regenerating the session client

`include/gateway/generated/session_rpc_client.hpp` is checked in.
Rebuild it only after the session method list changes: build
`gateway-session.wasm`, then run `rpc-codegen` from rpc-cpp with
`-l "C++ Client"`. v0.1.0 does not wire that step into CMake.

## License

MIT for this library. Upstream notices are in `NOTICE`. Conduct is in
`CODE_OF_CONDUCT.md`.
