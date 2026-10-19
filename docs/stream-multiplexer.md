> Authoritative home: `gateway/docs/` in the monorepo. A `gateway-protocol` repo is future work.

# Stream multiplexer — per-stream backpressure

Virtual sockets on the session channel isolate stream consumers. One slow consumer no longer fills the session output and stalls every other ref, fetch, pipe, or RPC on that connection.

RPC stays on `api_message` (logical socket 0). Stream events and duplex pipes travel on `mux_socket_*`. The **server allocates** `socket_id` (`uint32`, `0` reserved). Observe and each fetch get **independent** sockets. Inner frames stay **flat**; the session channel is **compressed** (uvarint for `socket_id`, `ref_id`, type index).

TS and C++ share the wire format. Design lives in `ts-rpc` / `cpp-rpc` (`SocketMux`); Gateway owns lifecycle and the client-side virtual socket that feeds `StreamMultiplexer`.

## Layers

```text
Endpoint stream_*  →  SessionServer SocketMux  →  session channel
                                                 mux_socket_data { socket_id, payload = inner frame }

session channel    →  SessionClient SocketMux  →  VirtualSocket input buffer
                                                 →  StreamMultiplexer (stream_id rewritten to socket_id)
                                                 →  GatewayObservable
                                                 →  mux_socket_window on drain
```

`api_message` is unchanged: inner RPC only (flat inner frames copied raw). `stream_cancel` from the client still rides as trailing `api_message` payload (`stream_id` = mux `socket_id`).

Duplex pipes skip `StreamMultiplexer`: raw `mux_socket_data` both ways.

## Wire format

Registered after `api_message` (index 3). Session codec is compressed: integers ≥ 16-bit are uvarint.

| Index | Type | Fields | Credit |
|-------|------|--------|--------|
| 4 | `mux_socket_open` | `socket_id: u32`, `kind: u8`, `ref_id: u32`, `cookie: u32` | 0 (control) |
| 5 | `mux_socket_data` | `socket_id: u32`, `payload: binary` | `payload.length` bytes |
| 6 | `mux_socket_close` | `socket_id: u32` | 0 (control) |
| 7 | `mux_socket_window` | `socket_id: u32`, `add_bytes: u32` | 0 (control) |

`kind`: `0=observe`, `1=fetch`, `2=pipe`. `ref_id` is set for observe/fetch, `0` for pipe. `cookie` is set for pipe, `0` otherwise.

Rules:

- Data consumes byte credit. Send only if `remaining > 0`. One frame may exceed remaining (send-one-past); the next send blocks.
- Control frames (open/close/window) always pass, even at `remaining = 0`.
- `mux_socket_window` is additive, saturating `u32`. Either peer grants the other side's send credit.
- Byte = raw inner frame (or raw pipe payload), i.e. `payload.length`.
- Unknown `socket_id`: no-op.
- `socket_id == 0` is reserved for session RPC / `api_message`.

Outer framing is length-prefixed. `mux_socket_data.payload` is a `binary` field (not trailing payload). Gateway does **not** re-serialize inner frames.

## ID allocation

The server holds a monotonic `uint32` counter starting at 1 (`0` skipped).

- `observe_reference(ref_id, since_timestamp) → socket_id` — one live socket per `ref_id` (deduped). Inner `start_stream` gets `flags=observe` and the RPC `since`.
- `fetch_reference(ref_id, flags, since_timestamp) → socket_id` — **new** socket every call (`kind=1`), even with `flags=observe`, even while canonical observe is live.
- `close_stream(socket_id)` — close one fetch socket and cancel its inner stream.
- `open_pipe(cookie) → socket_id` — duplex pipe.
- Inner endpoint `stream_id` is a separate counter. Gateway rewrites inner `stream_id` → mux `socket_id` on the way to the client. `since` is a field on the inner `stream_handle`; inner frames stay flat.

`ref_id` remains the session-RPC key (`uint32`). It is **not** equal to `socket_id`.

## Flow control

Each virtual socket has `remaining` and `sent` counters.

1. Server `openSocket(socket, kind, ref, cookie)` (and for pipes, grants the default 16 KiB window).
2. Client on `mux_socket_open` grants the default window (so the server can send).
3. Sender emits `mux_socket_data` while `remaining > 0`.
4. Receiver grants `add_bytes` equal to consumed payload length.
5. `GatewayObservable.onPause` stops grants on **that** socket. `onResume` flushes unacked bytes.
6. `unobserve_reference` closes the observe socket only. Fetches stay. `dispose_reference` closes observe + all fetches.

A blocked `sendData` is a pending promise.

Idle → subscribe in the **same** session sends `observe_reference` with `since = maxTs()` so the snapshot is not replayed. A full TCP reconnect is still a full reset (new session, refs gone).

## DRR scheduler

Ready sockets (`remaining > 0` and outbound queued) share the physical channel with deficit round-robin plus aging (`starved` cycles). The quantum defaults to 4 KiB. Control frames are not scheduled — they use `sendMessage` immediately.

## Lifecycle

| Event | Client | Server |
|-------|--------|--------|
| `observe_reference` / `fetch_reference` | register pending receiver, RPC (returns `socket_id`) | alloc socket, `mux_socket_open`, start inner stream, `sendData` |
| inner `stream_*` | demux on virtual socket (`stream_id == socket_id`) | rewrite inner `stream_id → socket_id`, `mux_socket_data` |
| fetch `stream_done` / `stream_error` | receiver `onDone` / `onError` | close **that** fetch socket only if `flags=none`; extra live (`flags=observe`) stays open |
| `unobserve_reference` | detach observe receiver | close observe socket; cancel inner observe; fetches stay |
| `close_stream` | detach that fetch receiver | close that fetch socket; cancel inner |
| `dispose_reference` | drop handle | close observe + fetches; cancel inner streams |
| `open_pipe` / server `openPipe` | `onPipe(socket, cookie)`, grant window | alloc, `mux_socket_open{kind=pipe}`, grant window |
| `close_pipe` / `closePipe` | local teardown | `mux_socket_close` |
| reconnect | new `SessionServer` / `SessionClient`; full reset | same |

## Backpressure isolation

Socket A at `remaining = 0` does not prevent socket B from sending. Session RPC does not consume mux credit. A paused observe socket does not stall a fetch or a pipe on the same `ref_id`.

The client virtual input buffer is larger than the default window (64 KiB vs 16 KiB) so a send-one-past frame still fits.

## Tests

- `ts-rpc/tests/socketMux.test.ts`, `cpp-rpc/src/test/socket_mux_ut.cpp` — open/data/close, window, duplex grant, kind/ref/cookie, isolation, DRR.
- `gateway/ts/tests/streamMux.test.ts`, `gateway/cpp/src/test/session_stream_ut.cpp` — observe + fetch coexist, `fetch()` bypass, `stream()` shortcut, since on resubscribe, raw `openStream`.
- `gateway/ts/tests/pipe.test.ts`, `gateway/cpp/src/test/pipe_ut.cpp` — client and server initiated pipes, echo, close, isolation vs paused observe.
- `gateway/ts/tests/netGatewayStream.test.ts` — same over TCP/WS/SockJS, TS and C++ servers.
