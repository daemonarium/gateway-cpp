> Authoritative home: `gateway/docs/` in the monorepo. A `gateway-protocol` repo is future work.

# Transport sesji

Kontrakt rury pod protokół sesji. Sam protokół: [`session-namespace.md`](session-namespace.md), przegląd: [`../README.md`](../README.md).

## Kontrakt

Rura ma ten sam kształt co wiadomości RPC: z jednej strony `send` / `try_queue_message` zarejestrowanego typu, z drugiej odczyt i dispatch, i z powrotem.

Implementacja korzysta z kolejek circular buffer i framingu, które już są — [`socket_message_framing.hpp`](../../cpp-rpc/src/serialization/socket_message_framing.hpp), [`SocketMessageChannel`](../../ts-rpc/src/SocketMessageChannel.ts) / [`MessageChannel`](../../ts-rpc/src/MessageChannel.ts). Nowego pakietu na wiadomości nie ma.

Wejście `SocketMessageChannel` to interfejs [`Socket`](../../ts-rpc/src/Socket.ts) po stronie procesu (`SocketMessageChannelSocket`): `inputBuffer` / `outputBuffer`, `flush` / `drain`, `peek` / `skipRead` na wejściu. `LocalSocket` i `HeapSocket` to spełniają. Host-side `ModuleSocket` (czyta output modułu) nie jest kanałem sesji.

Writer (`LocalCircularBufferWriter` / `FlatLocalCircularBufferWriter`) bierze dowolny `CircularBuffer`. `grow` jest opcjonalne: `LocalCircularBuffer` rośnie, `ModuleCircularBuffer` / HEAP zwraca `false`. Pełny bufor bez grow → throw (`flat circular writer: buffer full`). Backpressure strumienia obserwacji jest włączony (patrz niżej). Ogólny backpressure każdego RPC sesji — później.

Transport **nie** interpretuje `api_id` ani inner RPC. Widzi ramki kanału sesji: gołe `rpc_*` i `api_message`.

Jak bajty trafiają na TCP, `postMessage` albo HEAP — sprawa adaptera. TCP wchodzi: to strumień, na którym i tak stoi length-prefix.

## HEAP (`HeapSocket`)

Dwa structy `circular_buffer` (WASM32: `size` + `read_offset` + `write_offset` + `data`) w jednej `WebAssembly.Memory`. Brak `socket_table` i kernela. TS czyta/pisze przez `ModuleCircularBuffer` z adresem structu (struct się nie rusza — JS nie robi realloc).

Para: `client.out = server.in` i `server.out = client.in` (ten sam region). `flushCallback` woła `handleData` drugiej strony; transfer bajtów to no-op. Grow C-side (`waos_buffer_grow`) — poza tym cięciem; testy pre-alokują bufor.

Kod: [`../ts/src/heapSocket.ts`](../ts/src/heapSocket.ts).

## Socket między procesami

Dwa `LocalProcess` (A, B), każdy z `LocalSocket`. `kernel.bind(A, 0, B, 0, BOTH)` — [`pumpFlush`](../../ts-kernel/src/pump.ts) kopiuje `A.outputBuffer → B.inputBuffer` na `flush()` i z powrotem. Zero nowego kodu transportu: `LocalSocket` już jest `Socket`.

To **nie** jest endpoint `pid`+socket (to podpięcie publikacji pod `api_id`). Tu bind jest rurą sesji między dwoma procesami.

`ModuleProcess` / WASM: C++ `SessionServer` compiled to `gateway-session.wasm` sits on socket 0 of a `ModuleProcess`. `ts-net` binds that socket to the per-connection data plane. See [Gateway cross-lang (C++ WASM)](#gateway-cross-lang-c-wasm).

## Emscripten GL module (`EmscriptenProcess`)

A WebGL / WebGPU Emscripten app (for example astro-chart) owns its own `WebAssembly.Memory`, glue JS, and `requestAnimationFrame` loop. It cannot be loaded as `ModuleProcess` (that class instantiates memory and calls `_initialize` / `start`).

`EmscriptenProcess` is the same kernel `Process` contract with sockets. Generic attach is in ts-kernel (`attachEmscripten`); it does **not** call module exports.

1. `attachEmscripten({ kernel, loadGlue })` or `attachEmscripten({ kernel, scriptSrc })` — spawn + attach. `scriptSrc` injects `__waosKernelImports` / `__waosAttach` for Emscripten pre-js (for example [`../../game-dev/astro-chart/web/waoshook.js`](../../game-dev/astro-chart/web/waoshook.js)). `loadGlue` is for tests or custom instantiate.
2. Host `kernel.bind`s socket 0 (after C `sockets_init` / `socket_open`) to a `SessionServer` (TS `LocalProcess` or C++ `ModuleProcess`).
3. The application then calls its own exports. astro-chart: `createAstroChartProcess` / `connectAstroChartSession` in [`../../game-dev/astro-chart/web/astroChartProcess.ts`](../../game-dev/astro-chart/web/astroChartProcess.ts) (`start_session` then, after bind, `start_session_connect`).

Gateway is only the session protocol on that bound socket. Chart API: astro-chart `ChartConfig.hpp` and [`web/chartEndpoint.ts`](../../game-dev/astro-chart/web/chartEndpoint.ts). WASM client: [`../../game-dev/astro-chart/src/exe/session_main.cpp`](../../game-dev/astro-chart/src/exe/session_main.cpp).

## Backpressure

Tam, gdzie rura umie: `try_queue_message` / `onWritable`. `postMessage` może tego nie mieć — ograniczenie rury, nie protokołu.

## Życie sesji

Jedno otwarcie transportu = jedna sesja. Zamknięcie rury = koniec sesji. Nowe otwarcie = **reconnect** (zdarzenie protokołu: handshake od zera, patrz [`session-namespace.md`](session-namespace.md) §Handshake). Nie każdy transport je emituje.

## Handshake niesiony przez transport

Handshake (wersja protokołu, auth) może iść **w metadanych transportu**, nie przez `hello` RPC:

- **WebSocket** — nagłówki HTTP przy upgrade (np. `Authorization`, `Sec-WebSocket-Protocol` = wersja protokołu, cookies).
- **iframe** — `origin`, atrybuty w URL, `postMessage` z origin check.
- **TLS** — certyfikat klienta.
- **TCP (bez TLS)** — brak handshake w metadanych; trzeba `hello` RPC.

Adapter transportu wyciąga te dane z metadanych połączenia i przekazuje `SessionServer` jako strukturę `handshake_data` przy inicjalizacji (patrz [`session-namespace.md`](session-namespace.md) §Handshake). Wtedy `hello` RPC jest opcjonalny (lub pominięty — wersja wynegocjowana w metadanych).

Auth, origin iframe, token w URL — opcje konkretnej rury, nie protokołu. Ale wszystkie one lądują w `handshake_data.auth` (mapa), którą `SessionServer` dostaje przy inicjalizacji.

Kernel `bind(pid, socket)` jako podpięcie **endpointu** pod `api_id` to nie to samo co bind jako rura sesji (dwa procesy, ramki kanału sesji — powyżej). Endpoint: później.

## TCP / WebSocket / SockJS (`ts-net`)

Pakiet [`ts-net`](../../ts-net) to analog [`ts-fs`](../../ts-fs): `LocalProcess` w kernelu hostuje serwis sieciowy. WASM (albo inny `LocalProcess`) gada z nim po zwykłym RPC na sockecie 0 — zero customowych importów hosta. Same połączenia sieciowe są mostkowane na **per-connection sockety kernela** przez `kernel.bind` + pump.

Trzy protokoły (`protocol`: `0` TCP, `1` WebSocket, `2` SockJS). Tylko Node. Bez TLS i bez reconnect (to zdarzenie sesji).

Trzy płaszczyzny socketów:

| Płaszczyzna | Socket | Co niesie |
|-------------|--------|-----------|
| Control RPC | 0 | `net.connect` / `net.listen` / `net.unlisten` / `net.close` |
| Listener (obserwacje) | L, opcjonalny, per `net.listen` | `incoming_connection`, `accept_connection`, `reject_connection`, `connection_closed` |
| Data | D, per zaakceptowane / outbound połączenie | surowe bajty 1:1 z siecią |

Listener jest osobny od RPC, żeby kolejka przychodzących połączeń nie blokowała sterowania. Rekomendacja: zawsze podać `listener_socket`. Bez niego zdarzenia lądują na sockecie 0 (mieszanie odradzane).

### Outbound `net.connect`

WASM alokuje socket D, woła `net.connect(protocol, target, client_socket)`. NetworkProcess otwiera połączenie w Node (`net.createConnection` / `ws` / SockJS websocket), binduje swój `LocalSocket` do D. Od tej pory: sieć → `outputBuffer` netu → pump → `inputBuffer` WASM; w drugą stronę analogicznie.

### Inbound `net.listen` + accept / reject

`net.listen` startuje serwer Node na `(host, port)` (port `0` = efemeryczny; wynik zawiera faktyczny `port`) i binduje socket L.

Przy połączeniu NetworkProcess wysyła na L `incoming_connection { connection_id, listener_id, remote_addr, headers }`. `headers` to mapa stringów: dla TCP pusta, dla WS/SockJS nagłówki HTTP handshake (auth, cookies, origin, `Sec-WebSocket-Protocol`, `JSESSIONID`, …). To jest pierwsza wiadomość z metadanymi transportu — sesja może z nich złożyć `handshake_data` bez `hello` RPC.

WASM odpowiada na tym samym L:

- `accept_connection { connection_id, data_socket, response_headers? }` — alokuje D, NetworkProcess robi `kernel.bind` i (WS) kończy upgrade z `response_headers` (np. wybrany `Sec-WebSocket-Protocol`). TCP: nic poza bindem.
- `reject_connection { connection_id, status, response_headers?, body? }` — WS: zwykły HTTP response (`status` + nagłówki + body), **bez** upgrade. TCP: `destroy`. SockJS: `conn.close` po handshake biblioteki (prawdziwy HTTP reject przed handshake jest poza `sockjs`).

Sieć padła / klient zamknął → `connection_closed { connection_id }` na L (albo na 0, gdy nie ma L).

### Data plane

TCP: bajty 1:1. WebSocket: payload ramki binary/text (granice ramek = osobne `write` do circular buffer; session framing length-prefix reassembluje). SockJS: dekapsulacja do surowych bajtów (latin1, żeby przeżył dowolny oktet).

Backpressure (smoke): gdy `outputBuffer` netu nie ma miejsca, mostek woła `pause()` na sockecie Node; po `drain` — `resume()`. Pełne §7.5 (`tryQueueMessage` / `onWritable` na kanale sesji) to osobna warstwa.

Kod: [`ts-net/src/createNetProcess.ts`](../../ts-net/src/createNetProcess.ts), mostki `TcpBridge` / `WsBridge` / `SockJsBridge`.

## Gateway na ts-net

Adapter [`gateway/ts/src/netGateway.ts`](../ts/src/netGateway.ts) składa sesję na tej rurze.

- `createNetGatewayServer(kernel, pubs, filter?, options)` — `net.listen` na TCP / WS / SockJS. Przy `incoming_connection` buduje `handshake_data` z nagłówków (`handshakeFromHeaders`: `authorization` / `cookie` / `origin` → `auth`, `Sec-WebSocket-Protocol` jeśli to liczba → `protocol_version`) i stawia `SessionServer` na data sockecie D, potem `accept` (albo `reject` bez upgrade, gdy `shouldAccept` wraca `false`).
- `createNetGatewayClient(kernel, protocol, target, options)` — `net.connect`, `SessionClient` na D. `skipHello: true` gdy wersja poszła transportem (np. WS protocol `1`).
- Dwa `LocalProcess` na kernel: host `ts-net` i peer sesji (bind do siebie samego jest zabroniony). Sterowanie `NetService` jest in-process (nie RPC na sockecie 0).

TCP bez metadanych HTTP: serwer i tak dostaje `handshake_data` z `defaultProtocolVersion` (v1). Klient może wołać `hello` albo polegać na tym handshake. WS reject: HTTP 403, upgrade nie dochodzi do skutku.

Testy: [`gateway/ts/tests/netGateway.test.ts`](../ts/tests/netGateway.test.ts).

## Gateway cross-lang (C++ WASM)

C++ nie otwiera natywnych socketów POSIX/asio. `session_server` z `socket_c_transport` kompiluje się do WASM (`gateway-session.wasm`) i ładuje się jako `ModuleProcess`. `ts-net` mostkuje TCP / WebSocket / SockJS na socket kernela — ten sam socket 0, na którym siedzi C++ `SessionServer`.

`createModuleGatewayServer(kernel, wasmCode, pubs, filter?, options)`:

1. Host `ts-net` (`createNetProcess`) plus TS `LocalProcess` ze socketem listenera L. Accept/reject steruje `NetService` (TS), więc L nie jest w WASM.
2. Przy `incoming_connection` host startuje `ModuleProcess` z `wasmCode`. `_start` otwiera socket 0, stawia `session_server` z echo API (`api_id` 1 `alpha`, 3 `gamma`) oraz strumieniem (`api_id` 2 `stream`) i handshake v1.
3. `net.service.accept(..., data_socket=0, peer=moduleProc)` binduje N_D (ts-net) z D (WASM).
4. Klient sieciowy jest zawsze TS (`createNetGatewayClient`). WASM nie inicjuje I/O.

Kierunki w testach (klient zawsze TS `SessionClient`):

| Suite | Serwer |
|-------|--------|
| TS→TS | `createNetGatewayServer` |
| TS→C++ | `createModuleGatewayServer` |
| C++→TS | `createNetGatewayServer` (TS echo `CallbackEndpoint`) |
| C++→C++ | `createModuleGatewayServer` |

Build: `cmake --build cmake-build-emscripten --target gateway_session_wasm` → `cmake-build-emscripten/gateway-session.wasm`. Testy cross-lang skipują suity C++ gdy pliku nie ma.

TLS, natywne sockety C++, klient C++ inicjujący połączenia — poza zakresem.

Testy: [`gateway/ts/tests/netGatewayCrossLang.test.ts`](../ts/tests/netGatewayCrossLang.test.ts), strumienie: [`gateway/ts/tests/netGatewayStream.test.ts`](../ts/tests/netGatewayStream.test.ts).

## Obserwacje (v1.5 — bez AST)

Klient obserwuje `(api_id, function_id, args)` wprost. Nie ma ewaluatora `Member` / `Switch` i nie ma krawędzi `Ref`. Obsługa strumieni jest już docelowa: gateway nadaje własne `stream_id`, trzyma mapę `ref_id → stream_id` 1:1 i przepisuje `stream_id` na `ref_id` zanim ramka wyjdzie do klienta. v2 dokłada AST i mapę wiele-do-wielu na tym samym fundamencie.

RPC sesji (function_id, po `hello` / `list_apis` / `get_rpc_description` / `get_session_description`):

| id | RPC | Efekt |
|----|-----|--------|
| 4 | `set_reference(ref_id, expr)` | walidacja, alokacja `stream_id`, start strumienia na endpoincie |
| 5 | `observe_reference(ref_id)` | włącz forward i wyślij ramki zbuforowane gdy ref był nieobserwowany |
| 6 | `unobserve_reference(ref_id)` | stop forward; strumień i bufor zostają |
| 7 | `dispose_reference(ref_id)` | `stream_cancel` na endpoincie, usuń mapowanie |

`fetch` nie jest RPC. `Reference.fetch()` woła `observe`, po `readyPromise` czyta `getEntries()` i woła `unobserve`.

Wyrażenie na przewodzie to pełny `Expr` (sześć wariantów). v1.5 przyjmuje tylko `Call(api_id, function_id, Literal*)`. `Ref` jako korzeń, `Member`, `Switch`, `NonEmpty` → `unknown_expr`. `Ref` jako argument `Call` → `dependent_ref_not_supported`.

Serwer, przy `set_reference`:

1. Waliduje wyrażenie.
2. Nadaje `stream_id` od 1 i zapisuje `ref_id → stream_id`.
3. Karmi endpoint prawdziwym inner `rpc_request`: `stream_handle{stream_id, since: 0}` plus literały.
4. Ramki `stream_item` / `stream_done` / `stream_error` / `stream_reset` / `stream_opened` z endpointu: odwrotna mapa, podmiana 8 B `stream_id` (offset 12) na `ref_id`, forward jako `api_message` tylko gdy ref jest obserwowany. W przeciwnym razie ramka czeka w buforze tego refu.

`stream_cancel` od klienta (inner typ 9, `stream_id` = `ref_id`) jest przepisywany na `stream_id` endpointu i puszczany dalej. Inne inner typy poza `rpc_request` nadal są porzucane.

Klient: `ApiConnection` na `api_id` ma `StreamMultiplexer`. `Reference` podpina odbiornik na `ref_id` (to jest `stream_id` po rewrite) i trzyma `Map<Path, Observable>`. v1.5 ma jeden wpis. `sum()` zwraca ten `Observable`.

`StreamEndpoint` (i C++ `stream_endpoint`) zamienia inner RPC `echo_stream` / `echo_fail` / `echo_reset` / `echo_mark` na `stream_*`. Moduł WASM publikuje to jako `api_id` 2.

Backpressure: `StreamRelayConsumer.onChange` zwraca pending promise, gdy `try_queue_message` odmawia (kanał ma `minWritable` i w buforze wyjścia jest ciasno). Pętla flush po `drain` woła `onWritable` i dokleja zaległą ramkę. C++ `waos_socket_output_drain` zostaje na sockecie sesji; sam strumień pilnuje własnego bufora endpointu.

Poza tym cięciem (v2): ewaluator AST, `Ref` jako zależność, wiele `ref_id` na jeden `stream_id`, `RemoteRegistry`, `api_id = 0` przez graf, reconnect grafowy, limit pamięci.

## Przykłady

Nie jest to katalog zamknięty. Wszystko, co wozi te ramki w dwie strony, może być transportem.

| Rura | Kiedy |
|------|--------|
| HEAP / `HeapSocket` | circular_buffer w `WebAssembly.Memory`, bez socket_table |
| Socket między procesami | `LocalProcess` A↔B, `kernel.bind` + pump |
| `postMessage` (`ArrayBuffer`) | iframe, `DedicatedWorker`, `MessagePort` |
| TCP / WebSocket / SockJS | sieć Node przez `ts-net` → per-connection socket kernela; sesja na data sockecie |
