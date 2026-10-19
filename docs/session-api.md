> Authoritative home: `gateway/docs/` in the monorepo. A `gateway-protocol` repo is future work.

# Session RPC i mux

Kontrakt metod sesji, stanu połączenia, forwardu `api_message` i wirtualnych socketów. Wire: [`wire-format.md`](wire-format.md). Namespace: [`session-namespace.md`](session-namespace.md). Strumienie: [`stream-multiplexer.md`](stream-multiplexer.md).

To cięcie: katalog, blob, handshake, `api_message` → callback `endpoint_ref`, `SessionClient.openApi`, obserwacja bezpośrednia (`set_reference` / `observe_reference` / `fetch_reference` / `close_stream`), duplex `open_pipe` / `close_pipe`. **Świadomie nie wchodzi:** graf obserwacji v2, pid kernela / zagnieżdżona sesja, dziury `function_id` metod publikacji, zmiana listy API w żywej sesji, reconnect poza pełnym resetem.

Kanał sesji używa **compressed** (uvarint dla `socket_id`, `ref_id`, `api_id`, indeksu typu). Inner ramki (`api_message` trailing, `mux_socket_data.payload`) zostają **flat** — kopia surowych bajtów, bez re-serializacji.

## `function_id` (stałe)

Kolejność = kolejność `register_object_method` na **instancji** sesji (stan per połączenie, nie singleton). Blob z `get_session_description` nadaje te same id przez `assignMethodIds`.

| `function_id` | Metoda | Wejście | Wyjście |
|---------------|--------|---------|---------|
| 0 | `hello` | `client_protocol` | `server_protocol` |
| 1 | `list_apis` | — | `api_info[]` |
| 2 | `get_rpc_description` | `api_id: uint32` | `binary` (blob `[envelope u8][compressed type_registry]`) |
| 3 | `get_session_description` | — | `binary` (blob rejestru sesji) |
| 4 | `set_reference` | `ref_id: uint32`, `expr` | — |
| 5 | `observe_reference` | `ref_id: uint32`, `since_timestamp: uint64` | `socket_id: uint32` |
| 6 | `unobserve_reference` | `ref_id: uint32` | — |
| 7 | `dispose_reference` | `ref_id: uint32` | — |
| 8 | `fetch_reference` | `ref_id: uint32`, `flags: uint8`, `since_timestamp: uint64` | `socket_id: uint32` |
| 9 | `open_pipe` | `cookie: uint32` | `socket_id: uint32` |
| 10 | `close_pipe` | `socket_id: uint32` | — |
| 11 | `close_stream` | `socket_id: uint32` | — |

Klient woła te id bez wcześniejszego pobrania bloba. `observe_reference` / `fetch_reference` / `open_pipe` zwracają `socket_id` przydzielony przez serwer (`0` zarezerwowane).

`observe_reference` to kanoniczny 1:1 live socket (`mux kind=0`) dla `observable()`. `since_timestamp = 0` to pełny snapshot; `> 0` to delta `ts > since` (ten sam kontrakt co inner `stream_handle`). Po idle w tej samej sesji klient wysyła `maxTs()`.

`fetch_reference` **zawsze** alokuje nowy socket (`mux kind=1`), niezależnie od `flags`. `flags=0` (`none`) to skan: `stream_done` / `stream_error` zamyka tylko ten socket (`completeFetchIfNeeded`). `flags=1` (`observe`) to extra live: catch-up `stream_done` **nie** zamyka socketa — zamknięcie przez `close_stream(socket)` (cancel inner) albo `dispose_reference`.

`unobserve_reference` zamyka tylko kanoniczny socket obserwacji; fetche (w tym extra live) zostają. `dispose_reference` zamyka obserwację i wszystkie fetche. `close_stream` zamyka jeden fetch socket (skan lub extra live).

## Typy

```text
struct client_protocol {
  uint64 protocol_version;     // v1 = 1; na compressed = uvarint
  map<string, binary> auth;
};

struct server_protocol {
  uint64 protocol_version;
};

struct api_info {
  uint32 api_id;
  string name;
  binary description_hash;     // dokładnie 8 B, XXH3-64 LE
};

struct api_message {           // typ kanału, indeks 3; inner ramka = trailing payload (flat)
  uint32 api_id;
};

struct mux_socket_open {       // indeks 4
  uint32 socket_id;
  uint8 kind;                  // 0=observe, 1=fetch, 2=pipe
  uint32 ref_id;               // observe/fetch; 0 dla pipe
  uint32 cookie;               // pipe; 0 w przeciwnym razie
};

struct mux_socket_data {       // indeks 5; payload = surowa inner ramka (flat) albo bajty pipe
  uint32 socket_id;
  binary payload;
};

struct mux_socket_close {      // indeks 6
  uint32 socket_id;
};

struct mux_socket_window {     // indeks 7; addytywne, obie strony
  uint32 socket_id;
  uint32 add_bytes;
};
```

`handshake_data` z transportu to ta sama struktura co `client_protocol`.

## Stan sesji

`pre-handshake` → `ready`. Zła wersja → sesja **zakończona** (dalsze RPC też `rpc_error`).

- Konstruktor **bez** `handshake_data`: start w `pre-handshake`. Każde RPC oprócz `hello` → `rpc_error` (`handshake_required`).
- Konstruktor **z** `handshake_data` i `protocol_version == 1`: start w `ready`. `hello` opcjonalny.
- `hello` z `protocol_version != 1`: `rpc_error` (`unsupported_protocol_version`), stan zakończony. Kolejne `list_apis` / `hello` też błąd.
- `hello` z wersją 1: `server_protocol.protocol_version == 1`, stan `ready`.

## Widoczność i błędy

Filtr to funkcja podana przy konstrukcji: `(api_id, auth) → bool`. `list_apis` zwraca tylko widoczne publikacje. `api_id = 0` nie jest publikacją i nie jest na liście.

`get_rpc_description` dla ukrytego albo nieznanego `api_id` (w tym `0`) → **ten sam** `rpc_error` (`unknown_api`). Nie zdradza, czy id istnieje.

## `description_hash`

XXH3-64 dokładnych bajtów bloba (z envelope), wynik jako 8 B little-endian. Serwer liczy przy konstrukcji z `description_blob`. Klient porównuje bajty; nie musi znać algorytmu poza testami.

Dwa wywołania `get_rpc_description` tego samego widocznego `api_id` zwracają te same bajty. Hash na `list_apis` zgadza się z XXH3 tych bajtów.

## Katalog w tym cięciu

Lista publikacji jest stała przez życie serwera. Klient bierze katalog z `list_apis` przy starcie i po reconnect. Zmiana widoczności w trakcie żywej sesji nie jest w tym cięciu.

## Blob sesji

`get_session_description` zwraca blob type_registry, który po wczytaniu i `assignMethodIds` ma metody sesji z id 0–11 w ustalonej kolejności oraz typ `api_message` i cztery `mux_socket_*`. `mux_socket_open` ma cztery pola (`socket_id`, `kind`, `ref_id`, `cookie`). `observe_reference` / `fetch_reference` / `open_pipe` zwracają `uint32`. `observe_reference` ma `since_timestamp`; `fetch_reference` ma `flags` + `since_timestamp`.

## Mux: `SessionServer`, `SessionClient`, `ApiConnection`

Runtime publikacja to `{ api_id, name, description_blob, endpoint }`. Serializowane `api_publication` (katalog) **nie** niesie endpointu.

`endpoint_ref` (v1): `send_message(payload)` i `on_message(handler)`. W tym cięciu jedna implementacja: callback in-memory. Endpoint nie jest gateway-aware — widzi surowe bajty inner ramek.

`SessionServer` składa `session` (stan + RPC 0–11) z mapą `api_id → endpoint_ref`, handlerem `api_message` i `SocketMux`:

- sesja `ready` i widoczny `api_id` ≠ 0 → skopiować trailing payload do `endpoint->send_message` (bez deserializacji inner RPC);
- odpowiedź z `endpoint->on_message` → `api_message{api_id}` + trailing na kanał sesji (tylko gdy `ready`);
- inaczej odrzut (niżej). Widoczność to całe `api_id`; mux nie patrzy na inner `function_id`.

`SessionClient`: `hello` (pomijany gdy konstruktor dostał już `handshake_data`), `list_apis`, `get_rpc_description` z cache po `description_hash` (drugi raz ten sam hash → bez RPC), `get_session_description`, `openApi(api_id)` → jedna `ApiConnection` per id. `openApi` bierze blob z cache albo woła `get_rpc_description` i stawia inner `SocketRpcClient` z tego rejestru.

`ApiConnection` pakuje każdą kompletną inner ramkę RPC (`[len][body]`) jako trailing `api_message`. Odbiór: trailing → inner input → `handleData`. Strumienie i pipe-y idą przez `SocketMux` / wirtualny socket, nie przez `ApiConnection`. Serwer alokuje `socket_id`; `observe_reference` i `fetch_reference` zwracają ten id. `SessionServer.openPipe(cookie)` / `SessionClient.onPipe` to pipe inicjowany przez serwer. `Reference.stream()` reuse live gdy `activeCount() > 0`; `Reference.fetch()` zawsze skan; `openStream(receiver, flags, since)` to surowy `StreamReceiver` przez `fetch_reference`.

## Odrzut `api_message`

Nie drop i nie outer `rpc_error` (outer `request_id` nie pasuje do inner klienta).

Peek inner ramki: jeśli `type_index == 0` (`rpc_request`), odczytać `request_id` z headera inner RPC (**nie** argumenty) i odesłać `api_message{ten sam api_id}` z trailing = kompletna inner ramka `rpc_error` + `runtime_error`:

- nieznany, ukryty albo `api_id = 0` → `unknown_api`;
- `pre-handshake` → `handshake_required`;
- sesja failed → `unsupported_protocol_version`.

Jeśli inner typ ≠ `rpc_request`, nie forwardować i nie odpowiadać. Testy mux: sesja `failed` + `api_message`, inner `rpc_response`, trailing krótszy niż peek.

`get_rpc_description` dla ukrytego / nieznanego id nadal jest session `rpc_error` (`unknown_api`).
