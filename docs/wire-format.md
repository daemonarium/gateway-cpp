> Authoritative home: `gateway/docs/` in the monorepo. A `gateway-protocol` repo is future work.

# Wire format kanału sesji

Układ bajtów ramek na kanale sesji. Sygnatury RPC: [`session-api.md`](session-api.md). Namespace: [`session-namespace.md`](session-namespace.md). Mux: [`stream-multiplexer.md`](stream-multiplexer.md).

Kanał używa **istniejącego framingu** (`socket_message_framing` / `SocketMessageChannel`): każda ramka to `[len u32 LE][body]`. Codec kanału sesji to **compressed** (uvarint32/uvarint64 dla liczb ≥ 16-bit, indeks typu jako uvarint). Inner ramki w trailing `api_message` i w `mux_socket_data.payload` zostają **flat** — kopia surowych bajtów, bez re-serializacji.

## Indeksy typów kanału

Rejestr wiadomości kanału (kolejność `register_message_type` / `registerMessageType`):

| Indeks | Typ |
|--------|-----|
| 0 | `rpc_request` |
| 1 | `rpc_response` |
| 2 | `rpc_error` |
| 3 | `api_message` |
| 4 | `mux_socket_open` |
| 5 | `mux_socket_data` |
| 6 | `mux_socket_close` |
| 7 | `mux_socket_window` |

`api_message` (3) to mux na publikację `api_id` (RPC). `mux_socket_*` (4–7) to wirtualne sockety strumieni i pipe-ów. `socket_id = 0` jest zarezerwowane dla RPC / `api_message`. Serwer alokuje pozostałe `socket_id` (`uint32`). `socket_id` **nie** jest równy `ref_id`.

## Outer ramka

```text
[outer_len u32 LE][type_index uvarint][compressed body…][writeRest pointer payloads…][trailing payload]
```

`outer_len` liczy bajty po prefiksie długości (type_index + body + `writeRest` + trailing payload). Compressed `writeRest` dopisuje odroczone ciała pointerów; na kanale sesji **nie ma** sentinela `u32 0` (to cecha codecu flat).

Dla `rpc_request` / `rpc_response` / `rpc_error` trailing payload to argumenty / wynik / wyjątek — tak jak dziś w `begin_send` + zapis + `commit_send`.

## `api_message`

Struct na przewodzie ma **tylko** `api_id` (`uint32`, na compressed = uvarint32). Inner ramka **nie** jest polem `binary`.

Trailing payload po strukturze (ten sam slot co argumenty `rpc_request`): kompletna inner ramka **flat**

```text
[inner_len u32 LE][inner type_index][inner body…][inner writeRest…][inner sentinel][inner payload]
```

Mux czyta `api_id` z body `api_message`, kopiuje `payload_size` surowych bajtów **bez** deserializacji inner RPC. Endpoint `handle_data` / `handleData` czyta te bajty jak zwykły strumień ramek. Inner `stream_*` **nie** idą tą drogą — patrz `mux_socket_data` niżej.

Jedna outer ramka = jedna inner wiadomość.

```text
outer:  [outer_len][type=3][api_id uvarint][writeRest…][ inner_len | inner body… ]
inner:                                           kompletna ramka flat z własnym length-prefixem
```

## `mux_socket_*`

Struct na przewodzie (codec compressed, pole `binary` dla payload):

```text
mux_socket_open   { socket_id u32, kind u8, ref_id u32, cookie u32 }
mux_socket_data   { socket_id u32, payload binary }
mux_socket_close  { socket_id u32 }
mux_socket_window { socket_id u32, add_bytes u32 }
```

`kind`: `0=observe`, `1=fetch`, `2=pipe`. `ref_id` ustawione dla observe/fetch, `0` dla pipe. `cookie` ustawione dla pipe, `0` w przeciwnym razie.

`payload` observe/fetch to kompletna inner ramka `stream_*` (flat, z własnym length-prefixem). Inner `stream_id` jest przepisane na mux `socket_id`. Payload pipe to surowe bajty aplikacji. Okno bajtowe i DRR: [`stream-multiplexer.md`](stream-multiplexer.md).

## Zapis i odczyt trailing payload

Zapis (C++ / TS, ten sam wzorzec co argumenty RPC):

1. `begin_send(api_message{api_id})` / `beginSend('api_message', { api_id })` — length placeholder, type index, serializacja struct, `write_rest`.
2. Zapis surowych bajtów inner ramki (`write_raw` / `writeRaw` na compressed `circular_buffer_writer` / `LocalCircularBufferWriter`).
3. `commit_send` / `commitSend` — wstawia `outer_len` i flush.

Odczyt:

1. `on_message<api_message>(msg, payload_size)` / `onMessage('api_message', (msg, payloadSize) => …)`.
2. `payload_size` to liczba bajtów pozostałych po deserialize + `readRest`.
3. Skopiować `payload_size` bajtów z czytnika (`read_raw` / `readRaw`). To są bajty inner ramki, włącznie z `inner_len`.

Istniejący `handle_data` pomija nieprzeczytany payload ramki; kopia musi nastąpić w handlerze.

Odrzut `api_message` (nieznany / ukryty / `api_id = 0` / `pre-handshake` / sesja failed) buduje **inner** ramkę `rpc_error` tym samym framem co endpoint (`[inner_len][type=rpc_error][request_id][…][sentinel][exception]`) i wysyła ją jako trailing outer `api_message`. Nie outer `rpc_error` — outer `request_id` nie pasuje do inner klienta. Peek tylko `type_index` i `request_id` inner `rpc_request`; argumentów nie deserializować. Szczegóły: [`session-api.md`](session-api.md) §Odrzut `api_message`.

### Inner typy w trailing payload

Rejestr kanału endpointu (po `register_basic_types`, potem strumień). Indeksy muszą być takie same po obu stronach.

| Indeks | Typ | Kto wpuszcza przez `api_message` |
|--------|-----|----------------------------------|
| 0 | `rpc_request` | klient → endpoint (jednorazowe RPC) i gateway → endpoint (`set_reference`) |
| 1 | `rpc_response` | endpoint → klient; od klienta porzucane |
| 2 | `rpc_error` | endpoint → klient (i odrzut muxa); od klienta porzucane |
| 3 | `socket_opened` | nie forwardujemy jako zdarzenie refu |
| 4 | `stream_item` | endpoint → klient przez `mux_socket_data`, `stream_id` przepisane na `socket_id` |
| 5 | `stream_key` | zarejestrowane, żeby indeksy dalej się zgadzały; v1.5 go nie wysyła |
| 6 | `stream_done` | jak `stream_item` |
| 7 | `stream_error` | jak `stream_item` |
| 8 | `stream_reset` | jak `stream_item` |
| 9 | `stream_cancel` | klient → endpoint; `stream_id` w ramce to mux `socket_id`, gateway podstawia inner id endpointu |
| 10 | `stream_opened` | jak `stream_item` |

`stream_id` tych wiadomości to pierwsze pole, `u64` LE na offsecie 12 (po `len`, indeksie typu i `encoded_size`). Podmiana tych 8 B jest in-place. Klient → gateway wpuszcza tylko `rpc_request` (długość ≥ 20) i `stream_cancel` (długość ≥ 8). Krótka ramka i `rpc_response` od klienta zostają porzucone.

## Golden hex

Pliki w [`testdata/`](../testdata/). C++ test zrzuca, TS koduje ten sam scenariusz i wczytuje odpowiedzi C++.

- `hello_v1.hex`, `api_message_ping.hex`, `xxh3_abc.hex` — session RPC / ping
- `list_apis_req.hex` / `list_apis_res.hex` — katalog ASCII `blob-alpha` / `blob-beta`
- `get_rpc_description_req.hex` / `get_rpc_description_res.hex` — `api_id=1`
- `inner_rpc_error_unknown_api.hex` — inner `rpc_error` + `runtime_error` (`request_id=1`); ten sam frame co odrzut muxa
- `api_message_echo_req.hex` / `api_message_echo_res.hex` — outer `api_message{1}` + inner echo `7`

## `protocol_version` na compressed

`uint64` na kanale sesji to **uvarint**. V1 = `1`. Inner ramki (endpoint, `stream_*`) zostają na codec **flat** (`uint64` = 8 B LE).
