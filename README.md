# neve

**neve** is a small async Rust client that subscribes to Avalanche C-chain
`newHeads` over WebSocket, fetches each full block (and optionally its
receipts) from the
HTTPS RPC, and persists it to an
[`rkuris/blockstore`](https://github.com/rkuris/blockstore) instance with
a [`fjall`](https://github.com/fjall-rs/fjall) sidecar carrying three indexes
(hash → height, tx_hash → (height, idx), height → receipts). A jsonrpsee
server exposes a small read-only subset of the Ethereum JSON-RPC API backed
by that storage. A background backfill worker closes any gaps between the
local high-water and the upstream tip — both within-session (dropped
`newHeads` frames) and cross-restart.

This is a sketch toward the lightweight mirror client described in
[`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md) — it covers the block-tail half. State
mirroring via change proofs is not implemented here.

## Endpoints used

<https://avalabs.grafana.net/goto/sxp4p9?orgId=stacks-1371323k>

Mainnet (default):

- WebSocket: `wss://api.avax.network/ext/bc/C/ws`
- HTTPS RPC: `https://api.avax.network/ext/bc/C/rpc`

Testnet (`--network testnet`):

- WebSocket: `wss://api.avax-test.network/ext/bc/C/ws`
- HTTPS RPC: `https://api.avax-test.network/ext/bc/C/rpc`

The mainnet WS endpoint has a tight Cloudflare rate limit (3 upgrades/min,
24-hour block on trip). Testnet is far more permissive and is recommended
for dev work — use `--network testnet`.

## Storage layout

`--data-dir` (default `./blockstore-data-<network>`):

- `blocks/` — blockstore data + index files (`blockdb.idx`, `blockdb_N.dat`).
  Keyed by `u64` height; on first run, `minimum_height` is anchored at the
  first observed block.
- `index/` — fjall keyspace with four partitions:
  - `hash_to_height` — `blockHash (32 B) → height (u64 LE, 8 B)`
  - `tx_to_block` — `tx_hash (32 B) → height (u64 LE) ++ tx_index (u32 LE)` (12 B)
  - `receipts_by_height` — `height (u64 LE) → JSON array of receipts` (only
    populated when `--receipts` is passed)
  - `meta` — startup-only, holds the upstream-reported `chain_id` as a
    pollution guard; subsequent opens must match.

Block bodies are stored as the **JSON** returned by
`eth_getBlockByNumber(num, true)`. This is debuggable and trivial to serve
back; the format will need to switch to RLP-encoded `*types.Block` (matching
`graft/coreth/plugin/evm/wrapped_block.go`'s `Bytes()`) if/when this needs
to interop with a Go-side bootstrap snapshot.

## JSON-RPC methods

Listening on `--rpc-addr` (default `127.0.0.1:8545`). For block/hash/tx
identifiers we don't have in the local store, the response is a `result:
null` body rewritten to **HTTP 421** by a tower middleware, per the
api-worker contract in [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md).

- `eth_blockNumber` → highest stored height (hex).
- `eth_getBlockByNumber(tag, fullTx)` — supports `"latest"`, `"finalized"`,
  `"safe"`, and `0x`-prefixed hex heights. `"earliest"` / `"pending"` are
  rejected. `fullTx=false` collapses the transactions array to hashes.
- `eth_getBlockByHash(hash, fullTx)` — fjall lookup → blockstore read.
- `eth_getBlockTransactionCountByNumber(tag)` / `ByHash(hash)`.
- `eth_getTransactionByBlockNumberAndIndex(tag, idx)` /
  `ByBlockHashAndIndex(hash, idx)`.
- `eth_getTransactionByHash(hash)` — one fjall index hop, then the same
  projection used by the by-index methods.
- `eth_getTransactionReceipt(hash)` — **only when `--receipts` is set**.
  Without that flag the receipts index stays empty and the method returns
  421. The flag is off by default because fetching receipts doubles
  upstream bandwidth.

See `STATUS.md` for the full method status table.

## Health endpoint

`GET /health` on the same listen address returns a JSON snapshot of process
state — useful for liveness probes and ad-hoc inspection:

```sh
curl -s http://127.0.0.1:8545/health
```

Fields: `status`, `chain_id`, `uptime_secs` / `uptime` (humantime-formatted),
`blocks.{min_height,max_contiguous_height,high_water,behind}`,
`storage.{data_dir,blockdb_bytes,index_bytes,total_bytes}`, and
`memory.{physical_bytes,virtual_bytes}`. Every byte-valued field also has a
`*_human` sibling (e.g. `physical_human: "29.4 MiB"`) so logs and humans can
read the same payload as machines.

## Build

The `blockstore` crate is fetched from a public GitHub repo
([`rkuris/blockstore`](https://github.com/rkuris/blockstore), pinned by `rev`
in `Cargo.toml`) over HTTPS, so no SSH key or extra config is needed.

```sh
cargo build --release
```

## Run

```sh
# Dev quick start — permissive testnet endpoints.
cargo run --release -- --network testnet

# Mainnet ingest including receipts (eth_getTransactionReceipt support).
cargo run --release -- --receipts

# Bounded test run with verbose logging.
cargo run --release -- --network testnet --stop-time 30s --log-level debug
```

### Common flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `--network <mainnet\|testnet>` | `mainnet` | Picks the default WS/RPC URL pair and the default `--data-dir`. |
| `--ws-url <URL>` / `--rpc-url <URL>` | per `--network` | Override either endpoint explicitly. |
| `--data-dir <PATH>` | `./blockstore-data-<network>` | Storage root. The upstream-reported `chain_id` is stamped on first open and verified on every subsequent open. |
| `--rpc-addr <ADDR>` | `127.0.0.1:8545` | JSON-RPC listen address. |
| `--receipts` | off | Fetch + store per-block receipts. Doubles upstream bandwidth. |
| `--stop-time <DUR>` | none | Exit cleanly after this duration (e.g. `30s`, `5m`, `1h`, or bare seconds). |
| `--max-wait <DUR>` | `10m` | If upstream sends a `Retry-After` longer than this, log an ERROR and shut down rather than sleep. |
| `--ws-idle-timeout <DUR>` | `2m` | Drop and reconnect the WebSocket if no `newHeads` arrive within this window (guards against a silently-dead socket). |
| `--summary-period <DUR>` | `5m` | Cadence for the periodic `summary` INFO line. |
| `--log-level <trace\|debug\|info\|warn\|error>` | `info` | Logging verbosity. Overridden by `RUST_LOG` if set. |

A periodic summary (`summary` INFO line) fires shortly after startup and
then every `--summary-period` (default 5 minutes), reporting
`high_water`, `max_contiguous`, `behind`, blocks added in the period, and
rate. Steady-state per-block events live at DEBUG.

`SIGINT` / `SIGTERM` / `SIGQUIT` trigger graceful shutdown: it fsyncs the
fjall journal (so a power loss right after exit can't lose the un-synced
tail), then the runtime drops the storage handle so blockstore checkpoints
cleanly. The `Recovering keyspace` lines on the next start are fjall's normal
open path, not a sign of an unclean close.

### Example queries (in another terminal)

```sh
# Current head
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  http://127.0.0.1:8545

# Block by height, tx-hashes only
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest", false]}' \
  http://127.0.0.1:8545

# Transaction by hash
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionByHash","params":["0x<tx-hash>"]}' \
  http://127.0.0.1:8545
```

## Inspecting the store

Install the upstream CLI:

```sh
cargo install --git https://github.com/rkuris/blockstore.git \
  --branch main blockstore-cli
```

Then:

```sh
# Substitute the data dir for the network you ran against:
blockstore-cli -d ./blockstore-data-testnet/blocks get --height <N>     # hex-dump a block
blockstore-cli -d ./blockstore-data-testnet/blocks copy --target <dir>  # clone the store
```

## Layout

- `src/main.rs` — CLI parsing, bootstrap, WebSocket ingester, HTTPS block /
  receipt fetcher, reconnect loop, backfill worker, periodic summary,
  signal-driven shutdown.
- `src/storage.rs` — `Storage` handle wrapping blockstore + fjall, with
  the three index partitions and a `min_height / max_contiguous_height /
  high_water` accessor surface.
- `src/rpc.rs` — jsonrpsee server. `BlockSelector` enum +
  `lookup_block(sel, projection)` helper collapses each method body to
  one line.
- `src/middleware.rs` — tower layer that rewrites `200 OK` to `421
  Misdirected Request` when the JSON-RPC envelope reports `result: null`.
- `src/health.rs` — tower layer that short-circuits `GET /health` with a
  JSON status report (uptime, block range, on-disk sizes, RSS).

## Known limitations

- **Best-effort fork handling.** If `eth_getBlockByNumber`'s body hash
  doesn't match the `newHeads` hash, the block is skipped. C-chain finality
  means this is rare.
- **Numeric block tags below ingest start return 421.** The backfill worker
  fills *forward* from the first observed `newHead`; history older than
  that is not retrieved.
- **JSON storage**, not RLP — see "Storage layout".
- **Receipts behind a flag.** `eth_getTransactionReceipt` only works with
  `--receipts`, off by default to limit upstream bandwidth.

See `STATUS.md` for the more detailed status table and the open
quality-of-life list.
