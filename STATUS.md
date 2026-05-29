# Project status — picking this back up

## Where we are

A working C-chain block streamer + JSON-RPC server. Subscribes to `newHeads`
over WebSocket, fetches full block bodies (and optionally receipts) via
HTTPS, persists to blockstore with a fjall sidecar carrying four partitions
(`hash_to_height`, `tx_to_block`, `receipts_by_height`, `meta`), and serves
a small read-only subset of the Ethereum JSON-RPC API.

This is the "block-tail half" of the lightweight mirror described in
[`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md). The state-mirror half (Firewood
change proofs) is not started.

## What runs

```sh
cargo run --release -- --network testnet           # friendly dev path
cargo run --release                                # mainnet (rate-limited)
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  http://127.0.0.1:8545
```

- WS reconnect with exponential backoff on disconnect. Cloudflare 429 /
  503 on either WS or HTTPS is handled via `Retry-After`; if upstream asks
  us to wait longer than `--max-wait` (default 10m) we exit with an ERROR
  rather than sleep silently.
- 8 RPC methods: `eth_chainId`, `eth_blockNumber`,
  `eth_getBlockBy{Number,Hash}`,
  `eth_getBlockTransactionCountBy{Number,Hash}`,
  `eth_getTransactionByBlock{Number,Hash}AndIndex`,
  `eth_getTransactionByHash`, and `eth_getTransactionReceipt` (opt-in
  via `--receipts`).
- HTTP **421** (Misdirected Request) when a hash / height / tx-hash isn't
  in our store, per the api-worker contract in [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md).
- **Backfill worker** running alongside the ingester. Closes both
  within-session gaps (newHeads dropped a frame:
  `max_contiguous_height < height_highwater`) and cold-restart gaps
  (process was down: local high-water < upstream `eth_blockNumber`). Target
  each iteration is `max(local_high_water, upstream_tip)`; the worker walks
  `max_contiguous_height + 1` upward until it catches up. Logs "backfill
  starting / progress / caught up" with `contiguous`, `target`, `behind`,
  rate (blocks/sec), and a humanized ETA (e.g. `3h12m`) derived from the
  start-of-stretch reference point.
- **Periodic summary** at startup and every `--summary-period` (default 5m)
  reporting `high_water`, `max_contiguous`, `behind`, blocks added, rate.
  Per-block events are at DEBUG to keep INFO uncluttered.
- **Graceful shutdown** on SIGINT / SIGTERM / SIGQUIT — fsyncs the fjall
  journal, then the runtime drops the storage handle so blockstore
  checkpoints cleanly. A fatal Notify channel exits the same way when
  upstream throttle exceeds `--max-wait`.
- **WebSocket idle watchdog.** If no `newHeads` arrive within
  `--ws-idle-timeout` (default 2m), the session is dropped and the ingester
  reconnects with its existing backoff — guards against a half-open or
  stalled socket that never errors.
- **`GET /health` endpoint** on the JSON-RPC listen address. Returns a JSON
  snapshot with `chain_id`, uptime, block range
  (`min_height` / `max_contiguous_height` / `high_water` / `behind`),
  on-disk sizes (`blockdb_bytes` + `index_bytes`), and process memory
  (RSS + virtual via the `memory-stats` crate). Every byte field has a
  `*_human` sibling formatted by `human_bytes`; uptime is formatted by
  `humantime`. Implemented as a tower layer that short-circuits
  `GET /health` before the JSON-RPC dispatcher; everything else passes
  through unchanged.
- **Cross-network pollution guard.** At startup we query `eth_chainId`
  against the configured RPC URL and stamp it into a fjall `meta`
  partition; subsequent opens require the stamp to match. Default
  `--data-dir` is `./blockstore-data-<network>` so the two networks land
  in separate dirs by default.

## CLI surface

| Flag | Default | Purpose |
| --- | --- | --- |
| `--network <mainnet\|testnet>` | `mainnet` | Picks default WS/RPC URLs and `--data-dir`. |
| `--ws-url` / `--rpc-url` | per `--network` | Override either endpoint. |
| `--data-dir` | `./blockstore-data-<network>` | Storage root; chain_id-stamped on first open. |
| `--rpc-addr` | `127.0.0.1:8545` | JSON-RPC listen address (`0.0.0.0:8545` to serve externally). |
| `--max-connections` | `1024` | Max concurrent JSON-RPC connections; excess get HTTP 429. |
| `--receipts` | off | Fetch + store per-block receipts (doubles upstream bandwidth). |
| `--stop-time` | none | Exit after a duration; useful for bounded test runs. |
| `--max-wait` | `10m` | Cap on upstream `Retry-After` before we bail. |
| `--ws-idle-timeout` | `2m` | Reconnect the WebSocket if no `newHeads` arrive within this window. |
| `--summary-period` | `5m` | Cadence of the periodic summary line. |
| `--log-level` | `info` | One of `trace` / `debug` / `info` / `warn` / `error`. |
| `--version`, `-V` | — | Print version from Cargo.toml. |

## Layout

- `src/main.rs` — CLI parsing (clap), bootstrap, WS ingester
  (`connect_and_subscribe`, `next_ws_event`, `classify_frame`), HTTPS
  fetcher (`fetch_rpc` covering both `eth_getBlockByNumber` and
  `eth_getBlockReceipts`, with retry/throttle), startup `fetch_chain_id`,
  backfill worker + ETA, periodic summary, signal-driven shutdown, fatal
  Notify channel. `IngestCfg` bundles the cross-cutting runtime knobs.
- `src/storage.rs` — `Storage` handle wrapping blockstore + a fjall
  keyspace with four partitions. `Storage::put` writes block bytes to
  blockstore, then a single atomic fjall `Batch` covering all index
  writes. Chain-ID stamp lives in a `meta` partition, scoped to
  `Storage::open` (not held in `Inner`).
- `src/rpc.rs` — jsonrpsee server. A `BlockSelector` enum
  (`Number` / `Hash` / `Height`) plus a `lookup_block(sel, projection)`
  helper collapses each method body to one or two lines. Pure projection
  helpers (`tx_count_hex`, `nth_transaction`, `shape_block`) are
  unit-tested directly.
- `src/middleware.rs` — tower layer that rewrites `200 OK` to `421
  Misdirected Request` when the JSON-RPC envelope reports `result: null`.
- `src/health.rs` — tower layer that short-circuits `GET /health` with a
  JSON status report (uptime, block range, on-disk sizes, RSS). Layered
  before the `NotFound421` middleware so health requests bypass the
  result-null rewrite.

## Block-body format

We currently store the **JSON** returned by `eth_getBlockByNumber(num, true)`.
For Go-side bootstrap interop, the target format is RLP-encoded
`*types.Block` (matching `graft/coreth/plugin/evm/wrapped_block.go:546`'s
`(*wrappedBlock).Bytes()`). When that interop matters, the change is local:
swap `serde_json::to_vec(&block)` for `alloy_rlp::encode(...)` plus a
reciprocal decode in the RPC layer. Storage layer (blockstore + fjall)
stays unchanged because it's keyed by opaque bytes.

## Known limitations

- **Best-effort fork handling.** If the body's hash doesn't match the
  head's, we skip and warn. C-chain finality means this is rare.
- **Numeric block tags below ingest start return 421.** We don't backfill
  *history* below the first newHead we observe; the store's anchor
  (`minimum_height`) is set on cold start to that first observed height,
  and the backfill worker only fills forward from there.
- **One-block index gap possible on crash.** `Storage::put` writes the
  block to blockstore first, then commits an atomic fjall batch for the
  three indexes. A crash between the two stages leaves the block readable
  by height but not by hash / tx / receipt, and the backfill worker
  doesn't refill (since `max_contiguous_height` already advanced). The
  doc comment on `Storage::put` spells this out.

## JSON-RPC method status

| Method | Tier |
| --- | --- |
| `eth_blockNumber` | Implemented |
| `eth_call` | 4 |
| `eth_chainId` | Implemented |
| `eth_estimateGas` | 4 |
| `eth_getBalance` | 4 |
| `eth_getBlockByHash` | Implemented |
| `eth_getBlockByNumber` | Implemented |
| `eth_getBlockTransactionCountByHash` | Implemented |
| `eth_getBlockTransactionCountByNumber` | Implemented |
| `eth_getCode` | 4 |
| `eth_getLogs` | 3 (explicitly excluded by [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md)) |
| `eth_getProof` | 4 |
| `eth_getStorageAt` | 4 |
| `eth_getTransactionByBlockHashAndIndex` | Implemented |
| `eth_getTransactionByBlockNumberAndIndex` | Implemented |
| `eth_getTransactionByHash` | Implemented |
| `eth_getTransactionCount` (nonce) | 4 |
| `eth_getTransactionReceipt` | Implemented (opt-in: `--receipts`) |
| `eth_getUncleByBlockHashAndIndex` | 0 |
| `eth_getUncleByBlockNumberAndIndex` | 0 |
| `eth_getUncleCountByBlockHash` | 0 |
| `eth_getUncleCountByBlockNumber` | 0 |
| `eth_protocolVersion` | 0 |
| `eth_syncing` | 0 |
| `net_version` | 0 |
| `web3_clientVersion` | 0 |

**Tier definitions:**

- **Tier 0 — out of scope.** Handled by the api-worker Cloudflare
  frontend with hardcoded responses before they reach us.
- **Tier 1 — zero extra work, just dispatch into stored block JSON.**
  Implemented.
- **Tier 2 — `eth_getTransactionByHash` lookups.** Implemented. Ingest
  populates a `tx_to_block` fjall partition keyed by `tx_hash → (height,
  tx_index)`; the RPC method does a one-hop index lookup then projects
  the tx out of the stored block JSON via the existing `lookup_block`
  helper.
- **Tier 3 — needs an extra HTTPS fetch per block.**
  `eth_getTransactionReceipt` is implemented behind the `--receipts`
  CLI flag (off by default). When enabled, ingest does an extra
  `eth_getBlockReceipts(num)` call per block and writes the array to a
  `receipts_by_height` fjall partition; the RPC chains
  `tx_to_block → receipts_by_height[idx]`. Doubles upstream bandwidth,
  which is meaningful against the rate-limited public endpoint — hence
  the opt-in. `eth_getLogs` additionally needs a topic/address index;
  explicitly excluded by the [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md) design doc.
- **Tier 4 — needs state mirror (Firewood change proofs).** The
  change-proof half of [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md); out of scope for
  the block-tail half.

**Quality-of-life:**

- Consider RLP body format if/when bootstrap interop with a Go syncer
  becomes a concrete requirement.

## Test plan

How we want to compare this implementation against a real avalanchego /
coreth C-chain RPC server. None of this is built yet — captured here as
the next-pass plan.

- **Comparator:** local avalanchego node on **Fuji (testnet)**, not
  mainnet. Mainnet bootstrap is multi-day and several hundred GB; Fuji
  gives a working node in hours and the latency comparison generalizes.
  Mirror runs against the same network so both serve the same block
  range.
- **Apples-to-apples scope:** only the 7 read-only methods we
  implement (`eth_blockNumber`, `eth_getBlockBy{Number,Hash}`,
  `eth_getBlockTransactionCountBy{Number,Hash}`,
  `eth_getTransactionByBlock{Number,Hash}AndIndex`,
  `eth_getTransactionByHash`, `eth_getTransactionReceipt`). Anything
  state-touching (`eth_call`, `eth_getBalance`, …) is out of scope —
  that's what the change-proof half of
  [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md)
  exists to solve.
- **Workload:** synthetic load via `vegeta` or `wrk2`, driven from a
  request file of recorded mainnet calls translated onto Fuji heights
  both servers have. Distribution should roughly match the
  `X-Execution-Weight` mix we'd expect in production (mostly tip
  reads, some by-hash, fewer by-tx, occasional receipts).
- **Metrics:**
  - Latency p50 / p95 / p99 at a fixed concurrency (e.g. 50, 200, 500
    in-flight).
  - Throughput at saturation.
  - Steady-state RSS and CPU.
  - On-disk footprint (blockstore + fjall vs coreth state dir, for the
    same block range).
- **Controls:** both servers on the same host (eliminate network
  noise), warmed up before measurement, identical hardware. Record
  Fuji block range and a tip-block hash with each run so results are
  reproducible.
- **Honest caveats:**
  - Synthetic-on-Fuji is fast to iterate but doesn't capture the real
    mainnet request mix. A second pass with replayed (anonymized)
    mainnet traffic would be the credible follow-up.
  - The mirror is a *partial* server — "faster than avalanchego" for a
    subset of methods doesn't argue the broader architecture by itself.
    The architectural argument needs Tier 4 (state via change proofs)
    too.

## Branch state

- `main` carries everything above. jj-managed, colocated with git.
- Upstream `ava-labs/blockstore` is pinned to a commit on `main` that
  includes both the `height_highwater` accessor (PR #17, merged) and the
  later `recover()` fix that preserves real `max_contiguous_height`
  across restarts when gaps exist.
- All prototype quality-of-life items from earlier passes (CLI
  ergonomics, periodic summary, ETA, `--network` enum, chain-id stamp,
  `--max-wait` plumbed everywhere, `--log-level` enum, `--version`, unit
  tests for the RPC projection helpers and the ETA math) have landed.
  The only open item is RLP body format, gated on a real Go-side
  bootstrap interop requirement.
