# neve

<img src="assets/neve-logo.svg" alt="neve logo" width="128" align="right">

[![CI](https://github.com/rkuris/neve/actions/workflows/ci.yml/badge.svg)](https://github.com/rkuris/neve/actions/workflows/ci.yml)

**neve** is a small async Rust client that subscribes to Avalanche C-chain
`newHeads` over WebSocket, fetches each full block from the
HTTPS RPC, and persists it to an
[`rkuris/blockstore`](https://github.com/rkuris/blockstore) instance with
a [`fjall`](https://github.com/fjall-rs/fjall) sidecar carrying two indexes
(hash → height, tx_hash → (height, idx)). A jsonrpsee
server exposes a small read-only subset of the Ethereum JSON-RPC API backed
by that storage. A background backfill worker closes any gaps between the
local high-water and the upstream tip — both within-session (dropped
`newHeads` frames) and cross-restart.

This is a sketch toward the lightweight mirror client described in
[`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md) — it covers the block-tail half. State
mirroring via change proofs is not implemented here.

## Why neve exists

How cheaply — in latency, memory, and operational surface — can the read-heavy
slice of the C-chain JSON-RPC API be served from a purpose-built local cache
instead of a full node? Measured head-to-head against avalanchego on identical
hardware — full sweeps, costs, and methodology in
[`benchmark/`](benchmark/README.md):

- **Lower latency** — ~6× lower per-request latency than avalanchego
  (~0.21 ms vs ~1.24 ms p50), and a far larger win client-visible once deployed
  near callers.
- **Higher throughput, better under load** — ~28 % more peak requests/sec on the
  same box, and throughput holds flat past the knee where avalanchego degrades.
- **~25–40× smaller memory** — ~320 MiB RSS vs ~9–13 GiB; the RAM neve doesn't
  use stays free for page cache, so reads stay in memory even on networked disks.
- **Runs on small, cheap instances** — fits a 2 GiB box where a full node needs
  16 GiB, at a fraction of the monthly cost, and a single t4g.small still serves
  the whole projected volume (**~8 B requests/month**, ~3,100 req/s average).
- **Chains and bootstraps fast** — downstream neves mirror each other, and a
  fresh replica fills its whole retained tail — ~178k blocks / ~1.6 GB — from
  a peer in minutes.

The deliberate trade is scope. neve is **read-only**, serves a **subset** of the
API ([JSON-RPC methods](#json-rpc-methods)), and today only over its **retained
block tail** — anything outside that window returns HTTP 421 and the caller falls
back to a full node. It's a cache in front of the real thing, not a replacement
for it.

**Where it's heading.** Block serving is phase one. Next is a
[firewood](https://github.com/ava-labs/firewood)-backed state layer synced via
change proofs ([`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md)),
extending the same sync-and-serve model to **non-executing state reads** —
balances, code, storage, nonces — and most of the read-only API surface, still
without executing a transaction or joining consensus. It's a substantial
undertaking that will grow neve's footprint and narrow the cost gap above; the
advantages expected to persist are latency, memory, and operational simplicity
(details in [`benchmark/`](benchmark/README.md)).

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
- `index/` — fjall keyspace with three partitions:
  - `hash_to_height` — `blockHash (32 B) → height (u64 LE, 8 B)`
  - `tx_to_block` — `tx_hash (32 B) → height (u64 LE) ++ tx_index (u32 LE)` (12 B)
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

- `eth_chainId` → the upstream-reported chain id (hex). Static — always
  answers (e.g. `0xa86a` for mainnet), so wallets/tooling that probe it on
  connect work.
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
- `eth_getLogs(filter)` — logs over a block range from stored records, filtered
  by `address` / `topics` (and `blockHash` for a single block). Requires
  `--ingest-logs`; answers only when the whole requested range is present
  (otherwise returns not-found so the client falls back upstream), and caps the
  range at 2048 blocks like the upstream. Served via a full range scan today; an
  address index is a planned optimization.
- `eth_subscribe(kind)` / `eth_unsubscribe` — **WebSocket only.** Two kinds:
  - `"newHeads"` — pushes each freshly-ingested block header (transactions
    stripped, matching geth's `newHeads`).
  - `"newBlocks"` — a **neve extension** that pushes the _whole_ block
    (transactions included) as it lands, so a downstream mirror persists it
    directly with no follow-up `eth_getBlockByNumber`. One WS frame per block
    instead of header-then-fetch. This is what `--mirror-from` uses.
  - `"oldBlocks"(from, to?)` — a **neve extension** that replays a stored height
    range for mirror bootstrap. See [Extensions](#extensions-beyond-the-standard-api).

  `logs` / `newPendingTransactions` / `syncing` are rejected, since they
  aren't backed by the block store. See [Mirroring / chaining](#mirroring--chaining).

For a one-shot streaming download of a finite range over plain HTTP, see
`GET /blocks` under [Extensions](#extensions-beyond-the-standard-api).

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

## Metrics endpoint

`GET /metrics` on the same listen address serves Prometheus metrics in the text
exposition format (works with any Prometheus / Grafana Mimir scraper — no
native-histogram feature required):

```sh
curl -s http://127.0.0.1:8545/metrics
```

Every series carries an inline `# HELP` line describing it and its labels, so
the scrape output is self-documenting. The authoritative list of series, types,
labels, and histogram buckets lives in [`src/metrics.rs`](src/metrics.rs).

## Extensions beyond the standard API

neve is a read-only mirror, so most of its surface follows avalanchego's
behavior. The items below are **neve-specific** — flag them when pointing
non-neve clients at it.

### `eth_subscribe("newBlocks")` — whole-block push (WebSocket)

Like `newHeads`, but each frame carries the **entire** block (transactions
included) rather than just the header, so a consumer persists it with no
follow-up `eth_getBlockByNumber`. This is what `--mirror-from` rides. `newHeads`
remains available and geth-compatible.

### `eth_subscribe("oldBlocks", from, to?)` — historical replay (WebSocket)

Streams a stored height range as whole blocks, oldest first, for bootstrapping a
downstream mirror:

- `from` (hex, required) — inclusive start.
- `to` (hex, optional) — inclusive end. With `to` omitted the stream follows the
  contiguous tip as it advances and **completes once caught up** — the mirror's
  "bootstrap done" signal.
- A range neve can't serve gaplessly (`from` below the earliest stored block, or
  `to` past the contiguous tip) is rejected at subscribe time.

Note: an `oldBlocks` subscription completing ends that _subscription_ but, per
jsonrpsee, leaves the **WebSocket open** (it can carry more subscriptions). For a
one-shot bulk download where you want the connection to end on its own, use
`GET /blocks`.

### `GET /blocks?from=[&to=]` — NDJSON bulk export (HTTP)

A one-shot streaming download of a height range — one block per line
(newline-delimited JSON), read on demand from storage so an arbitrarily large
range streams without buffering. The response sets `Connection: close`, so the
client gets EOF and exits when the range is done:

```sh
curl -sS 'http://127.0.0.1:8545/blocks?from=86686273&to=87113713' > blocks.ndjson
# from/to accept decimal or 0x-prefixed hex
```

- `from` is required; `to` is **optional** and defaults to a full
  `--max-blocks-per-request` window from `from`, clamped to the contiguous tip.
  So `?from=X` (no `to`) streams the next chunk, and you page forward by
  advancing `from` to the last height you received plus one.
- Capped at `--max-blocks-per-request` blocks (default `10000`); a larger
  _explicit_ range gets **HTTP 400**. Window a bigger pull into successive
  ranges, or raise the cap.
- A `from`/`to` outside the stored, gapless window gets **HTTP 416**.
- This is the recommended way to pull a finite range; `oldBlocks` is for the
  mirror-bootstrap-then-follow-the-tip case.

### Behavioral deviations

- **HTTP 421 (Misdirected Request)** in place of a `result: null` / `-32601`
  body: when neve can't authoritatively answer — a block/hash/tx not in its local
  tail, or a method it doesn't implement — it returns 421 so a front-end pool
  retries against a full node. See the api-worker contract in
  [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md).
- **Idle-connection reaping**: a connection with no read _or_ write activity for
  `--idle-timeout` (default `60s`, `0` disables) is closed — a slowloris /
  leaked-keepalive defense the underlying RPC framework can't do itself. Active
  WebSocket subscriptions are unaffected while blocks keep flowing (each pushed
  block counts as activity); only a fully silent connection is dropped.

## Mirroring / chaining

Because neve both serves the `newHeads` WebSocket and answers
`eth_getBlockByNumber`, one neve can ingest from another instead of from the
public Avalanche endpoint. This is the way to fan out read capacity: a single
neve ingests from Avalanche (subject to Cloudflare's tight WS limit — 3
upgrades/min), and any number of downstream neves subscribe to _it_,
multiplying serving capacity without ever touching the rate-limited upstream
again.

```sh
# Downstream mirror of an upstream neve at 10.0.0.5:8545.
neve --mirror-from http://10.0.0.5:8545 --data-dir ./mirror --rpc-addr 0.0.0.0:8545
```

`--mirror-from <URL>` does the whole job from one endpoint, since neve serves
RPC, the WebSocket, and `/health` on the same socket:

- **Endpoint derivation.** The WS and RPC URLs are derived from the one URL
  (`http`→`ws`, `https`→`wss`), overriding `--network` / `--ws-url` /
  `--rpc-url`.
- **Full-range backfill.** On an empty local store, neve probes the upstream's
  `/health` for `blocks.min_height` and anchors its store floor there, so the
  backfill worker reproduces the upstream's whole retained range rather than
  only growing forward from the current tip. (Without mirroring, a fresh store
  anchors at the first observed `newHead` and never fills history older than
  that.)
- **Unthrottled backfill.** The 40 ms inter-fetch delay (which exists only to
  be polite to Cloudflare) is dropped — the upstream is another neve with no
  such limit.
- **`newBlocks` live tail.** The mirror subscribes to the upstream's
  `newBlocks` (not `newHeads`), so each live block arrives whole on the
  WebSocket and is persisted with no `eth_getBlockByNumber` round-trip. A
  mirror re-publishes what it ingests, so its own `newHeads` / `newBlocks`
  subscribers work and mirror chains propagate.

Caveats: the upstream only retains a tail, so a chained mirror can go back no
further than the upstream still holds (out-of-range heights return 421, which
the backfill path treats as a soft miss). Latency stacks one hop's
newHead→persist lag per link, so this favors a shallow fan-out tree over a
deep chain.

## Build

The block store dependency is published on crates.io as
[`blockdb`](https://crates.io/crates/blockdb) and pulled in like any other
crate (it's renamed to `blockstore` in `Cargo.toml`), so no SSH key or extra
config is needed.

```sh
cargo build --release
```

### Git hooks

A shared `pre-commit` hook (in `.githooks/`) runs `cargo fmt --check` so
formatting issues never reach CI. Git config isn't version-controlled, so
enable it once per clone:

```sh
git config core.hooksPath .githooks
```

## Run

```sh
# Dev quick start — permissive testnet endpoints.
cargo run --release -- --network testnet

# Bounded test run with verbose logging.
cargo run --release -- --network testnet --stop-time 30s --log-level debug
```

### Common flags

| Flag                                            | Default                       | Purpose                                                                                                                                                                                                                                                                                                                                                                        |
| ----------------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `--network <mainnet\|testnet>`                  | `mainnet`                     | Picks the default WS/RPC URL pair and the default `--data-dir`.                                                                                                                                                                                                                                                                                                                |
| `--ws-url <URL>` / `--rpc-url <URL>`            | per `--network`               | Override either endpoint explicitly.                                                                                                                                                                                                                                                                                                                                           |
| `--mirror-from <URL>`                           | none                          | Mirror another neve. Derives the WS + RPC endpoints from one URL (`http`→`ws`, `https`→`wss`), overriding `--network` / `--ws-url` / `--rpc-url`. On an empty store, probes the upstream's `/health` and anchors the floor at its earliest retained block so backfill reproduces the whole range. Backfill runs unthrottled. See [Mirroring / chaining](#mirroring--chaining). |
| `--data-dir <PATH>`                             | `./blockstore-data-<network>` | Storage root. The upstream-reported `chain_id` is stamped on first open and verified on every subsequent open.                                                                                                                                                                                                                                                                 |
| `--rpc-addr <ADDR>`                             | `127.0.0.1:8545`              | JSON-RPC listen address. Use `0.0.0.0:8545` to serve externally (then scope access with a firewall / security group).                                                                                                                                                                                                                                                          |
| `--max-connections <N>`                         | `1024`                        | Max concurrent JSON-RPC connections; excess are rejected with HTTP 429.                                                                                                                                                                                                                                                                                                        |
| `--idle-timeout <DUR>`                          | `60s`                         | Close a connection with no read or write activity for this long (slowloris / leaked-keepalive defense). `0` disables it. Active WS subscriptions stay alive while blocks flow.                                                                                                                                                                                                 |
| `--max-blocks-per-request <N>`                  | `10000`                       | Largest range a single `GET /blocks?from=&to=` bulk export may return; larger ranges get HTTP 400. See [Extensions](#extensions-beyond-the-standard-api).                                                                                                                                                                                                                      |
| `--stop-time <DUR>`                             | none                          | Exit cleanly after this duration (e.g. `30s`, `5m`, `1h`, or bare seconds).                                                                                                                                                                                                                                                                                                    |
| `--max-wait <DUR>`                              | `10m`                         | If upstream sends a `Retry-After` longer than this, log an ERROR and shut down rather than sleep.                                                                                                                                                                                                                                                                              |
| `--ws-idle-timeout <DUR>`                       | `2m`                          | Drop and reconnect the WebSocket if no `newHeads` arrive within this window (guards against a silently-dead socket).                                                                                                                                                                                                                                                           |
| `--summary-period <DUR>`                        | `5m`                          | Cadence for the periodic `summary` INFO line.                                                                                                                                                                                                                                                                                                                                  |
| `--log-level <trace\|debug\|info\|warn\|error>` | `info`                        | Logging verbosity. Overridden by `RUST_LOG` if set.                                                                                                                                                                                                                                                                                                                            |

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

Install the upstream CLI from crates.io:

```sh
cargo install blockstore-cli
```

Then:

```sh
# Substitute the data dir for the network you ran against:
blockstore-cli -d ./blockstore-data-testnet/blocks get --height <N>     # hex-dump a block
blockstore-cli -d ./blockstore-data-testnet/blocks copy --target <dir>  # clone the store
```

## Layout

- `src/main.rs` — CLI parsing, bootstrap, WebSocket ingester, HTTPS block
  fetcher, reconnect loop, backfill worker, periodic summary,
  signal-driven shutdown.
- `src/storage.rs` — `Storage` handle wrapping blockstore + fjall, with
  the two index partitions and a `min_height / max_contiguous_height /
high_water` accessor surface.
- `src/rpc.rs` — jsonrpsee server. `BlockSelector` enum +
  `lookup_block(sel, projection)` helper collapses each method body to
  one line.
- `src/middleware.rs` — tower layer that rewrites `200 OK` to `421
Misdirected Request` when the JSON-RPC envelope reports `result: null`.
- `src/health.rs` — tower layer that short-circuits `GET /health` with a
  JSON status report (uptime, block range, on-disk sizes, RSS).
- `src/metrics.rs` — Prometheus recorder, the `GET /metrics` tower layer, and
  the typed recording helpers (one per series).

## Known limitations

- **Best-effort fork handling.** If `eth_getBlockByNumber`'s body hash
  doesn't match the `newHeads` hash, the block is skipped. C-chain finality
  means this is rare.
- **Numeric block tags below ingest start return 421.** The backfill worker
  fills _forward_ from the first observed `newHead`; history older than
  that is not retrieved.
- **JSON storage**, not RLP — see "Storage layout".
- **No receipts / logs yet.** `eth_getTransactionReceipt` and log queries are
  not served; the public Avalanche endpoint doesn't support
  `eth_getBlockReceipts` anyway. A logs-first activity index is the planned
  next step — see `docs/core-wallet-research.md`.

See `STATUS.md` for the more detailed status table and the open
quality-of-life list.
