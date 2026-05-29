# neve

<img src="assets/neve-logo.svg" alt="neve logo" width="128" align="right">

[![CI](https://github.com/rkuris/neve/actions/workflows/ci.yml/badge.svg)](https://github.com/rkuris/neve/actions/workflows/ci.yml)

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

## Why neve exists

neve is an experiment with one question: how cheaply — in latency, memory, and
operational surface — can the read-heavy slice of the C-chain JSON-RPC API be
served from a purpose-built local cache instead of from a full node behind a
public endpoint? The early numbers make the case worth taking seriously.

- **One to two orders of magnitude lower read latency.** A cache hit is answered
  from a local blockstore read plus a fjall index lookup in well under a
  millisecond — benchmarked p50 ≈ 0.8 ms, p99 low-single-digit ms, saturating at
  ~4k req/s on a *single* t4g.small core. A round trip to the public,
  Cloudflare-fronted endpoint costs tens-to-hundreds of milliseconds — network
  round-trips, proxy hops, and backend work — all of which a local cache hit
  collapses into one in-process lookup. For the reads neve answers, that's a
  1–2 order-of-magnitude client-visible win.
- **One small instance covers the whole request volume.** ~8 billion
  requests/month works out to ~3,100 req/s on average; a single t4g.small core
  already sustains ~4,100. Capacity isn't the reason to scale out — running two
  or three instances, regionally placed, is about redundancy, lower client
  latency, and peak headroom, not about one instance keeping up.
- **And it costs a fraction of a node.** All-in at us-east-1 list price, neve
  serves that volume for an estimated **$339.94/month** (on-demand t4g.small +
  ~4 TB gp3 EBS). The recommended full-node hardware for the same job — a
  c6i.2xlarge — runs an estimated **$575.88/month** provisioned with the same
  4 TB, its 8 vCPU largely spent on the consensus and execution neve doesn't run.
  Serving the same data, that's a cut from **~$575.88 to ~$339.94/month** — and
  with storage held equal, every dollar of the difference is compute you stop
  paying for.
- **A footprint that matches the job, not the whole chain.** Today's block-tail
  cache runs at ~30 MiB RSS over the retained blocks plus three compact indexes —
  several fit on one small box. The point isn't the exact size (the planned state
  layer below will add to it) but the model: neve *syncs and serves*, it never
  runs consensus or executes transactions, so it carries none of the machinery a
  full node exists for.
- **Predictable tail and no shared rate limiter.** Serving from cache removes
  the upstream's 429/503 throttling and the per-request network tail outright,
  so p99 stays flat under load instead of competing with everything else
  pointed at the public endpoint.
- **Horizontal read fan-out.** One neve ingests from Avalanche; any number of
  downstream neves mirror *it* (see [Mirroring / chaining](#mirroring--chaining)),
  multiplying serving capacity without adding pressure on the rate-limited
  upstream.

The deliberate trade is scope. neve is **read-only**, serves a **subset** of the
API ([JSON-RPC methods](#json-rpc-methods)), and today only over its **retained
block tail** — anything outside that window returns HTTP 421 and the caller falls
back to a full node. It's a cache in front of the real thing, not a replacement
for it.

**Where it's heading.** Block serving is phase one. Next is a
[firewood](https://github.com/ava-labs/firewood)-backed state layer synced via
change proofs ([`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md)),
which extends the same sync-and-serve model to the **non-executing state reads** —
balances, code, storage, nonces — and with them most of the read-only API
surface, still without ever executing a transaction or joining consensus.

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
- `eth_getTransactionReceipt(hash)` — **only when `--receipts` is set**.
  Without that flag the receipts index stays empty and the method returns
  421. The flag is off by default because fetching receipts doubles
  upstream bandwidth.
- `eth_subscribe(kind)` / `eth_unsubscribe` — **WebSocket only.** Two kinds:
  - `"newHeads"` — pushes each freshly-ingested block header (transactions
    stripped, matching geth's `newHeads`).
  - `"newBlocks"` — a **neve extension** that pushes the *whole* block
    (transactions included) as it lands, so a downstream mirror persists it
    directly with no follow-up `eth_getBlockByNumber`. One WS frame per block
    instead of header-then-fetch. This is what `--mirror-from` uses.

  `logs` / `newPendingTransactions` / `syncing` are rejected, since they
  aren't backed by the block store. See [Mirroring / chaining](#mirroring--chaining).

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

## Mirroring / chaining

Because neve both serves the `newHeads` WebSocket and answers
`eth_getBlockByNumber`, one neve can ingest from another instead of from the
public Avalanche endpoint. This is the way to fan out read capacity: a single
neve ingests from Avalanche (subject to Cloudflare's tight WS limit — 3
upgrades/min), and any number of downstream neves subscribe to *it*,
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
  WebSocket and is persisted with no `eth_getBlockByNumber` round-trip. (With
  `--receipts`, receipts are still fetched separately — they aren't carried by
  the block payload.) A mirror re-publishes what it ingests, so its own
  `newHeads` / `newBlocks` subscribers work and mirror chains propagate.

Caveats: the upstream only retains a tail, so a chained mirror can go back no
further than the upstream still holds (out-of-range heights return 421, which
the backfill path treats as a soft miss). Latency stacks one hop's
newHead→persist lag per link, so this favors a shallow fan-out tree over a
deep chain.

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
| `--mirror-from <URL>` | none | Mirror another neve. Derives the WS + RPC endpoints from one URL (`http`→`ws`, `https`→`wss`), overriding `--network` / `--ws-url` / `--rpc-url`. On an empty store, probes the upstream's `/health` and anchors the floor at its earliest retained block so backfill reproduces the whole range. Backfill runs unthrottled. See [Mirroring / chaining](#mirroring--chaining). |
| `--data-dir <PATH>` | `./blockstore-data-<network>` | Storage root. The upstream-reported `chain_id` is stamped on first open and verified on every subsequent open. |
| `--rpc-addr <ADDR>` | `127.0.0.1:8545` | JSON-RPC listen address. Use `0.0.0.0:8545` to serve externally (then scope access with a firewall / security group). |
| `--max-connections <N>` | `1024` | Max concurrent JSON-RPC connections; excess are rejected with HTTP 429. |
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
- `src/metrics.rs` — Prometheus recorder, the `GET /metrics` tower layer, and
  the typed recording helpers (one per series).

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
