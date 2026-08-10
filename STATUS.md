# Project status — picking this back up

## Where we are

An Avalanche block streamer + JSON-RPC server that persists blocks to
blockstore with a fjall index sidecar and serves a read-only subset of the
node's API from that storage. This is the "block-tail half" of the lightweight
mirror in [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md);
the state-mirror half (Firewood change proofs) is not started.

It mirrors the **C-chain**, the **P-chain**, or both in one process, selected
with `--chains c|p|c,p`. The C-chain path ingests `newHeads` over WebSocket and
fetches full bodies via HTTPS, serving `eth_*`. The P-chain path polls
`platform.getHeight` and walks the frontier — there is no push mechanism to
subscribe to — serving `platform.*`. Both share one listening socket, and a
request selects its chain by method namespace. P-chain support covers Phase 0
(Tier-0 block/tx serving) and Phase 1's streaming half (subscriptions +
neve→neve mirroring); reward ingestion is the remaining Phase 1 piece. See
[`docs/p-chain-indexing-plan.md`](docs/p-chain-indexing-plan.md) for the
roadmap.

It is **deployable today** — not a prototype that needs babysitting. What
makes it operable:

- **Observability built in.** Prometheus `/metrics` (ingest freshness,
  upstream/WS health, subscriptions, per-request served-RPC latency) and a
  `GET /health` JSON snapshot (block range, on-disk sizes, process memory) —
  enough to alert on a stalled tip or a throttled upstream without bolting on
  a sidecar.
- **Minimal-downtime updates.** `deploy/update.sh` rebuilds the new binary
  while the old one keeps serving; only the binary swap + restart is downtime
  (seconds), and `/etc/neve/neve.env` + the on-disk store are left untouched.
- **Runs as a hardened service.** `deploy/neve.service` runs as an
  unprivileged user with `ProtectSystem=strict` / `NoNewPrivileges`,
  `Restart=always`, and a 120s stop timeout that leaves room for a clean
  shutdown. One-shot provisioning via `cloud-init.yaml` + `bootstrap.sh`.
- **Survives the messy parts.** WebSocket reconnect with backoff, an idle
  watchdog for half-open sockets, Cloudflare 429/503 `Retry-After` handling,
  and a backfill worker that closes both within-session and cold-restart
  gaps. Graceful SIGINT/TERM/QUIT fsyncs the journal and checkpoints the
  blockstore.
- **Guards its own data.** Every store is stamped with its chain, its network,
  and its record-format version, all verified on open, so mainnet/testnet or
  C/P data can never be mixed in one dir; index writes commit as a single
  atomic fjall batch. P-chain records are additionally self-verifying — a block
  whose stored bytes don't hash to its reported block ID is refused at ingest
  and counted.
- **Replicates fast.** Mirror mode (`--mirror-from`) bootstraps a fresh
  replica's whole retained tail — ~178k blocks / ~1.6 GB — from another neve
  in minutes, with no public-endpoint rate-limit and no multi-day node
  bootstrap.
- **Measured, not asserted.** A same-hardware head-to-head against
  avalanchego is documented in [`benchmark/`](benchmark/README.md)
  (+28% peak RPS, ~6× lower latency, ~22× less RAM).

The sections below record _what_ runs and _how_, for picking the work back up.

## What runs

```sh
cargo run --release -- --network testnet           # friendly dev path, C-chain
cargo run --release -- --network testnet --chains c,p   # both chains, one socket
cargo run --release                                # mainnet (rate-limited)
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  http://127.0.0.1:8545
```

- WS reconnect with exponential backoff on disconnect. Cloudflare 429 /
  503 on either WS or HTTPS is handled via `Retry-After`; if upstream asks
  us to wait longer than `--max-wait` (default 10m) we exit with an ERROR
  rather than sleep silently.
- 9 read-only RPC methods: `eth_chainId`, `eth_blockNumber`,
  `eth_getBlockBy{Number,Hash}`,
  `eth_getBlockTransactionCountBy{Number,Hash}`,
  `eth_getTransactionByBlock{Number,Hash}AndIndex`, and
  `eth_getTransactionByHash`.
- **`eth_subscribe` over WebSocket** with three kinds: `newHeads`
  (geth-compatible stripped headers off the live broadcast), `newBlocks`
  (full bodies as they're ingested), and `oldBlocks` — a neve extension
  that replays a historical range `[from..=to]` straight from storage.
  `oldBlocks` is what a fresh mirror consumes to bootstrap its tail (see
  mirror mode below).
- **Mirror mode (`--mirror-from <url>`).** Points neve at _another neve_
  rather than the public Cloudflare endpoint: it derives both WS and RPC
  endpoints from the one URL, anchors a fresh store's floor at the
  upstream's earliest retained block (probed via `/health`), and bootstraps
  the whole tail through an `oldBlocks` subscription — unthrottled, since
  the upstream is our own. A fresh mirror fills its whole retained tail
  (~178k blocks / ~1.6 GB in the benchmark setup) in minutes. While
  the bootstrap streams, the HTTPS backfill worker holds off (a
  `bootstrap_done` Notify) so it doesn't race the ascending frontier with
  redundant fetches. `--backfill-floor <HEIGHT>` overrides the auto-floor
  (e.g. `--backfill-floor 0` to mirror the whole chain).
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
- **`GET /metrics` endpoint** on the same listen address. Prometheus text
  exposition (classic histograms, no native-histogram feature needed),
  covering:
  - process metadata — `neve_build_info{version,commit}` and
    `neve_process_start_time_seconds` (Prometheus derives uptime as
    `time() - start_time`);
  - ingest freshness — `neve_ingest_*` (head/contiguous height, `behind_blocks`,
    `blocks_total`, `last_block_timestamp_seconds`);
  - upstream fetch/WS health — `neve_upstream_*` (request count/duration,
    `retry_after_seconds`, `connected_since_seconds`, `ws_reconnects_total`,
    `ws_idle_timeouts_total`);
  - subscriptions — `neve_sub_*` (open gauge, lagged, sent bytes);
  - **served RPC** — `neve_rpc_requests_total{method,status}`,
    `neve_rpc_request_duration_seconds`, `neve_rpc_open_connections`, and
    `neve_rpc_misdirected_total`. These land via a jsonrpsee middleware
    (`RpcMetricsService` in `src/metrics.rs`) — no longer deferred.

  Sibling tower layer to `/health`; the global recorder and the full series
  list live in `src/metrics.rs`.

- **Cross-network pollution guard.** At startup we query `eth_chainId`
  against the configured RPC URL and stamp it into a fjall `meta`
  partition; subsequent opens require the stamp to match. Default
  `--data-dir` is `./blockstore-data-<network>` so the two networks land
  in separate dirs by default.

## CLI surface

The clap definitions in `src/main.rs` are authoritative — run
`cargo run --release -- --help` for the full flag list, defaults, and
per-flag help. Mirror mode (`--mirror-from` / `--backfill-floor`) is
described under "What runs" above; everything else is standard ingester
plumbing (`--network`, `--ws-url` / `--rpc-url`, `--data-dir`,
`--rpc-addr`, timeouts, logging).

Chain selection is `--chains c|p|c,p` (default `c`). The unprefixed flags are
C-chain-scoped; the P-chain's equivalents carry a `--p-` prefix
(`--p-rpc-url`, `--p-data-dir`, `--p-backfill-floor`, `--p-poll-interval`,
`--p-request-interval`). Genuinely global flags — `--network`, `--rpc-addr`,
`--max-connections`, `--idle-timeout`, `--max-wait`, `--summary-period`,
`--max-blocks-per-request` — apply to every selected chain. Note
`--p-request-interval` defaults to a deliberately slow 200ms because the public
P-chain endpoint rate-limits far harder than the C-chain's (see the README's
"Endpoints used").

## Layout

- `src/main.rs` — CLI parsing (clap), bootstrap, WS ingester
  (`connect_and_subscribe`, `next_ws_event`, `classify_frame`), HTTPS
  fetcher (`fetch_rpc` covering `eth_getBlockByNumber`, with
  retry/throttle), startup `fetch_chain_id`,
  backfill worker + ETA, periodic summary, signal-driven shutdown, fatal
  Notify channel. `IngestCfg` bundles the cross-cutting runtime knobs.
- `src/storage.rs` — `Storage` handle wrapping blockstore + a fjall
  keyspace with three partitions. The blockstore handle is held under an
  `RwLock<Option<Store>>` (not a `Mutex`): block reads take a shared read
  lock and run concurrently — the blockstore reads via positional `read_at`
  (no shared cursor) and is `Sync` — while only the rare lazy-open and
  `put` take the exclusive write lock. (This RwLock split removed a single
  global mutex that was serializing every read; see the benchmark.)
  `Storage::put` writes block bytes to blockstore, then a single atomic
  fjall `Batch` covering all index writes. An `anchor_floor` (set in mirror
  mode) lets a fresh store's `minimum_height` be anchored below the first
  ingested block so backfill can reproduce the whole upstream range. The
  chain-ID stamp lives in a `meta` partition, scoped to `Storage::open`.
- `src/rpc.rs` — jsonrpsee server. A `BlockSelector` enum
  (`Number` / `Hash` / `Height`) plus a `lookup_block(sel, projection)`
  helper collapses each method body to one or two lines. Hosts the
  `eth_subscribe` handler with the `SubKind` enum (`newHeads` / `newBlocks` /
  `oldBlocks`): live kinds forward off the ingest broadcast channel, while
  `oldBlocks` replays a stored range. Pure projection helpers
  (`tx_count_hex`, `nth_transaction`, `shape_block`) and the subscription
  paths are unit-tested directly.
- `src/middleware.rs` — tower layer that rewrites `200 OK` to `421
Misdirected Request` when the JSON-RPC envelope reports `result: null`.
- `src/health.rs` — tower layer that short-circuits `GET /health` with a
  JSON status report (uptime, block range, on-disk sizes, RSS via the
  `memory-stats` crate). Layered before the `NotFound421` middleware so
  health requests bypass the result-null rewrite.
- `src/metrics.rs` — Prometheus recorder + `GET /metrics` tower layer, the
  typed recording helpers, and the `RpcMetricsService` jsonrpsee middleware
  that records per-request served-RPC metrics and the open-connection gauge.
  Names/labels are defined and unit-tested here.

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
  _history_ below the first newHead we observe; the store's anchor
  (`minimum_height`) is set on cold start to that first observed height,
  and the backfill worker only fills forward from there.
- **One-block index gap possible on crash.** `Storage::put` writes the
  block to blockstore first, then commits an atomic fjall batch for the
  two indexes. A crash between the two stages leaves the block readable
  by height but not by hash / tx, and the backfill worker
  doesn't refill (since `max_contiguous_height` already advanced). The
  doc comment on `Storage::put` spells this out.

## JSON-RPC method status

| Method                                    | Tier                                                                                        |
| ----------------------------------------- | ------------------------------------------------------------------------------------------- |
| `eth_blockNumber`                         | Implemented                                                                                 |
| `eth_call`                                | 4                                                                                           |
| `eth_chainId`                             | Implemented                                                                                 |
| `eth_estimateGas`                         | 4                                                                                           |
| `eth_getBalance`                          | 4                                                                                           |
| `eth_getBlockByHash`                      | Implemented                                                                                 |
| `eth_getBlockByNumber`                    | Implemented                                                                                 |
| `eth_getBlockTransactionCountByHash`      | Implemented                                                                                 |
| `eth_getBlockTransactionCountByNumber`    | Implemented                                                                                 |
| `eth_getCode`                             | 4                                                                                           |
| `eth_getLogs`                             | Implemented (range scan; requires `--ingest-logs`; address index planned)                   |
| `eth_getProof`                            | 4                                                                                           |
| `eth_getStorageAt`                        | 4                                                                                           |
| `eth_getTransactionByBlockHashAndIndex`   | Implemented                                                                                 |
| `eth_getTransactionByBlockNumberAndIndex` | Implemented                                                                                 |
| `eth_getTransactionByHash`                | Implemented                                                                                 |
| `eth_getTransactionCount` (nonce)         | 4                                                                                           |
| `eth_getTransactionReceipt`               | 3 (not implemented — needs a logs index; see `docs/core-wallet-research.md`)                |
| `eth_getUncleByBlockHashAndIndex`         | 0                                                                                           |
| `eth_getUncleByBlockNumberAndIndex`       | 0                                                                                           |
| `eth_getUncleCountByBlockHash`            | 0                                                                                           |
| `eth_getUncleCountByBlockNumber`          | 0                                                                                           |
| `eth_protocolVersion`                     | 0                                                                                           |
| `eth_syncing`                             | 0                                                                                           |
| `net_version`                             | 0                                                                                           |
| `web3_clientVersion`                      | 0                                                                                           |

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
- **Tier 3 — needs a logs index.** Not implemented. An earlier `--receipts`
  flag fetched `eth_getBlockReceipts` per block into a `receipts_by_height`
  partition; it was removed because the public Avalanche endpoint doesn't
  support `eth_getBlockReceipts` (`-32601`), so it never worked off the
  default upstream. The planned replacement is a **logs-first** index built
  from `eth_subscribe("logs")` / `eth_getLogs`, which serves the wallet
  activity feed (`listTransactionsV2`) and can back `eth_getTransactionReceipt`
  and `eth_getLogs` later — see [`core-wallet-research.md`](docs/core-wallet-research.md).
- **Tier 4 — needs state mirror (Firewood change proofs).** The
  change-proof half of [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md); out of scope for
  the block-tail half.

### `platform.*` (P-chain)

Phase 0 serves the Tier-0 set below; everything else in the `platform.*` surface
answers 421 so the fronting pool absorbs it. Tiers are from
[`docs/p-chain-indexing-plan.md`](docs/p-chain-indexing-plan.md): Tier 1 is
fetch-at-ingest (reward UTXOs), Tier 2 is UTXO-set replay, Tier 3 is
staking/fee replay, and some methods are unservable by any mirror.

| Method                                                                 | Tier                                                          |
| ---------------------------------------------------------------------- | ------------------------------------------------------------- |
| `platform.getHeight`                                                   | Implemented (contiguous tip)                                  |
| `platform.getTimestamp`                                                | Implemented (tip block time; 421 for a pre-Banff tip)         |
| `platform.getBlockByHeight`                                            | Implemented (all four encodings, served verbatim)             |
| `platform.getBlock`                                                    | Implemented (by CB58 block ID)                                |
| `platform.getTx`                                                       | Implemented, `json` only (byte encodings need a codec parser) |
| `platform.getTxStatus`                                                 | Implemented (`Committed`; mempool states unservable)          |
| `platform.getRewardUTXOs`                                              | 1 (fetch-at-ingest)                                           |
| `platform.getUTXOs`                                                    | 2 (`sourceChain=X/C` unservable — atomic UTXOs)               |
| `platform.getBalance`                                                  | 2                                                             |
| `platform.getStake`                                                    | 2                                                             |
| `platform.getCurrentValidators`                                        | 3 (`uptime`/`connected` unservable)                           |
| `platform.getValidatorsAt`                                             | 3                                                             |
| `platform.getTotalStake`                                               | 3                                                             |
| `platform.getCurrentSupply`                                            | 3                                                             |
| `platform.getFeeState`                                                 | 3                                                             |
| `platform.getL1Validator`                                              | 3                                                             |
| `platform.validates` / `validatedBy` / `getSubnets` / `getBlockchains` | 0, ingest-derivable registries (not built)                    |
| `platform.issueTx`                                                     | Unservable (write path)                                       |
| `platform.sampleValidators`                                            | Unservable (randomness)                                       |
| `platform.getProposedHeight`                                           | Unservable (preferred-block dependent)                        |

`platform.subscribe` serves `newBlocks` / `oldBlocks` / `newRecords` /
`oldRecords` (no `newHeads` — a P-chain block has no header/body split), and
`--mirror-from` works for `--chains p`. Since avalanchego has no P-chain block
push at all, a neve-P instance is the only streaming source of P-chain blocks
anywhere.

## Benchmark

The head-to-head against a real avalanchego C-chain RPC server is **done** —
full methodology, sweeps, costs, and caveats live in
[`benchmark/`](benchmark/README.md). Summary: on identical c6i.2xlarge
hardware (mainnet, `wrk` closed-loop sweeps over the shared block range),
neve beats avalanchego's CPU-bound peak by **~28%** (23.6k vs 18.4k RPS),
holds a flat plateau past the knee where avalanchego degrades, at **~6×
lower latency** (212µs vs 1.24ms at c1) and **~22× less RAM** (~0.4 GiB vs
~8.8 GiB RSS). Both ceilings are genuine CPU saturation. `benchmark/` also
carries the t4g.small baseline (~4.1k RPS) and the wrk scripts
(`randblock.lua` / `randblock-node.lua`).

**Honest caveat (unchanged):** the mirror is a _partial_ server — "faster
than avalanchego" for a read-only subset of methods doesn't argue the
broader architecture by itself, and neve has **no state yet**. The
architectural argument needs Tier 4 (state via Firewood change proofs);
that work will grow neve's footprint and narrow the cost gap. The
advantages expected to persist are latency, memory, and operational
simplicity.

## Performance experiments / TODO

Not bottlenecks today (neve is CPU-bound at its plateau), but worth
measuring:

- **Wire up the blockstore LRU (`CachedStore`).** blockstore ships a
  byte-budgeted read-through LRU (`CachedStore`, `lru-mem`-accounted, with
  `blockstore.cache.{read,populate}` hit/miss metrics) — and it's already in
  our pinned rev, so this is a neve-side change only: swap `Store` →
  `CachedStore` in `src/storage.rs` and thread a `cache_size` through
  `StoreOptions`. Interesting to measure either way. Open questions: it may
  just **double-cache** bytes the OS page cache already holds (eroding the
  memory win that frees RAM _for_ the page cache), and `CachedStore` guards
  its map with a `parking_lot::Mutex` — the same per-read serialization
  shape we just removed with the `Mutex`→`RwLock` storage fix, so it could
  reintroduce contention. Try it, sweep it, keep it only if it pays.
- **Benchmark mirror bootstrap.** We claim a fresh replica fills its
  ~178k-block / ~1.6 GB tail "in minutes," but the wall-clock time was never
  actually measured (neve-big was torn down before we logged it). Stand up a
  fresh mirror against an existing neve and time the `oldBlocks` bootstrap to
  `bootstrap_done`, across a range of tail depths (block counts) so we can
  state a real blocks/sec and a defensible duration — and see how it scales
  with tail size. Pairs with the backfill-parallelization item below.
- **Parallelize backfill.** Backfill is serial, one block at a time, so a
  cold mirror bootstrap saturates a single core while the rest sit idle.
  Pipeline fetch vs. persist+index, or `buffer_unordered(N)` the fetches,
  to fill faster. Only bites in unthrottled mirror mode (the public
  endpoint is rate-limited to ~25 req/s anyway).
- **Skip the per-block JSON re-encode on ingest.** Every block is fetched,
  fully parsed into a `serde_json::Value`, then _re-serialized_ with
  `serde_json::to_vec` before storage (`subscribe.rs:588`,
  `backfill.rs:382`). The parse is mostly needed (we read `hash` and the
  per-tx hashes to build the indexes), but the re-encode is avoidable:
  capture the raw `result` slice verbatim (`serde_json::value::RawValue` /
  a borrowed parse) and store that, parsing only the few indexed fields.
  Drops a full serialization per block and preserves the upstream's exact
  bytes/key order. Off the serving hot path (ingest is once per block), so
  this only matters for unthrottled bootstrap/backfill fill speed — a lever
  alongside the parallelization item above.
- **200→421 middleware re-parse.** The result-null rewrite re-parses the
  JSON-RPC envelope on the response path — a secondary hot-path cost noted
  during the benchmark, not the cap.

## Branch state

- `main` carries everything above. jj-managed, colocated with git.
- The blockstore dependency is pinned in `Cargo.toml` to
  `rkuris/blockstore` at rev `e039d10` (the `height_highwater` accessor,
  PR #17). That rev already contains the byte-budgeted `CachedStore` LRU
  wrapper (#11) and its cache hit/miss metrics (#14), so wiring up the LRU
  needs no blockstore bump (see Performance experiments above). Later
  blockstore commits — the `recover()` `max_contiguous_height` fix and the
  advisory file-locking on open — are **not** in the pinned rev; bump the
  rev if those are wanted.
- Everything in the sections above has landed: the read-only RPC methods,
  `eth_subscribe` (newHeads/newBlocks/oldBlocks), mirror mode
  (`--mirror-from` / `--backfill-floor`), `/health` and `/metrics`
  (including served-RPC middleware metrics), the `Mutex`→`RwLock` read
  fix, and all earlier quality-of-life items (CLI ergonomics, periodic
  summary, ETA, `--network` enum, chain-id stamp, `--max-wait`,
  `--log-level`, `--version`, unit tests). Open items: RLP body format
  (gated on a real Go-side bootstrap interop requirement) and the
  Performance experiments listed above. The next milestone is the state
  layer (Tier 4, Firewood change proofs).
