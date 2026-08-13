# neve

<img src="assets/neve-logo.svg" alt="neve logo" width="128" align="right">

[![CI](https://github.com/rkuris/neve/actions/workflows/ci.yml/badge.svg)](https://github.com/rkuris/neve/actions/workflows/ci.yml)

**neve** is a small async Rust client that mirrors Avalanche blocks into an
[`rkuris/blockstore`](https://github.com/rkuris/blockstore) instance with
a [`fjall`](https://github.com/fjall-rs/fjall) sidecar carrying two indexes
(hash → height, tx_hash → (height, idx)), and serves a read-only subset of
the node's JSON-RPC API from that storage.

It mirrors either or both of two chains — each one a `[chains.<x>]` table in the
config file, both enabled by default:

- **C-chain** — subscribes to `newHeads` over WebSocket, fetches each
  full block from the HTTPS RPC, and serves `eth_*`. A background backfill
  worker closes any gaps between the local high-water and the upstream tip —
  both within-session (dropped `newHeads` frames) and cross-restart.
- **P-chain** — polls `platform.getHeight` and walks the contiguous frontier up
  to it, serving `platform.*`. There is no push mechanism to subscribe to (no
  `eth_subscribe` analog exists, and the X-chain pubsub was removed in
  avalanchego v1.11.13), and accepted P-chain blocks are final with contiguous
  heights, so one polling loop is both the live path and the gap-closer.

Both run in one process on one socket: each has its own store and upstream
connection, and a request selects its chain by method namespace (`eth_*` vs
`platform.*`). `--chains c` (or `--chains p`) restricts a run to one of them.
See
[`docs/p-chain-indexing-plan.md`](docs/p-chain-indexing-plan.md) for the
P-chain design and roadmap.

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

Mainnet (default) / testnet (`--network testnet`) differ only by host —
`api.avax.network` and `api.avax-test.network`. Every per-chain URL is derived
from one `upstream.base`, so pointing neve at your own node is a single key;
setting a chain's URL explicitly overrides the derivation for that chain alone:

| Chain | Endpoint | Derived from `upstream.base` | Override |
| --- | --- | --- | --- |
| C | WebSocket `/ext/bc/C/ws` | `ws_scheme(base) + /ext/bc/C/ws` | `chains.c.ws_url` |
| C | HTTPS RPC `/ext/bc/C/rpc` | `{base}/ext/bc/C/rpc` | `chains.c.rpc_url` |
| P | HTTPS RPC `/ext/bc/P` | `{base}/ext/bc/P` | `chains.p.rpc_url` |

There is no `chains.p.ws_url`: the P-chain has no upstream push mechanism to
subscribe to. `upstream.kind = "neve"` (what `--mirror-from` sets) derives
differently — every chain's RPC is the base itself and its WebSocket is the same
URL under a `ws`/`wss` scheme, because one neve serves all of it on one socket.
A token from `upstream.token_file` / `NEVE_UPSTREAM_TOKEN` is appended to
whichever URL results, derived or explicit.

Rate limits are the dominant operational constraint, and they bite differently
per chain:

- The mainnet **WS** endpoint allows 3 upgrades/min, with a 24-hour block on
  trip. Testnet is far more permissive and is recommended for dev work.
- The **P-chain** HTTPS path is limited far more tightly than the C-chain's:
  measured 2026-08-10, a sustained ~14 req/s of `platform.*` drew HTTP 429 with
  `Retry-After: 3600`. The limit applies **per IP to the whole host**, so a hard
  P-chain backfill will throttle a co-located C-chain instance too. Each
  P-chain height costs two calls (`hexnc` + `json`), so
  `chains.p.request_interval` paces individual requests — globally, across
  everything in flight — and defaults to a polite 200ms (~5 req/s). Filling deep
  P-chain history therefore wants your own node or a neve mirror; see
  [Bootstrapping the P-chain from genesis](#bootstrapping-the-p-chain-from-genesis).
- **Because that limit is per host, so is neve's own cap.** `upstream.max_rps`
  is one pacer shared by every chain reading from `upstream.base` — 25 req/s by
  default, off when a bypass token is configured or when mirroring another neve.
  The per-chain `request_interval` still applies on top and a fetch waits on
  both, so no chain can spend the host's whole budget and no combination of them
  can exceed it. Two qualifications: a chain whose `rpc_url` points elsewhere
  (your own node) is not on that host and is not charged against the cap, and the
  cap governs backfill and the P-chain fill — the paths that issue sustained
  request volume — while the C-chain's live `newHeads` fetch stays unpaced so a
  fresh head is never queued behind a backfill request.

## Storage layout

Each chain gets its own single-chain store. `--data-dir` (default
`./blockstore-data-<network>`) is the base: the **C-chain store sits at the base
itself**, which is where C-chain stores in the field live, so moving it would
mean a resync. Other chains nest — the P-chain's is
`<data-dir>/p`. Either can be placed explicitly with `chains.<x>.data_dir`,
which is how you put one chain's store on a separate volume.

Within a chain's directory:

- `blocks/` — blockstore data + index files (`blockdb.idx`, `blockdb_N.dat`).
  Keyed by `u64` height; on first run, `minimum_height` is anchored at the
  configured floor, or at the first observed block.
- `index/` — fjall keyspace with three partitions:
  - `hash_to_height` — `blockID (32 B) → height (u64 LE, 8 B)`
  - `tx_to_block` — `txID (32 B) → height (u64 LE) ++ tx_index (u32 LE)` (12 B)
  - `meta` — startup-only stamps, all three verified on every open: `chain`
    (`c`/`p`), `chain_id` (an upstream-derived network fingerprint — decimal
    `eth_chainId` on the C-chain, the genesis block ID on the P-chain), and
    `format_version`. A mismatch refuses the open rather than silently mixing
    data. Missing stamps are treated by what can still be verified: no `chain`
    stamp means a store written before the stamp existed, adopted as C-chain; no
    `format_version` means the pre-combined-record layout, adopted and read as
    described below; but block data with **no `chain_id` at all** is refused,
    since nothing in the store says which network it belongs to and adopting it
    would bind that data to whatever endpoint happened to be configured.

Each stored value is a JSON array whose **element 0 is always the block, exactly
as the upstream RPC returned it**, followed by that chain's derived data:

| Chain | Record |
| --- | --- |
| C | `[blockJSON, logs]` — `eth_getBlockByNumber(n, true)` plus the block's logs |
| P | `[blockJSON, blockBytesHex, rewardUTXOs]` |

The P-chain stores the block twice on purpose: `platform.getBlock` has both a
canonical-bytes encoding and a `json` encoding and clients use both, so storing
each verbatim serves either without a codec parser — and the bytes make the
record **self-verifying**, since `blockID == cb58(sha256(blockBytes))`. Ingest
checks that on every height and refuses any block whose halves disagree
(counted as `neve_ingest_rejected_total`). A derived element with nothing in it
stores `[]`, so turning a feed on later needs no migration.

**Reading older stores.** Stores written before the combined record hold the
bare block object at each height, with no element array and no `format_version`
stamp. They are read, not rejected: the block serves as element 0, and every
derived element reports as **absent** rather than empty — so `eth_getLogs` over
those heights returns not-found (421) instead of claiming they emitted no
events. Both layouts coexist in one store, so upgrading keeps its history.

Storing JSON is debuggable and trivial to serve back; the C-chain format will
need to switch to RLP-encoded `*types.Block` (matching
`graft/coreth/plugin/evm/wrapped_block.go`'s `Bytes()`) if/when this needs
to interop with a Go-side bootstrap snapshot.

## JSON-RPC methods

Listening on `server.addr` / `--rpc-addr` (default `127.0.0.1:8545`) — **one
socket for every selected chain**, with the method namespace selecting which
store answers. For block/hash/tx identifiers we don't have in the local store,
the response is a `result: null` body rewritten to **HTTP 421** by a tower
middleware, per the
api-worker contract in [`docs/StreamingChangeProofs.md`](docs/StreamingChangeProofs.md).

### C-chain — `eth_*`

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
  by `address` / `topics` (and `blockHash` for a single block). **Requires
  `--ingest-logs`**: without it every height stores an empty logs element
  meaning "never fetched", so the method defers upstream rather than reporting
  `[]` and telling a client those blocks emitted nothing. It also answers only
  when the whole requested range is present, and caps the range at 2048 blocks
  like the upstream. Served via a full range scan today; an address index is a
  planned optimization.

  Note the same caution applies to heights ingested *before* `--ingest-logs` was
  turned on: they carry an empty logs element that is indistinguishable from a
  genuine one. Enabling the flag on an existing store does not backfill them.
- `eth_subscribe(kind, from?, to?)` / `eth_unsubscribe` — **WebSocket only.**
  - `"newHeads"` — pushes each freshly-ingested block header (transactions
    stripped, matching geth's `newHeads`).
  - `"newBlocks"` — a **neve extension** that pushes the *whole* block
    (transactions included) as it lands, so a downstream mirror persists it
    directly with no follow-up `eth_getBlockByNumber`. One WS frame per block
    instead of header-then-fetch. This is what `--mirror-from` uses.
  - `"oldBlocks"(from, to?)` — a **neve extension** that replays a stored height
    range for mirror bootstrap.
  - `"oldRecords"(from, to?)` — replays the same range as whole **records**
    (block plus the chain's derived elements) rather than bare blocks. See
    [Extensions](#extensions-beyond-the-standard-api).
  - `"newRecords"` is named by the dialect but not currently servable on the
    C-chain: the live path announces a block *before* its logs are joined, so no
    complete record exists at that moment. The rejection says so.

  `logs` / `newPendingTransactions` / `syncing` are rejected, since they
  aren't backed by the block store. See [Mirroring / chaining](#mirroring--chaining).

### P-chain — `platform.*`

Served with avalanchego's conventions, not eth's: dot-separated method names,
**named object params** (`{"height": 700, "encoding": "json"}`), and unsigned
numbers as strings. Heights are accepted as either a JSON number or a string,
matching avalanchego's own leniency.

- `platform.getHeight` → our contiguous tip, as a string. Reports the
  *contiguous* frontier rather than the high-water mark, so it can never
  advertise a height whose predecessors are missing.
- `platform.getTimestamp` → the tip block's chain time, RFC 3339. Blocks older
  than Banff carry no timestamp, so this answers 421 for a store whose tip is
  pre-Banff.
- `platform.getBlockByHeight({height, encoding})` — the blockstore's primary
  key. All four upstream encodings are served from storage: `json` hands back
  the stored JSON untouched, and `hex` (the default), `hexc`, and `hexnc`
  re-render the stored canonical bytes, with `hex`/`hexc` appending the 4-byte
  checksum.
- `platform.getBlock({blockID, encoding})` — the same record, addressed by CB58
  block ID.
- `platform.getTx({txID, encoding})` — the transaction, sliced out of its
  block's stored JSON (verified identical to what upstream's own `getTx`
  returns). **`json` only**: a tx's canonical bytes aren't separately stored, so
  the byte encodings answer 421 rather than reserialize a guess.
- `platform.getTxStatus({txID})` → `Committed` for anything stored. A mirror can
  never report `Processing` or `Dropped` — those describe a node's local
  mempool — so a miss is a 421 rather than a guess.

Note this deviates from avalanchego deliberately: upstream answers an unknown
height with a JSON-RPC *error*, while neve returns `result: null` → 421, because
"ask someone else" is the correct answer from a mirror and an error would be
indistinguishable from a real failure.

- `platform.subscribe({kind, from?, to?})` / `platform.unsubscribe` —
  **WebSocket only**, notifications under `platform.subscription`. Since
  avalanchego has no push mechanism for P-chain blocks at all, this is a neve
  extension with no upstream counterpart rather than a mirror of one: **a neve-P
  instance is the only streaming source of P-chain blocks anywhere.** Kinds are
  `"newBlocks"`, `"oldBlocks"`, `"newRecords"`, `"oldRecords"` (no `"newHeads"`
  — a P-chain block has no header/body split). Heights are avalanchego unsigned
  integers, not hex quantities.

Everything else in the `platform.*` surface — anything needing UTXO or staking
replay — answers 421 today, so the fronting pool absorbs it exactly as it
absorbs `eth_call`.

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

Note: an `oldBlocks` subscription completing ends that *subscription* but, per
jsonrpsee, leaves the **WebSocket open** (it can carry more subscriptions). For a
one-shot bulk download where you want the connection to end on its own, use
`GET /blocks`.

### `"newRecords"` / `"oldRecords"` — whole-record streams (WebSocket)

Same two streams, carrying the whole stored **record** array instead of just the
block: element 0 is the block, and the rest is that chain's derived data (see
[Storage layout](#storage-layout)).

This is what a downstream mirror should subscribe to. A P-chain mirror fed only
block JSON could serve neither the `hex`/`hexnc` encodings nor verify a block ID,
because the canonical bytes live in element 1 — so `--mirror-from` with
`--chains p` uses `oldRecords` to bootstrap and `newRecords` to follow.

`oldRecords` works on any chain (a stored record is complete by definition).
`newRecords` needs the live path to hold the finished record when it announces,
which the P-chain does and the C-chain deliberately doesn't — it publishes a tip
block before joining its logs so reads don't wait on that round-trip. Asking for
`newRecords` on the C-chain is rejected with that explanation.

### `platform.subscribe({kind, from?, to?})` — the P-chain block stream

The P-chain equivalent of `eth_subscribe`, with notifications under
`platform.subscription` and avalanchego's named-object params. Kinds:
`newBlocks`, `oldBlocks`, `newRecords`, `oldRecords` — no `newHeads`, since a
P-chain block has no header/body split.

avalanchego offers no P-chain block push of any kind (the X-chain pubsub was
removed in v1.11.13 and nothing replaced it), so this has no upstream
counterpart: **a neve-P instance is the only streaming source of P-chain blocks
anywhere.**

```sh
# Follow the P-chain tip. Heights are plain integers, not hex quantities.
websocat ws://127.0.0.1:8545 <<<'{"jsonrpc":"2.0","id":1,"method":"platform.subscribe","params":{"kind":"newBlocks"}}'
```

### `GET /blocks?from=[&to=][&chain=]` — NDJSON bulk export (HTTP)

A one-shot streaming download of a height range — one block per line
(newline-delimited JSON), read on demand from storage so an arbitrarily large
range streams without buffering. The response sets `Connection: close`, so the
client gets EOF and exits when the range is done:

```sh
curl -sS 'http://127.0.0.1:8545/blocks?from=86686273&to=87113713' > blocks.ndjson
# from/to accept decimal or 0x-prefixed hex
```

- `from` is required; `to` is **optional** and defaults to a full
  `server.max_blocks_per_request` window from `from`, clamped to the contiguous
  tip. So `?from=X` (no `to`) streams the next chunk, and you page forward by
  advancing `from` to the last height you received plus one.
- Capped at `server.max_blocks_per_request` blocks (default `10000`); a larger
  *explicit* range gets **HTTP 400**. Window a bigger pull into successive
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
- **Idle-connection reaping**: a connection with no read *or* write activity for
  `server.idle_timeout` (default `60s`, `0` disables) is closed — a slowloris /
  leaked-keepalive defense the underlying RPC framework can't do itself. Active
  WebSocket subscriptions are unaffected while blocks keep flowing (each pushed
  block counts as activity); only a fully silent connection is dropped.

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
RPC, the WebSocket, and `/health` on the same socket. It is sugar for two config
keys — `upstream.kind = "neve"` and `upstream.base = <URL>` — and everything
below follows from that `kind`:

- **Endpoint derivation.** Every chain's RPC endpoint *is* the base URL and its
  WebSocket is the same URL with the scheme swapped (`http`→`ws`,
  `https`→`wss`), rather than the `/ext/bc/...` paths an avalanchego upstream
  gets. This overrides `--network`; an explicit `chains.<x>.rpc_url` still wins.
- **Full-range backfill.** On an empty local store, neve probes the upstream's
  `/health` for `blocks.min_height` and anchors its store floor there, so the
  backfill worker reproduces the upstream's whole retained range rather than
  only growing forward from the current tip. (Without mirroring, a fresh store
  anchors at the first observed `newHead` and never fills history older than
  that.)
- **Unthrottled backfill.** `request_interval` defaults to 0 and the shared
  `upstream.max_rps` host cap is off — both exist only to be polite to
  Cloudflare, and the upstream here is another neve with no such limit.
- **`newBlocks` live tail.** The mirror subscribes to the upstream's
  `newBlocks` (not `newHeads`), so each live block arrives whole on the
  WebSocket and is persisted with no `eth_getBlockByNumber` round-trip. A
  mirror re-publishes what it ingests, so its own `newHeads` / `newBlocks`
  subscribers work and mirror chains propagate.

### Bootstrapping the P-chain from genesis

`chains.p.backfill_floor` is `0` by default, so a fresh P store anchors at
height 0 and fills the whole chain. Where the blocks come from decides whether
that takes minutes or months:

| Source | Measured | Mainnet (~25.3M) |
| --- | --- | --- |
| Public endpoint (default pacing) | 2.5 heights/s | ~117 days — don't |
| Your own node, `concurrency = 1` | ~350 heights/s | ~20 h |
| Your own node, `concurrency = 8` (default) | ~2,400 heights/s | ~3 h |
| Your own node, `concurrency = 32` | ~6,400 heights/s | ~1.1 h |
| Mirroring another neve | ~28,000 heights/s | ~15 min |

(Node figures are against a stand-in at 2 ms/request; a real node's latency and
your disk decide where you land. Mirroring is measured neve→neve.)

So: **the first fill needs your own node, and everything after it should
mirror.**

```toml
# fill.toml — first fill, from a node. Any fully-bootstrapped avalanchego works;
# neve uses platform.getBlockByHeight, not the Index API, so no --index-enabled.
[chains.p]
rpc_url          = "http://your-node:9650/ext/bc/P"
request_interval = 0        # your node, not the public endpoint
concurrency      = 32       # only useful once the pacer is out of the way
```

Repointing `rpc_url` is enough on its own: `upstream.max_rps` applies to chains
reading from `upstream.base`, so the P-chain here is not charged against the
public endpoint's budget, and a C-chain left on the public endpoint keeps its
own. neve says which chains the cap covers in the startup log.

```sh
neve --config fill.toml --chains p

# Or, without a file at all — --set writes into the same key space:
neve --chains p \
  --set chains.p.rpc_url=http://your-node:9650/ext/bc/P \
  --set chains.p.request_interval=0 --set chains.p.concurrency=32

# 2. Fan out. Minutes, not hours, and no load on the node.
neve --chains p --mirror-from http://first-instance:8545
```

Storage runs about **520 B/height** (≈370 B blocks + ≈150 B index, measured on
~370 B canonical blocks), so mainnet's full history is on the order of 13 GB.
Real blocks vary in size, so treat that as a floor.

Notes:

- The floor is baked in at store **creation** and ignored on later opens. To
  re-anchor, delete the data dir. Restarts otherwise resume from the contiguous
  frontier, so a fill is safely interruptible.
- Every height is verified on the way in (`sha256(bytes)` against the reported
  block ID), on the direct path and the mirror path alike, so neither a bad node
  nor a bad upstream can poison the store.
- A store is self-contained: `rsync`ing the data dir from a cleanly-stopped
  instance is the fastest transfer of all, and the `meta` stamps make it refuse
  to open against the wrong chain or network.

### P-chain mirroring

`--mirror-from` works for `--chains p` too, and matters more there. avalanchego
has no push mechanism for P-chain blocks at all, so neve→neve is the *only*
streaming replication path this chain has — and it sidesteps the public
endpoint's harsh per-IP rate limit entirely, which is what makes deep P-chain
history practical.

```sh
# One instance ingests the P-chain from a node; any number mirror it.
neve --chains p --set chains.p.rpc_url=http://my-node:9650/ext/bc/P \
                --set chains.p.request_interval=0
neve --chains p --mirror-from http://10.0.0.5:8545
```

The P mirror differs from the C mirror in one way that matters: it streams whole
**records** (`oldRecords` to bootstrap, then `newRecords` to follow) rather than
bare blocks. A P-chain record's element 1 is the block's canonical bytes, and a
mirror fed only block JSON could serve neither the `hex`/`hexnc` encodings nor
verify a block ID. Streaming records also means the mirror re-runs the same
`sha256(bytes) == blockID` check on every arriving height — it trusts its
upstream exactly as far as a node, which is to say only as far as the bytes
verify.

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

## Configuration

neve is configured by one TOML file, and **every key in it is optional** — a
file containing nothing but `[chains.c]` is a valid config, and so is no file at
all. The annotated reference, with the reasoning behind each default, is
[`deploy/config.toml.example`](deploy/config.toml.example); `neve
--print-config-example` prints the same text, so a deployed binary carries its
own documentation. What follows is the shape of the thing, deliberately not a
second copy of the key list.

```toml
# /etc/neve/config.toml
network = "mainnet"

[upstream]
base       = "https://api.avax.network"   # every per-chain URL derives from this
token_file = "/etc/neve/token"            # appended to each as ?token=…

[server]
addr = "0.0.0.0:8545"

[defaults]                # applies to every enabled chain
summary_period = "1m"

[chains.c]                # presence of the table is what enables the chain
[chains.p]
concurrency = 32          # per-chain, overriding [defaults] and the built-in
```

**Chains are a keyed map, not a pair of flag families.** Every per-chain
knob — `enabled`, `rpc_url`, `ws_url`, `data_dir`, `backfill_floor`, `request_interval`,
`concurrency`, `poll_interval`, `max_wait`, `ws_idle_timeout`,
`prefetch_delay_cap`, `ingest_logs`, `join_buffer_cap`, `summary_period` — is
valid in both `[defaults]` and `[chains.<x>]`, with the chain's own value
winning. Omit the `[chains]` table entirely and **both `c` and `p` run**.

**Turning a chain off** has three spellings, for three situations. Omit its
table — once a `[chains]` table exists it is the whole set, so a file naming only
`[chains.c]` serves only the C-chain. Set `enabled = false` in its table to keep
a tuned block without running it. Or pass `--chains <list>` for one run: that
overrides both, since a selector a config file could veto would be useless in an
incident.

**Precedence, lowest to highest:**

1. built-in per-chain defaults
2. `[defaults]`
3. `[chains.<x>]`
4. environment
5. command-line flags and `--set`

**Durations** are TOML strings: `"40ms"`, `"1m"`, `"1h"`. Units compose, largest
first, with or without spaces — `"1h2m50ms"` and `"1h 2m 50ms"` are the same
duration — and a bare integer means seconds. `backfill_floor` is either a
height or the string `"tip"` — "anchor at the first live block and fill forward
only" — since TOML has no null. The C-chain defaults to `"tip"`, the P-chain
to `0`.

**One-off overrides:** `--set <dotted.key>=<value>`, repeatable, applied to the
parsed file *before* it is deserialized, so it shares the file's key space and
its validation — a typo is a startup error that names the key, not a setting
that silently does nothing.

```sh
neve --config /etc/neve/config.toml --set chains.p.request_interval=0 \
                                    --set chains.p.concurrency=32
```

**The token is never in the config file.** `upstream.token_file` points at a
file (`0640 root:neve` in production) and `NEVE_UPSTREAM_TOKEN` is the
alternative; `token_file` wins when both are set. neve appends it as
`?{token_param}={token}` to every upstream URL, derived or explicit, and holds
it in a type whose `Debug` and `Display` both render `<redacted>` so it cannot
reach a log by accident. `--print-config` prints the path, or
`"<redacted, from NEVE_UPSTREAM_TOKEN>"`, never the value. Configuring a token
also switches **off** the default `upstream.max_rps` host cap — a bypass token
is the reason to have one.

**What is this instance actually running?** `--print-config` resolves the whole
chain above and prints the result. Reach for it before reading the file: an
instance with flags or `--set` on its command line is exactly the case where the
file alone misleads you.

### CLI flags

The command line carries what a human genuinely types ad hoc; everything else
lives in the file. Flags win over the file, which is what makes them a usable
override channel.

| Flag                                            | Config key                        | Purpose                                                                                                                                                           |
| ----------------------------------------------- | --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--config <PATH>`                               | — (`NEVE_CONFIG`)                 | The TOML file to read; must exist if named. Absent, `/etc/neve/config.toml` is read when present, else built-in defaults.                                         |
| `--set <KEY=VALUE>`                             | any                               | Override one dotted key, repeatable. Parsed as a TOML scalar, falling back to a string.                                                                           |
| `--chains <LIST>`                               | `[chains.<x>]` tables             | **Selector**: restrict this run to the listed chains, enabling any the file doesn't mention (with pure defaults). The ergonomic way to do a single-chain dev run. |
| `--network <mainnet\|testnet>`                  | `network`                         | Picks the default upstream host for every chain and the default data dir. Testnet's rate limits are far more permissive; use it for dev work.                     |
| `--data-dir <PATH>`                             | base for `chains.<x>.data_dir`    | Base storage dir (default `./blockstore-data-<network>`). The C-chain store sits here directly (no migration for existing dirs); other chains nest.               |
| `--rpc-addr <ADDR>`                             | `server.addr`                     | JSON-RPC listen address (default `127.0.0.1:8545`). Use `0.0.0.0:8545` to serve externally, then scope access with a firewall / security group.                   |
| `--mirror-from <URL>`                           | `upstream.kind` + `upstream.base` | Mirror another neve: sugar for `kind = "neve"` + that base. See [Mirroring / chaining](#mirroring--chaining).                                                     |
| `--ingest-logs`                                 | `chains.c.ingest_logs`            | Ingest C-chain event logs alongside blocks, which is what makes `eth_getLogs` servable.                                                                           |
| `--log-level <trace\|debug\|info\|warn\|error>` | `log_level`                       | Logging verbosity. Overridden by `RUST_LOG` if set.                                                                                                               |
| `--stop-time <DUR>`                             | —                                 | Exit cleanly after this duration (e.g. `30s`, `5m`, `1h`, or bare seconds). The way to bound a test run — see the note on graceful shutdown below.                |
| `--print-config`                                | —                                 | Print the fully resolved configuration, secrets redacted, and exit.                                                                                               |
| `--print-config-example`                        | —                                 | Print the annotated example config and exit. `neve --print-config-example > config.toml` is a reasonable way to start one.                                        |

Eighteen further flags — `--rpc-url`, `--p-rpc-url`, `--request-interval`,
`--p-concurrency`, `--backfill-floor`, `--summary-period` and the rest of the
per-chain family — still work but are **hidden and deprecated**, each warning
once when used, and will be removed in the next release. Their config keys are
in the table's shape: `--p-request-interval` is `chains.p.request_interval`.

### Environment

| Variable              | Effect                                                        |
| --------------------- | ------------------------------------------------------------- |
| `NEVE_CONFIG`         | Config file path, same as `--config`.                         |
| `NEVE_UPSTREAM_TOKEN` | Upstream token. `upstream.token_file` wins when both are set. |
| `RUST_LOG`            | Standard `tracing` filter; overrides `log_level`.             |

`NEVE_RPC_URL`, `NEVE_WS_URL` and `NEVE_P_RPC_URL` are still honored (mapping to
`chains.c.rpc_url`, `chains.c.ws_url` and `chains.p.rpc_url`) and warn once.
They existed because those URLs carried a `?token=…` credential and a flag would
have exposed it through `/proc/<pid>/cmdline`; `upstream.token_file` is the
answer to that now, and keeps the secret out of the environment too.

## Run

```sh
# Dev quick start — permissive testnet endpoints, both chains.
cargo run --release -- --network testnet

# Bounded test run with verbose logging.
cargo run --release -- --network testnet --stop-time 30s --log-level debug

# C-chain only.
cargo run --release -- --network testnet --chains c

# P-chain only. Its floor defaults to genesis, which is slow against the public
# endpoint on purpose — see chains.p.request_interval.
cargo run --release -- --network testnet --chains p

# A deployed instance: everything in the file, nothing on the command line.
neve --config /etc/neve/config.toml
```

A periodic summary (`summary` INFO line) fires shortly after startup and
then every `summary_period` (default 5 minutes) — **one line per chain**,
tagged with `chain=` — reporting `high_water`, `max_contiguous`, `behind`,
blocks added in the period, and rate. Steady-state per-block events live at
DEBUG.

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

Per-chain pipelines live in `src/eth/` and `src/platform/`; everything else is
shared by both.

- `src/main.rs` — CLI parsing, and per selected chain: the upstream identity
  handshake, store open, pipeline spawn, and signal-driven shutdown.
- `src/chain.rs` — the `Chain` enum and everything that follows from it:
  upstream endpoints, on-disk location, metric label, per-chain ingest config.
- `src/storage.rs` — `Storage` handle wrapping blockstore + fjall, with the two
  index partitions, the `meta` stamps, and a `min_height /
max_contiguous_height / high_water` accessor surface.
- `src/record.rs` — the stored-record codec: element 0 is always the block
  JSON, trailing elements are the chain's derived data, and nothing is ever
  reserialized.
- `src/rpc.rs` — the accept loop and tower stack, plus the merged method table
  (one namespace per running chain).
- `src/eth/` — C-chain: `ingest.rs` (`newHeads` WebSocket + HTTPS fetch),
  `backfill.rs` (gap closing + `eth_getLogs` windows), `rpc.rs` (the `eth_*`
  dialect and the block subscriptions).
- `src/platform/` — P-chain: `ingest.rs` (the polling loop and the
  block-ID verification), `rpc.rs` (the `platform.*` dialect), `codec.rs`
  (CB58 and the hex encodings).
- `src/join.rs` — in-memory buffer joining a block to derived data fetched on a
  separate stream, so only complete records reach the store.
- `src/progress.rs` — backfill progress/ETA lines and the periodic summary,
  one tracker per chain.
- `src/upstream.rs` — the browser UA and `Retry-After` throttle handling both
  chains share.
- `src/middleware.rs` — tower layer that rewrites `200 OK` to `421
Misdirected Request` when the JSON-RPC envelope reports `result: null`.
- `src/health.rs` — tower layer that short-circuits `GET /health` with a JSON
  status report (uptime, per-chain block ranges, on-disk sizes, RSS).
- `src/bulk.rs` — tower layer serving `GET /blocks` as streaming NDJSON.
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
- **No receipts / logs yet.** `eth_getTransactionReceipt` and log queries are
  not served; the public Avalanche endpoint doesn't support
  `eth_getBlockReceipts` anyway. A logs-first activity index is the planned
  next step — see `docs/core-wallet-research.md`.

See `STATUS.md` for the more detailed status table and the open
quality-of-life list.
