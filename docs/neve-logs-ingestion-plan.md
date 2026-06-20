# Logs Ingestion — Design Plan

Working design doc (like `core-wallet-research.md`): forward-looking, not yet
implemented. Started 2026-06-20; architecture settled the same day after a full
review. This is the artifact to implement from.

## Goal

Ingest EVM **event logs** into neve so it can serve `eth_getLogs` from local
storage and back the logs-first core-wallet account-history endpoint
(`listTransactionsV2`, see `core-wallet-research.md`) and the eventual non-executing state
API. Logs are the backbone of that feed: address → tx postings come from Transfer
logs, transfers are decoded from log topics/data, and (because a reverted tx emits
no logs) every log-derived item is status-1 by construction.

## Decision summary

The settled architecture, with the reasoning recorded in the sections below so it
isn't relitigated:

- **Store raw logs, colocated with the block** in a single blockstore record per
  height: `[0]` = the block JSON (byte-identical to the RPC response), `[1]` = the
  block's logs as a JSON array. One zstd-compressed value per height.
- **Join the two halves in memory before writing.** A height is written to the
  blockstore only once both block and logs are present, so the store holds only
  complete, atomic records.
- **Build the fjall indexes inline** as each record is finalized (existing
  `hash_to_height` / `tx_to_block`, plus the new `addr_txs` / `tx_transfers` /
  `token_meta` history indexes from `core-wallet-research.md`).
- **Synthesize receipts from logs** at serve time rather than storing receipt
  objects; full-fidelity receipts (with gas scalars) are a separate future
  milestone, gated on a batchable receipt source.
- **Mirror the parse results, not raw logs.** A downstream neve receives parsed
  index entries (`oldIndex`), never raw logs.

## Baseline: what neve serves today

Driven by a real sample of upstream API usage (9,502,628 calls). neve answers only
methods it can serve from its own store — no proxy/passthrough.

**Baseline today ≈ 41.9% of traffic** is already servable from stored blocks:

| Method                                  |          % |
| --------------------------------------- | ---------: |
| eth_getBlockByNumber                    |      33.1% |
| eth_getTransactionByHash                |       7.3% |
| eth_blockNumber                         |       1.2% |
| eth_getTransactionByBlockNumberAndIndex |       0.2% |
| eth_getBlockByHash                      |       0.1% |
| eth_getBlockTransactionCountByNumber    |     0.007% |
| **served today**                        | **≈41.9%** |

neve serves three more methods absent from the sample, for different reasons — so
the baseline reflects *traffic reaching this layer*, not total client traffic:

- `eth_chainId` — **intercepted/served at the Cloudflare worker (api-worker)
  edge**, so it never reaches the origin this sample measures. Real, high-volume
  client traffic, but not neve's to serve on this path regardless.
- `eth_getBlockTransactionCountByHash`, `eth_getTransactionByBlockHashAndIndex` —
  likely genuinely unused, or composed away upstream (the caller fetches a
  block / `...ByHash` form and slices client-side). Not measurable demand here.

So ~41.9% is a lower bound on what neve could offload from this layer.

**`eth_getLogs` is 7.6%** of traffic and **`eth_getTransactionReceipt` is 14%** —
the second-largest method after `getBlockByNumber`. The remaining large buckets are
all state (`eth_callDetailed` 27%, `eth_getBalance` 3.5%, `eth_getTransactionCount`
1%, `eth_getCode` 0.4%, `eth_getStorageAt` 0.1% ≈ **32% total**) and belong to the
state-layer roadmap, not this milestone. (`eth_getCode` specifically is *not*
firewood-servable: code is stored separately by hash; firewood can surface code
hashes from proofs but fetching the code is beyond the firewood implementation.)

Storing raw logs takes coverage from **~42% → ~49.5%** (`getLogs` directly) and
opens a partial path into the 14% receipt slice via synthetic receipts (below).

## Why logs are a separate ingestion (not derivable from stored blocks)

neve already stores full blocks (`eth_getBlockByNumber(.., true)` bodies). **Logs
are not in those bodies** — they live in transaction *receipts*. So:

- We cannot reconstruct logs from blocks already on disk; this is genuinely new
  data we must fetch.
- A downstream mirror cannot derive logs from mirrored blocks either, and it does
  **not** mirror raw logs — the upstream parses logs once and streams the parse
  results (index entries) downstream. The expensive decode happens once, upstream.
- The public endpoint does **not** support `eth_getBlockReceipts` (`-32601`) but
  **does** support `eth_getLogs`. So `eth_getLogs` is the only viable bulk source
  there — ~13.4k range requests for a year vs. ~27.5M per-block receipt calls.

## Storage-engine selection: blockstore vs fjall (reusable principle)

neve has two storage engines with opposite strengths. **Every "where does this
data live?" decision in this plan — and in future work — comes down to this
distinction**, so it's stated once here and referenced below.

- **blockstore (`blockdb`)** — append-only, positional `read_at` (no shared
  cursor, many concurrent readers), per-value zstd compression, keyed by `u64`
  height, with built-in `min` / `max_contiguous` / high-water tracking. Crucially
  it does **no compaction rewrite**: a value is written once and never moved, so
  **write amplification ≈ 1×**. Ideal for **large, immutable, height-keyed blobs**
  written once and read many times.
- **fjall (LSM tree)** — sorted keys, range scans, point lookups by arbitrary key.
  But an LSM **rewrites values repeatedly during compaction**, so large values pay
  real **write amplification (commonly 5–10× over their lifetime)**. Ideal for
  **small KV pairs**, and for **anything that must be scanned/looked-up by a key
  other than height** (address, tx hash).

**The rule:** classify each dataset by *(size, mutability, access key)*. Big
immutable blobs keyed by height → blockstore. Small records, or anything needing
sorted scans / lookups by a non-height key → fjall. Keep big blobs out of fjall;
the compaction write-amplification is the cost you're avoiding.

How this plan applies it:

| Dataset                                    | Engine         | Why                                               |
| ------------------------------------------ | -------------- | ------------------------------------------------- |
| blocks + logs                              | blockstore     | large, immutable, height-keyed, write-once        |
| `hash_to_height` / `tx_to_block`           | fjall          | small, looked up by hash, not height              |
| `addr_txs` / `tx_transfers` / `token_meta` | fjall          | small; scanned by address, not height             |
| receipt scalars (future)                   | fjall side-car | tiny per-tx KV; no write-amp concern at that size |

This is also the reason a single combined `[block, logs]` blockstore value beats
per-row fjall logs, and why the future receipt scalars go in a fjall side-car
rather than a third blockstore element or a rewrite of the record.

## Storage architecture — combined `[block, logs]` record

### The record

One blockstore value per height: a JSON array `[block, logs]`.

- `[0]` = the block exactly as `eth_getBlockByNumber(.., true)` returns it. Keeping
  it byte-identical preserves the existing invariant (debuggable, trivial to serve;
  README §Storage layout).
- `[1]` = a JSON array of that block's logs, in the shape `eth_getLogs` returns
  (so serving is near pass-through: filter by address/topics, concatenate).
- Heights with no logs store `[block, []]` — an explicit empty array, never a
  missing entry, so the contiguous frontier stays unambiguous.
- The whole value is zstd-compressed by the blockstore (already the default;
  see Compression).

### Existing databases — no migration, no compatibility

This record format is a **breaking change**: today a height holds the bare block
JSON, now it holds a `[block, logs]` array. There is **no migration path and we
explicitly do not care about compatibility with existing neve stores.** neve is a
read-only cache that re-syncs from upstream, so discarding a store costs a resync,
not data — operators delete the data dir and let ingestion rebuild it. (The single
production instance to date is disposable for exactly this reason.)

To make this safe and obvious rather than silently mis-parsing old values, **bump
an on-disk format version in the `meta` keyspace** (alongside the existing
chain-ID stamp): the new code refuses to open a store written by a pre-logs
version with a clear "wipe and resync" error, instead of reading a bare block as
`[block, logs]` and failing per-request. Same pattern applies to every future
record-format change.

### In-memory join buffer

Block and logs arrive on independent streams, so they're joined in memory before
the durable write:

- **On a block** (newHeads→fetch, or newBlocks): insert `height → [block, _]` into
  a `HashMap`. If the logs half is already present, the record is complete → write
  it and remove the entry.
- **On logs** for a height: if the block half is present, complete → write and
  remove; otherwise stash `height → [_, logs]` and wait for the block.
- **Reads for an in-flight height** (block present, not yet persisted) are served
  from the buffer, so deferring the durable write doesn't make a just-arrived block
  unserveable. (Optimization for later: only consult the buffer for heights above
  `max_contiguous_height`.)

### Buffer cap / one-side-stalls behavior

If one side stalls (e.g. `getLogs` throttled while blocks flow), the buffer grows.
Bound it with a cap on the number of pending heights. **When the cap is hit, flush
the whole buffer — drop every pending half — and defer the triggering height; all
of it is left for backfill to re-derive.** Never write a block-only record (it
would need a later rewrite, orphaning the original on the append-only blockstore,
or a side record for the late logs — both defeating the combined record).

Crucially, *don't* merely refuse the new height while keeping the buffered ones:
once a stalled live source recovers it reconnects at the **tip**, not the gap, so
the stranded one-sided halves can never complete and would pin the buffer full
forever (a wedge). Flushing returns the buffer to a working state and hands the
gap to backfill — the same recovery path a crash already uses (in-flight buffer
lost → resume from `max_contiguous_height`). The "store holds only complete
records" invariant holds throughout.

An entry-count cap suffices, so ingest needs no per-write byte bookkeeping; the
per-half resident-bytes gauge exists for memory-pressure alerting, not for the cap.

Note this stall is rarer than it first seems: blocks and logs share one upstream
behind one Cloudflare rate limiter, so throttling hits both together and neve
backs both off in step. Fully-asymmetric failure (blocks fine, logs dead) is a
fluke, not a design center.

### Durability & recovery

The blockstore only ever contains complete `[block, logs]` records, so there are
no torn writes. On crash, whatever was in the in-memory buffer is lost; recovery is
just the normal resume from `max_contiguous_height` with backfill replaying the
missing heights. Cost of a crash is bounded re-fetch of the in-flight window.

### Why combined, not separate stores (considered & rejected)

The core framing: **the in-memory join is unavoidable; the only question is when
it materializes.** A combined record is an *eager* join (at write time); two
separate stores are a *lazy* join (at read time, or via the index). Eager pays
coupling and an in-memory buffer to get one atomic record; lazy pays a second
lookup (usually cache-warm) to keep the halves independent. We chose eager.

A long evaluation; the alternatives and why they lost:

- **Two separate blockstores (block store + log store), independent frontiers.**
  The strongest alternative. Wins on: blocks durable/serveable the instant they
  arrive regardless of logs, decoupled backfill, no crash re-fetch of in-flight
  blocks, and cheap single-sided reads. Lost because those wins are mostly
  **backfill-time only**: at steady state live logs arrive via `eth_subscribe`
  aligned with `newHeads` (same cadence, ms apart), so the join buffer is tiny and
  the fetch-shape mismatch (per-height block vs 2048-range `getLogs`) only exists
  during catch-up. With failure-isolation conceded (shared throttling) and the
  combined record still holding raw logs (so no flexibility lost — it can still
  serve `eth_getLogs`, re-parse, etc.), the remaining two-store edge didn't justify
  the extra store, second write, and lost read-locality. **Simplicity + atomic
  records decided it.**
- **Combined 3-way `[block, logs, receipts]` written together.** Rejected for the
  *current* plan: receipts are a laggy, separately-gated third stream; a 3-way join
  would couple block durability to receipt-source availability. Receipts go in a
  fjall side-car instead (see Future milestone).
- **Per-row fjall logs** (`(height,txIndex,logIndex) → log`). Rejected: `getLogs`
  is a block-range scan, so per-height granularity matches; and big-ish values in
  an LSM pay compaction write-amplification. The address index is a separate fjall
  keyspace either way, so fjall wins nothing on access pattern here.
- **Embed logs inside the block object.** Rejected: breaks the "stored block ==
  exact RPC response" invariant and couples log ingest to block rewrites.

### Read-side cost & mitigation

A zstd frame is all-or-nothing, so serving `eth_getBlockByNumber` decompresses the
logs half it doesn't use (~20% extra), and an **address-less** `getLogs` range scan
decompresses every block half in the range (~6× the bytes). Mitigations:
address-filtered `getLogs` uses the `addr_txs` index and only touches matching
heights (mild overhead); `cached_store`'s decompressed cache amortizes hot reads;
and the locality cuts the other way for correlated access (one decompressed record
serves both a block read and a logs read). Net: a small, query-mix-dependent tax,
accepted in exchange for the single-store simplicity.

Note the tension is inherent to a combined record: storing the two halves as one
zstd frame gives the shared-vocabulary compression win but forces all-or-nothing
decompression; storing them as two frames in one value would give cheap
single-sided reads but throw away the shared-vocabulary saving. You can't have
both in one record — separate stores would, at the cost of the second write and
lost locality. We accept the single-frame form (space win + read tax).

### Compression

Blocks are **already** zstd-compressed (level 3) by the blockstore — `blockdb`'s
`default = ["zstd"]` is active (neve doesn't disable defaults), and `write_block`
always compresses. So the ~690 GB/yr block figure is *logical* JSON; on disk it's
~3–5× smaller. Logs compress even better (zero-padded
topics, repeated addresses). A combined blob also shares vocabulary across halves
(addresses, the txHash each log carries also appears in the block) for a small
extra saving over compressing separately.

## Contiguity frontiers

- **Record frontier** — the blockstore's `max_contiguous_height`, now meaning
  "have a complete `[block, logs]` record for every height up to here." Drives
  backfill and post-restart serveability.
- **Index frontier** — how far the parsed history indexes (`addr_txs` etc.) reach.
  Built inline so it normally tracks the record frontier; exposed on `/health` as
  `index.max_contiguous_height` because it's what mirroring follows.

A **chain-ingesting node** has both (built together). A **pure mirror node** has
no raw logs at all: it receives blocks via `oldBlocks` and index entries via
`oldIndex`, so it stores block-only records + the index, serves the history
endpoint, but cannot serve raw `eth_getLogs`.

## Ingestion sources

### 1. Live tip — `newHeads` + per-block `eth_getLogs` (pull, **implemented**)

Live logs are **pulled, not subscribed.** On each `newHeads` N the live path
fetches the block (as before) and `eth_getLogs(N, N)` for that one block, then
joins them via the in-memory buffer into a `[block, logs]` write. Chosen over
`eth_subscribe("logs")` deliberately:

- **No extra subscription** → no exposure to the public-endpoint subscribe-ban
  (`getLogs` is an ordinary request; neve keeps just its one `newHeads` sub).
- **Authoritative, no completeness wrinkle.** `eth_getLogs(N,N)` returns block
  N's complete log set, so there are no silently-dropped log events and **no
  reconciliation audit is needed** — the wrinkle a subscription would have.
- The cost is one `getLogs` request per tip block (~0.5/s, negligible) and one
  round-trip of latency on the *logs*, which is fine for a cache.

**The join buffer keeps `getBlockByNumber` fast.** `on_block(N)` buffers the
block (immediately serveable via `buffered_block`, and announced to subscribers)
*before* the `getLogs(N,N)` round-trip; `on_logs(N)` completes the join into the
durable write. So the #1 method never waits on the logs fetch — block reads of an
in-flight tip are served straight from memory. If the `getLogs` fetch fails the
block stays buffered (still serveable) and backfill re-derives it; if the cap was
hit the block was flushed and isn't announced.

*Future optimization:* issue the per-block `getLogs` over the WebSocket (the
socket the `newHeads` sub already holds), avoiding the per-request HTTPS connect
— this needs id-correlated request/response multiplexing against the
notification stream (none exists yet, since block bodies are also fetched over
HTTPS). A push-based `eth_subscribe("logs")` path could also be added later for
lower log latency; *that* is where the buffer's stream-alignment role and a
reconciliation audit would return.

### 2. Backfill & reconciliation — `eth_getLogs` ranges

`eth_getLogs` chunked to `api-max-blocks-per-request` (~2048-block cap — see
`avalanche-public-endpoint-quirks`). Used for cold-start/catch-up backfill
(window-structured: fetch the block range and its `getLogs` sweep, join, write
the window) and as the reconciliation audit for the live subscription.

**Backfill joins window-locally — it does *not* use the live join buffer.**
Backfill controls both fetches, so it pairs logs to blocks within the window
directly; the in-memory join buffer (above) is only for the live tip, where the
two streams arrive independently. (Routing backfill through the buffer would let
it defer heights back to itself.)

**Transport (implemented):** backfill issues `eth_getLogs` over the **HTTPS** RPC,
reusing the same client as its `eth_getBlockByNumber` fetches (confirmed working
there) — one request per ~2048-block window, gated by `--ingest-logs`. *Future
optimization:* issue it over the WebSocket instead (paced against the WS CPU quota
`ws-cpu-refill-rate` / `ws-cpu-max-stored`), which avoids a per-request connection
setup; the same socket the live logs subscription uses. Public-endpoint
subscribe-ban caution still applies to the live subscription (one clean probe;
prefer testnet) but not to `getLogs`, which is an ordinary request.

### 3. Mirror — parsed index entries (`oldIndex`)

See Mirroring.

## Parsing & indexes (fjall)

When a record is finalized, decode its logs into the history indexes in one fjall
batch (same write as the existing `hash_to_height` / `tx_to_block`):

- `addr_txs` — `address ‖ BE(MAX-height) ‖ BE(txIndex)` → posting. **Big-endian**
  for range scans (opposite of neve's LE point-lookup convention), so an address's
  history reads as one descending range scan; keyset pagination via opaque token.
- `tx_transfers` — `BE(height,txIndex)` → decoded transfers (materialized so the
  history endpoint doesn't re-parse logs at read time).
- `token_meta` — `eth_call` name/symbol/decimals cache.

These are the schema in `core-wallet-research.md`; serving `listTransactionsV2` on top of
them is tracked there, but the **encoding is in scope here** because it's also the
`oldIndex` mirror wire format.

## Mirroring — stream parse results, not raw logs

A neve extension subscription (working name `oldIndex`) replays the upstream's
**index entries** over a height range — the parse result, not raw logs — so a
downstream neve never re-fetches or re-decodes logs.

- `eth_subscribe("oldIndex", fromHex, toHex?)`, parallel to `oldBlocks`
  (`neve-oldblocks-design`): finite range, or `to` omitted to follow the index
  frontier; refuse-at-subscribe anything not gaplessly satisfiable; bootstrap
  target from `/health` (`index.max_contiguous_height`).
- Live counterpart: as each record is parsed, republish its index entries on an
  `index` broadcast channel for downstream live subscribers (parallels the block
  `blocks` broadcast).
- **Wire format is the index-entry encoding** (above), versioned so an
  upstream/downstream skew is detectable. `token_meta` must travel too (inline or a
  companion subscription) or the mirror can't render name/symbol/decimals.

## Space requirements (order-of-magnitude)

Anchored to blocks ≈ **690 GB/yr logical JSON** over **~27.5M blocks/yr**
(~25 KB/block). Backing tx count out of that gives **~600M txs/yr** and
**~750M logs/yr** (~1.3/tx). All figures are **on-disk after zstd** unless noted;
remember the block figure above is the *logical* size, so on disk blocks are
~3–5× smaller.

| What you store               | Annual (on disk) | Notes                                   |
| ---------------------------- | ---------------: | --------------------------------------- |
| Blocks (existing)            |     ~140–230 GB  | 690 GB logical, zstd-compressed         |
| + raw logs (combined record) |      ~70–130 GB  | compresses well; shares blob vocabulary |
| Parsed index (fjall)         |       ~70–90 GB  | `addr_txs`+`tx_transfers`+`token_meta`  |
| Receipt scalars (future)     |        ~5–25 GB  | tiny per-tx; fjall side-car             |

The logsBloom is **recomputable from the logs**, so it is never stored (it would
otherwise be ~154 GB/yr of derivable data).

## Metrics

Parallel the existing `upstream_request` / `block_persisted` / `ingest_heights`
families: `logs_persisted` and `index_entries_persisted` (by source: live /
backfill / mirror), record-frontier and index-frontier gauges plus "behind tip"
gauges, `eth_getLogs` request latency/outcome, per-window log counts, and parse
throughput.

### Join-buffer health (first-class — this is the early-warning surface)

The in-memory join is where the two ingestion streams can silently drift apart, so
it needs **solid, specific** instrumentation from day one — not a single size
counter. Follow neve's established label convention (one metric name, a
discriminating label, like `SUB_OPEN{kind=...}` / `*{source=...}`): use a `half`
label rather than separate metric names. `half="block"` = a block held waiting for
its logs; `half="log"` = logs held waiting for their block. Track each half in both
**count and bytes** — blocks (~25 KB) and logs (smaller) have very different memory
weight, so a count alone hides an impending OOM:

- `join_buffer_incomplete{half="block"|"log"}` — **gauge**, current entry count of
  each half. A steady climb in one localizes which source is lagging or stalled.
- `join_buffer_incomplete_bytes{half="block"|"log"}` — **gauge**, resident bytes of
  each half. The real memory-pressure signal; alert on these, not on counts.
- `join_buffer_oldest_pending_seconds{half="block"|"log"}` — **gauge**, dwell time
  of the oldest incomplete entry of that half. Catches a *stall* before the buffer
  is large — a rising oldest-age with a still-small count means one side stopped
  advancing.
- `join_latency_seconds` — **histogram**, time from the first half arriving to the
  record completing. At a healthy tip (subscription logs aligned with `newHeads`)
  this sits at milliseconds; p99 creeping up is the earliest drift signal.
- `join_completed_total{first="block"|"log"}` — **counter**, completions labeled by
  which half arrived first. Establishes the normal ordering so the gauges above are
  interpretable (tip is block-first; backfill may be log-window-first).
- `join_buffer_cap_hit_total` — **counter**, times the cap was reached and the
  buffer was flushed (its pending halves dropped) and the height deferred to
  backfill. **Any nonzero value is an actionable problem**, not noise — a stream
  stalled long enough to exhaust the buffer.
- `join_buffer_capacity` — **gauge** (static), the configured cap, so dashboards
  can render the count/bytes gauges as a utilization fraction.

Together these answer the three questions that matter early: *is one side lagging*
(per-half count/bytes), *is one side stalled* (oldest-pending age, latency p99),
and *did the safeguard fire* (cap-hit). Wire them up with the ingestion code, not
as a later add-on.

### Metrics for the known limitations

Each limitation called out in this design fails *quietly*, so each needs a metric
that makes it observable. Mapped limitation → detector:

- **Silent log-incompleteness** (the `eth_subscribe("logs")` completeness wrinkle —
  a dropped event leaves a block's log set short with no contiguity signal). This
  is the most dangerous one because nothing else catches it; the reconciliation
  audit is its only detector, so instrument the audit:
  - `log_reconciliation_runs_total` — **counter**, audit passes performed.
  - `log_reconciliation_mismatch_total` — **counter**, blocks where the
    authoritative `getLogs` set differed from what the subscription ingested.
    **Nonzero means the wrinkle is real and biting** — a primary alert.
  - `log_reconciliation_repaired_total` — **counter**, logs added/corrected by the
    audit; quantifies the magnitude of the leak.
  - `log_reconciliation_lag_blocks` — **gauge**, how far behind the tip the audit
    frontier is (a stalled auditor hides mismatches).
- **Logs-subscription reliability** (the root cause of the above). Parallel the
  existing `upstream_ws_*` family for the logs socket specifically:
  - `upstream_logs_reconnects_total` — **counter**, logs-subscription reconnects
    (each reconnect is a window where events can be lost).
  - `upstream_logs_gap_total` — **counter**, detected ordering gaps (a log for a
    far-ahead block arrives before the current one finalizes).
- **Read-side decompression tax** (single-frame combined record forces decompress
  of the unused half; address-less `getLogs` range scans pay ~6×). Measure the
  query mix and the waste so you know if it's actually hurting:
  - `getlogs_served_total{scan="address"|"range"}` — **counter**, labeled by
    whether the query used the `addr_txs` index (cheap) or a full range scan (the
    expensive case). The label split *is* the tax exposure.
  - `record_decompress_wasted_bytes_total{served="block"|"logs"}` — **counter**,
    bytes of the *unused* half decompressed on single-sided reads (the all-or-
    nothing-frame cost made concrete).
  - blockstore `cached_store` hit/miss (if exposed via its `metrics` feature) —
    the amortization that makes the tax tolerable; track it to confirm.
- **Index-parse lag** (indexes built inline "normally" track the record frontier —
  but can fall behind):
  - `index_behind_records` — **gauge**, record-frontier minus index-frontier.
    Growing = parsing can't keep up with ingestion.
- **Crash re-fetch amplification** (in-flight join buffer is lost on crash and
  re-fetched):
  - `join_buffer_inflight_at_shutdown` — **gauge**, set at graceful shutdown to the
    buffer size that will be lost; pair with `block_persisted{source="backfill"}`
    after restart to see the actual re-fetch volume.
- **Mirror index-schema skew** (`oldIndex` wire-format version mismatch):
  - `mirror_index_rejected_total{reason="version"|"decode"}` — **counter**, index
    entries dropped over a mirror link; nonzero means an upstream/downstream skew
    a downstream would otherwise persist and misread.

## CLI

`--backfill-floor <height>` already sets historical depth and is reused as the log
floor (no new depth flag). Likely add a toggle to enable/disable log ingestion
independently of blocks (logs ~double ingest work and add storage), defaulting off
until the feed is proven, plus a join-buffer-cap flag.

## Future milestone — full receipts (NOT current scope)

Today's plan synthesizes receipts from logs: `logs`, recomputed `logsBloom`,
`status` defaulted to `1` (a reverted tx emits no logs), and the tx/block position
fields neve already indexes. The only missing fields are the gas scalars. This
milestone fills them in — kept out of current scope, gated on ingestion source.

- **What to store:** a fjall side-car keyed `height → packed scalar array`
  (one entry per block; `getTransactionReceipt` serves via `tx_to_block →
  (height, txIndex)` then slices `[txIndex]`). Per-tx scalars: `status`,
  `gasUsed`, `cumulativeGasUsed`, `effectiveGasPrice`, and `contractAddress` for
  creates (itself derivable as `keccak(rlp(from, nonce))`). ~25–50 B/tx.
- **Why fjall side-car, not a 3rd combined element:** these are tiny KV pairs (no
  LSM write-amplification concern), the source is laggy/optional, and a side-car
  fills on its own schedule with a plain `put` — appending to the blockstore record
  would mean a rewrite (orphaning). Same decision criteria as block+logs, landing
  the other way because the data characteristics differ.
- **Recreate, don't store:** assemble the receipt at serve time from block tx
  fields + logs (combined record) + scalars (fjall) + recomputed bloom. The
  synthetic and real receipts are the **same code path** with one optional input:
  `status` and gas come from scalars when present, fall back to synthetic when not.
  Also enables serving `eth_getBlockReceipts` downstream.
- **Gate:** `eth_getBlockReceipts` is `-32601` on the public endpoint and per-tx
  receipts are ~600M/yr — infeasible. This milestone is **predicated on a batchable
  receipt source** (a self-run node, or a coreth fleet with `internal-blockchain` /
  `getBlockReceipts` enabled). Storage is cheap; the source is the blocker.
- **Payoff:** completes `eth_getTransactionReceipt` (14%) with real gas numbers.

## Documentation changes (to land with the implementation — not yet applied)

Captured so they aren't forgotten; no doc edits made yet:

- **`README.md`** — add `eth_getLogs` to the served methods list (§ JSON-RPC
  methods); update the coverage framing (~42% today → ~49.5% with logs; name the
  ~32% state bucket as the next prize) (§ Why neve exists / scope); document the
  `oldIndex` mirror subscription (streams parsed index entries, not raw logs)
  (§ Extensions); note that a pure mirror serves the history endpoint but not raw
  `eth_getLogs` (§ Mirroring); and, when synthetic receipts land, document
  `eth_getTransactionReceipt` with recomputed bloom and placeholder gas (§
  Behavioral deviations).
- **`STATUS.md`** — move `eth_getLogs` to served in the method status table;
  record receipts as "partial (logs-derived, no gas scalars)" pending a batch
  source.

## Open questions

1. Join-buffer cap *value* (entry count). Behavior on overflow is settled —
   flush-all + defer to backfill (per above); confirm the backfill re-derive path
   handles a flushed window cleanly.
2. Reconciliation cadence for the `getLogs` audit vs the live subscription.
3. `oldIndex` wire format details — versioned encoding; how `token_meta` rides
   along (inline vs companion); per-block batching for gapless ordering.
4. Filtering at parse time (only Transfer topics) vs indexing all logs.
   (Leaning index-all; the wallet feed is one consumer of many.)
5. Index-schema versioning across a mirror link.

## Source-of-truth pointers

- Block ingestion this mirrors: `src/subscribe.rs` (live + `oldBlocks` bootstrap),
  `src/backfill.rs` (`backfill_loop`, `persist_backfilled`), `src/rpc.rs`
  (`SubKind`, `serve_live`, `serve_old_blocks`).
- Storage layout to extend: `src/storage.rs` (`Inner` keyspaces, `put`); blockstore
  compression in `blockdb` `src/store.rs` (`compress`/`decompress`, zstd level 3).
- WS namespace facts: `neve-onsocket-fetch-mainnet-fail`.
- Range/CPU caps: `avalanche-public-endpoint-quirks`, coreth `config.md`.
- Consumer of this data: `core-wallet-research.md` (logs-first history endpoint).
