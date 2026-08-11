# Changelog

Notable changes to neve. This file starts at 0.2.0; for 0.1.x see the
[GitHub releases](https://github.com/rkuris/neve/releases) and the git history.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/),
and neve follows [Semantic Versioning](https://semver.org/).

## [0.2.0] — 2026-08-10

neve gained a second chain. One process can now mirror the C-chain, the P-chain,
or both, and the P-chain side ships with a block stream that has no upstream
counterpart — avalanchego has no P-chain block push of any kind, so a neve-P
instance is currently the only streaming source of P-chain blocks anywhere.

Existing C-chain deployments are unaffected by default and need no resync. See
[Upgrading](#upgrading-from-01x) below.

### Added

- **Multi-chain operation** (`--chains c|p|c,p`). One instance per chain, each
  with its own store, upstream connection, and `chain=` metric label, sharing a
  single listening socket. Requests route by method namespace (`eth_*` vs
  `platform.*`), not by URL path. Stores are stamped with chain + network
  identity + record-format version and verified on open, so a store belonging to
  one chain can't be opened as another's.
- **P-chain block serving** in avalanchego's dialect (dot-separated method
  names, named object params, string numbers, CB58 IDs): `platform.getHeight`,
  `getTimestamp`, `getBlockByHeight`, `getBlock`, `getTx`, and `getTxStatus`.
  All four encodings (`json`, `hex`, `hexc`, `hexnc`) are served straight from
  storage without reserialization.
- **P-chain ingest with verification.** A single polling loop — there is no push
  to split live from backfill, and final contiguous heights mean a gap is only
  ever "not fetched yet". Every height is verified before it is stored:
  `sha256(bytes)` must reproduce the CB58 block ID the JSON reports, and the
  JSON's height must match what was requested. Failures are refused and counted
  in `neve_ingest_rejected_total`.
- **P-chain streaming and mirroring.** `platform.subscribe` serves `newBlocks`
  and `oldBlocks`, plus `newRecords`/`oldRecords`, which carry the whole stored
  record rather than just the block — a P-chain mirror fed block JSON alone
  could serve neither the hex encodings nor verify a block ID. `--mirror-from`
  works for `--chains p`, bootstrapping over `oldRecords` and following over
  `newRecords`, re-verifying every arriving record.
- **Event-log ingestion and serving on the C-chain** (`--ingest-logs`). Logs are
  fetched per block via `eth_getLogs` and joined to their block through a new
  in-memory join buffer (`--join-buffer-cap`) on both the live and backfill
  paths, then stored in a combined record. `eth_getLogs` is served from those
  stored records.
- **New flags:** `--chains`, `--p-rpc-url`, `--p-poll-interval`,
  `--p-request-interval`, `--p-concurrency`, `--p-data-dir`,
  `--p-backfill-floor`, `--ingest-logs`, `--join-buffer-cap`,
  `--prefetch-delay-cap`.
- `/health` reports per-chain sections (keeping its previous top-level shape for
  existing consumers) and `/blocks` accepts `?chain=`.

### Changed

- **On-disk record format.** A stored height is now a JSON array — `[block,
  logs]` on the C-chain, `[blockJSON, blockBytesHex, rewards]` on the P-chain —
  rather than a bare block object, gated by a format-version stamp in `meta`.
  The block half stays byte-identical to the upstream response; readers split
  the array without reserializing it.
- **P-chain fill is pipelined**, taking a from-genesis mainnet fill from ~20 h
  to ~1.1 h. A height's two encodings are now fetched concurrently rather than
  back-to-back, and `--p-concurrency` (default 8) heights are kept in flight
  while results are still yielded in height order, so writes stay sequential and
  the contiguity frontier is unaffected. Request pacing is global via
  `--p-request-interval`, because the public endpoint's rate limit is per-IP for
  the whole host and each height costs two requests.
- **blockdb 0.3.0 → 0.4.0**, picking up corrupt-payload errors instead of
  panics, recovery/checkpoint consistency gating, reservation rollback on failed
  writes, a byte-based checkpoint trigger, incremental recovery checkpointing,
  and a buffered recovery scan. No API break and no on-disk format change —
  existing stores open as-is.
- `--log-level` is scoped to neve rather than applied globally, and per-block
  log fetches are traced.
- Ingest, RPC, and subscription code is now split per dialect (`src/eth/`,
  `src/platform/`), with the subscription machinery and WebSocket transport
  shared between them.

### Fixed

- **Stores written before the combined record are readable again.** The
  format-version gate introduced with the logs work refused any populated store
  lacking the stamp, which crash-looped upgrades of instances built before it
  and implied a full resync. Both layouts are now unambiguous on the first
  non-whitespace byte (array vs. object) and coexist in one store: old heights
  keep serving, new writes land as full records beside them, and a rollback to
  the previous binary stays available.
- **`eth_getLogs` is gated on `--ingest-logs`.** Without the gate, an instance
  that does not ingest logs stored an empty logs element for every height and
  served it back as "these blocks emitted no events" — turning a missing answer
  into a wrong one. It now defers upstream unless the instance actually ingests
  logs.
- **Block data carrying no network stamp is refused** rather than adopted and
  stamped with whatever network the process happens to point at, which would
  have silently bound (say) mainnet blocks to a testnet identity forever. A
  store missing only the format-version stamp still adopts.

### Known limitations

- Heights stored before `--ingest-logs` was turned on are indistinguishable from
  genuine empties. Distinguishing them needs a coverage floor in `meta`; until
  then, enable the flag from the start of a store's life.
- A corrupt stored block now surfaces as a JSON-RPC error rather than the
  `result: null` that drives the 421 fallback, so that one height is unservable
  instead of deferring to the pool.
- P-chain **staking rewards are not ingested yet** — that is the remaining half
  of the P-chain Phase 1 work.
- `platform.subscribe("newHeads")` is deliberately absent: a P-chain block has
  no header/body split, so a geth-shaped kind would be a lie.
- `getTx` byte encodings answer 421; a transaction's canonical bytes are not
  separately stored.
- `--mirror-from` is global, so mirroring the P-chain while the C-chain ingests
  from an upstream is not expressible yet.

### Upgrading from 0.1.x

- **No resync, and no config change required.** `--chains` defaults to `c`, so
  an existing C-chain deployment behaves as before, and this release reads
  stores written by 0.1.x, which the format gate previously refused.
- To add the P-chain, place its store directory before enabling it: an
  unreachable P upstream aborts startup *before* the RPC server binds, which
  would take C-chain serving down with it.
- Full history from the public P-chain endpoint is impractical — it answered a
  sustained ~14 req/s with HTTP 429 and `Retry-After: 3600`, per-IP for the whole
  host. Fill from your own node or another neve instance
  (`--p-request-interval 0`), then follow the tip against the public endpoint.
  See `docs/p-chain-indexing-plan.md` for the run book.

[0.2.0]: https://github.com/rkuris/neve/releases/tag/v0.2.0
