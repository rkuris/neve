# Changelog

Notable changes to neve. This file starts at 0.2.0; for 0.1.x see the
[GitHub releases](https://github.com/rkuris/neve/releases) and the git history.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/),
and neve follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Upstream URLs can be supplied through the environment**: `NEVE_P_RPC_URL`,
  `NEVE_RPC_URL`, `NEVE_WS_URL`. The matching flags still work and still win. This
  exists because the public endpoint's rate-limit bypass is a `?token=…` query
  argument, and command-line arguments are world-readable through
  `/proc/<pid>/cmdline` — so a token passed as a flag is visible to every local user,
  while a process's environment is not. `--help` does not print the values.

### Changed

- **jemalloc is now the allocator** (`tikv-jemallocator`, on by default; opt out with
  `--no-default-features`). neve's memory is almost entirely allocator-held rather
  than live: measured on a `t4g.small` under an active C-chain backfill, 440 MiB of
  anonymous memory with *every page private and dirty* — none of it reclaimable under
  pressure — of which 216 MiB sat on 2 MiB transparent huge pages. That is glibc's
  classic profile: per-thread arenas retain freed chunks and rarely return them, and
  huge pages round the retention up. jemalloc purges on a decay timer and does not
  request THP by default. No new build requirement: `zstd-sys` already makes a C
  compiler mandatory, and `deploy/cloud-init.yaml` installs `build-essential`.

- **neve identifies itself as `neve/<version>` upstream**, tracking `Cargo.toml`
  automatically. It previously sent an impersonated Chrome user-agent, added to
  qualify for a Cloudflare WAF rule that skips rate limiting for browser-like
  clients. That rule excludes datacenter ASNs — including the one neve is deployed
  in — so the impersonation could never help where it mattered, while making neve
  indistinguishable from a browser in upstream logs everywhere else. An operator
  looking at a long sequential backfill can now tell what it is and which version
  is doing it.

### Fixed

- **The `summary` line's `behind` no longer reads 0 during a P-chain fill.** It was
  computed as `high_water - max_contiguous`, which is the gap *inside* the store —
  the right measure on the C-chain, where `newHeads` writes at the tip while
  backfill closes holes underneath. A P-chain fill from genesis is strictly forward
  and gapless, so those two heights are equal and the summary claimed `behind=0`
  while the neighbouring `backfill progress` line correctly reported 21.9M. The
  summary now reports whichever gap is larger, and the P-chain refreshes its
  distance-to-tip every height instead of once per 8192-height chunk (which also
  un-staled `/health`'s `behind`).

- **URL query strings are redacted from logs and errors.** neve logged upstream URLs
  verbatim at startup and on failure, which would have written a bypass token into
  journald and anywhere logs are shipped. Every site that renders an upstream URL now
  goes through a redactor, and reqwest errors — whose `Display` embeds the URL — go
  through `without_url()`. Nothing diagnostic is lost: scheme, host and path all
  survive.

## [0.2.2] — 2026-08-11

C-chain backfill is **2.1× faster while making fewer upstream requests**, a
P-chain indexing bug is fixed, and a bad deploy can now be undone by copying a
file instead of waiting out a rebuild. No on-disk format change.

### Added

- **`deploy/rollback.sh`**, and `deploy/update.sh` now archives the binary it
  replaces. Recovering from a bad deploy was previously a rebuild — minutes of
  downtime while you are trying to *end* an outage. Superseded binaries go to
  `/var/backups/neve` (not on `PATH`, outside the repo checkout, so nothing needs
  gitignoring), named by UTC timestamp and the commit they were built from, with
  the newest 5 retained (`ARCHIVE_DIR`, `ARCHIVE_KEEP` to override).

  `rollback.sh` with no arguments lists what is available, marking the one
  currently installed by comparing bytes rather than trusting the filename; pass a
  number or a commit to switch to it. Listing and selector validation need no root,
  so a typo or an already-installed target is reported without a password prompt.
  A rollback archives what it displaces, so it is itself reversible. `update.sh`
  prints the rollback recipe when a deploy fails to come up healthy.

- **`neve --version` now reports the build's commit** — `neve 0.2.2 (abc1234)`.
  The SHA was already compiled in for `/health` and `neve_build_info`, but both
  need a *running* instance, so a binary sitting on disk could not be identified
  at all (Rust string literals aren't NUL-terminated, so `strings` can't isolate
  it either). `rollback.sh` uses this to label archives from the binaries
  themselves.

- **`--request-interval`** paces C-chain backfill upstream requests, the
  counterpart to `--p-request-interval`. Enforced through the shared pacer, so it
  caps requests per second regardless of upstream latency rather than being a nap
  appended to each block. Default 40ms (~25 req/s).

### Changed

- **C-chain backfill no longer re-reads the upstream tip on every block**, which
  was costing a second request per block — half the entire request budget — to
  learn something that moves by ~1 block/s. The tip is now cached for 10s, and a
  cached value is used whenever the frontier is below anything already known
  (local high-water, or the last tip read) — there is work to do regardless of
  what a fresh poll would say. A request is made only to *confirm caught-up*,
  which is the one decision a stale tip could get wrong.

  Measured on the mainnet instance: **11.64 blocks/s at ~23.4 req/s became 19.6
  blocks/s at ~20 req/s** at the initial 50ms default — 1.69× faster while making
  *fewer* upstream requests — and **24.75 blocks/s at ~25 req/s** once the default
  moved to 40ms. A 2.75M-block backfill went from a 65-hour ETA to 30 hours. The
  old 40ms nap plus two round trips per block is what kept the documented ~25 req/s
  intent running at less than half of it.

  If you see HTTP 429, raise `--request-interval`.

- **`--max-wait` default is now 65m** (was 10m). This changes what a long throttle
  does. A throttled Avalanche public endpoint answers `Retry-After: 3600`, which
  under the old default exceeded `--max-wait` and shut neve down — and under a
  `Restart=always` unit, as `deploy/neve.service` ships, that became an hour-long
  crash loop with RPC unavailable throughout, re-paying store recovery each cycle.
  neve now sleeps out the hour with serving intact, logging a WARN and recording
  `neve_upstream_retry_after_seconds`. Set it lower to restore exit-on-throttle if
  an orchestrator should make that call instead.

### Fixed

- **P-chain proposal-block transactions are now indexed.** A Banff proposal
  block carries its standard transactions in `txs` *and* its proposal
  transaction — typically a `RewardValidatorTx` — in `tx`, but `block_txs`
  returned early whenever `txs` was present, including when it was the empty
  array that proposal blocks normally have (mainnet height 25345668 is exactly
  this shape). Every staking reward transaction on the chain was therefore
  missing from `tx_to_block`, so `platform.getTx` and `getTxStatus` answered 421
  for them. Both spellings are now read, `txs` first and the singular `tx`
  appended, and `take_nth_tx` shares that index space so a recorded index always
  resolves to the same transaction.

  **No action needed.** The fix does not rewrite index entries for
  already-stored heights, but nothing is affected in practice: no P-chain store
  predates it, since `--chains` defaults to `c` and the from-genesis P-chain fill
  has not been run anywhere. Any store built from here indexes correctly.

- **`deploy/update.sh` no longer reports a healthy upgrade as `down`.** The
  post-restart health check waited 30s, but neve opens the blockstore and
  recovers the fjall index *before* it binds the RPC port, and that scales with
  store size — on the mainnet host (~5 GiB index) recovery took 43s, so every
  upgrade printed `status: down` about 13s before the service was actually up.
  The wait is now 180s (`HEALTH_TIMEOUT` to override), reports how long it
  actually took, bails out immediately if the unit dies rather than sitting out
  the window, and exits non-zero when the service never answers so an unattended
  run fails loudly.

## [0.2.1] — 2026-08-11

Moves off three yanked crates, picks up two storage-correctness fixes from
`lsm-tree`, and adds contributor documentation.

### Added

- **`CLAUDE.md`** — conventions and traps for agents and newcomers: graceful
  shutdown via `--stop-time` (never `timeout` or `kill -9`, which risk a torn
  index), jj-not-git version control, the local check suite and its markdownlint
  traps, the release flow and why the tag must be the commit you publish from,
  and the storage invariants worth preserving in new code (records vs. bare
  blocks, absent-is-not-empty, big-endian keys for range scans).

### Changed

- **`fjall` 3.1.4 → 3.1.8 and `lsm-tree` 3.1.4 → 3.1.9.** 0.2.0 shipped with a
  lockfile pinning versions that have since been yanked upstream. Also updates
  `spin` 0.9.8 → 0.9.9 (the last remaining yanked package) and refreshes the rest
  of the lockfile, notably `tokio` 1.53.1 and `thiserror` 2.0.20.
- **Minimum supported Rust version is now 1.90** (was 1.85), required by
  `fjall` 3.1.8.

### Notes on the yanks

Neither bug behind the yanks affected neve, but the details are worth recording:

- The yank was caused by
  [lsm-tree#300](https://github.com/fjall-rs/lsm-tree/issues/300): a compaction
  that relocates blobs persisted the wrong compression type, so after a reopen
  reads silently returned LZ4-compressed bytes as the value. It applies only to
  blob trees using KV separation; neve creates every keyspace with
  `KeyspaceCreateOptions::default` and uses no blob trees, so it was never
  exposed.
- `lsm-tree` 3.1.9 additionally fixes
  [lsm-tree#315](https://github.com/fjall-rs/lsm-tree/issues/315), where
  `optimize_runs` could place a newer table behind an older overlapping run and
  make point reads return stale versions. That one is not blob-specific, but
  neve's fjall keys are write-once — a block hash maps to one height, a tx hash
  to one location, and `meta` is written only at store creation — so there is no
  older version to return and no read-modify-write to lose.
- Also included: a memtable-flush/`Tree::clear` data race (`lsm-tree` 3.1.7), a
  panic when a poisoned database is dropped, and a buffered-write error check
  (`fjall` 3.1.7).

No on-disk format change; existing stores open as-is.

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

[0.2.2]: https://github.com/rkuris/neve/releases/tag/v0.2.2
[0.2.1]: https://github.com/rkuris/neve/releases/tag/v0.2.1
[0.2.0]: https://github.com/rkuris/neve/releases/tag/v0.2.0
