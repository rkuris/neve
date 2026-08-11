# P-Chain Indexing — Research & Plan

Working research doc (like `neve-logs-ingestion-plan.md`): findings first, then a
phased plan. Started 2026-07-23 to study **"can neve be changed to index the
Avalanche P-chain instead of the C-chain?"** — and if so, what it could serve and
what carries over.

The answer turned out to be "yes, and it needn't be *instead*": neve now mirrors
the C-chain, the P-chain, or both from one process. **Phase 0 and the streaming
half of Phase 1 shipped 2026-08-10** (neve 0.2.0). See §Status for what exists
today; the research below is kept as the reasoning behind it, annotated where
implementation corrected it.

External facts were verified against avalanchego v1.14.2 / master source,
build.avax.network docs, developers.avacloud.io, and live probes of
`api.avax.network` on 2026-07-23 and 2026-08-10. Items that could not be
verified are marked UNVERIFIED.

## Status

### Shipped

**Multi-chain plumbing** (`--chains c|p|c,p`). One instance per chain, each with
its own store, upstream connection, and `chain=` metric label; one listening
socket shared between them, with requests routed by method namespace (`eth_*` vs
`platform.*`) rather than URL path. The C-chain store stays at `--data-dir`
itself so existing deployments need no migration; other chains nest. Stores are
stamped with chain + network identity + record-format version, all verified on
open. `/health` reports per-chain sections (keeping its old top-level shape for
existing consumers), and `/blocks` takes `?chain=`.

**P-chain Tier-0 serving.** `platform.getHeight`, `getTimestamp`,
`getBlockByHeight`, `getBlock` (by CB58 ID), `getTx`, and `getTxStatus` — in
avalanchego's dialect: dot-separated names, named object params, string numbers,
CB58 IDs. All four encodings (`json`, `hex`, `hexc`, `hexnc`) are served from
storage without reserialization. Misses return `result: null` → 421, unlike
upstream's error, because "ask someone else" is the right answer from a mirror.

**P-chain ingest.** One polling loop — no live/backfill split, because there is
no push to split from and final contiguous heights mean a gap is only ever "not
fetched yet". Each height is fetched in both encodings concurrently, with
`--p-concurrency` heights in flight, paced globally by `--p-request-interval`.
Every height is verified before it is stored: `sha256(bytes)` must reproduce the
CB58 block ID the JSON reports, and the JSON's height must match what was asked
for. Failures are refused and counted in `neve_ingest_rejected_total`.

**P-chain streaming and mirroring.** `platform.subscribe` serves `newBlocks`,
`oldBlocks`, `newRecords`, and `oldRecords`. Since avalanchego has no P-chain
block push of any kind, **a neve-P instance is the only streaming source of
P-chain blocks anywhere**. `--mirror-from` works for `--chains p`, bootstrapping
over `oldRecords` and following over `newRecords`, re-verifying every arriving
record.

### Measured (2026-08-10)

| | |
| --- | --- |
| Ingest ceiling | ~8,400 heights/s (own node, pipelined) |
| Mainnet from genesis | ~50 min at that rate — *faster than avalanchego's own p2p bootstrap fetch* (2,925–5,251 blocks/s, from a live node's logs) |
| neve→neve mirroring | ~28,000 heights/s |
| Disk | ~520 B/height ⇒ ~13 GB for mainnet's 25.3M |

### Not built

- **Rewards** — the remaining half of Phase 1, and the one thing blocking the
  staking story. Needs `RewardValidatorTx` commit/abort tracking across adjacent
  heights and `getRewardUTXOs` fetch-at-ingest. Blocked on confirming the
  upstream JSON shapes: a 12-height scan near the Fuji tip found no proposal
  block to sample.
- Phase 2 product indexes and Phase 3 state replay — untouched. Note that of
  everything core-wallet asks of the P-chain, what shipped covers exactly one
  method (`getTxStatus`) — see §Core-wallet coverage.
- **X-chain** — not in `--chains` at all, and the wallet needs it alongside P
  (§Core-wallet coverage gap 1).
- `platform.subscribe("newHeads")` — deliberately absent; a P-chain block has no
  header/body split, so the geth-shaped kind would be a lie.
- `getTx` byte encodings — a tx's canonical bytes aren't separately stored, and
  slicing them out of the block's bytes needs the codec parser Decision 1 avoids.
- `--p-mirror-from` — `--mirror-from` is global, so a P-chain-only mirror while
  the C-chain ingests elsewhere isn't expressible yet.

### Run book — bringing P-chain up on the production instance

Written 2026-08-10 for the next session; **revised 2026-08-11 against the live
hosts** — three of its assumptions had gone stale or were wrong. Production is
`ssh neve`, store at `/var/lib/neve/blockstore-data-mainnet`, serving C-chain
only because `--chains` defaults to `c`.

#### Preflight (do these first — each one blocks step (a) or (c))

1. **Deploy current `main` to production before enabling the P-chain.** The
   earlier claim that "nothing below needs a rebuild there" is now **false**.
   Production runs 0.2.1 (`aa5f796`), which predates
   `fix(pchain): index the proposal tx that lives beside txs` — so a 0.2.1 binary
   ingesting the P-chain tip silently drops every `RewardValidatorTx` from
   `tx_to_block`. Enabling `--chains c,p` on the current binary would start
   accumulating exactly the bug that was just fixed. `sudo bash
   /opt/neve/deploy/update.sh` (which also picks up the health-check fix, so the
   restart no longer misreports `down` while the ~5 GiB index recovers).

   Secondary reason: the store is built locally against `fjall` 3.1.8 /
   `lsm-tree` 3.1.9 while 0.2.1 pins 3.1.4. Same patch series, so it should be
   compatible, but *newer writer → older reader* is the riskier direction and
   deploying first removes the question.

2. **Check the disk arithmetic, not just free space.** Measured 2026-08-11:

   | | |
   | --- | --- |
   | Available on `/` | 65 GB |
   | C-chain still to backfill | 3.12M blocks × 9.3 KB ⇒ **+29 GB** |
   | P-chain store (full history) | **~13 GB** |
   | Remaining after both | **~23 GB** |
   | Ongoing C growth once caught up | ~0.7 GB/day ⇒ ~33 days of headroom |

   It fits, but not with the comfort "need ~13 GB" implied — and the 29 GB is a
   floor, since it assumes later blocks average the same 9.3 KB as the ones
   already stored. Given the host has already been taken out for a month by a
   full disk, decide here whether to grow it or set a P-chain floor.

3. **Own node must have finished bootstrapping.** `info.isBootstrapped` for `P`,
   not just a responding API. A node still state-syncing the C-chain will also
   contend with the P fill for CPU and disk, so the ~50 min estimate assumes an
   otherwise-idle node.

**Decide next: how much history?** Full genesis→tip is ~25.3M heights, ~13 GB,
~50 min to build — **and ~6.7 h to upload to production** on the measured
4.4 Mbit/s link, which is the real cost (see step (b)). If only recent P-chain
activity matters, a `--p-backfill-floor <height>` makes this dramatically
cheaper — the last 1M heights is ~520 MB, a couple of minutes to build and ~16
min to transfer. The floor is baked in at store creation and cannot be lowered
later without starting over, so choose before step (a).

**(a) Build the store locally, from the avalanchego node.**

```sh
# Node must be past P-chain bootstrap AND serving the API.
curl -s -X POST -H 'content-type:application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"platform.getHeight","params":{}}' \
  http://localhost:9650/ext/bc/P

cargo build --release            # from a checkout of main
./target/release/neve --chains p \
  --p-rpc-url http://localhost:9650/ext/bc/P \
  --p-request-interval 0 --p-concurrency 32 \
  --p-backfill-floor 0 \
  --data-dir ~/neve-p
```

`--p-request-interval 0` removes the public-endpoint politeness delay (it is
pointless against your own node); `--p-concurrency 32` hides round-trip latency.
Watch the `summary chain="p"` lines for rate and ETA. **Stop it with Ctrl-C or
SIGTERM, never `kill -9`** — the graceful path fsyncs the fjall journal and
checkpoints the blockstore, and copying a store that did not shut down cleanly
risks a torn index. The store lands at `~/neve-p/p/`.

**(b) Copy it over.** Production is C-chain-only right now, so the `p/`
directory is unused and can be copied in **while neve keeps serving** — the only
downtime is the restart in step (c).

⚠️ **Do not stage in `/tmp`.** The earlier version of this run book said
`rsync … neve:/tmp/p-store/`; on this host `/tmp` is a **tmpfs of 918 MB**, so
rsyncing a 13 GB store there fails partway and spends RAM on a box that is
concurrently serving. Stage in the login user's home instead — it is on `/`, the
same filesystem as `/var/lib/neve`, so the `mv` is a rename: instant, atomic, and
it never needs space for two copies.

```sh
ssh neve 'df -h / && findmnt -no FSTYPE,SIZE /tmp'   # confirm the above still holds
rsync -aP ~/neve-p/p/ neve:p-store/                  # ~13 GB — see the note below
ssh neve 'sudo mv p-store /var/lib/neve/blockstore-data-mainnet/p && \
          sudo chown -R neve:neve /var/lib/neve/blockstore-data-mainnet/p'
```

**The transfer dominates this run book — measure before planning around it.**
Measured 2026-08-11, 200 MB over one SSH stream to production: **362 s ⇒
4.4 Mbit/s**, so the full 13 GB store is about **6.7 hours**. That is an order of
magnitude longer than building the store (~50 min) and longer than everything
else here combined.

Confirmed to be the uplink rather than contention: avalanchego's own traffic at
the time was 1.4 Mbit/s in, 0.5 Mbit/s out, nowhere near the cap. The uplink is
Starlink, whose upstream is both modest and lossy, and a single TCP stream over a
high-latency lossy path underperforms the nominal capacity — `rsync` is one
stream, so it sees exactly this. Whether several concurrent streams recover any of
it is **untested**: the attempt failed because the YubiKey-resident SSH key
refuses concurrent signing (`agent refused operation`), and SSH multiplexing would
not help since it shares one TCP connection.

Consequences worth internalising before step (a):

- **`--p-backfill-floor` is a transfer-time lever, not just a build-time one**,
  and transfer is what actually costs. Full history is ~6.7 h of upload; the last
  1M heights (~520 MB) is ~16 min. If deep P-chain history is not needed on day
  one, this is where the decision pays.
- **`rsync -P` is resumable**, so the full copy can run overnight and survive a
  dropped link. `--compress` buys nothing on an already-zstd payload.
- **`--mirror-from` is not a shortcut around the bandwidth** — the same bytes
  cross the same uplink. It is still the better mechanism if the link is flaky,
  because it re-verifies every record on arrival instead of trusting a file copy.

Architecture is a non-issue: `arm64` (macOS) and `aarch64` (Linux) are the same
ISA, and every on-disk integer is written with explicit endianness
(`to_le_bytes` in blockdb, `byteorder` in lsm-tree) with snappy/zstd payloads.
A plain copy is correct.

**(c) Turn it on.** Edit `/etc/neve/neve.env`:

```text
NEVE_ARGS=--summary-period 1m --rpc-addr 0.0.0.0:8545 --chains c,p \
  --p-rpc-url https://api.avax.network/ext/bc/P --p-poll-interval 10s
```

then `sudo systemctl restart neve`.

- ⚠️ **Do not add `--chains c,p` before the `p/` directory is in place and the
  P endpoint is reachable.** An unreachable P upstream aborts startup *before*
  the RPC server binds, taking C-chain serving down with it. Verified.
- The public endpoint is fine for tip-following once the store is filled: the
  chain produces ~0.14 blocks/s and the budget is ~2.5 heights/s. `--p-poll-interval
  10s` keeps P's share to ~0.4 req/s, which matters because the rate limit is
  **per-IP for the whole host** and the C-chain backfill is already spending ~25
  req/s against it.

**Verify:**

```sh
curl -s localhost:8545/health | jq '.default_chain, .chains | keys'
curl -s -X POST -H 'content-type:application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"platform.getHeight","params":{}}' \
  localhost:8545
```

Also confirm the P-chain is actually advancing, not merely served — `getHeight`
answers from a filled store even if the tip poller is wedged:

```sh
curl -s localhost:8545/health | jq '.chains.p.blocks'   # behind should shrink
sudo journalctl -u neve -n 20 | grep 'chain="p"'        # summary lines
```

**Rollback:** drop the `--chains c,p …` flags from `NEVE_ARGS` and restart. The
`p/` directory is inert when the P-chain isn't selected, so it can stay. Note the
`main` deploy from preflight step 1 is *not* rolled back by this and does not need
to be — it is C-chain-safe on its own.

### Implementation notes worth carrying forward

- **`platform.*` is registered by hand**, not via jsonrpsee's `#[rpc]` macro: the
  macro derives JSON keys from Rust parameter names, so `blockID`/`txID` would
  need non-snake-case identifiers, and it cannot accept avalanchego's
  number-or-string integers.
- **Apricot-era blocks carry a single `tx` object, not a `txs` array** (and
  commit/abort blocks carry neither). On Fuji's first 294 heights the census is
  185 singular, 108 none, 1 array — so reading only `txs` silently fails to index
  essentially the whole pre-Banff chain.
  **Corrected 2026-08-11: the two spellings are not alternatives.** A Banff
  *proposal* block carries both — standard transactions in `txs`, the proposal
  transaction (a `RewardValidatorTx`) in `tx` — and normally with `txs: []`,
  which is present enough to short-circuit an either/or reader. Mainnet height
  25345668 is that shape. Reading only the first spelling found left every
  staking reward on the chain out of `tx_to_block`; the Fuji genesis range that
  validated Phase 0 contains no proposal block, so the census above could not
  reveal it. Fixed by reading both, `txs` first and singular `tx` appended.
- **Commit and abort blocks are indistinguishable in JSON** (both are just
  `{height, id, parentID}`). Telling them apart — which the rewards work requires
  — means reading the 4-byte type ID at offset 2 of the stored canonical bytes.
  A fixed-offset discriminant, not a codec parser, and another thing storing the
  bytes pays for.
- **The record's element 0 is the block JSON on every chain**, which is what lets
  `oldBlocks`, `/blocks`, and the by-hash path stay chain-blind.

## Verdict

**Yes — and the P-chain is in several ways a *better* fit for neve's
architecture than the C-chain**, with two big caveats.

Better fit:

- **Accepted blocks are final and heights are contiguous** (one block per
  height, no gaps). Polling by height only ever sees accepted blocks, so the
  reorg question disappears entirely — stronger than the C-chain's best-effort
  hash check (`subscribe.rs:660`).
- **Most P-chain "state" is derivable from blocks without a VM.** The P-chain
  is UTXO-based: replaying consumed/produced UTXOs per tx reconstructs
  balances and the staking tables. There is no EVM-execution wall like the one
  that pushes C-chain `eth_call`/`eth_getBalance` into the firewood state-layer
  roadmap. A non-executing indexer can eventually serve nearly the whole
  read API.
- **The chain is small.** Mainnet height ≈ 25.3M (2026-07-23 probe), blocks are
  typically well under a few KB. Whole-history mirroring is cheap; the
  blockstore's contiguity model (`min_height` / `max_contiguous_height`) maps
  1:1.
- **The market has a hole.** The node's own history methods
  (`getRewardUTXOs`, `getStake`, `getBalance`, `getSubnets`, `getBlockchains`)
  are deprecated since v1.9.12 with "use an indexer" as the official answer,
  Glacier is the only real indexer (closed, hosted), ortelius is dead, and Ava
  Labs' new OSS indexer (`ava-labs/avalanche-indexer`, ClickHouse, active) is
  **EVM-only as of 2026-07**. A self-hostable P-chain read mirror has no
  first-party OSS competitor today.

Caveats:

1. **No push mechanism exists for P-chain blocks — at all.** No `eth_subscribe`
   analog; the old X-chain pubsub was removed in v1.11.13 (PR #3490). Live
   ingestion must poll (`platform.getHeight` or the Index API). Public-endpoint
   responses carry `cache-control: s-maxage=5`, so tip-following through the
   CDN is up to ~5s stale. Ironically, neve's own `newBlocks`/`oldBlocks` WS
   extensions would make a neve-P instance *the only streaming source of
   P-chain blocks anywhere* — a genuine differentiator.
2. **Staking reward amounts are not in block bytes.** `RewardValidatorTx`
   carries only the staker's txID; reward UTXOs are minted at execution from
   state the VM tracks (`PotentialReward`, supply). They must be fetched at
   ingest (`platform.getRewardUTXOs` — deprecated but still served) or the
   reward calculator must be replicated. Same shape as the C-chain logs
   problem, same fetch-at-ingest answer.

The ingestion (`subscribe.rs`) and serving (`rpc.rs`) layers are chain-specific
and get replaced. The storage engine, contiguity/backfill mechanics, join
buffer, connection handling, HTTP extensions, and metrics plumbing — the
majority of the hard-won infrastructure — carry over nearly untouched (§Overlap).

## P-chain primer (what's different from the C-chain)

- **API**: JSON-RPC 2.0 at `/ext/bc/P` (aliases `/ext/P`, `/ext/platform`,
  `/ext/bc/platform`, `/ext/bc/11111111111111111111111111111111LpoYY`),
  namespace `platform.*`, **named object params** (not positional arrays),
  numbers serialized as strings. IDs are CB58 (base58 + 4-byte checksum),
  addresses are bech32 (`P-avax1…`).
- **Blocks**: post-Banff there are exactly four block types —
  BanffProposal (29), BanffAbort (30), BanffCommit (31), BanffStandard (32) —
  plus five historical Apricot types (0–4). Commit/abort are separate blocks
  (children of a proposal block), so a staking-reward outcome spans *two
  consecutive heights*. Serialization is the avalanchego linear codec:
  2-byte codec version (always 0) + 4-byte type ID + declaration-order fields,
  big-endian, length-prefixed slices. `blockID = sha256(blockBytes)` —
  stored bytes are self-verifying.
- **ProposerVM wrapping**: `platform.getBlock`/`getBlockByHeight` return the
  **inner** platformvm block. The Index API (`/ext/index/P/block`) returns
  **proposervm-wrapped** containers (3 wrapper types incl. the post-Granite
  epoch-carrying one, plus raw pre-activation blocks). The wrapper carries the
  proposer identity — needed for a proposer index, invisible via `getBlock`.
- **Encodings**: `getBlock`/`getBlockByHeight`/`getTx` accept
  `hex` (default; **appends a 4-byte SHA-256 checksum**), `hexc` (same),
  `hexnc` (plain), and `json`.
- **Transactions**: ~20 types (IDs shared with the block codec). Currently
  active: AddSubnetValidatorTx (13), CreateChainTx (15), CreateSubnetTx (16),
  ImportTx (17), ExportTx (18), RewardValidatorTx (20, system-issued),
  RemoveSubnetValidatorTx (23), AddPermissionlessValidatorTx (25),
  AddPermissionlessDelegatorTx (26), TransferSubnetOwnershipTx (33),
  BaseTx (34), and the five Etna/ACP-77 L1 txs (35–39). Historical/obsolete:
  AddValidatorTx (12), AddDelegatorTx (14), AdvanceTimeTx (19),
  TransformSubnetTx (24). **Helicon (ACP-236, Fuji 2026-07-28, mainnet
  unscheduled) adds types 40–42** (auto-renewed staking) — any parser must
  tolerate unknown type IDs.
- **Cadence**: ~25.3M blocks since Sept 2020 ⇒ long-run average ≈ 7s/block,
  but production is demand-driven and bursty (L1 continuous-fee activity has
  raised the recent rate). A poll loop, not a firehose.
- **Upgrade context**: latest stable avalanchego v1.14.2; Granite (v1.14.0,
  mainnet 2025-11-19) added `getProposedHeight`, `getAllValidatorsAt`, and the
  `proposervm.*` handler at `/ext/bc/P/proposervm`. The platform.* method set
  is identical between v1.14.2 and master.

## RPC surface: what a neve-P could serve

Tiered by what the server needs. Tier 0/1 is "neve as it works today, pointed
at different bytes." Tier 2/3 is state replay — tractable here, unlike the
C-chain, because it's UTXO/staking arithmetic rather than EVM execution.

**Tier 0 — stored blocks + light fjall indexes** (direct analog of today's
block-serving tier):

| Method | Notes |
| --- | --- |
| `platform.getBlock` | by blockID via `hash_to_height` analog; serve stored bytes (any hex variant) or stored JSON verbatim |
| `platform.getBlockByHeight` | primary key of the blockstore |
| `platform.getHeight` | local contiguous tip (same semantics as `eth_blockNumber` today) |
| `platform.getTx` | `tx_to_block`-style index; per-tx JSON sliced zero-copy from block JSON (same trick as `BlockBytes`) |
| `platform.getTxStatus` | `Committed` for stored txs; miss → 421 (a mirror can never say `Processing`/`Dropped` — node-local mempool) |
| `platform.getTimestamp` | timestamp of the served tip block |
| `platform.validates` / `validatedBy` / `getBlockchains` / `getSubnets`* | derivable from CreateChainTx/CreateSubnetTx/ConvertSubnetToL1Tx scan — tiny registries, buildable inline at ingest |

\* `getSubnets`/`getBlockchains` are deprecated upstream but trivially cheap to
serve; `getSubnet` (the replacement) needs the L1-conversion fields — also
ingest-derivable.

**Tier 1 — fetched-at-ingest derived data** (the `eth_getLogs` pattern):

| Method | Notes |
| --- | --- |
| `platform.getRewardUTXOs` | fetch from upstream when a RewardValidatorTx commits; store; serve forever. Deprecated upstream — being the place that still serves it *is the point* |

**Tier 2 — UTXO-set replay** (maintain the UTXO set by applying each tx's
consumed/produced UTXOs; pure bookkeeping, no VM):

| Method | Notes |
| --- | --- |
| `platform.getUTXOs` | P-chain-local UTXOs only. **`sourceChain=X/C` is unservable** — atomic UTXOs live in shared memory, deposited by X/C-chain exports, invisible in P-chain blocks → 421. A multi-chain instance can *approximate* the pending set as exports-seen-on-C/X minus imports-seen-on-P; see §Core-wallet coverage gap 2 |
| `platform.getBalance` (deprecated) | sum of the replayed set, bucketed by locktime/stakeable-lock |
| `platform.getStake` (deprecated) | from the staking subset |

**Tier 3 — staking/fee replay** (validator lifecycle + reward calculator +
fee accumulators; the most VM-like logic, still far short of an EVM):

| Method | Notes |
| --- | --- |
| `platform.getCurrentValidators` | full staking tables replayable **except `uptime`/`connected`** — those are the *queried node's* local observations, fundamentally unservable by any indexer (omit/null; primary-network-only fields anyway). ⚠️ core-wallet *filters and sorts its node picker on exactly those two fields*, so for the wallet this method is effectively unservable, not Tier 3 — see §Core-wallet coverage gap 8 |
| `platform.getValidatorsAt` / `getAllValidatorsAt` | validator-set diffs at arbitrary height — the replay done right gives this for free (Glacier doesn't offer it) |
| `platform.getTotalStake`, `getCurrentSupply` | supply requires the reward calculator (or summing fetched reward UTXOs) |
| `platform.getMinStake`, `getStakingAssetID`, `getFeeConfig`, `getValidatorFeeConfig` | static config — hardcode per network |
| `platform.getFeeState` / `getValidatorFeeState` | ACP-103 gas + ACP-77 continuous-fee accumulators — small deterministic state machines over block timestamps/contents |
| `platform.getL1Validator` | weight/nonce from txs; `balance` needs the continuous-fee accumulator |

**Never faithfully servable** (miss → 421, same contract as today):
`issueTx` (write path), `sampleValidators` (randomness),
`getBlockchainStatus` (`Validating` means "this node validates it"),
`getProposedHeight` / `proposervm.*` (preferred-block dependent),
`getTxStatus` beyond Committed, `getUTXOs` with `sourceChain`, and the
`uptime`/`connected` fields above. The api-worker fronting pattern absorbs all
of these exactly as it absorbs `eth_call` today.

**Index API surface** (`index.getContainerByIndex` etc.): only meaningful if
neve ingests proposervm-wrapped containers from its own node (public endpoint
404s the Index API and `getBlock` returns unwrapped blocks). Defer; revisit if
own-node ingestion lands (§Decision 2).

Unlike the logs plan there is **no traffic sample** behind this tiering — the
C-chain plan was anchored on a 9.5M-call sample; the equivalent
`platform.*` breakdown from the api-worker would tell us whether Tier 2/3 is
worth building at all, or whether Tier 0/1 + product indexes covers real
demand. Getting that sample is the single highest-value open question.

## Beyond node parity: the indexes that matter

Glacier's Data API is the de facto market-standard index set — it is
explicitly "the engine behind the Avalanche Explorer and the Core wallet."
What the market converged on, in priority order:

1. **Address → transactions**, with per-tx consumed/emitted UTXOs, server-side
   txType + time filters. This one index backs Core's activity tab **and its
   entire staking view** (Core's stake list is literally
   `listLatestPrimaryNetworkTransactions(addresses, txTypes=[AddPermissionlessDelegatorTx, AddDelegatorTx])`).
   Direct analog of the planned C-chain `addr_txs` index from
   `core-wallet-research.md` — same fjall shape, different extraction.
2. **UTXO index with spent-tracking** (`consumingTxHash`) + staking/lock
   classification per UTXO → enables Glacier's 8-bucket balance decomposition
   and point-in-time balances.
3. **The staking join**: staking tx ↔ RewardValidatorTx ↔ reward UTXOs
   (rewardType VALIDATOR / DELEGATOR / VALIDATOR_FEE), queryable by reward
   address and by nodeID. Core reads *actual* earned rewards from reward UTXOs
   attached to the original staking tx — never from the node.
4. **Validator registry**: per-node validation periods with
   pending/active/completed/removed status, fee, delegator rollups, capacity —
   the delegation-marketplace queries.
5. **Delegator listing** by nodeID/rewardAddress/status with gross/net reward.
6. **Subnet/L1 registry**: subnets + ownership history, chains, L1 conversion
   linkage, L1 validators with weight + fee balance.
7. Secondary: proposer → blocks (needs wrapped containers), address →
   chains-touched, network staking time series.

Differentiators Glacier does **not** offer: self-hostability, global
txType-filtered scans (Glacier requires an address filter), and per-validator
historical time series. Estimated-reward computation (`estimatedReward` on
staking txs) requires the Tier-3 reward calculator; everything else in 1–6 is
extraction + fjall keys, no execution.

These product indexes ride the same pipeline as the RPC tiers: extract at
ingest while the record is in hand, exactly like the planned C-chain history
indexes. Serving them (Glacier-shaped REST vs. custom JSON-RPC methods) is a
separate, deferrable decision.

## Core-wallet coverage (the demand evidence)

**Added 2026-08-10**, from reading `core-mobile` and the published
`@avalabs/avalanche-module` bundle — the same method the C-chain plan used on
`evm-module`. This answers the §Open-questions "who consumes Phase 2" for the
wallet, and it reorders the phases: the wallet's XP needs are *narrower* than
Tier 2/3 but *deeper* than Tier 0/1, and they cut across the tiering diagonally.

`core-wallet-research.md` inventoried Glacier from `evm-module` only, so it
missed this surface entirely — it lists P-chain as a single category-E line.

### What the wallet actually calls

The whole XP surface is **two Glacier REST endpoints**, both parameterized by
`blockchainId` so they serve **P and X from one implementation**:

| Endpoint | Drives |
| --- | --- |
| `primaryNetworkTransactions.listLatestPrimaryNetworkTransactions` | the XP activity feed, *and* the entire stake list (`EarnService.ts:370`, paged to exhaustion with `txTypes=[ADD_PERMISSIONLESS_DELEGATOR_TX, ADD_DELEGATOR_TX]`) |
| `primaryNetworkBalances.getBalancesByAddresses` | P and X balances, 8-bucket decomposition — fires on every account refresh |

Plus direct node RPC through avalanchejs, which is **not** an indexer surface
and stays upstream permanently:

| Method | Site | Status here |
| --- | --- | --- |
| `getApiP().getTxStatus` | `exportP.ts:80`, `importP.ts:92`, `EarnService.ts:327` | **shipped** |
| `getApiP().getFeeState` | `useDefaultFeeState.ts:24` | Tier 3 |
| `getApiP().getCurrentValidators` | `EarnService.ts:53` ← `useNodes.ts` | Tier 3, and see below |
| `getApiP().getCurrentSupply` | `EarnService.ts:345` | Tier 3 (reward estimation) |
| `getUTXOs('P')` / `getAtomicUTXOs('P','C')` | `AvalancheWalletService.ts:44,92,184,226` | Tier 2 + unservable half |
| `issueTx` | `NetworkService.ts:101` | never servable |
| `getApiX().getTxStatus`, `getApiC().getAtomicTxStatus` | avalanche-module | X-chain |

**Of that whole list, neve 0.2.0 serves exactly one method: `getTxStatus`.**
The wallet's transaction-*construction* and write path (`getUTXOs`,
`getAtomicUTXOs`, `getFeeState`, `issueTx`) is permanently upstream — a wallet
can never point at neve alone, only at an api-worker that fronts it. That is
the existing 421 contract, but it should be said out loud: neve's XP story is
read-side only.

### Gaps against the plan above

1. **X-chain is not in the plan at all.** `--chains c|p|c,p` has no `x`, yet the
   module treats P and X symmetrically (`BlockchainId.P`/`.X`,
   `PrimaryNetworkChainName.X`, an `isXchainBalance` branch, `getApiX()`).
   A wallet fronted by neve gets no X activity and no X balance. Since both
   Glacier endpoints are one handler over a `blockchainId` path param, X is
   mostly the same work again — but the plan needs to state a decision either
   way. (X is AVM/UTXO and post-Cortina linearized, so the height-keyed
   blockstore should still map; the vertex-era history and the `getVertexByHash`
   /`listLatestXChainVertices` surface would not. UNVERIFIED.)

2. **`atomicMemoryUnlocked` / `atomicMemoryLocked` are structurally
   unservable.** Two of the eight balance buckets are shared-memory atomic
   UTXOs, which §Tier 2 already flags as invisible in P-chain blocks
   (`getUTXOs` with `sourceChain` → 421). So even a complete UTXO replay cannot
   reproduce the balance response faithfully.

   **But neve is multi-chain now, which opens a route the single-chain framing
   closed off:** the *export* side of an atomic transfer is a plain tx on C or
   X, and the *import* side is a plain tx on P. Pending atomic UTXOs for an
   address are therefore (exports-to-P observed on C/X) minus (imports observed
   on P) — a cross-chain join that only an instance ingesting both sides can
   do. That is a genuine differentiator rather than a 421, and it is an argument
   for gap 1: doing X buys the X leg of this join too.

3. **Balances are the hot path, and the plan files them under Phase 3 "gate on
   demand evidence."** This section is the evidence. `getBalancesByAddresses`
   is called on every account refresh; `listLatestPrimaryNetworkTransactions`
   only when a view opens.

4. **Both endpoints take a CSV address *list*.** Core passes its whole XP
   address set (internal + external BIP44 chains, `getCachedXPAddresses`) and
   expects one merged, paged, newest-first result. The `addr_txs` prefix scan
   inherited from `core-wallet-research.md` is single-address; serving this
   needs a k-way merge across k prefix cursors with a composite `pageToken`.
   Cheap, but it is not what either doc specifies, and k grows with the wallet.

5. **Server-side `txTypes` + `startTimestamp` filtering is load-bearing, not a
   nicety.** EarnService pages to exhaustion with a two-type filter; a plain
   address scan would over-fetch and break `pageSize` semantics. txType wants to
   be *in the key* (`addr ‖ txType ‖ BE(MAX-height) ‖ BE(tx_index)`) or in a
   companion index — otherwise a delegator query against a busy address scans
   its whole history.

6. **Phase 2 cannot be built without Phase 3's UTXO table.** `PChainTransaction`
   embeds full `consumedUtxos`/`emittedUtxos` as `PChainUtxo` objects carrying
   `consumingTxHash`, `consumingBlockNumber`/`Timestamp`, `staked`, `rewardType`,
   `platformLocktime`, `stakeableLocktime`, `utxoStartTimestamp`/`EndTimestamp`.
   Consumed UTXOs must resolve back to their *creating* tx for amount/addresses/
   asset. So "Phase 2 = extraction + fjall keys, no execution" understates it:
   even the activity feed needs the persistent, spent-tracked UTXO set that
   §Beyond-node-parity lists as index 2. That half of Tier 2 should graduate
   into Phase 2; only the staking/fee replay stays gated.

7. **The two reward fields the stake UI reads are the two hardest.**
   - `estimatedReward` is rendered on every pending stake card
     (`new/features/stake/utils/index.ts:31,110`) — and §Beyond-node-parity says
     plainly it "requires the Tier-3 reward calculator."
   - The realized reward is read as
     `emittedUtxos.find(rewardType === DELEGATOR|VALIDATOR)` on the **original
     staking tx** (`stake/utils/index.ts:45,123`), not on the
     `RewardValidatorTx`. Phase 1 stores reward UTXOs at the height where the
     reward tx commits, so serving this is a read-time join by
     `stakingTxHash` ↔ `rewardTxHash` — index 3 of §Beyond-node-parity, and the
     reason element `[2]`'s placement in the record needs a reverse index, not
     just storage.

8. **Core filters and sorts validators by `uptime` and `connected`**
   (`services/earn/utils.ts:202,204,229,289`) — precisely the two fields §Tier 3
   calls "fundamentally unservable by any indexer," because they are the queried
   node's local observations. The delegation node-picker therefore cannot run
   off neve at any fidelity. `getCurrentValidators` should move to the
   **never-faithfully-servable** list for the wallet's purposes, rather than
   sitting in Tier 3 as if replay would finish the job.

9. **Serving surface is REST, not JSON-RPC.** These are
   `/v1/networks/{network}/blockchains/{blockchainId}/…` paths reached via the
   wallet's `GLACIER_URL`, while neve routes one socket by *method namespace*
   (`eth_*` vs `platform.*`) with no path dispatch. Path-based routing is a new
   dimension in the serve stack. Note this is orthogonal to the §Open-questions
   "serve at `/ext/bc/P`" item: the wallet does not reach these through
   `/ext/bc/P` at all, so there are two independent fronting stories — node
   dialect on `/ext/bc/{P,C}`, Glacier dialect on `/v1/...`.

### What this implies for phasing

The plan's phase order is inherited from the C-chain (blocks → derived → indexes
→ state). The wallet inverts it: **a merged, txType-filtered, multi-address tx
index plus a spent-tracked UTXO set delivers both wallet endpoints**, and
neither needs the validator-set reconstruction that §Decision 6 correctly calls
the hard part. Concretely:

- Pull the UTXO-set-with-spent-tracking half of Tier 2 **into Phase 2**.
- Keep staking/fee replay and `getValidatorsAt` in Phase 3, still demand-gated.
- Treat `estimatedReward` and the `atomicMemory*` buckets as explicit v1
  divergences (omit / best-effort), the same way the C-chain plan accepts the
  failed-logless-tx leak.
- Decide X-chain before Phase 2 keys are designed, since the index shapes are
  shared and retrofitting a chain discriminant into the key is a migration.

## Overlap with C-chain neve

| Component | Fate | Notes |
| --- | --- | --- |
| `blockstore` (height → zstd bytes) | **as-is** | opaque u64 → opaque bytes; contiguity frontier maps perfectly to a final chain |
| fjall `hash_to_height` | **as-is** | 32-byte blockID (sha256 of stored bytes — self-verifying) |
| fjall `tx_to_block` | **as-is** | 32-byte txID → height + index |
| `meta` gate (`storage.rs:23`) | tweak | new format version; replace `chain_id` with `{chain: P, network_id}` — a store is one chain's, mixing is rejected |
| `record.rs` two-element codec | generalize | `[block, logs]` becomes `[blockBytes-as-JSON-string?, blockJSON, rewards]` — see Decision 1; `BlockBytes` zero-copy split reused |
| `join.rs` join buffer | **reuse** | joins block ↔ reward-UTXO fetches at the live tip (same two-half problem as block ↔ logs) |
| `backfill.rs` mechanics | reuse | same frontier/race-guard/rate-limit/ETA loop; swap fetch calls; `LogWindow` → per-RewardValidatorTx reward fetches (no 2048-window analog needed) |
| `subscribe.rs` live ingest | **replace** | WS subscribe → height poller (no push exists). `fetch_rpc` retry/throttle/`Retry-After`/fatal plumbing (`subscribe.rs:517`) survives; the WS session machinery, idle watchdog, and AIMD prefetch don't |
| `rpc.rs` eth surface | **replace** | new `platform.*` trait: named object params, string numbers, CB58/bech32. Height-tag logic, contiguity checks, and the completeness rule (`never partial → 421`) carry over as patterns |
| serve stack (hand-rolled accept loop, `conn.rs` IdleTimeout, max-conns) | **as-is** | chain-agnostic |
| `middleware.rs` 421 contract | **as-is** | identical fronting model: neve answers what it has, pool handles the rest |
| `bulk.rs` `/blocks` NDJSON | as-is | records are opaque NDJSON lines |
| `health.rs`, `metrics.rs` | reuse | drop WS families, add poll families; the rest renames cleanly |
| mirror mode (`newBlocks`/`oldBlocks`, `/health` bootstrap) | **reuse** | the subscription payload is an opaque record; a neve-P mirror chain works day one — and gives the P-chain the streaming interface avalanchego never had |
| CLI (`main.rs`) | tweak | `--network` gains P-chain URLs; `--ws-url`/`--ws-idle-timeout`/`--prefetch-delay-cap` become C-chain-only or retire; add `--poll-interval` |

Net: storage, durability, connection handling, fronting contract, mirroring,
bulk export, and observability — reused. The two chain-facing surfaces
(ingest source, RPC dialect) — rewritten. That is the same split the logs
milestone already forced, so the codebase is pre-fractured along the right
seam.

## Key design decisions

### 1. What to store per height

The C-chain record stores the upstream JSON byte-identically and serves it
back verbatim — no reserialization, no drift risk. The P-chain complicates
this: `getBlock` has both a canonical-bytes encoding (`hex*`) and a `json`
encoding, and clients use both.

- **(a) Store hex bytes only, hand-roll a parser, render JSON at serve time.**
  Most compact; one upstream call per height. But avalanchego's JSON shape
  must be reproduced exactly, the parser must cover every tx type including
  ones that don't exist yet (Helicon 40–42), and `avalanche-types` is not a
  shortcut — the crate is alpha/unmaintained (last real activity 2024),
  has **no block parsing at all**, and stops at Banff-era txs. Everything
  post-Durango would be hand-written and fork-tracking forever.
- **(b) Store JSON only.** Matches the C-chain philosophy exactly, but the
  `hex` encodings of `getBlock`/`getTx` become unservable, and stored data
  can't be integrity-checked (blockID = sha256 of *bytes*, not of JSON).
- **(c) Store both: record = `[blockBytes, blockJSON, rewards]`.** Two upstream
  calls per height (`encoding: "hexnc"` + `encoding: "json"`). Serve either
  encoding verbatim; verify `sha256(bytes) == blockID` at ingest (a
  correctness check the C-chain path never had); build all fjall/product
  indexes from the JSON with serde — **no codec parser required at all**.
  Unknown future tx types flow through untouched, byte-identical — the
  store is forward-compatible by construction. Cost: double the fetch calls
  and roughly 3–5× the pre-compression bytes of (a); the chain is small
  enough that this is noise (§Sizing).

**Recommendation: (c).** It keeps neve's core bet — *store what upstream said,
serve it back verbatim* — on both encodings at once, and it removes the
codec parser from the critical path entirely. A minimal bytes-parser can come
later as a validation/hardening layer (or to reduce backfill to one call per
height), not as a launch dependency. Element `[2]` holds reward UTXOs
(usually absent — the `EMPTY_LOGS` trick reused).

### 2. Ingestion source

- **(a) Public endpoint polling** (`api.avax.network/ext/bc/P`). Verified: all
  `platform.*` methods are routed (no per-method filtering like the C-chain
  `eth-apis` config), plain HTTP polling drew no throttle, and — unlike the
  C-chain WS story — there's no subscribe path to get banned from. Costs: 5s
  CDN staleness at the tip (`s-maxage=5`), unpublished rate limits
  (`x-execution-weight: pchain` header implies weighted limiting —
  UNVERIFIED thresholds), backfill politeness budget.
- **(b) Own avalanchego with `--index-enabled`.** No CDN lag, proposervm
  containers (proposer index + Index API surface become possible), no rate
  concerns. Costs: running a node, and the index must be enabled from the
  node's first launch (or `--index-allow-incomplete`). Also the *only* way to
  get container bytes — `getBlock` never returns the wrapper.
- **(c) Mirror another neve-P** — free once (a) or (b) exists anywhere;
  identical to today's `--mirror-from`.

**Recommendation: (a) as the default** (zero-infra, matches neve's C-chain
posture), with the fetch layer written so (b) is a URL swap. Tip loop:
poll `getHeight` at ~1s (server-side cache makes faster pointless), fetch
missing heights through the existing backfill-style frontier. Proposer
indexes and Index-API serving are explicitly out of scope until (b) matters
to someone.

### 3. Rewards

- **(a) Fetch-at-ingest** via `platform.getRewardUTXOs(stakerTxID)` when a
  RewardValidatorTx is followed by a commit block (abort → no reward, store
  empty). Exact structural repeat of the logs decision: derived data the
  block doesn't carry, fetched once, served forever. Risk: the method is
  deprecated (since v1.9.12) — it could someday be removed from the public
  endpoint; verified still routed today.
- **(b) Replicate the reward calculator** (stake × duration × supply-curve,
  delegation fee split). Deterministic and removes the upstream dependency,
  but requires the Tier-3 staking replay to know `PotentialReward` inputs —
  wrong milestone to block on.
- **(c) Skip rewards.** Guts the product story (§indexes 3 is the
  staking join; Core's realized-rewards view is exactly this data).

**Recommendation: (a) now, (b) later as validation + hedge** (when Tier 3
lands, computed rewards should reproduce fetched ones bit-for-bit — a strong
correctness oracle for the replay).

### 4. One binary, one store per chain

Same repo, same binary; a chain mode (implied by `--network`/`--chain`)
selects the ingest pipeline and RPC dialect at startup. Stores are
single-chain, enforced by the meta gate. No multi-chain multiplexing inside
one process/store — "instead of", not "as well as", per instance. A fork or
separate crate would duplicate the 60%+ of the code the overlap table says is
shared, and the flat single-crate layout makes the module split
(`chain/pchain/{ingest,rpc,extract}.rs` alongside the eth equivalents) cheap.
Extracting a formal chain-agnostic core trait can wait until the second chain
actually compiles — premature abstraction before then.

### 5. Encodings and dialect

CB58 (base58 + checksum) and bech32 address codecs are small, stable,
hand-rollable (or `bs58` + `bech32` crates); avalanchego's string-numbers and
named-params conventions live in the new RPC trait. No blocker, just
diligence. The JSON-RPC engine question (jsonrpsee handles object params fine;
the hand-rolled migration owns the accept loop either way) is unaffected.

### 6. Validator-set reconstruction (the hard part of Tier 3)

"Which validators are actually valid" is the hardest single problem in this
plan, and the difficulty is not tx parsing. It lives in four places:

- **The set changes without any transaction.** Validators enter/exit when
  *chain time* (post-Banff: the block timestamp) crosses their
  start/end times; the VM force-issues `RewardValidatorTx` proposals as the
  expiry queue drains. Replay must reproduce that time-advancement machinery,
  not just fold txs into a table.
- **The rules are era-dependent.** Pre-Banff, time advanced via
  `AdvanceTimeTx`; post-Banff, via timestamps. Pre-Durango there was a
  pending→active promotion at `startTime`; Durango made stakers active at
  acceptance (why `getPendingValidators` was removed). Cortina moved
  delegatee fee-reward minting to the validator's end (accrued). Etna added
  the L1 lifecycle. Each boundary is a network-specific activation timestamp
  to gate on.
- **Rewards and supply are sequentially coupled.** `potentialReward` is
  computed at add time from current supply, and supply is immediately
  incremented by the earmarked reward (decremented on abort). Supply at
  height H depends on every prior staker's reward, which depends on supply at
  *their* add time — integer-rounding drift compounds forever.
- **L1 validators are "valid" as a function of a fee accumulator.** Active
  only while the continuous-fee balance is positive; drain rate is a dynamic
  price over the whole active set (ACP-77). Deactivation is deterministic but
  emergent from the fee math, and weight/nonce changes arrive inside warp
  payloads embedded in `RegisterL1ValidatorTx`/`SetL1ValidatorWeightTx`.

What *isn't* hard, because of a principle worth enforcing everywhere —
**read outcomes from the chain, never re-derive decisions**: commit vs. abort
(the uptime vote) is just whichever child block was accepted;
`RewardValidatorTx` ordering appears explicitly in blocks (apply it; keep our
own expiry queue as a validation check only); actual reward amounts come from
the Phase-1 fetched reward UTXOs. The only things forcing a bit-exact reward
calculator are supply earmarking and aborted stakers (whose potential reward
never materializes as UTXOs).

Design moves that follow:

- **Split two fidelity bars.** The *product* bar — who validates now, with
  what stake/fee — is what Core shows, and Core derives it client-side from
  `startTimestamp`/`endTimestamp` on staking txs. That is Phase-2 material:
  timestamp math over the tx index; errors are cosmetic. The *consensus* bar
  is `getValidatorsAt`: exact weights + BLS keys at a height, consumed by
  ICM/warp relayers to verify signature quorums — a slightly wrong weight
  breaks downstream verification. That needs the real replay and stays in
  Phase 3 behind the demand gate. Don't let the hard version block the
  useful version.
- **Floor-anchor to amputate eras.** A replay floor at the Etna (or Durango)
  activation removes `AdvanceTimeTx`, pending-set promotion, and the Cortina
  reward-flow change from scope entirely. Cost: no replay from genesis —
  seed from a **state snapshot at the floor** (page `getCurrentValidators`,
  `getUTXOs`, `getCurrentSupply`, subnets/chains from upstream; analogous to
  the mirror `/health` bootstrap). Heights below the floor answer 421, same
  contract as everything else. The single biggest complexity lever available.
- **Differential-test against the node continuously.** The oracle is sitting
  right there: call upstream `getValidatorsAt` at every (or sampled) heights
  during ingest and diff against the replay; cross-check computed rewards
  against fetched reward UTXOs. A mirror that self-verifies every block
  against the reference implementation turns "did we port avalanchego's
  staker state machine correctly" from a code-review question into a
  monitored invariant.

Net: the challenge is real but concentrated — consensus-grade
`getValidatorsAt` plus L1 fee accounting. The staking views people use
day-to-day don't need either.

## Sizing & cadence

**Measured 2026-08-10** (synthetic ~370 B canonical blocks through the real
ingest + verify + store path): **~520 B/height on disk** — ~370 B blockstore
plus ~150 B fjall index — so mainnet's ~25.3M heights land around **13 GB**,
matching the O(10 GB) guess below. Real blocks vary in size, so treat it as a
floor.

Fill rate, against a stand-in node at 2 ms/request: 347 heights/s serial,
2,371 at the default `--p-concurrency 8`, 6,360 at 32 — i.e. mainnet from
genesis in ~20 h, ~3 h, or ~1.1 h. neve→neve mirroring measured ~28,000
heights/s (200k records in ~7 s), so once one instance holds the history,
replicating it is ~15 minutes rather than hours. The original estimate below
(~23 days at the C-chain's polite rate) assumed the serial two-calls-per-height
shape and the public endpoint; both have since changed.

### Original estimates (superseded above)

Rough, deliberately soft numbers — Phase 0's first job is replacing them with
probes: ~25.3M heights; inner blocks typically hundreds of bytes to a few KB;
JSON ~2–4× the bytes. Order tens of GB pre-compression for `[bytes, json]`,
and zstd does very well on repetitive JSON — plausibly O(10 GB) on disk for
full history. Backfill at the C-chain's polite ~25 req/s with 2 calls/height ≈
23 days for full history (half that with 1 call/height; near-zero from a
mirror; days from an own node). A `--backfill-floor` anchored at the Banff or
Etna activation height is the pragmatic default for a first deployment, with
deep history filled later — same playbook as the C-chain.

## Phased plan

**Phase 0 — spike (prove the shape). LANDED 2026-08-10.** Fetch loop against
`api.avax.network/ext/bc/P`: `getHeight` poll + `getBlockByHeight` in both
encodings; `[bytes, json]` record behind a new format version; blockID
verification at ingest; `hash_to_height` + `tx_to_block` extraction from JSON;
serve `getBlock`/`getBlockByHeight`/`getHeight`/`getTx`/`getTimestamp` +
`/health` + `/metrics` + `/blocks`. Measure real block sizes, rate-limit
behavior, and CDN staleness. Exit criterion: a Fuji instance tracking the tip
and serving the Tier-0 set, with sizing numbers to correct §Sizing.

*As built* (`src/platform/`, plus the multi-chain plumbing in `src/chain.rs`):
one polling loop rather than a live/backfill split — with no push mechanism
there is nothing to split, and final contiguous heights mean a gap is only ever
"not fetched yet". The record is `[blockJSON, blockBytesHex, rewards]` with the
block JSON at element 0, so every chain-blind reader (`oldBlocks`, `/blocks`,
by-hash) works unchanged; element 2 is an empty array until Phase 1, so adding
rewards needs no migration. Ingest refuses any height whose
`sha256(bytes) → CB58` disagrees with the JSON's `id`, counted as
`neve_ingest_rejected_total`. `platform.getTxStatus` was added to Tier 0 for
free (anything stored is `Committed`); `getTx`'s byte encodings answer 421,
since a tx's canonical bytes aren't separately stored. Verified on Fuji from
genesis: 294 heights, every one self-consistent across all four encodings, and
all 186 transactions round-tripping through `getTx` + `getTxStatus`. Two
findings folded back into §Open questions above (the rate limit and the
Apricot `tx` shape); §Sizing still wants real numbers from a deeper fill,
which the rate limit defers to an own-node run.

**Phase 1 — rewards + streaming. Streaming half LANDED 2026-08-10; the rewards
half is the current front.** RewardValidatorTx → commit/abort tracking across
adjacent heights; `getRewardUTXOs` fetch-at-ingest through the join buffer;
serve `getRewardUTXOs` + `getTxStatus`. Port `newBlocks`/`oldBlocks`
subscriptions and `--mirror-from`. Exit criterion: a mirror chain works, and
the first-anywhere P-chain block stream exists.

*Streaming, as built.* `platform.subscribe` serves `newBlocks`/`oldBlocks` plus
two kinds the C-chain never had: `newRecords`/`oldRecords`, carrying the whole
stored record rather than the block. That split exists because a P-chain mirror
fed only block JSON could serve neither the hex encodings nor verify a block ID
— the canonical bytes are element 1 — so `--mirror-from` bootstraps over
`oldRecords` and follows over `newRecords`, re-running the same
`sha256(bytes) == blockID` check on every arriving height. The subscription
machinery moved out of the eth dialect into a shared `src/subscribe.rs` (the
trait extraction §Overlap deferred until a second chain existed) and the
WebSocket transport into `src/upstream.rs`; only frame classification stayed
per-dialect. `newRecords` is refused on the C-chain, whose live path announces a
block before joining its logs — `oldRecords` works there and would close the
logs-mirroring gap when wired up. Verified neve→neve on 294 Fuji heights:
bootstrap in under a second, all 882 encoding responses byte-identical to the
source, every block ID re-derived from the *mirror's own* stored bytes, and all
186 transactions reachable through the mirror's own index.

*Rewards, what's left.* Blocked on confirming the upstream JSON shapes: a
12-height scan near the Fuji tip on 2026-08-10 found only BanffStandard blocks,
no proposal block to sample. Three things are already settled:

- Commit and abort blocks are **indistinguishable in JSON** (both serialize to
  `{height, id, parentID}`), so telling them apart means reading the 4-byte type
  ID at offset 2 of the stored canonical bytes — a fixed-offset discriminant,
  not a codec parser, and another thing the stored bytes pay for. Types:
  Apricot Proposal/Abort/Commit/Standard/Atomic = 0–4; Banff
  Proposal/Abort/Commit/Standard = 29–32. Confirmed against live blocks (genesis
  reads as type 2, height 292000 as type 32).
- The record already reserves element 2 for reward UTXOs and stores `[]` there,
  so turning the feed on needs no migration.
- `join.rs` generalizes to the block↔rewards join, but its halves are currently
  one block blob plus one derived blob; the P record's "block half" is two
  elements, so the buffer needs to carry a record prefix rather than a single
  payload.

**Phase 2 — product indexes (the actual value).** `addr_txs`, UTXO
spent-tracking, the staking join, validator/delegator registries, subnet/L1
registry — extraction inline at ingest, Glacier's converged shapes as the
spec. Serving surface (REST vs. RPC extensions) decided here, informed by who
the consumer is (core-wallet endpoint? explorer? both?). **Keys, values, and the
decisions behind them are worked out in §Phase 2 index design below.**

*Scoped by §Core-wallet coverage (2026-08-10).* The wallet's two XP endpoints
pin the minimum: a **multi-address, txType-filtered, timestamp-bounded, merged
newest-first tx index** (so txType belongs in the key and `pageToken` must
encode a k-way merge cursor) plus the **spent-tracked UTXO set** — which is the
Tier-2 half that has to move *up* into this phase, since `consumedUtxos`/
`emittedUtxos` are embedded in the response shape. The staking join must be
keyed both ways (`stakingTxHash` ↔ `rewardTxHash`), because the wallet reads
realized rewards off the *staking* tx's `emittedUtxos`. Serving is REST on
`/v1/networks/{network}/blockchains/{blockchainId}/…`, a path-dispatch
dimension the serve stack doesn't have yet. Accepted v1 divergences:
`estimatedReward` omitted (needs the Phase-3 calculator) and the
`atomicMemory*` balance buckets best-effort or zero. **Decide X-chain before
these keys are designed** — the shapes are shared and adding a chain
discriminant later is a migration.

### Phase 2 index design (2026-08-11)

Written after sampling real mainnet transactions rather than reasoning from the
schemas. Three findings reshape the earlier sketch; the index set follows from
them.

#### What the tx JSON actually gives you

Sampled `AddPermissionlessValidatorTx` at mainnet height 25345673. The
`unsignedTx` keys are exactly `blockchainID, inputs, memo, networkID, outputs,
rewardsOwner, stake, subnetID, validator`.

**1. There is no `txType` field anywhere.** Not on the transaction, not on the
block. Glacier's `txTypes` filter — the one Core's stake list pages against — is
a *derived* value here, not a copied one. Note this corrects the standing watch
item above, which says extraction "must skip-not-crash on unknown `txType`
strings": there is no such string to skip on.

**2. Inputs name no addresses.** An input is `{txID, outputIndex, assetID,
input.amount}` — a UTXO reference. Only `outputs[].output.addresses`,
`stake[].output.addresses` and `rewardsOwner.addresses` name anyone. So **an
address that funds a transaction appears nowhere in that transaction**, and
attributing a spend to its sender requires resolving each input against the
UTXO that created it.

That makes **UTXO resolution a prerequisite** for `addr_txs`, not a sibling of
it — the concrete form of the §Core-wallet-coverage gap 6 argument that Tier 2's
UTXO half has to move up into Phase 2. The dependency order is
`UTXO resolution` → `addr_txs` → everything Core renders. Resolution turns out
not to need an index of its own (see §Index set), but it does have to happen
first: an `addr_txs` posting for the *funding* address cannot be written until
each input has been resolved to the addresses that own it.

**3. A proposal block carries `txs` *and* `tx`.** Found as a live bug rather
than a design question: `block_txs` returned on the first spelling it saw, so
`txs: []` plus a `RewardValidatorTx` in `tx` — mainnet height 25345668, the
normal proposal-block shape — indexed nothing. Every staking reward on the chain
was missing from `tx_to_block`. Fixed; the tx index space is now `txs` first,
singular `tx` appended.

#### Index set

Keys are big-endian in every range-scanned component, per the ordered-key rule
above. Addresses are the 20-byte bech32 payload, not the `P-avax1…` string.
A UTXO id is its natural key, `txID ‖ outputIndex`.

| # | Keyspace | Key | Value | Serves |
| --- | --- | --- | --- | --- |
| ~~1~~ | ~~`utxo_index`~~ | — | — | **dropped — redundant, see below** |
| 2 | `utxo_spent` | `txID(32) ‖ BE(outIdx u32)` | `BE(height) ‖ BE(txIdx)` | `consumingTxHash`, the unspent set |
| 3 | `addr_utxos` | `addr(20) ‖ txID(32) ‖ BE(outIdx u32)` | ∅ | balances by address |
| 4 | `addr_txs` | `addr(20) ‖ BE(u64::MAX - height) ‖ BE(txIdx u32)` | txType byte | the activity feed, newest-first |
| 5 | `staker_rewards` | `stakerTxID(32)` | `BE(rewardHeight) ‖ outcome` | the staking join |
| 6 | `node_stakes` | `nodeID(20) ‖ BE(start) ‖ txID(32)` | ∅ | validator / delegator registries |

2–4 cover both wallet endpoints; 5 completes Phase 1's rewards half; 6 is the
explorer/marketplace tier and can wait for demand. As in the C-chain design,
`addr_txs` is unique per `(address, tx)`, so an address that both funds a
transaction and receives an output in it collapses to one posting.

**Why `utxo_index` is dropped.** A P-chain UTXO id *is* `txID ‖ outputIndex`, and
`tx_to_block` already maps `txID → (height, txIdx)`. So a UTXO's addresses,
amount and locktime resolve through indexes that already ship:

```text
utxoId → txID → tx_to_block → (height, txIdx) → read block → tx output[outIdx]
```

Exact, deterministic, no new keyspace. The entry existed only because the design
started from Glacier's `PChainUtxo` response shape and carried its fields over
without noticing the key is self-locating. Materialising those fields is a *cache*
decision — worth revisiting if balance queries prove read-bound — not an index
the correctness of anything depends on.

One trap on that read path: a staking transaction numbers its `stake[]` outputs
**continuously after** its `outputs[]`, so resolving `outIdx` has to walk both in
order. Same class of either/or mistake as the `block_txs` bug. UNVERIFIED against
a real staking UTXO; verify before relying on it.

#### Decisions with real alternatives

**Type discrimination** (index 4's value). Options: (a) infer from field shape;
(b) read the 4-byte type ID from the canonical bytes; (c) hybrid. (b) is exact
but needs per-tx offsets, i.e. the codec walk Decision 1 keeps off the critical
path — the block-level trick of reading offset 2 does not generalise, because a
transaction's bytes are nested inside the block's. (a) needs no parser and is
what storing verbatim buys, but cannot cleanly separate `AddValidatorTx` (12)
from `AddPermissionlessValidatorTx` (25), and Helicon's 40–42 will arrive
unannounced.

**Recommendation: (a), with an explicit `Unknown` rather than a guess.** An
unrecognised shape stores a reserved byte and is counted; a rising count is the
signal to revisit, in the same spirit as `neve_ingest_rejected_total`. Guessing
a type is worse than admitting ignorance, because a wrong type silently omits
transactions from a filtered query.

**txType in the key or the value.** In the key (`addr ‖ type ‖ BE(MAX-height)`)
makes Core's two-type stake query a tight scan, but turns the unfiltered
activity feed into a ~20-way merge across type prefixes. In the value costs a
full scan when the filter is selective — and Core's stake query *pages to
exhaustion*, which is the selective case. **Recommendation: value first, measure,
add a secondary `addr_type_txs` only if a real address hurts.** The k-way merge
machinery is already required for multi-address queries (gap 4), so adding type
prefixes later reuses it rather than inventing it.

**Spent-tracking versus write-once.** Every fjall key neve writes today is
written exactly once, which is why `lsm-tree`'s stale-read bug (#315, see the
0.2.1 entry in `CHANGELOG.md`) is inert here. Recording spentness by rewriting a
UTXO's existing row would end that property and put a read-modify-write on the
ingest hot path. **Recommendation: spentness lives in its own write-once
keyspace** (index 2), never as a mutation of a UTXO record — which also holds if
a materialised UTXO cache is ever reintroduced. The bloom pyramid below satisfies
this constraint for free, since its filters are sealed per group and never
updated.

#### Open comparison: posting lists vs. a bloom pyramid

**Unresolved. Do not treat either side as chosen.**

Two of the remaining indexes exist only because nothing in the data points the
way the query needs to go: nothing in a transaction points at whoever later
spent its outputs (`utxo_spent`), and nothing points from an address to its
transactions (`addr_txs`). Those are the two candidates for replacing a posting
list with a **bloom filter used as a locator**: test "might height H touch X",
then read the candidate block and confirm by parsing it.

The first objection to try is that a filter cannot return a value. It does not
apply: the *store* holds the value, and the filter only has to locate it. Nor is
the classic false-positive worry fatal, because the block read **is** the
verification — a false positive costs a wasted read, never a wrong answer. And
the guarantee that matters runs the useful direction: blooms have **no false
negatives**, so "absent from every filter" is a proof, which is exactly what
"this UTXO is unspent" requires.

So the axis is space and latency, not correctness. At ~3 entries per height over
25.3M heights (~76M entries):

| bits/key | FPR | filter size | wasted block reads/query |
| --- | --- | --- | --- |
| 10 | 8.2e-03 | 95 MB | 207,261 |
| 20 | 6.7e-05 | 190 MB | 1,698 |
| 24 | 9.8e-06 | 228 MB | 248 |
| 28 | 1.4e-06 | 266 MB | 36 |

Against ~2.4 GB for full `addr_txs` postings (~1.4 GB truncated). Note the
wasted-read count is `N × FPR` and so is independent of how heights are grouped;
grouping buys *test* speed, not read amplification. Testing 25.3M per-height
filters is ~1–2 s of random memory access, so a viable design is a two-level
pyramid — one filter per ~4096 heights (~6,200 of them) to prune, per-height
filters inside candidate groups only. Roughly doubles the memory; puts a query
around 1–3 ms against ~100 µs for an index scan.

Three things genuinely favour the pyramid:

- **For real hits both designs read the same blocks**, since the response needs
  the block either way. The bloom's *marginal* cost is only the false-positive
  reads and the test time — not the whole scan it first appears to be.
- **Append-only and sealed per group**, so no read-modify-write on the ingest
  path — which was the objection to a mutable `utxo_spent`. Blooms sidestep it.
- **Cheap to retune.** Getting bits/key wrong is normally a migration; here a
  rebuild from the verbatim store is ~15 minutes with no upstream traffic, which
  is precisely the condition under which a probabilistic structure is safe to
  adopt.

Against it:

- **`consumingTxHash` per page.** Glacier embeds it for every consumed UTXO, so
  each one becomes its own forward scan. **This is the decider** — a page of 100
  transactions with several inputs each could mean hundreds of independent scans
  where a posting list is hundreds of point lookups.
- **No aggregates.** A balance is a sum; anything precomputed needs a value.
- **Worst case is a scan** for an address present in most blocks — though a
  posting list degrades similarly, since the reads are real either way.

**How to settle it:** build both for `addr_txs` over a Fuji range, then measure
(a) `consumingTxHash`-per-page latency at `pageSize` 100, (b) resident memory,
(c) p99 for an address with pathological density. The traffic sample in §Open
questions matters here too: if deep history is rarely queried, the pyramid's
latency penalty is paid rarely while its space saving is permanent.

#### Build indexes as a resumable pass, not only an ingest side effect

The store holds the block JSON verbatim, so **every index above can be built by
re-reading the local store with zero upstream traffic** — at the measured
~28,000 heights/s, a full 25.3M-height pass is on the order of 15 minutes. That
is the single most useful property to design around: it makes indexes cheap to
add or rebuild later, and it is the only way to recover the proposal-block
transactions the `block_txs` bug dropped.

It also means **not repeating the `--ingest-logs` mistake**. The C-chain has no
coverage floor in `meta`, so heights stored before the flag was enabled are
indistinguishable from genuine empties — still an open limitation in the 0.2.0
changelog. Stamp a per-index coverage range in `meta` from the first commit, and
answer 421 outside it. An index that is present but incomplete must say so; the
absent-is-not-empty rule applies to indexes exactly as it applies to records.

#### Sizing — the number to get before committing

Unmeasured, and it should not stay that way. The original estimate here —
15–25 GB, larger than the 13 GB block store — was dominated by `utxo_index`
materialising a value per UTXO, and that index is now dropped as redundant. What
remains is postings: order 100M entries at 18–32 bytes of key, so **1.4–2.4 GB**
for `addr_txs` and a similar order for `addr_utxos`/`utxo_spent`. The bloom
pyramid above would put the same coverage at a few hundred MB. Either way it is
now a fraction of the store rather than a multiple of it, which weakens the case
for a flag gate — but measure on Fuji before believing any of these numbers.

**Phase 3 — state replay (Tier 2/3).** UTXO set → `getUTXOs`/`getBalance`;
staking replay → `getCurrentValidators`/`getValidatorsAt`/`getTotalStake`/
supply; fee accumulators → `getFeeState`/`getValidatorFeeState`/L1 balances.
Per Decision 6: snapshot-seeded at an Etna-era floor, outcomes read from the
chain rather than re-derived, and differentially tested against upstream
(`getValidatorsAt` diffs per height; computed rewards must reproduce Phase-1
fetched reward UTXOs bit-for-bit). Gate this phase on demand evidence (the
traffic sample), not on completionism.

**Standing watch items.** Helicon tx types 40–42 activate on Fuji 2026-07-28 —
the `[bytes, json]` store absorbs them with zero code, which is exactly why
Decision 1(c) matters; extraction code must skip-not-crash on unknown
transaction shapes. ~~unknown `txType` strings~~ **corrected 2026-08-11: there is
no `txType` field in the JSON at all** (see §Phase 2 index design), so a new type
presents as an unfamiliar *field shape* rather than an unfamiliar string — which
is why type discrimination stores an explicit `Unknown` instead of guessing.
Granite epochs and any future proposervm changes only matter if own-node
container ingestion (2b) is picked up.

## Open questions

- **Traffic sample**: get a `platform.*` method breakdown from the api-worker
  (analog of the 9.5M-call C-chain sample). This decides how much of Tier 2/3
  is worth building and should precede Phase 2 scoping.
- ~~**Public-endpoint limits**: sustained-backfill behavior at ~25 req/s~~
  **ANSWERED 2026-08-10 (Phase 0)** — and the answer is much harsher than the
  C-chain's. `api.avax-test.network` answered a sustained **~14 req/s** of
  `platform.getBlockByHeight` with HTTP 429 and **`Retry-After: 3600`** after
  roughly 200 heights (~30s of backfill). Two consequences:
  - The limit is **per-IP for the whole host, not per chain path**: while
    throttled, `/ext/bc/C/rpc` returned 429 too. A hard P-chain backfill will
    take a co-located C-chain instance down with it.
  - Each height costs **two** requests (`hexnc` + `json`), so pacing must be
    per-request; pacing per height silently doubles the real rate. Hence
    `--p-request-interval` (default 200ms ≈ 5 req/s) rather than reusing the
    C-chain's 40ms.

  Full history from the public endpoint is therefore impractical — at 5 req/s,
  25.3M mainnet heights is years. Deep backfill needs an own node or a neve
  mirror (`--p-request-interval 0`), which §Sizing already assumed; what's new
  is that this is a hard constraint rather than a politeness preference. Still
  open: does `getRewardUTXOs` return correct data for arbitrarily old stakers
  through the CDN?
- **`getValidatorsAt` retention depth**: how far back the public endpoint
  answers it (validator-diff pruning behavior UNVERIFIED) — determines the
  differential-testing oracle's coverage for backfill replay (Decision 6).
- ~~**JSON fidelity**: confirm avalanchego's `getTx` JSON is byte-identical to
  the tx as embedded in `getBlock` JSON~~ **ANSWERED 2026-08-10 (Phase 0):
  yes.** `platform.getTx(txID, json).tx` is structurally identical to
  `getBlock(json).txs[i]` (checked on Fuji height 292000), so `getTx` is served
  by slicing the stored block JSON and no per-tx JSON needs storing. Still open:
  which consumers (avalanchejs, explorers) are sensitive to field order in
  `json`-encoding responses — verbatim storage moots it for `getBlock`.

  One shape trap found while implementing: **Apricot-era blocks carry a single
  `tx` object, not a `txs` array**, and commit/abort blocks carry neither. On
  Fuji's first 294 heights the census is 185 singular `tx`, 108 with neither,
  and 1 `txs` array — so reading only `txs` silently fails to index ~all
  pre-Banff transactions. Any extraction must handle both spellings.
- ~~**Who consumes Phase 2**~~ **ANSWERED for the wallet, 2026-08-10** — see
  §Core-wallet coverage: exactly two Glacier REST endpoints
  (`listLatestPrimaryNetworkTransactions`, `getBalancesByAddresses`), both
  serving P *and* X. Still open for the explorer and for api-worker offload,
  and the traffic sample above is still the way to size Tier 2/3 beyond the
  wallet.
- **X-chain: in or out?** The wallet's two endpoints are `blockchainId`-
  parameterized and Core renders X alongside P, so a P-only neve leaves the
  wallet half-served. Blocks Phase-2 key design (§Core-wallet coverage gap 1).
  Open sub-questions: does the post-Cortina linearized X-chain map onto the
  height-keyed blockstore, and is the pre-linearization vertex era in scope at
  all?
- **Atomic-UTXO reconstruction**: can exports-on-C/X minus imports-on-P
  reproduce the `atomicMemory*` balance buckets closely enough to be worth
  shipping, or is a documented divergence better? (§Core-wallet coverage
  gap 2.) Needs a diff against Glacier on real accounts with in-flight
  cross-chain transfers.
- **Naming/dispatch**: does a P-chain neve serve at `/ext/bc/P` on `:8545` so
  an api-worker can front it path-transparently? (Presumably yes — confirm
  worker routing assumptions.) Note this is a *separate* surface from the
  Glacier-shaped `/v1/...` REST the wallet uses; both would have to coexist on
  the one socket, which today routes only by JSON-RPC method namespace.

## Source-of-truth pointers

- avalanchego: `vms/platformvm/service.go` (method set),
  `vms/platformvm/block/codec.go` + `txs/codec.go` (type registry),
  `vms/platformvm/txs/reward_validator_tx.go` + `txs/executor/proposal_tx_executor.go`
  (reward mechanics), `vms/proposervm/block/codec.go` (wrapper),
  `indexer/examples/p-chain/main.go` (container parsing pattern),
  `genesis/aliases.go` (endpoint aliases).
- Docs: build.avax.network → API reference → P-chain API / Index API /
  P-chain tx format; developers.avacloud.io (Data API primary-network
  endpoints — the market-standard index shapes).
- Reference implementations: `ava-labs/avalanche-rosetta` `mapper/pchain/`
  (Go P-chain tx parsing + UTXO semantics), `ava-labs/avalanche-indexer`
  (new first-party OSS indexer, EVM-only today), ortelius migrations
  (dead, but its bolted-on address/reward indexes are a cautionary tale:
  design them in from Phase 2 day one).
- neve internals referenced above: `record.rs:31` (byte-identical record
  concat), `storage.rs:23` (format gate), `subscribe.rs:517` (`fetch_rpc`),
  `subscribe.rs:660` (reorg best-effort check), `join.rs` (two-half join),
  `middleware.rs` (421 contract).
