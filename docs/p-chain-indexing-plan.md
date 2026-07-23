# P-Chain Indexing — Research & Plan

Working research doc (like `neve-logs-ingestion-plan.md`): findings first, then a
phased plan. Started 2026-07-23. Nothing here is implemented; the question under
study is **"can neve be changed to index the Avalanche P-chain instead of the
C-chain?"** — and if so, what it could serve and what carries over.

External facts below were verified against avalanchego v1.14.2 / master source,
build.avax.network docs, developers.avacloud.io, and live probes of
`api.avax.network` on 2026-07-23. Items that could not be verified are marked
UNVERIFIED.

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
| `platform.getUTXOs` | P-chain-local UTXOs only. **`sourceChain=X/C` is unservable** — atomic UTXOs live in shared memory, deposited by X/C-chain exports, invisible in P-chain blocks → 421 |
| `platform.getBalance` (deprecated) | sum of the replayed set, bucketed by locktime/stakeable-lock |
| `platform.getStake` (deprecated) | from the staking subset |

**Tier 3 — staking/fee replay** (validator lifecycle + reward calculator +
fee accumulators; the most VM-like logic, still far short of an EVM):

| Method | Notes |
| --- | --- |
| `platform.getCurrentValidators` | full staking tables replayable **except `uptime`/`connected`** — those are the *queried node's* local observations, fundamentally unservable by any indexer (omit/null; primary-network-only fields anyway) |
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

## Sizing & cadence (to be measured in Phase 0)

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

**Phase 0 — spike (prove the shape).** Fetch loop against
`api.avax.network/ext/bc/P`: `getHeight` poll + `getBlockByHeight` in both
encodings; `[bytes, json]` record behind a new format version; blockID
verification at ingest; `hash_to_height` + `tx_to_block` extraction from JSON;
serve `getBlock`/`getBlockByHeight`/`getHeight`/`getTx`/`getTimestamp` +
`/health` + `/metrics` + `/blocks`. Measure real block sizes, rate-limit
behavior, and CDN staleness. Exit criterion: a Fuji instance tracking the tip
and serving the Tier-0 set, with sizing numbers to correct §Sizing.

**Phase 1 — rewards + streaming.** RewardValidatorTx → commit/abort tracking
across adjacent heights; `getRewardUTXOs` fetch-at-ingest through the join
buffer; serve `getRewardUTXOs` + `getTxStatus`. Port `newBlocks`/`oldBlocks`
subscriptions and `--mirror-from`. Exit criterion: a mirror chain works, and
the first-anywhere P-chain block stream exists.

**Phase 2 — product indexes (the actual value).** `addr_txs`, UTXO
spent-tracking, the staking join, validator/delegator registries, subnet/L1
registry — extraction inline at ingest, Glacier's converged shapes as the
spec. Serving surface (REST vs. RPC extensions) decided here, informed by who
the consumer is (core-wallet endpoint? explorer? both?).

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
`txType` strings. Granite epochs and any future proposervm changes only
matter if own-node container ingestion (2b) is picked up.

## Open questions

- **Traffic sample**: get a `platform.*` method breakdown from the api-worker
  (analog of the 9.5M-call C-chain sample). This decides how much of Tier 2/3
  is worth building and should precede Phase 2 scoping.
- **Public-endpoint limits**: sustained-backfill behavior at ~25 req/s
  (weighted limiting suspected via `x-execution-weight`, thresholds
  UNVERIFIED); does `getRewardUTXOs` return correct data for arbitrarily old
  stakers through the CDN?
- **`getValidatorsAt` retention depth**: how far back the public endpoint
  answers it (validator-diff pruning behavior UNVERIFIED) — determines the
  differential-testing oracle's coverage for backfill replay (Decision 6).
- **JSON fidelity**: which consumers (avalanchejs, explorers) are sensitive to
  field order/shape in `json`-encoding responses? Verbatim storage moots it
  for `getBlock`; `getTx` needs the per-tx zero-copy slice out of block JSON —
  confirm avalanchego's `getTx` JSON is byte-identical to the tx as embedded
  in `getBlock` JSON, or store per-tx JSON separately.
- **Who consumes Phase 2**: core-wallet's P-chain views (Glacier-shaped
  REST?), the explorer, or api-worker offload? Shapes the serving surface.
- **Naming/dispatch**: does a P-chain neve serve at `/ext/bc/P` on `:8545` so
  an api-worker can front it path-transparently? (Presumably yes — confirm
  worker routing assumptions.)

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
