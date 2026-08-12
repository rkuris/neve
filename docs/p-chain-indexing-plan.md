# P-Chain Indexing — Research & Plan

Working research doc (like `neve-logs-ingestion-plan.md`): findings first, then a
phased plan. Started 2026-07-23 to study **"can neve be changed to index the
Avalanche P-chain instead of the C-chain?"** — and if so, what it could serve and
what carries over.

The answer turned out to be "yes, and it needn't be *instead*": neve now mirrors
the C-chain, the P-chain, or both from one process. **Phase 0 and the streaming
half of Phase 1 shipped 2026-08-10** (neve 0.2.0). See §Status for what exists
today, and §Non-starters at the end for approaches that were evaluated and
rejected, with the reasoning that killed each one.

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
  method (`getTxStatus`) — see §Core-wallet coverage. Phase 2's **design is
  closed** (§Phase 2 index design and its step order); nothing is implemented, and
  only `meta`, `hash_to_height` and `tx_to_block` exist in the store.
- **X-chain** — not in `--chains` at all, and the wallet needs it alongside P
  (§Core-wallet coverage gap 1).
- `platform.subscribe("newHeads")` — deliberately absent; a P-chain block has no
  header/body split, so the geth-shaped kind would be a lie.
- `getTx` byte encodings — a tx's canonical bytes aren't separately stored, and
  slicing them out of the block's bytes needs the codec parser Decision 1 avoids.
- `--p-mirror-from` — `--mirror-from` is global, so a P-chain-only mirror while
  the C-chain ingests elsewhere isn't expressible yet.

### Run book — bringing P-chain up on the production instance

Verified against the live hosts 2026-08-11. Production is `ssh neve`, store at
`/var/lib/neve/blockstore-data-mainnet`, serving C-chain only because `--chains`
defaults to `c`.

**The approach: shallow backfill straight from the public endpoint.** No local
store build, no file transfer, no own avalanchego node — the P-chain is enabled
in place with a floor a short way below the tip, and it fills itself in minutes.
Deep history is explicitly not a day-one goal; §Non-starters records why the
build-locally-and-copy route was dropped.

#### Preflight

1. **Deploy current `main` first.** Production runs 0.2.1 (`aa5f796`), which
   predates the current P-chain extraction rules, so enabling `--chains c,p`
   there would write an index that has to be rebuilt.
   `sudo bash /opt/neve/deploy/update.sh`.

2. **Wait for the C-chain backfill to finish — roughly 22 hours out.** This is
   the one hard ordering constraint, and it is about the rate limit rather than
   CPU. A P-chain rate-limit trip **blocks every POST to `api.avax.network`**, not
   just the P-chain path (§Rate limits explains the mechanism), so a 429 with
   `Retry-After: 3600` would stall the C-chain's ingest for an hour too. Check
   before starting:

   ```sh
   curl -s localhost:8545/health | jq '.chains.c.blocks'   # behind should be ~0
   ```

3. **Pick the floor — recommended: 1,000,000 heights below the tip.**

   ⚠️ **The floor is baked in at store creation and cannot be lowered later
   without starting the store over.** That is the only irreversible decision in
   this run book, so it is worth costing out properly rather than picking a round
   number.

#### What a floor costs, measured

Measured against `api.avax.network/ext/bc/P` on 2026-08-11 from tip 25,349,360.
Two requests per height (`hexnc` + `json`); **~3.7 KB of wire traffic per height**
including HTTP/CDN headers, which dominate for the many small blocks. Disk uses the
measured ~520 B/height, which is a floor.

Fill time is set by the edge rate limit (§Rate limits, read out of the Terraform
config rather than guessed). Ingesting via the `/ext/bc/platform` alias leaves only
the weight-based rule, whose cap is 100 requests per 10 s ⇒ 10 req/s; at a 20%
margin that is **8 req/s, `--p-request-interval 125ms`**, and since `--p-concurrency`
overlaps round-trips while the pacer meters every individual request, a height
costs 250 ms:

| Heights | Fill @8 req/s | Wire | Disk | History bought |
| --- | --- | --- | --- | --- |
| 10 k | 42 min | 37 MB | 5 MB | 2.2 days |
| 100 k | 6.9 h | 369 MB | 52 MB | 20.4 days |
| **1 M** | **2.9 days** | **3.7 GB** | **0.5 GB** | **6.5 months** |
| 3 M | 8.7 days | 11.1 GB | 1.6 GB | 11.5 months |
| 25.3 M (all) | 73 days | 93 GB | 13 GB | 5.9 years |

For comparison, on the un-aliased `/ext/bc/P` path the cap is 50 requests per
60 seconds, which puts 1 M heights at ~35 days and full history in years — a 12×
difference that comes entirely from which rule applies.

**The wire column is informational, not a cost.** This fill runs *on production*,
which is in AWS, and AWS does not charge for inbound transfer — so the download
volume is free and unmetered. There is no monthly transfer budget on this path.
(The metered link is the local one, and it only matters for the rejected
build-locally-and-copy route — §Non-starters.)

The history column is measured from real block timestamps, not modelled, and it is
the whole reason to think in days rather than heights:

| Offset from tip | Span | Mean cadence |
| --- | --- | --- |
| 10 k | 2.2 days | 18.8 s/block |
| 100 k | 20.4 days | 17.6 s/block |
| 1 M | 196.6 days | 17.0 s/block |
| 2 M | 265.2 days | 11.5 s/block |
| 3 M | 350.3 days | 10.1 s/block |
| 5 M | 592.5 days | 10.2 s/block |

**Recent cadence is ~17–19 s/block, not the ~7 s long-run average** — the chain
has slowed, so *recent* history is unusually cheap in blocks-per-day and there are
sharply diminishing returns to going deeper. The first million heights buy 6.5
months; the second buys only 2.3 more, and the third only 2.8 more. That knee is
what makes 1 M the right floor: 3× the fill time past it returns under 2× the
history.

**Elapsed fill time is the only real cost.** Transfer volume is free (AWS inbound)
and disk is ample below ~3 M heights, so the choice is purely how many days of
unattended trickle to spend.

**Recommended floor: 1 M** — 2.9 days of fill for 6.5 months of history. That is the
knee: the first million heights buy 6.5 months, the second only 2.3 more, and going
to 3 M triples the time for under double the coverage. 10 k and 100 k are too
shallow to be interesting (2 days and 3 weeks) now that depth is affordable, and
the floor cannot be lowered afterwards.

Once a bypass token is in place (§Rate limits) the pacer can be relaxed further and
even deeper floors become practical, but 1 M does not need to wait for it.

##### Rate limits — the actual configured numbers

These do not need to be guessed or probed. They are declared in Terraform, in
`terraform/cloudflare/<zone>/rate_limits/default/terragrunt.hcl`, with defaults from
`terraform-modules/cloudflare/rate_limit_ruleset`. For `api.avax.network`, **three
rules can bind a P-chain client**:

| Rule | Counts | Limit | Effective |
| --- | --- | --- | --- |
| `Protect P-Chain API` | responses tagged `x-execution-weight: pchain` | 100 / 10 s | 10 req/s |
| `Protect P-Chain API Expensive` | responses tagged `pchain-expensive` | 1 / 10 s | 0.1 req/s |
| **`P-Chain API`** | **all requests to `/ext/bc/P`** | **50 / 60 s** | **0.83 req/s** |

The third is the one that matters and is easy to miss, because it is keyed on the
**path** rather than on the execution weight — so it counts every request
regardless of how cheap the method is. There is an identical `P-Chain API (ALT)`
rule for `/ext/P`. **0.83 req/s is therefore the real mainnet ceiling**, twelve
times tighter than the weight-based rule everyone looks at first.

Module defaults fill in the rest: `mitigation_timeout = 3600` (exactly the
`Retry-After: 3600` seen in practice) and `characteristics = ["ip.src",
"cf.colo.id"]`, so counting is **per source IP per Cloudflare colo**.

Two consequences worth stating plainly:

- **`--p-request-interval` must be ≥ 1200 ms on mainnet.** The 200 ms default is
  5 req/s — six times over the cap — and would trip the limiter after ~50 requests,
  i.e. within about ten seconds of starting. 1500 ms leaves a 20% margin.
- **This is why a P-chain backfill takes the C-chain down with it, precisely.** The
  weight-based rule's *match* expression is any POST to `api.avax.network`, while
  only its *counting* expression is P-chain-specific. So the counter fills from
  P-chain traffic but the mitigation, once triggered, applies to the whole match
  set — every POST to the host, `/ext/bc/C/rpc` included. Counting is per-chain;
  blocking is host-wide.

**Fuji is not a proxy for mainnet here.** `api.avax-test.network` has the two
weight-based rules (both 100 / 10 s) and **no path-based rule at all**, so its
ceiling is 10 req/s. That also explains the one empirical datapoint on record: a
sustained ~14 req/s of `platform.getBlockByHeight` against Fuji drew a 429 because
it exceeded 10 req/s. Consistent, and a reminder that a Fuji-derived rate is ~12×
too fast for mainnet.

##### The browser user-agent does nothing on production

neve sends a Chrome user-agent on every upstream request and WS handshake
(`BROWSER_UA`, `src/upstream.rs:113`), added to qualify for Cloudflare's
`Human Rate Limit Bypass` rule — a `skip` in the `http_ratelimit` phase that, when
it matches, disables **all** rate limiting for `api.avax.network`. Reading the rule
against where neve actually runs shows it cannot help where it matters:

- **The rule excludes 39 datacenter ASNs, including AWS (16509).** Production is in
  AWS, so the bypass can never apply there **regardless of user-agent**. On
  production the UA buys exactly nothing and the full P-chain limits apply.
- **Starlink (14593) is not on the exclusion list**, so a *local* run plausibly does
  qualify — which is why local fetching can feel unthrottled while production is
  not. Local behaviour is not evidence about production.
- **Fuji has no `Human Rate Limit Bypass` rule at all.** That explains the 429 at
  ~14 req/s recorded against `api.avax-test.network`: no bypass existed to catch it,
  so the 10 req/s `pchain` cap simply applied.
- **The TLS fingerprint is not impersonated** — JA3 comes from rustls, and the rule
  denies 16 specific JA3 hashes plus requiring `cf.bot_management.score gt 38`.
  rustls is not on today's denylist, but that is luck rather than design: adding it,
  or retuning the bot score, silently removes the bypass.

**Recommendation: drop `BROWSER_UA` once P-chain block reads are reclassified.** It
is ineffective in production, fragile everywhere else, and it misrepresents the
client to a WAF operated by the same organisation that operates neve. Send an honest
descriptive agent (`neve/<version>`) instead. Until the reclassification lands the
UA is harmless, but it should not be mistaken for part of the rate-limit strategy.

##### Why the C-chain backfill is never throttled

Worth understanding before asking for anything, because it explains the asymmetry
and points at the cleanest fix. The C-chain fill sustains ~25 req/s indefinitely
not through any client-side cleverness but because of how its method is
*classified*: `eth_getBlockByNumber` is tagged `weight: 'cheap'`
(`api-worker/src/config/evm.ts`), and **no rate-limit rule on `api.avax.network`
counts the `cheap` class at all**. The rules count only `errored`, `expensive`,
`expired`, `large`, `logs`, `xxl`, `pchain`, and `pchain-expensive`. So C-chain
block reads are structurally uncounted.

`platform.getBlockByHeight` is the same *kind* of operation — a flat on-disk read,
cached at the edge for a year, cheap at any depth — but it is tagged `pchain`,
which is counted twice over (weight rule *and* path rule). The 12× gap between the
two chains is a classification artifact, not a difference in cost to serve.

That suggests the fix with the best argument behind it: **reclassify P-chain block
reads into an uncounted or high-limit class**, on the grounds that
`platform.getBlockByHeight` and `eth_getBlockByNumber` are the same workload. It is
a policy change with blast radius beyond neve — it would uncap P-chain block reads
for every caller — so it is infra's call, not ours. But it is the honest version of
the request, and it is worth putting alongside the token ask.

##### The real unlock: a rate-limit bypass token

The public endpoint already has an established exemption mechanism, and neve is a
reasonable candidate for it. `terraform/cloudflare/avax.network/waf/default/terragrunt.hcl`
carries a set of `action = "skip"` rules in the `http_ratelimit` phase, described as
`Public API RL Bypass (<consumer>)` — currently including Coinbase, Ava Labs FinOps,
the Data Platform, the Bridge, and others. Each matches
`http.host eq "api.avax.network" and any(http.request.uri.args["token"][*] == "<secret>")`,
i.e. **a `?token=…` query argument** that skips rate limiting entirely.

Adding one more such rule for neve is a change to a repository we already own, not
a favour to negotiate. With a token, `--p-rpc-url` becomes
`https://api.avax.network/ext/bc/P?token=<token>`, the 50/60 s cap stops applying,
and fill rate is bounded by round-trip latency and `--p-concurrency` instead — which
puts 1 M heights in hours rather than weeks and makes deep history a real option.

**Do not probe for the threshold instead.** The numbers above are authoritative, and
the cost of discovering them empirically from production is exactly the hour-long,
host-wide outage the limits are there to cause.

##### The alias gap — what the run book currently relies on

The two path rules match `api.avax.network/ext/bc/p` and `api.avax.network/ext/p`.
But avalanchego serves the P-chain on more aliases than that —
`PChainAliases = []string{"P", "platform"}` plus the blockchain ID
(`genesis/aliases.go`), so `/ext/bc/platform`, `/ext/platform`, and
`/ext/bc/11111111111111111111111111111111LpoYY` all answer the same JSON-RPC and
**none is covered by the 50/60 s rule**. Requests through an alias are counted only
by the weight rule: 10 req/s rather than 0.83.

The run book uses `/ext/bc/platform` for the initial fill, which is what makes 1 M
heights a 2.9-day job instead of a 35-day one. **Treat it as a stopgap with two
strings attached:**

- **It is a coverage bug in the rule set, and the fix is ours to make.** The rule is
  named "P-Chain API" and clearly intends to cover P-chain traffic; the aliases were
  missed. Raise it with the infra team and land the rule-set fix — the same
  conversation as the token request.
- **It can vanish without warning.** One WAF commit closing the gap drops the
  effective rate from 10 req/s to 0.83 mid-fill. That is not a data-integrity
  problem (the fill just slows, and 429s are retried), but a 2.9-day job silently
  becomes a 35-day one. Watch `neve_upstream_requests_total{outcome="throttled"}`
  and re-check the pacer if it starts climbing.

Which is the argument for doing both asks below *now* rather than after the fill:
with a token or a corrected weight class, the alias stops mattering and the run book
can go back to the canonical `/ext/bc/P`.

One further lever if fill time still matters: each height costs **two** requests
(`hexnc` + `json`). Dropping to one would halve the wall clock, but gives up either
the hex encodings or the `sha256(bytes) == blockID` check — a record-format decision
(§Decision 1), not a tuning knob.

Steady state after the fill is negligible: ~0.06 blocks/s at current cadence
⇒ ~0.12 req/s and well under 1 GB/month.

#### Turn it on

Get the current tip and compute the floor:

```sh
curl -s -X POST -H 'content-type:application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"platform.getHeight","params":{}}' \
  https://api.avax.network/ext/bc/platform
```

Then edit `/etc/neve/neve.env`, substituting the floor (tip − 1,000,000):

```text
NEVE_ARGS=--summary-period 1m --rpc-addr 0.0.0.0:8545 --chains c,p \
  --p-rpc-url https://api.avax.network/ext/bc/platform \
  --p-backfill-floor <tip-1000000> \
  --p-request-interval 125ms --p-poll-interval 10s
```

Note the URL is the **`/ext/bc/platform` alias**, not `/ext/bc/P`. Both serve the
identical JSON-RPC (`PChainAliases = []string{"P", "platform"}` in avalanchego's
`genesis/aliases.go`), but only `/ext/bc/P` is covered by the tight path-based rate
limit — §Rate limits records why this is a stopgap rather than the resting state.

and `sudo systemctl restart neve`. The `p/` store directory is created on first
start; nothing needs to be staged.

- ⚠️ **The P endpoint must be reachable before this restart.** An unreachable P
  upstream aborts startup *before* the RPC server binds, taking C-chain serving
  down with it. Verified — this is why the `getHeight` probe above is not
  optional.
- `--p-request-interval 1500ms` (40 req/min) sits at 80% of the configured
  50-requests-per-60-seconds cap on `/ext/bc/P`; the 200 ms default is six times
  over it and would be throttled within seconds. `--p-poll-interval 10s` keeps
  steady-state P traffic to ~0.3 req/s.

#### Verify

```sh
curl -s localhost:8545/health | jq '.default_chain, .chains | keys'
curl -s -X POST -H 'content-type:application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"platform.getHeight","params":{}}' \
  localhost:8545
```

Then confirm the P-chain is advancing, not merely served — `getHeight` answers
from a filled store even if the tip poller is wedged:

```sh
curl -s localhost:8545/health | jq '.chains.p.blocks'   # behind should shrink to 0
sudo journalctl -u neve -n 20 | grep 'chain="p"'        # summary lines
```

A height below the floor must answer 421, not an error — that is the coverage
contract working.

#### Rollback

Drop the `--chains c,p` and `--p-*` flags from `NEVE_ARGS` and restart. The `p/`
directory is inert when the P-chain isn't selected, so it can stay; deleting it
is only necessary if the floor is being changed. The `main` deploy from preflight
step 1 is not rolled back by this and does not need to be — it is C-chain-safe on
its own.

### Implementation notes worth carrying forward

- **`platform.*` is registered by hand**, not via jsonrpsee's `#[rpc]` macro: the
  macro derives JSON keys from Rust parameter names, so `blockID`/`txID` would
  need non-snake-case identifiers, and it cannot accept avalanchego's
  number-or-string integers.
- **A block's transactions live under two field names, and they are not
  alternatives.** Apricot-era blocks carry a single `tx` object rather than a
  `txs` array (commit/abort blocks carry neither): on Fuji's first 294 heights the
  census is 185 singular, 108 none, 1 array, so reading only `txs` misses
  essentially the whole pre-Banff chain. A Banff *proposal* block carries
  **both** — standard transactions in `txs`, the proposal transaction (a
  `RewardValidatorTx`) in `tx` — normally with `txs: []`, which is present enough
  to short-circuit an either/or reader; mainnet height 25345668 is that shape.
  Read both, `txs` first and singular `tx` appended, and treat that as the
  transaction index space. Note a Fuji genesis range contains no proposal block at
  all, so it cannot validate this on its own.
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
  TransformSubnetTx (24). **Helicon (ACP-236) adds types 40–42** —
  `AddAutoRenewedValidatorTx` (40), `SetAutoRenewedValidatorConfigTx` (41),
  `RewardAutoRenewedValidatorTx` (42). Live on Fuji since 2026-07-28; mainnet
  activation is being scheduled. See §ACP-236 auto-renewed staking, which is where
  the reward model changes.
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
| `platform.getCurrentValidators` | full staking tables replayable **except `uptime`/`connected`** — those are the *queried node's* local observations, unservable by any indexer (omit/null; primary-network-only fields anyway). ⚠️ core-wallet *filters and sorts its node picker on exactly those two fields*, so the node-picker specifically cannot run off neve however good the replay is — §gap 8, §Non-starters |
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
   Same fjall shape as the C-chain `addr_txs` index, different extraction.
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
historical time series. Everything in 1–6 is extraction + fjall keys, no
execution, with one exception: `estimatedReward` on staking txs needs a reward
calculation, though only *current supply* is a state input and that can be
proxied upstream (§gap 7).

These product indexes ride the same pipeline as the RPC tiers: extract at
ingest while the record is in hand, exactly like the planned C-chain history
indexes. Serving them (Glacier-shaped REST vs. custom JSON-RPC methods) is a
separate, deferrable decision.

## Core-wallet coverage (the demand evidence)

From reading `core-mobile` and the published `@avalabs/avalanche-module` bundle.
This is the demand evidence for Phase 2, and it reorders the phases: the wallet's
XP needs are *narrower* than Tier 2/3 but *deeper* than Tier 0/1, and they cut
across the tiering diagonally.

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
   /`listLatestXChainVertices` surface would not — §Non-starters. UNVERIFIED.)

   **X does not block Phase 2's keys**, though. neve runs **one store per chain**,
   stamped with chain identity and verified on open (`src/storage.rs`), so the
   store *is* the chain discriminant and no key carries one. X-chain is a real
   scope question — AVM tx shapes mean different extraction, which is most of the
   work — but it is not a key-format hazard, so Phase 2 keys can be cut for P now
   without foreclosing it.

2. **`atomicMemoryUnlocked` / `atomicMemoryLocked` are structurally
   unservable.** Two of the eight balance buckets are shared-memory atomic
   UTXOs, which §Tier 2 already flags as invisible in P-chain blocks
   (`getUTXOs` with `sourceChain` → 421). So even a complete UTXO replay cannot
   reproduce the balance response faithfully.

   Being multi-chain opens a route in principle — the *export* side of an atomic
   transfer is a plain tx on C or X and the *import* side is a plain tx on P, so
   pending atomic UTXOs are (exports-to-P seen on C/X) minus (imports seen on P).
   But that is a bigger job than it looks and does not land in v1; the reasoning
   is in §Non-starters. `atomicMemory*` is a declared divergence.

3. **Balances are the hot path, and the plan files them under Phase 3 "gate on
   demand evidence."** This section is the evidence. `getBalancesByAddresses`
   is called on every account refresh; `listLatestPrimaryNetworkTransactions`
   only when a view opens.

4. **Both endpoints take a CSV address *list*.** Core passes its whole XP
   address set (internal + external BIP44 chains, `getCachedXPAddresses`) and
   expects one merged, paged, newest-first result. The `addr_txs` prefix scan
   design is single-address; serving this needs a k-way merge across k prefix
   cursors, and k grows with the wallet.

   **The `pageToken` stays simple, though.** The merge key
   `(u64::MAX - height, txIdx)` is a **global** ordering, so a single
   `(height, txIdx)` cursor re-seeks all k prefix scans correctly and the token is
   a fixed 12 bytes regardless of k — no per-cursor state. It also makes
   cross-address dedup free: a transaction touching two of the wallet's addresses
   produces identical merge keys in two scans and collapses on comparison, with no
   seen-set and no page-boundary special case. The merge is ~30 lines.

5. **Server-side `txTypes` + `startTimestamp` filtering is load-bearing, not a
   nicety.** EarnService pages to exhaustion with a two-type filter; a plain
   address scan would over-fetch and break `pageSize` semantics. So txType has to
   be *somewhere* — in the key, in the value, or in a companion index — otherwise
   a delegator query against a busy address scans its whole history. §Decisions
   with real alternatives settles where: the value first, with a secondary index
   only if a real address hurts.

6. **Phase 2 needs the spent-tracked UTXO set, which the original tiering filed
   under Phase 3.** `PChainTransaction` embeds full
   `consumedUtxos`/`emittedUtxos` as `PChainUtxo` objects carrying
   `consumingTxHash`, `consumingBlockNumber`/`Timestamp`, `staked`, `rewardType`,
   `platformLocktime`, `stakeableLocktime`, `utxoStartTimestamp`/`EndTimestamp`.
   Consumed UTXOs must resolve back to their *creating* tx for amount/addresses/
   asset. So "Phase 2 = extraction + fjall keys, no execution" understates it:
   even the activity feed needs the persistent, spent-tracked UTXO set that
   §Beyond-node-parity lists as index 2. That half of Tier 2 graduates into
   Phase 2; only the staking/fee replay stays gated.

7. **The two reward fields the stake UI reads are the two hardest.**
   - `estimatedReward` is rendered on every pending stake card
     (`new/features/stake/utils/index.ts:31,110`). **Probably not Tier 3,
     though.** The estimate is a pure function of stake amount, duration and
     current supply, and the only *state* input is supply — which can be
     **proxied upstream** via `platform.getCurrentSupply` (a single cheap call,
     cacheable for minutes) with the formula evaluated locally. That renders the
     stake cards without any validator-set reconstruction. Try this before
     accepting the divergence; if the formula fails to reproduce Glacier's
     number, fall back to omitting the field.
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
   calls unservable by any indexer, because they are the queried node's local
   observations. So **the delegation node-picker cannot run off neve at any
   fidelity**, however good the validator-set replay gets.

   Note the scope of that claim, because it is easy to overstate: it is about
   those two *fields*, not about `getCurrentValidators` as a whole. The validator
   set itself — membership, weights, expiry — is reconstructible, and §Decision 6
   is about exactly that. §Non-starters records why the two fields never will be.

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
- Accept three explicit v1 divergences (omit / best-effort), the same way the
  C-chain plan accepts the failed-logless-tx leak:
  - the `atomicMemory*` balance buckets;
  - `estimatedReward`, unless the supply-proxy route in gap 7 works;
  - **compounded rewards for ACP-236 auto-renewed validators**, which emit no
    UTXO and so are invisible to any extraction-based index. Recoverable as a
    bounded estimate by inverting the withdrawn portion, except at 100%
    compounding, and it falls on validator operators rather than on the
    delegation flow Core queries. Mainnet activation is being scheduled, so this
    is a production divergence, not a testnet one.
- X-chain can be decided *after* Phase 2's keys, since one store per chain means
  no key carries a chain discriminant (gap 1).

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
  the L1 lifecycle. **Helicon (ACP-236) added auto-renewal**, which is the first
  boundary that makes a validator's *weight and expiry* evolve after the adding
  transaction rather than being fixed by it — see below and §ACP-236. Each
  boundary is a network-specific activation timestamp to gate on.
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
  what stake/fee — is what Core shows, and for pre-Helicon stakers Core derives
  it client-side from `startTimestamp`/`endTimestamp` on staking txs. That is
  Phase-2 material: timestamp math over the tx index; errors are cosmetic. The
  *consensus* bar is `getValidatorsAt`: exact weights + BLS keys at a height,
  consumed by ICM/warp relayers to verify signature quorums — a slightly wrong
  weight breaks downstream verification. That needs the real replay and stays in
  Phase 3 behind the demand gate. Don't let the hard version block the
  useful version.

  **ACP-236 moves part of the product bar into Phase 3**, because auto-renewal
  ends the assumption that a staker's parameters are fixed by its adding
  transaction. An `AddAutoRenewedValidatorTx` (type 40) has no
  `startTimestamp`/`endTimestamp` at all — only a `period` — so the client-side
  derivation has nothing to read. Splitting it finer:

  - **Set membership and expiry stay on the product bar.** Expiry becomes
    *last-renewal time + current period*, and both inputs are observable in
    blocks: each renewal is a `RewardAutoRenewedValidatorTx` (42) and each
    config change a `SetAutoRenewedValidatorConfigTx` (41). More work than
    reading two fields, but still extraction plus a small state machine over the
    tx index, and still cosmetic if slightly stale.
  - **Weight does not.** It grows by the compounded portion of each cycle's
    reward, which emits no UTXO and appears in no transaction (§ACP-236).
    Recovering it exactly needs the reward calculator and supply tracking — i.e.
    the consensus-bar machinery — so *current weight for an auto-renewed
    validator* is Phase-3 work, or a bounded estimate via the withdrawn-portion
    inversion, or an upstream proxy. This is the first case where a **product**
    surface needs consensus-bar inputs, and it is a consequence of the upgrade
    rather than of any choice made here.

- **Delegator rollups are their own tracking problem.** `delegatorCount` and
  `delegatorWeight` (both in the live `getCurrentValidators` response) require
  maintaining the delegation set per validator, with each delegation's own
  expiry, not just the validator set. Cheaper than weight — delegations are
  ordinary `AddPermissionlessDelegatorTx`es that cannot auto-renew, so their
  parameters *are* fixed by their adding transaction — but it is a second set to
  fold, and the rollup is only as fresh as the expiry machinery in the first
  bullet.
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
day-to-day don't need either — **with one Helicon-era exception**: the displayed
weight of an auto-renewed validator, which is a day-to-day view that does need
consensus-bar inputs (above). Everything else in this section's optimism survives
ACP-236 intact.

One framing point worth keeping straight: nothing here says
`getCurrentValidators` is unservable. The **set**, with weights and expiry, is
reconstructible — that is this whole section. What no indexer can ever produce is
`uptime` and `connected`, which are the queried node's *local observations* rather
than chain state; two honest nodes will disagree on them. The sting is only that
those are the exact two fields Core sorts and filters on (§gap 8), so the
delegation node-picker specifically cannot run off neve.

And note the strategic shape: `getCurrentValidators` is a weak target regardless,
being cheap upstream and dependent on fields that must be proxied anyway.
Reconstruction earns its keep on **history** — `getValidatorsAt(height)` and
per-validator time series, which upstream nodes serve poorly or not at all beyond
a window, and which §Beyond-node-parity lists as a differentiator Glacier does not
offer.

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
replicating it is ~15 minutes rather than hours.

A `--p-backfill-floor` anchored at the Banff or Etna activation height is the
pragmatic default for a first deployment, with deep history filled later — same
playbook as the C-chain. Note that against the *public* endpoint full history is
impractical regardless of the floor (§Open questions); deep backfill needs an own
node or a neve mirror.

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
all 186 transactions round-tripping through `getTx` + `getTxStatus`. §Sizing still
wants real numbers from a deeper fill, which the public endpoint's rate limit
defers to an own-node run.

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

*Scoped by §Core-wallet coverage.* The wallet's two XP endpoints pin the minimum:
a **multi-address, txType-filtered, timestamp-bounded, merged newest-first tx
index** plus the **spent-tracked UTXO set** — the Tier-2 half that moves *up* into
this phase, since `consumedUtxos`/`emittedUtxos` are embedded in the response
shape. The staking join must be keyed both ways
(`stakingTxHash` ↔ `rewardTxHash`), because the wallet reads realized rewards off
the *staking* tx's `emittedUtxos`. Serving is REST on
`/v1/networks/{network}/blockchains/{blockchainId}/…`, a path-dispatch dimension
the serve stack doesn't have yet.

### Phase 2 step order

Blocking items first, because two of them can invalidate key shapes or leave an
index that has to be rebuilt. Keys and values themselves are in §Phase 2 index
design, which follows; read §UTXO index numbering and §ACP-236 before starting
step 4, since both change the write path.

1. **Finish Phase 1 rewards** (`getRewardUTXOs` at ingest + commit/abort
   tracking). `staker_rewards` has nothing to index until this lands. The
   commit/abort discriminant is required for *index resolution*, not only for
   reward capture, and there are two reward-attachment conventions to handle
   (§ACP-236).
2. **bech32 decoding** for the 20-byte address payload — a new dependency; there
   is none in the tree today.
3. **Type discrimination by field shape**, with an explicit `Unknown` byte and a
   counter metric, txType in the value. **Must cover Helicon types 40–42 from the
   start** — they are live on Fuji and mainnet activation is being scheduled, so
   this is the one step with an external deadline. Landing it before mainnet
   Helicon is what keeps activation from forcing a reindex.
4. **UTXO resolution + the write path** — indexes 2, 3, 4 and 7 written at
   ingest, over the per-type address sources. Two traps: **`stake[]` UTXOs are
   written from the reward transaction, never from the staking transaction**
   (doing otherwise inflates every staker's balance by its whole principal),
   while **`addr_stakes` is written from the staking transaction** — the two
   halves of the same fact live at different heights.
5. **Reindex driver** (`neve reindex`), resumable, progress in `meta`. Not
   optional: it is the only way to repair a store whose indexes were written by an
   older binary, and the only thing that makes a key-format revision cheap later.
6. **Per-index coverage ranges in `meta`**, 421 outside them — from the first
   commit, not retrofitted. This is the `--ingest-logs` mistake. Stamp an
   **extraction-rules version** alongside the range, so indexes built by
   pre-Helicon rules are detectable rather than merely wrong.
7. **REST path dispatch** on `/v1/networks/{n}/blockchains/{id}/…`. Independent
   of everything above and spikeable early against `tx_to_block` alone.
8. **Fuji sizing measurement** to replace the estimates in §Sizing.

Out of scope for Phase 2: `node_stakes` (index 6), X-chain, the atomic-memory
cross-chain join, and everything in Phase 3.

### Phase 2 index design

Worked out against real mainnet and Fuji transactions rather than from the
schemas. Three findings drive the index set.

#### What the tx JSON actually gives you

Sampled `AddPermissionlessValidatorTx` at mainnet height 25345673. The
`unsignedTx` keys are exactly `blockchainID, inputs, memo, networkID, outputs,
rewardsOwner, stake, subnetID, validator`.

**1. There is no `txType` field anywhere.** Not on the transaction, not on the
block. Glacier's `txTypes` filter — the one Core's stake list pages against — is
a *derived* value here, not a copied one. A new transaction type therefore
presents as an unfamiliar *field shape*, never as an unfamiliar string.

**2. Inputs name no addresses.** An input is `{txID, outputIndex, assetID,
input.amount}` — a UTXO reference. Only `outputs[].output.addresses`,
`stake[].output.addresses` and `rewardsOwner.addresses` name anyone. So **an
address that funds a transaction appears nowhere in that transaction**, and
attributing a spend to its sender requires resolving each input against the
UTXO that created it.

That makes **UTXO resolution a prerequisite** for `addr_txs`, not a sibling of
it — the concrete form of gap 6's argument that Tier 2's UTXO half has to move up
into Phase 2. The dependency order is
`UTXO resolution` → `addr_txs` → everything Core renders. Resolution needs no
index of its own (see §UTXO resolution needs no index), but it does have to happen
first: an `addr_txs` posting for the *funding* address cannot be written until
each input has been resolved to the addresses that own it.

**3. Stake and reward UTXOs are numbered in the transaction's own index space,
and they are created at a different height than the transaction that names
them.** This is the subtlest fact in the design and it has its own section below
(§UTXO index numbering); it is what forces `addr_stakes` to exist and dictates
which transaction each write is driven from.

#### Index set

Keys are big-endian in every range-scanned component, per the ordered-key rule
above. Addresses are the 20-byte bech32 payload, not the `P-avax1…` string.
A UTXO id is its natural key, `txID ‖ outputIndex`.

| # | Keyspace | Key | Value | Serves |
| --- | --- | --- | --- | --- |
| 2 | `utxo_spent` | `txID(32) ‖ BE(outIdx u32)` | `BE(height) ‖ BE(txIdx)` | `consumingTxHash`, the unspent set |
| 3 | `addr_utxos` | `addr(20) ‖ txID(32) ‖ BE(outIdx u32)` | classification tuple — see below | balances by address |
| 4 | `addr_txs` | `addr(20) ‖ BE(u64::MAX - height) ‖ BE(txIdx u32)` | txType byte | the activity feed, newest-first |
| 5 | `staker_rewards` | `stakerTxID(32) ‖ BE(rewardHeight)` | `rewardTxID(32) ‖ outcome` | the staking join |
| 6 | `node_stakes` | `nodeID(20) ‖ BE(start) ‖ txID(32)` | ∅ | validator / delegator registries |
| 7 | `addr_stakes` | `addr(20) ‖ stakerTxID(32)` | `BE(amount) ‖ BE(endTime) ‖ flags(1)` | the three *staked* balance buckets |

Numbering starts at 2 because the design originally had a `utxo_index` at 1;
§Non-starters records why it was dropped.

Indexes 2–4 plus 7 cover both wallet endpoints, and 5 completes Phase 1's rewards
half. **6 is out of Phase 2 scope**: it serves nothing core-wallet calls, since
the wallet's validator list comes from `getCurrentValidators` upstream. It belongs
with the explorer/marketplace tier and waits for its own demand evidence — it is
deferred for lack of demand, not for impossibility, since the validator set is
reconstructible (§Decision 6).

As in the C-chain design, `addr_txs` is unique per `(address, tx)`, so an address
that both funds a transaction and receives an output in it collapses to one
posting.

**Write path: the four address sources.** Finding 2 above enumerates the three
places a transaction *names* anyone, which is not the same as the list of who
gets an `addr_txs` posting. The write path takes the union of:

1. **resolved input owners** — via the UTXO resolution below, the whole reason
   resolution is a prerequisite;
2. `outputs[].output.addresses`;
3. `stake[].output.addresses`;
4. **the reward/authority owners** — easy to miss and load-bearing: Core finds a
   stake card by its *reward owner*, who need not appear in `outputs` or
   `stake` at all. Omitting this source silently empties the stake list for any
   wallet that stakes to a separate reward address.

   **The field name is per-type**, so this cannot be a single lookup — see
   §ACP-236 below. Pre-Helicon staking txs use `rewardsOwner`;
   `AddAutoRenewedValidatorTx` (type 40) has no such field and instead carries
   `validationRewardsOwner`, `delegationRewardsOwner` and `validatorAuthority`.
   Extraction dispatches on the discriminated type rather than probing one name.

**No `time_to_height` index is needed.** `startTimestamp` is load-bearing
(EarnService pages against it) and `addr_txs` is height-keyed, so the filter has
to become a height bound — which looks like it wants an index. It does not.
P-chain chain time is non-decreasing and the store is height-keyed and randomly
readable, so a binary search over the height range costs ~25 block reads once per
query. Cheaper than an index, and it cannot go stale or acquire its own coverage
floor. Noted explicitly because the index is the obvious wrong move.

**One caveat, and it has a Banff floor.** The binary search needs a timestamp *on
the block*. Banff blocks carry one; **Apricot-era blocks do not** — §Phase 1
records that Apricot commit/abort blocks serialize to exactly
`{height, id, parentID}`, and pre-Banff chain time advanced through
`AdvanceTimeTx` rather than block timestamps. So timestamp-bounded queries can be
answered by search only down to the Banff activation height. Below it, either
answer 421 for the timestamp filter or derive time by scanning `AdvanceTimeTx`
into a sparse checkpoint — and 421 is the better first answer, consistent with
every other coverage floor here. UNVERIFIED which Apricot block kinds (standard,
proposal) carry a `time` field, if any; check before choosing the floor.

#### UTXO resolution needs no index

A P-chain UTXO id *is* `txID ‖ outputIndex`, and `tx_to_block` already maps
`txID → (height, txIdx)`. So a UTXO's addresses, amount and locktime resolve
through indexes that already ship:

```text
utxoId → txID → tx_to_block → (height, txIdx) → read block → tx output[outIdx]
```

Exact, deterministic, no new keyspace. Materialising those fields anywhere is
therefore a *cache* decision, never a correctness one.

**`addr_utxos` should take that cache in its value**, because balance is the hot
path: `getBalancesByAddresses` fires on *every account refresh* (gap 3), and with
an empty value a balance costs, per UTXO, one `utxo_spent` point lookup **plus a
block read** to recover amount, asset and locktimes. An address holding 2,000 live
UTXOs means 2,000 block decompressions per refresh, times the wallet's whole
address set. With the fields in the value a balance is a pure keyspace scan with
**zero block reads**:

```text
addr_utxos: addr(20) ‖ txID(32) ‖ BE(outIdx u32)
         →  BE(amount u64) ‖ assetID(32) ‖ flags(1)
            ‖ BE(platformLocktime u64) ‖ BE(stakeableLocktime u64)
```

That is a 56 B key plus a 57 B value, so **~113 B per entry**. Two constraints on
the value:

- **Store the raw locktimes, never a precomputed bucket.** Which bucket a UTXO
  falls in depends on wall-clock *now* (locked vs. unlocked), so bucketing is
  necessarily a read-time computation over stored raw fields.
- **Still write-once.** The value is fixed at creation; spentness continues to
  live in its own keyspace (index 2), so the write-once property the design
  protects below is untouched.

Only the *by-address* direction is materialised, and it has to exist as a posting
anyway — resolution by UTXO id still goes through `tx_to_block`.

**`addr_utxos` cannot serve the whole balance response, which is why index 7
exists.** **Staked principal is not in the UTXO set at all while it is staked** —
it lives in the staker record, and a UTXO appears only when the stake is returned
(§UTXO index numbering). So a UTXO-set scan can never see it, and Glacier's eight
buckets decompose by *source* rather than coming from one:

| Bucket group | Source | Servable |
| --- | --- | --- |
| `unlockedUnstaked`, `lockedPlatform`, `lockedStakeable` | `addr_utxos` (index 3) | yes, exactly |
| the three *staked* buckets (unlocked/locked/pending) | **active staking txs** — index 7 | yes, exactly |
| `atomicMemory{Unlocked,Locked}` | shared memory | no — §gap 2 |

Three of eight from UTXOs, three from the staker side, two unservable. A design
that reads balances only from `addr_utxos` would silently report zero staked
balance for every staker — a large, confidently wrong number for exactly the
users the staking views are for.

Hence **index 7, `addr_stakes`**, built to mirror the `addr_utxos`/`utxo_spent`
pair so the write-once property survives:

- Written once, at the **staking** transaction, keyed by each address that owns a
  `stake[]` output. (Unlike the *UTXO* write, this one belongs at the staking tx —
  the stake exists as a stake from that moment, which is exactly what
  `addr_utxos` cannot represent.)
- Liveness is *not* stored. It is determined at read time by probing
  `staker_rewards` (index 5) for the `stakerTxID`: absent ⇒ still staked, present
  ⇒ returned, and the returned principal is by then visible in `addr_utxos`
  anyway. Exactly the same absent-means-live join as `addr_utxos` + `utxo_spent`,
  so no mutation and no read-modify-write.
- `pendingStaked` vs. active is a read-time comparison against `endTime` and the
  stake's start. Note Durango made stakers active at acceptance and removed
  `getPendingValidators` (§Decision 6), so this bucket is likely always zero on
  current networks; keep it computed rather than hard-zeroed.

Post-Helicon caveat: for an auto-renewed validator the `endTime` in index 7 is
not final — each renewal extends it, and the compounded weight growth is not
observable (§ACP-236). So index 7 is exact for delegations and for legacy stakes,
and best-effort for auto-renewed validators, which is the same divergence
boundary as everything else in that section.

#### UTXO index numbering

Verified against avalanchego `master` @ `f7ae5c593f4c`
(`vms/platformvm/txs/executor/proposal_tx_executor.go`). This has to be settled
from source rather than from sampled JSON: **the P-chain JSON never shows a UTXO's
output index**, so no amount of `getTx` sampling can answer it.

**(a) A staking transaction's index space has three segments.** `unstakeUTXOs`
sets `outputIndexOffset := len(stakerTx.Outputs())` and emits `stake[i]` at
`outputIndexOffset + i`; the legacy reward path then places reward UTXOs at
`len(outputs) + len(stake) + offset`. So one staking transaction owns a single
contiguous index space:

```text
[0, len(outputs))                          → outputs[i]
[len(outputs), len(outputs)+len(stake))    → stake[i]        (the returned principal)
[len(outputs)+len(stake), …)               → reward UTXOs
```

Resolving an `outIdx` therefore walks all three segments in order; stopping at
`outputs` and `stake` mis-resolves every reward UTXO.

**(b) The stake UTXOs do not exist at the staking transaction's height.**
`unstakeUTXOs` is called during execution of the **`RewardValidatorTx`**, with
`txID = validator.TxID` — the *staking* tx's ID. So a stake UTXO bears the staking
tx's ID and index while being *created* at a much later height, when the staking
period ends. During the staking period the principal is not in the UTXO set at
all; it lives in the staker record.

This is the trap in the write path: **writing `addr_utxos` entries for `stake[]`
at ingest of the staking tx would report staked principal as spendable balance for
the entire staking period**, inflating the reported balance by the whole stake
amount. The write for `stake[]` UTXOs must be driven by the *reward* transaction,
not by the staking transaction that names them. Same family as "absent is not
empty": deriving existence from the wrong event turns a missing answer into a
confidently wrong one.

**(c) Commit/abort changes what lives at a given index.** On the abort path the
delegatee reward is written at exactly `len(outputs) + len(stake)`, which on the
commit path is where the *validator* reward goes (the delegatee reward moving to
`+utxosOffset`). So the same `(txID, outputIndex)` pair denotes different UTXOs
depending on which branch committed. Reward-UTXO indices cannot be interpreted
without resolving commit vs. abort, so the 4-byte type ID at offset 2 of the
stored bytes — the only way to tell those blocks apart, since they are identical
in JSON — is load-bearing for Phase 2 as well as for Phase 1's reward capture.

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

#### ACP-236 auto-renewed staking

Helicon's tx types 40–42 *are* ACP-236, and they change the reward model rather
than merely adding shapes to tolerate. Confirmed three ways:

- **Type IDs are exactly 40, 41, 42.** Counted through the linear codec in
  `vms/platformvm/txs/codec.go` (`SkipRegistrations(5)` for blocks → Apricot →
  Banff → `SkipRegistrations(4)` → Durango → Etna → Helicon):
  `AddAutoRenewedValidatorTx` = 40, `SetAutoRenewedValidatorConfigTx` = 41,
  `RewardAutoRenewedValidatorTx` = 42. The count is trustworthy because it
  independently reproduces the known values `AddValidatorTx` = 12 and
  `AddPermissionlessValidatorTx` = 25, and because its two `SkipRegistrations`
  gaps land exactly on the block type IDs (Apricot 0–4, Banff 29–32) that share
  the codec.
- **Live on Fuji, not on mainnet.** Helicon activated on Fuji 2026-07-28; no
  mainnet date has been announced as of 2026-08-11. So this lands in the *test*
  network first, which is where Phase 2 was going to be measured.
- **Observed in real Fuji state.** `platform.getCurrentValidators` at Fuji height
  292434 returned 80 validators, exactly one carrying the auto-renew fields
  (`nextPeriod` 604800, `autoCompoundRewardShares` 900000, `validatorAuthority`),
  with `weight` 5,006,729,532 against an original stake of 5,000,000,000 — i.e. it
  has already compounded at least one cycle.

Note the ACP's own status line still reads `Proposed` while the code is live; the
ACP repo's metadata lags deployment. **Do not use ACP status as an activation
signal.**

**1. Staking transactions do not share a field shape.** The sampled
`AddAutoRenewedValidatorTx` (Fuji `258CFXhtwDJu3UtuK5M5JjWMxyDhAiJgvqptmw8jSXsGeytEiq`)
has `unsignedTx` keys `autoCompoundRewardShares, blockchainID,
delegationRewardsOwner, delegationShares, inputs, memo, networkID, nodeID,
outputs, period, signer, stake, validationRewardsOwner, validatorAuthority`.
Against `AddPermissionlessValidatorTx` that is a different shape, not an extended
one:

| Field | Pre-Helicon staking tx | `AddAutoRenewedValidatorTx` |
| --- | --- | --- |
| validator period | `validator{nodeID,start,end,weight}` | **no `validator` object** — flat `nodeID` + `period` |
| reward owner | `rewardsOwner` | **`validationRewardsOwner`** + `delegationRewardsOwner` |
| commission | `shares` | `delegationShares` |
| extra owner | — | **`validatorAuthority`** |
| `subnetID` | present | absent (primary network only) |

Shape discrimination still works cleanly — `period` + `autoCompoundRewardShares`
are unmistakable. Two consequences:

- **Address extraction is per-type, not a generic field walk.**
  `rewardsOwner.addresses` **does not exist** on type 40; the owners are
  `validationRewardsOwner`, `delegationRewardsOwner` and `validatorAuthority`, so
  there are five owner-bearing field names across types. Getting this wrong
  silently empties the stake list for exactly the validators the upgrade is
  designed to attract.
- **`node_stakes` (index 6) has no `start` to key on.** Its key
  `nodeID(20) ‖ BE(start) ‖ txID(32)` reads `validator.start`. Type 40 has no
  `validator` object and no start — only a `period` duration. Another reason index
  6 stays out of Phase 2; when it returns, its key needs a start derived from the
  *block* timestamp rather than from the transaction.

**2. Reward UTXOs do not always attach to the staking transaction.** This is the
consequential one. The legacy path (`rewardValidatorTx`) uses
`txID = validator.TxID`, so rewards hang off the **staking** tx — which is what
gap 7 relies on, because Core reads realized rewards as
`emittedUtxos.find(rewardType === …)` on the original staking tx. ACP-236's
`mintRewards` instead uses `txID := e.tx.ID()` and
`outputIndexOffset := len(e.tx.Unsigned.Outputs())` — the **`RewardAutoRenewed`
ValidatorTx's own** ID and outputs.

So the chain now carries **two incompatible reward-attachment conventions
simultaneously**, and which applies depends on the staker's type. Confirmed
empirically: `platform.getRewardUTXOs` on the auto-renewed staking tx above
returns `numFetched: 0` despite that validator having demonstrably compounded a
cycle. A Phase 2 built to the legacy convention will report **zero lifetime
rewards** for every auto-renewed validator, with no error anywhere.

Consequences for the index set:

- **Index 5 is one-to-many.** An auto-renewed validator has one reward event *per
  cycle*, indefinitely, so `staker_rewards` is keyed
  `stakerTxID(32) ‖ BE(rewardHeight)` with the reward txID in the value, and the
  "staking join" is a prefix scan rather than a point lookup.
- **The three-segment index space does not apply to type 42's rewards.** They sit
  after the *reward* tx's own outputs, in its own space. Resolution must branch on
  which convention produced the UTXO.

**3. Compounded rewards are not observable from blocks at all.** Only the
*withdrawn* portion of an auto-renewed reward becomes a UTXO. The compounded
portion emits **no UTXO whatsoever** — `restakeAutoRenewedValidatorOnCommit` adds
it to `validator.Weight` and `AccruedValidationRewards` in state and nothing else.
At `autoCompoundRewardShares` = 900000 (90%, the live Fuji validator's setting),
**90% of that validator's earnings are invisible to any UTXO- or tx-derived
index.** Lifetime-rewards-earned for an auto-renewed validator is therefore *not*
a Phase 2 extraction question at all; it requires the staker state that §Phase 3
replay owns, or an upstream `getCurrentValidators` proxy for the live value
(`accruedDelegateeReward` is already in that response, observed above).

This is a declared v1 divergence alongside `atomicMemory*`: for auto-renewed
validators, neve serves withdrawn rewards exactly and compounded rewards not at
all (§Non-starters). It argues for pulling the `getCurrentValidators` *proxy*
forward, since the accrued figure is right there upstream even though the
`uptime`/`connected` fields never will be.

##### Treat 40–42 as a production certainty

Mainnet Helicon is being scheduled, so "Fuji only" is a statement about *today*,
not a scoping decision: plan as though mainnet will carry types 40–42.

**Ship per-type extraction before mainnet activation, and stamp an
extraction-rules version.** The activation is a non-event for the *store* —
verbatim `[bytes, json]` absorbs new types with zero code, as predicted. It is not
a non-event for the *indexes*: any index built by pre-Helicon extraction code has
silently wrong address postings and reward joins for every type-40/42 transaction
that follows. Two requirements fall out:

- Land 40–42 extraction **before** activation, so no reindex is forced at a moment
  when production is also absorbing a network upgrade.
- Stamp an **extraction-rules version** in `meta` beside the per-index coverage
  range (§Build indexes as a resumable pass). A coverage range alone cannot express
  "these heights were indexed, but by rules that predate a tx type they contain."
  With the version stamped, a binary that knows about 40–42 can detect
  older-rules indexes and trigger the ~15-minute rebuild by itself. Without it,
  this is indistinguishable from a correct index — the same failure the
  `--ingest-logs` coverage floor exists to prevent, one level up.

**The blast radius on core-wallet is narrow.** Worth stating plainly, because the
three findings above read as alarming for the wallet milestone and mostly are not:

- **There is no auto-renewed *delegator* type.** Helicon registers exactly three
  types, all validator-side; `AddPermissionlessDelegatorTx` is untouched, and
  `rewardDelegatorTx` still uses `txID = delegator.TxID`, i.e. the legacy
  attach-to-the-staking-tx convention.
- **Core's stake list is delegator-shaped.** It pages to exhaustion with
  `txTypes=[ADD_PERMISSIONLESS_DELEGATOR_TX, ADD_DELEGATOR_TX]`
  (`EarnService.ts:370`, §What-the-wallet-actually-calls).

So the compounded-reward blind spot and the reward-attachment switch land on
**validator operators**, not on the delegation flow the wallet actually queries. A
Core user is affected only if they run an auto-renewed validator from wallet
addresses. That does not make it a non-issue — an explorer or a staking dashboard
hits it immediately, and it is a correctness gap regardless — but it should not
reorder the Phase 2 wallet work.

**Compounded rewards are partially recoverable by arithmetic, not only by state
replay.** `reward.Split(total, shares)` returns
`(total - remainder, remainder)` with
`remainder = floor((1e6 - shares) × total / 1e6)`, and it is the *remainder* that
gets minted as the withdrawn UTXO. Since `autoCompoundRewardShares` is in the
staking tx and the withdrawn amount is an observable UTXO, the cycle total inverts:

```text
total ≈ withdrawn × 1e6 / (1e6 - shares)
compounded ≈ total - withdrawn
```

Three limits, all of which must be stated wherever this is served:

- **It is an estimate, not exact.** Floor rounding leaves an ambiguity of up to
  `1e6 / (1e6 - shares)` nAVAX — 10 nAVAX at the live Fuji validator's 900000,
  negligible in practice but *not* bit-exact. That matters because §Phase 3's
  differential test demands computed rewards reproduce fetched reward UTXOs
  bit-for-bit; this technique cannot meet that bar and must not be fed into it.
- **`shares = 1e6` is a total blind spot.** At 100% compounding the withdrawn
  amount is zero, no UTXO is minted, and there is no information to invert.
  Given the upgrade's whole purpose is compounding, expect this setting to be
  common.
- **The `MaxValidatorStake` cap breaks the relationship.** When the cap binds,
  `restakingValidationRewards` is recomputed through `MulDiv` and the split is no
  longer `shares`-proportional. Detectable — weight at or near the cap — so the
  estimate should be suppressed rather than silently wrong.

**Two smaller structural notes for the write path:**

- **`SetAutoRenewedValidatorConfigTx` (41) is the P-chain's first parameter
  *mutation*.** Every prior staking tx is immutable once written; type 41 changes
  a live validator's cycle duration and reward split. Any materialised stake
  record — and `node_stakes`, whenever it returns — must treat type 41 as an
  invalidation event rather than just another posting. This is a new class of
  event for a design whose every key is write-once.
- **Reward UTXO indices are position-dependent on which rewards are nonzero.**
  `mintRewards` only emits a UTXO when an amount is `> 0` and advances
  `outputIndexOffset` only then, so index `len(outputs)` is the validation reward
  in one transaction and the delegatee reward in another. An index cannot be
  mapped to a meaning positionally; the amounts have to be read. This compounds
  §UTXO index numbering (c), where commit/abort already changes what lives at a given
  index.

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
keyspace** (index 2), never as a mutation of a UTXO record. This is what lets
`addr_utxos` carry a materialised value (above) without giving up write-once:
the value is fixed at creation and spentness is a separate key. A read-time
balance therefore joins two write-once keyspaces rather than reading one mutable
row.

#### Build indexes as a resumable pass, not only an ingest side effect

The store holds the block JSON verbatim, so **every index above can be built by
re-reading the local store with zero upstream traffic** — at the measured
~28,000 heights/s, a full 25.3M-height pass is on the order of 15 minutes. That
is the single most useful property to design around: it makes indexes cheap to
add, to rebuild after a rules change, or to repair in a store written by an older
binary.

It also means **not repeating the `--ingest-logs` mistake**. The C-chain has no
coverage floor in `meta`, so heights stored before the flag was enabled are
indistinguishable from genuine empties — an open limitation today. Stamp a
per-index coverage range in `meta` from the first commit, and
answer 421 outside it. An index that is present but incomplete must say so; the
absent-is-not-empty rule applies to indexes exactly as it applies to records.

#### Sizing — the number to get before committing

Unmeasured, and it should not stay that way. The index set is postings: order
100M entries at 18–32 bytes of key, so **1.4–2.4 GB** for `addr_txs`, plus
`utxo_spent` at a similar order and `addr_utxos` at **~113 B per UTXO** given its
classification value (only the *unspent* set is ever scanned, but every UTXO gets
an entry, so size tracks total UTXOs created). `addr_stakes` is negligible by
comparison — one entry per (address, staking tx), and stakes are rare next to
transfers. Call the whole set **~4–6 GB** pending measurement: a fraction of the
13 GB store rather than a multiple of it, which weakens the case for a flag gate —
but measure on Fuji before believing any of these numbers.

**Phase 3 — state replay (Tier 2/3).** UTXO set → `getUTXOs`/`getBalance`;
staking replay → `getCurrentValidators`/`getValidatorsAt`/`getTotalStake`/
supply; fee accumulators → `getFeeState`/`getValidatorFeeState`/L1 balances.
Per Decision 6: snapshot-seeded at an Etna-era floor, outcomes read from the
chain rather than re-derived, and differentially tested against upstream
(`getValidatorsAt` diffs per height; computed rewards must reproduce Phase-1
fetched reward UTXOs bit-for-bit). Gate this phase on demand evidence (the
traffic sample), not on completionism.

**Standing watch items.** New transaction types reach the store with zero code —
the `[bytes, json]` record absorbs them byte-identically, which is the whole point
of Decision 1(c) — but they do *not* reach the indexes for free, since extraction
dispatches on field shape and an unrecognised shape stores `Unknown`. Helicon
(types 40–42) is the worked example: see §ACP-236. Granite epochs and any future
proposervm changes only matter if own-node container ingestion (Decision 2b) is
picked up.

## Open questions

- **Traffic sample**: get a `platform.*` method breakdown from the api-worker
  (analog of the 9.5M-call C-chain sample). This decides how much of Tier 2/3
  is worth building and should precede Phase 2 scoping.
- **Public-endpoint limits — answered, and much harsher than the C-chain's.**
  `api.avax-test.network` answered a sustained **~14 req/s** of
  `platform.getBlockByHeight` with HTTP 429 and **`Retry-After: 3600`** after
  roughly 200 heights (~30s of backfill). Two consequences:
  - **A P-chain trip blocks the whole host**: while throttled, `/ext/bc/C/rpc`
    returned 429 too, because the rule counts P-chain-weighted responses but
    mitigates against its match set, which is every POST to the host (§Rate
    limits). A hard P-chain backfill takes a co-located C-chain instance with it.
  - Each height costs **two** requests (`hexnc` + `json`), so pacing must be
    per-request; pacing per height silently doubles the real rate. Hence
    `--p-request-interval` (default 200ms ≈ 5 req/s) rather than reusing the
    C-chain's 40ms.

  Full history from the public endpoint is out of reach — **years**, not days: the
  configured mainnet cap on `/ext/bc/P` is 50 requests per 60 seconds and each
  height costs two requests, so 25.3M heights is ~5.7 years of continuous fill
  (§Rate limits). Even a 1M-height partial fill is ~35 days. Deep backfill therefore
  needs either a **rate-limit bypass token** (§Rate limits — the established
  mechanism, and the cheapest unlock by far) or an own node / neve mirror with
  `--p-request-interval 0`. Still open: does `getRewardUTXOs` return correct data
  for arbitrarily old stakers through the CDN?
- **`getValidatorsAt` retention depth**: how far back the public endpoint
  answers it (validator-diff pruning behavior UNVERIFIED) — determines the
  differential-testing oracle's coverage for backfill replay (Decision 6).
- **JSON field-order sensitivity**: which consumers (avalanchejs, explorers) care
  about field order in `json`-encoding responses. Verbatim storage moots it for
  `getBlock`. (`platform.getTx(txID, json).tx` is structurally identical to
  `getBlock(json).txs[i]`, checked on Fuji height 292000, which is why `getTx` is
  served by slicing the stored block JSON and no per-tx JSON is stored.)
- **Who consumes Phase 2 beyond the wallet.** Answered *for* the wallet — exactly
  two Glacier REST endpoints (§Core-wallet coverage), both serving P *and* X.
  Still open for the explorer and for api-worker offload; the traffic sample above
  is the way to size Tier 2/3 past that.
- **Apricot block timestamps**: which Apricot-era block kinds carry a `time` field,
  if any. Sets the floor below which timestamp-bounded queries must 421 (§Index
  set).
- **X-chain: in or out?** The wallet's two endpoints are `blockchainId`-
  parameterized and Core renders X alongside P, so a P-only neve leaves the
  wallet half-served. Does *not* block Phase-2 keys (gap 1). Open sub-question:
  does the post-Cortina linearized X-chain map onto the height-keyed blockstore?
  (The pre-linearization vertex era does not — §Non-starters.)
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

## Non-starters

Approaches that were worked out far enough to evaluate and then rejected, and data
that cannot be served at all. Recorded so they are not re-proposed, and so the
reasoning is available if the constraints change. Each entry says what would
reopen it.

### Rejected designs

#### A bloom-filter pyramid instead of posting lists

Two indexes exist only because nothing in the data points the way the query needs
to go: nothing in a transaction points at whoever later spent its outputs
(`utxo_spent`), and nothing points from an address to its transactions
(`addr_txs`). Both are candidates for replacing a posting list with a **bloom
filter used as a locator**: test "might height H touch X", read the candidate
block, confirm by parsing it.

Two objections do *not* apply, which is why this was worth costing out. A filter
cannot return a value — but the store holds the value and the filter only has to
locate it. And false positives are not fatal, because the block read **is** the
verification: a false positive costs a wasted read, never a wrong answer. The
guarantee even runs the useful direction, since blooms have no false negatives, so
"absent from every filter" is a proof — exactly what "this UTXO is unspent"
requires.

The space case looked strong. At ~3 entries per height over 25.3M heights (~76M
entries):

| bits/key | FPR | filter size | wasted block reads/query |
| --- | --- | --- | --- |
| 10 | 8.2e-03 | 95 MB | 207,261 |
| 20 | 6.7e-05 | 190 MB | 1,698 |
| 24 | 9.8e-06 | 228 MB | 248 |
| 28 | 1.4e-06 | 266 MB | 36 |

against ~2.4 GB for full `addr_txs` postings. Testing 25.3M per-height filters is
~1–2 s of random memory access, so the viable shape is a two-level pyramid — one
filter per ~4096 heights (~6,200 of them) to prune, per-height filters inside
candidate groups only. Roughly doubles the memory and puts a query around 1–3 ms
against ~100 µs for an index scan.

**What kills it: absence costs a full-chain sweep.** "No false negatives" is what
makes absence provable, but the proof requires testing *every* filter — nothing
may be skipped. So for an address with three transactions a posting scan costs
`O(hits)` while a filter sweep costs `O(chain length)`, independent of hits. Two
reasons that is the common case rather than the corner case:

- **Most queried addresses have never appeared.** Core passes its entire BIP44 XP
  address set — internal and external chains, dozens of addresses
  (`getCachedXPAddresses`) — and the overwhelming majority have no P-chain history
  at all. Every one costs a full sweep to say "nothing here."
- **Proving a page is the last page is itself an absence proof.** Even a non-empty
  address pays the sweep on the final page of any paginated query, so there is no
  traffic pattern in which this cost is rarely paid — including one where deep
  history is queried rarely.

At the parameters above (6,200 group filters, 24 bits/key) a group-level sweep is
~6,200 probes ≈ 3 ms per address, so a 40-address feed open spends ~120 ms in
filter tests before reading anything, against a few µs of prefix seeks that return
empty. `consumingTxHash` then stacks on top: Glacier embeds it for every consumed
UTXO, so each becomes its own forward sweep — roughly 400 ms per 100-tx page
against ~200 µs of point lookups. Three orders of magnitude, and the absence cost
is there by construction and cannot be tuned away with bits/key.

**And every repair reintroduces postings.** The natural fix is a presence index —
`addr → the groups it appears in` — so a sweep can skip. That *is* a posting list,
just coarser; having paid for it, the fine-grained one costs little more. (A single
whole-chain "addresses ever seen" filter fixes only the never-seen case, not
pagination termination, and fjall already answers an absent prefix in about one
read, so it earns no keyspace.)

Two further points, neither decisive alone: blooms cannot serve balances at all,
since a balance is a sum and `addr_utxos` carries a materialised value — so the
pyramid was always going to be partial, collapsing the headline 228 MB vs. 2.4 GB
into roughly 456 MB of two-level filters *plus* `addr_utxos` postings against ~2 GB
of all-postings, under 1.5 GB saved next to a 13 GB block store. And "cheap to
retune from the verbatim store" de-risks both designs equally, so it was never a
differentiator.

**Would reopen if:** a query appears that is genuinely existence-shaped over a
mostly-negative key space with no absence deadline. A *global* txType-filtered scan
with no address filter — a differentiator Glacier does not offer — is the plausible
candidate. It is not a wallet query.

#### A materialised `utxo_index` keyspace

The first index sketch had `utxo_index` mapping a UTXO id to its addresses, amount
and locktimes, carried over from Glacier's `PChainUtxo` response shape. It is
redundant: a P-chain UTXO id *is* `txID ‖ outputIndex` and `tx_to_block` already
maps `txID → (height, txIdx)`, so the key is self-locating and resolution is a
block read, exactly (§UTXO resolution needs no index).

**Would reopen if:** resolution proves read-bound somewhere that `addr_utxos`'
materialised value does not already cover. Note this is a cache decision, not a
correctness one, so it can be added at any time without a migration.

#### A `time_to_height` index

`startTimestamp` filtering is load-bearing and `addr_txs` is height-keyed, so the
filter has to become a height bound — which looks like it wants an index. It does
not: chain time is non-decreasing and the store is height-keyed and randomly
readable, so a binary search costs ~25 block reads once per query. An index would
be more state to keep, another coverage floor to track, and no faster in any
regime that matters.

**Would reopen if:** timestamp-bounded queries become hot enough that ~25 reads per
query dominates, which would require far more traffic than the wallet generates.

#### txType in the `addr_txs` key

Putting the type in the key (`addr ‖ type ‖ BE(MAX-height)`) makes Core's two-type
stake query a tight scan, but turns the unfiltered activity feed into a ~20-way
merge across type prefixes. Keeping it in the value costs a full scan when the
filter is selective — and Core's stake query *pages to exhaustion*, which is the
selective case. Value wins on the balance of the two, and the k-way merge machinery
needed for multi-address queries means a secondary `addr_type_txs` can be added
later without inventing anything.

**Would reopen if:** a real address's delegator query measurably hurts. Add the
secondary index then rather than reshaping the primary key.

#### Recording spentness by mutating the UTXO row

Every fjall key neve writes today is written exactly once, which is why
`lsm-tree`'s stale-read bug (#315) is inert here. Recording spentness by rewriting
a UTXO's existing row would end that property and put a read-modify-write on the
ingest hot path. Spentness lives in its own write-once keyspace instead
(`utxo_spent`), and liveness is a read-time probe. The same pattern covers stakes:
`addr_stakes` is write-once and its liveness is a probe of `staker_rewards`.

**Would reopen if:** never, realistically. The write-once property is load-bearing
for more than this one decision.

#### Building the store locally and copying it to production

The natural way to get full P-chain history onto production is to build the store
against a local avalanchego node — measured at ~8,400 heights/s, so mainnet's
25.3M heights in ~50 minutes and ~13 GB on disk — then `rsync` it into place while
neve keeps serving. Technically it works, and architecture is a non-issue
(`arm64` and `aarch64` are the same ISA, and every on-disk integer is written with
explicit endianness, so a plain byte copy is correct). It was dropped on
bandwidth economics.

**The transfer, not the build, is the cost.** Measured 2026-08-11 over one SSH
stream to production: 200 MB in 362 s ⇒ **4.4 Mbit/s**, putting the full 13 GB
store at about **6.7 hours** — an order of magnitude longer than building it and
longer than everything else in the run book combined. This is the uplink, not
contention: avalanchego's own traffic at the time was 1.4 Mbit/s in / 0.5 out,
nowhere near any cap. The link is Starlink, whose upstream is modest and lossy,
and a single TCP stream over a high-latency lossy path underperforms its nominal
capacity — `rsync` is one stream, so it sees exactly that. Whether several
concurrent streams recover any of it is **untested**: the attempt failed because
the YubiKey-resident SSH key refuses concurrent signing (`agent refused
operation`), and SSH multiplexing would not help since it shares one TCP
connection.

**The decisive constraint is the local link's monthly cap, not the hours.** Keep
the two hosts straight, because it is easy to conflate them: **production is in
AWS**, where inbound transfer is free and unmetered — nothing in the run book's
public-endpoint fill touches a data cap. This route is different precisely because
it runs on the **local** machine, whose uplink is metered at **300 GB/month**, and
then pushes bytes *out* of it.

A 13 GB store copy is ~4% of that cap, which alone would be tolerable — but it is
the small half of the bill. Getting a local node to the point where it can *serve*
the fill means bootstrapping avalanchego's P-chain and state-syncing the C-chain,
which pulls tens to hundreds of GB down the same metered link. Spending most of a
month's budget to seed history nobody has asked for yet is the wrong trade, and
`--mirror-from` is not a way around it: the same bytes cross the same uplink, it
just re-verifies each record on arrival instead of trusting a file copy.

**Would reopen if:** the store is built somewhere with a fat uplink and shipped
from there, an unmetered link appears, or real demand for deep P-chain history
turns up. If it does, note that `rsync -P` is resumable so the copy can run
overnight and survive a dropped link, and `--compress` buys nothing on an
already-zstd payload. Note the public endpoint is a *shallow* alternative only: its
configured cap on `/ext/bc/P` is 50 requests per 60 seconds, so 100k heights is
3.5 days and 1M is ~35 days (§Rate limits). A **rate-limit bypass token** — the
mechanism several other internal consumers already use — is the cheaper way to make
deep history reachable, and it needs no local bandwidth at all.

### Data that cannot be served

#### `uptime` and `connected` on `getCurrentValidators`

These are the *queried node's own local observations*, not chain state — two honest
nodes will disagree on them, and no amount of replay produces them. The validator
set itself, with weights and expiry, **is** reconstructible (§Decision 6); only
these two fields are impossible. The practical sting is that they are exactly what
Core sorts and filters its delegation node-picker on, so that picker cannot run off
neve at any fidelity.

**Would reopen if:** never for the fields themselves. A caller wanting them must
reach a node; the honest serving answer is to omit or null them and let the
fronting api-worker fill them in.

#### The `atomicMemory*` balance buckets

Two of Glacier's eight balance buckets are shared-memory atomic UTXOs, deposited by
X/C-chain exports and invisible in P-chain blocks. A complete P-chain UTXO replay
still cannot reproduce them.

A cross-chain route exists in principle — pending atomic UTXOs are
(exports-to-P seen on C/X) minus (imports seen on P), which only an instance
ingesting both sides can compute. What makes it a project rather than a join: a
C-chain export to P is an **atomic transaction**, and atomic txs do not appear in
`eth_getBlockByNumber` output, so neve's C-chain ingest has never seen one.
Supplying that side means a new fetch source (an `avax.getAtomicTx`-shaped call)
with its own coverage floor and verification story, plus a keyspace to hold it, and
doing X-chain buys the X leg of the same problem again.

**Would reopen if:** atomic-transaction ingestion lands on the C-chain for its own
reasons, at which point the join is cheap. Until then this is a declared
divergence, and it needs a diff against Glacier on real accounts with in-flight
transfers before anyone trusts an approximation.

#### Compounded rewards for ACP-236 auto-renewed validators

Only the *withdrawn* portion of an auto-renewed reward becomes a UTXO. The
compounded portion is added to `validator.Weight` and `AccruedValidationRewards` in
state and emits nothing — so at a 90% compound setting, 90% of that validator's
earnings are invisible to any UTXO- or transaction-derived index (§ACP-236).

Partial recovery is possible by arithmetic rather than replay. `reward.Split` mints
the *remainder* as the withdrawn UTXO, and `autoCompoundRewardShares` is in the
staking tx, so `total ≈ withdrawn × 1e6 / (1e6 − shares)` and
`compounded ≈ total − withdrawn`. Three limits, all of which must be stated
wherever this is served:

- **It is an estimate.** Floor rounding leaves an ambiguity of up to
  `1e6 / (1e6 − shares)` nAVAX — 10 nAVAX at a 900000 setting. Negligible in
  practice but *not* bit-exact, so it must not feed Phase 3's differential test,
  which demands computed rewards reproduce fetched reward UTXOs exactly.
- **`shares = 1e6` is a total blind spot.** At 100% compounding nothing is minted
  and there is nothing to invert. Given the upgrade's purpose, expect this setting
  to be common.
- **The `MaxValidatorStake` cap breaks the proportionality.** When the cap binds the
  split is recomputed through `MulDiv` and is no longer `shares`-proportional.
  Detectable — weight at or near the cap — so suppress the estimate rather than
  serve a wrong number.

**Would reopen if:** Phase 3 state replay lands, which produces the exact figure as
a side effect. An upstream `getCurrentValidators` proxy also serves the *live*
accrued value today.

#### `getTx` byte encodings

A transaction's canonical bytes are not separately stored, and slicing them out of
the block's bytes needs the codec parser Decision 1 exists to avoid. The `json`
encoding is served; the hex encodings answer 421.

**Would reopen if:** a bytes-parser lands as a hardening layer (Decision 1 already
allows for one), or if per-tx bytes are stored — which is pure duplication and
should be justified by real demand.

#### The pre-linearization X-chain vertex era

Post-Cortina the X-chain is linearized and should map onto the height-keyed
blockstore. The DAG-era history before that does not — there are no heights to key
on — and the `getVertexByHash` / `listLatestXChainVertices` surface has no analog in
neve's model. UNVERIFIED in detail, since X-chain is out of scope entirely.

**Would reopen if:** X-chain lands *and* someone needs vertex-era history. The
linearized era is the part that maps; a floor at linearization is the natural
answer.

#### The transaction-construction and write path

`issueTx` (write), `getUTXOs` with `sourceChain` (shared memory), `getFeeState`
and `getAtomicUTXOs` are what the wallet uses to *build and submit* transactions.
None is an indexer surface, and they stay upstream permanently — a wallet can never
point at neve alone, only at an api-worker that fronts it. Also permanently
unservable for their own reasons: `sampleValidators` (randomness),
`getBlockchainStatus` (`Validating` means "this node validates it"),
`getProposedHeight` / `proposervm.*` (preferred-block dependent), and `getTxStatus`
beyond `Committed` (node-local mempool).

**Would reopen if:** never. This is the 421 contract working as designed, and it is
worth saying out loud that neve's XP story is read-side only.
