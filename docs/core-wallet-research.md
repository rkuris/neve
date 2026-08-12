# neve account history — logs-first design

**Goal:** serve Avalanche Glacier's `listTransactionsV2` from a neve instance so
core-wallet's entire **EVM** activity feed works when pointed at neve. That one endpoint is
the whole job for the C-chain — see [Reference](#reference) for why nothing else in the
_EVM_ Glacier surface is needed by the wallet.

> **Scope: the inventory is complete, the design is EVM.** The Glacier inventory below
> covers **both** wallet packages — `@avalabs/evm-module` for the EVM surface and
> `@avalabs/avalanche-module` for the X- and P-chains — so the endpoint tables are the whole
> picture of what core-wallet asks Glacier for. What is EVM-scoped is the _design_: the
> logs-first `listTransactionsV2` implementation worked out here. The XP endpoints are
> summarised under [XP surface](#xp-surface-x-and-p-chains), and their index design,
> gap analysis, and phasing live in
> [`p-chain-indexing-plan.md` §Core-wallet coverage](p-chain-indexing-plan.md). The two
> surfaces share no endpoints, so the design here stands independently.

See [Non-starters](#non-starters) at the end for approaches evaluated and rejected.

**Thesis: logs are the backbone, receipts are optional enrichment.** The activity feed can
be built from **blocks + event logs** alone. Receipts (`txStatus`, per-tx `gasUsed`) are a
later fidelity upgrade, not a prerequisite — and dropping them removes the hard dependency
on an archive node, because the public endpoint can't bulk-serve receipts but _can_ serve
logs.

## Status & next session (resume here)

### Done

- Receipts ingestion is **not part of neve** — no flag, no fetch, no `receipts_by_height`
  keyspace, and no occurrence of `receipt` anywhere in `src/`
  (see [Non-starters](#receipts-as-the-ingestion-backbone)). `tx_to_block` stays; it powers
  `eth_getTransactionByHash`.
- **Logs ingestion shipped**, behind `--ingest-logs` (C-chain only, **off by default**).
  Backfill fetches each ~2048-block window via `eth_getLogs`; the live path fetches each tip
  block's logs and joins them into the combined `[block, logs]` record. Coreth WS `logs`
  subscription confirmed working on Fuji.
- **Not shipped: the index and the read path.** The store has only `meta`,
  `hash_to_height`, and `tx_to_block` — no `addr_txs`, no `tx_transfers`, no `token_meta`. So
  logs are _stored_ but nothing queries them by address yet.

**Production runs neither.** `NEVE_ARGS=--summary-period 1m --rpc-addr 0.0.0.0:8545` — no
`--ingest-logs`, so the deployed mirror is blocks-only. Two consequences worth holding onto:

- Enabling `--ingest-logs` on the existing production store would leave every
  previously-stored height with no logs and **no way to distinguish that from a height that
  genuinely had none** — there is no coverage floor in `meta`. Either accept it, backfill
  logs over the whole store, or add the floor first.
- The retention question is therefore still open and is now the main thing to settle:
  ~27.5M blocks/yr; 1 yr ago ≈ C-Chain height 59–60M (tip ~86.97M). The index adds
  ~70–90 GB/yr on top of the block mirror. Decide a horizon / `--backfill-floor` target, and
  whether logs depth tracks block depth or is shallower.

**Next — (a) the write path** (see [Storage](#storage--index-design-fjall)): decode Transfer
events out of the stored `[block, logs]` record and write `tx_transfers` + `addr_txs` in the
ingest batch.

**Next — (b) serve `listTransactionsV2`** once the index exists: read path in
[Storage](#read-path-listtransactionsv2); native fields from blocks, transfers from
`tx_transfers`, `txStatus:"1"`/`gasUsed:"0"` placeholders, token metadata via `token_meta`.

**First code step:** the `addr_txs` / `tx_transfers` write path as a diff against the ingest
batch in `src/storage.rs` (`Storage::put`) and the log-decode step in `src/subscribe.rs`.

---

## Why receipts aren't required (confirmed against the wallet)

We traced `@avalabs/evm-module@3.8.1` and the core-mobile UI for the two receipt-only
fields:

- **`gasUsed` / `gasPrice` / fee — never displayed.** The activity list and detail screens
  show type icon, amount, and timestamp only; tapping a tx opens the block explorer
  (`ActivityScreen.tsx:52`). `gasUsed`/`gasPrice` ride along in the normalized object but
  no render path reads them. Returning `gasUsed:"0"` is safe; `gasPrice` we have from the
  block anyway.
- **`txStatus` — only filters, never shown.** The sole consumer is
  `.filter(m => m.nativeTransaction.txStatus === "1")` in evm-module. No success/failed
  badge exists in the feed.

The key fact that makes logs sufficient: **a reverted tx emits no logs.** So every item we
discover via a Transfer log is status-1 by construction. The _only_ thing receipts buy is
filtering **failed, logless txs** (a failed AVAX send, a reverted contract call that
emitted nothing). Without receipts those leak into the feed — but since status and fees are
never rendered, the leak is cosmetically invisible: a failed tx shows up as an ordinary
entry instead of being hidden.

**Accepted v1 divergence from Glacier:** failed logless transactions are not filtered out.
Everything else is faithful.

---

## Architecture: three data planes

| Plane        | Source                                                | Role                                                            | Status in neve      |
| ------------ | ----------------------------------------------------- | --------------------------------------------------------------- | ------------------- |
| **Blocks**   | `newHeads` + `eth_getBlockByNumber`                   | native tx fields (hash, from, to, value, nonce, gas\*, type)    | **have it**         |
| **Logs**     | `eth_subscribe("logs")` live · `eth_getLogs` backfill | **backbone:** all ERC-20/721/1155 transfers + the address index | **flag-gated**      |
| **Receipts** | `eth_getBlockReceipts` (archive upstream)             | optional: exact `txStatus`/`gasUsed`, failed-tx filtering       | **not built**       |

"Flag-gated" means the logs plane's _ingestion_ is built and lands in the stored
`[block, logs]` record under `--ingest-logs`, but the flag is off by default and off in
production, and no index reads it yet.

There is no "receipts subscription" in JSON-RPC — the streaming types are `newHeads`,
`logs`, `newPendingTransactions`, `syncing`. `logs` _is_ the streaming analog, and it
carries exactly the transfer data we need.

## Field provenance under logs-first

Per `TransactionDetailsV2` (full schema in [Reference](#deep-dive-listtransactionsv2)):

| Field                                     | Source          | Logs-first handling                                         |
| ----------------------------------------- | --------------- | ----------------------------------------------------------- |
| `nativeTransaction` block/tx fields       | block body      | as-is (have it)                                             |
| `nativeTransaction.gasPrice`              | block body (tx) | real value                                                  |
| `nativeTransaction.gasUsed`               | receipt         | **placeholder `"0"`** (never rendered)                      |
| `nativeTransaction.txStatus`              | receipt         | **always `"1"`** (logless-failed leak accepted)             |
| `nativeTransaction.method`                | derive          | `methodHash` = first 4 bytes of input (free); name optional |
| `erc20/721/1155Transfers`                 | **logs**        | decode Transfer events                                      |
| token `name`/`symbol`/`decimals`          | contract        | lazy `eth_call`, cached (`token_meta`)                      |
| token `logoUri`/`price`/`tokenReputation` | off-chain       | omit in v1; `filterSpamTokens` = no-op                      |
| `internalTransactions`                    | traces          | omit — wallet ignores it                                    |

## Decoding Transfer logs

Group logs by `(blockNumber, transactionIndex)`; classify by `topic0` and shape:

| Standard                  | `topic0`          | Shape                                                                     |
| ------------------------- | ----------------- | ------------------------------------------------------------------------- |
| ERC-20 `Transfer`         | `0xddf252ad…b3ef` | 3 topics + data → from=`topics[1]`, to=`topics[2]`, value=`data`          |
| ERC-721 `Transfer`        | `0xddf252ad…b3ef` | 4 topics, no data → from=`topics[1]`, to=`topics[2]`, tokenId=`topics[3]` |
| ERC-1155 `TransferSingle` | `0xc3d58168…0f62` | operator/from/to in topics, (id,value) in data                            |
| ERC-1155 `TransferBatch`  | `0x4a39dc06…f7fb` | expand to one transfer per id                                             |

(ERC-20 vs ERC-721 share `topic0`; disambiguate by topic count — 3 ⇒ ERC-20 value, 4 ⇒
ERC-721 indexed tokenId.)

---

## Ingestion

### Live (forward from tip)

- Blocks: existing `newHeads` → fetch path. Unchanged.
- Logs: **as built**, the live path does _not_ use `eth_subscribe("logs")`. It announces the
  block, then pulls that one block's logs with `eth_getLogs(N, N)` and joins the two halves
  through the join buffer into the `[block, logs]` record. A per-block fetch costs one extra
  request per block but keeps the live and backfill paths on the same code
  (`fetch_logs` is shared with `backfill.rs`) and avoids a second subscription to keep alive.
  The `logs` subscription remains a viable optimisation — it is confirmed working on Coreth
  WS — and would remove that request from the tip path.
- Still to build: decode Transfer events out of the stored record and write `tx_transfers` +
  `addr_txs` in the same atomic batch as the block's other indexes.

### Backfill (historical depth)

- **`--backfill-floor <height>` already exists** (`src/main.rs:179`); `--backfill-floor 0`
  mirrors the whole chain. So historical depth needs no new CLI — just the target height.
- Blocks backfill: existing path, throttled `40ms` (~25 req/s) on public endpoints, `0` in
  `--mirror-from`.
- Logs backfill: `eth_getLogs` in **2048-block chunks** (measured cap on the public
  endpoint), joined to the backfilled blocks by `blockNumber`.

**Sizing (measured, mainnet, 2026-06):** tip ≈ 86.97M, ~1.1 s/block ⇒ ~75.4k blocks/day,
**~27.5M blocks/yr**; one year ago ≈ height **59–60M**. A year of logs via `eth_getLogs` is
**~13.4k requests** (27.5M ÷ 2048) — request-count bound, **~9 min even at 25 req/s** — vs
27.5M per-block receipt calls. Public endpoint confirmed: **no `eth_getBlockReceipts`**
(`-32601`), but `eth_getLogs` works. This is why logs-first also wins on backfill.

---

## Storage / index design (fjall)

Three new keyspaces. fjall is an LSM store (sorted keys) — prefix/range scans are native;
neve just hasn't used them yet (all point lookups today).

**Ordered-key rule:** components you range-scan MUST be **big-endian** so byte order matches
numeric order — opposite of neve's current `to_le_bytes()` convention (safe there only
because every existing access is a point lookup).

```text
addr_txs     key = address(20) ‖ BE(u64::MAX - height)(8) ‖ BE(tx_index)(4)   value = ∅
             → "which txs touch this address", newest-first via prefix scan on address.
             Unique per (address,tx): an address that both sent the tx and received a
             transfer in it collapses to one posting = the one-item-per-tx de-dup we want.

tx_transfers key = BE(height)(8) ‖ BE(tx_index)(4)    value = compact encoded transfer list
             → the decoded ERC-20/721/1155 transfers for a tx. Materialized at read time
             instead of re-parsing a receipt blob. Stored once per tx, shared across all
             participant postings.

token_meta   key = contract(20)    value = encoded { name, symbol, decimals, ercType }
             → lazy eth_call cache; logoUri/price/reputation omitted in v1.
```

Native tx fields are NOT duplicated — they come from the block body already in the
blockstore (read the block, take the tx by index).

### Write path (per block, in the existing atomic batch — `src/storage.rs:324`)

```text
participants(tx) = { tx.from, tx.to }                       // block body
                 ∪ { from, to of each decoded Transfer log } // logs plane
for addr in participants(tx):
    batch.insert(addr_txs, addr ‖ BE(MAX-height) ‖ BE(tx_index), ∅)
if tx has transfers:
    batch.insert(tx_transfers, BE(height) ‖ BE(tx_index), encode(transfers))
```

The transfer side **requires logs** — a pure-recipient address appears only in a log, never
in the block body. (This is the same reason a reindex over historical depth must pull logs,
not just blocks.)

### Read path (`listTransactionsV2`)

```text
1. prefix-scan addr_txs on address(20), take pageSize keys      // newest-first
2. each key → (height, tx_index)
3. read block body (blockstore) → native tx fields
4. get tx_transfers[height,tx_index] → decode → transfer arrays  (∅ if none)
5. fill gasUsed:"0", txStatus:"1"; enrich tokens from token_meta
6. assemble TransactionDetailsV2
```

### Pagination — keyset, not offset

The opaque `pageToken` encodes the last `(height, tx_index)` returned; resume with a
`.range()` after that key. O(pageSize) regardless of depth — strictly better than the
reference's 1-indexed offset paging (which computes totalCount/totalPages, O(offset)). The
wallet treats `pageToken` as opaque, so we choose the encoding.

### Storage cost

Blocks are the mirror's existing footprint (~690 GB/yr as _logical_ JSON; the blockstore
zstd-compresses it to ~140–230 GB/yr on disk) — not
new. The **history index adds** roughly: `tx_transfers` (~480M transfer logs/yr, compact
≈ tens of GB) + `addr_txs` postings (~tens of GB) + `token_meta` (negligible). Order
**~70–90 GB/yr on top of the block mirror** — versus the ~1 TB/yr that storing full receipt
JSON would have cost. Receipts stay optional precisely because we don't pay that.

---

## Phasing

1. **v1 — logs-first feed.** `eth_subscribe("logs")` + `eth_getLogs` backfill; `addr_txs` +
   `tx_transfers`; native fields from blocks; `txStatus:"1"`, `gasUsed:"0"`; token metadata
   via `eth_call` cache; `filterSpamTokens` no-op. Serves `listTransactionsV2` end-to-end.
2. **v2 — fidelity.** Receipts ingest for real `txStatus` (filter failed logless txs) and
   `gasUsed`. This requires an **archive upstream** — it cannot be done against the default
   public endpoint at all ([Non-starters](#receipts-as-the-ingestion-backbone)), so it is
   gated on someone having one. Plus off-chain token price/logo/reputation for real spam
   filtering, which is not a chain-data problem
   ([Non-starters](#off-chain-token-enrichment-and-spam-filtering)).
3. **later — balances (category D).** `:balances` endpoints need synced state — the
   firewood state-layer roadmap, separate effort. See
   [Serving balances from state](#serving-balances-from-state-known-contract-eth_call-synthesis).

## XP surface (X and P chains)

_Summary only; the full inventory, gap analysis, and phasing consequences live in
[`p-chain-indexing-plan.md` §Core-wallet coverage](p-chain-indexing-plan.md)._

The wallet does **not** reach the X- and P-chains through `evm-module` or through any
endpoint in the tables above. It uses `@avalabs/avalanche-module`, whose entire Glacier
surface is two REST endpoints — both parameterized by `blockchainId`, so one implementation
serves **P and X**:

| Endpoint | Drives |
| --- | --- |
| `primaryNetworkTransactions.listLatestPrimaryNetworkTransactions` | XP activity feed, and the whole stake list (`EarnService.ts:370`, paged to exhaustion with `txTypes=[ADD_PERMISSIONLESS_DELEGATOR_TX, ADD_DELEGATOR_TX]`) |
| `primaryNetworkBalances.getBalancesByAddresses` | P and X balances, 8-bucket decomposition, on every account refresh |

Everything else the wallet needs on XP is **direct node RPC** through avalanchejs
(`getUTXOs`, `getAtomicUTXOs`, `getFeeState`, `getCurrentValidators`, `getCurrentSupply`,
`getTxStatus`, `issueTx`) — the transaction-construction and write path, which is
permanently upstream. Unlike the C-chain story, where `listTransactionsV2` alone makes the
feed work, **an XP wallet can never point at neve alone**; it points at an api-worker that
fronts neve for the read half.

Three structural differences worth knowing before reusing this document's designs on XP.
The P-chain plan works each of them out in full; these are the pointers:

- **The `addr_txs` shape here is single-address.** Both XP endpoints take a CSV address
  _list_ and return one merged, paged result, because Core queries its whole BIP44 XP
  address set at once. Reuse needs a k-way merge across k prefix cursors. The `pageToken`
  stays simple, though: the merge key is a global ordering, so one `(height, tx_index)`
  cursor re-seeks every scan and cross-address de-dup falls out for free.
- **Server-side `txTypes`/`startTimestamp` filtering is required**, not optional. On the
  P-chain the type is a _derived_ value — there is no `txType` field in the JSON at all —
  and it lives in the index value rather than the key, since the wallet's selective query
  pages to exhaustion while the unfiltered feed would otherwise need a ~20-way merge.
- **Two of the eight balance buckets (`atomicMemory*`) aren't in P-chain blocks at all** —
  they're shared-memory atomic UTXOs. A cross-chain join against C/X export txs would
  reconstruct them in principle, but C-chain exports are _atomic_ transactions that never
  appear in `eth_getBlockByNumber`, so that leg needs an ingest source neve does not have.
  It is a declared divergence rather than a planned feature.

A fourth difference has no analog here: on the P-chain, **staked principal is not in the
UTXO set while it is staked**, so a balance is a union of a UTXO scan and a separate
active-stake index rather than a single scan.

## Serving balances from state: known-contract `eth_call` synthesis

> **Status: design note / not built.** Depends on the firewood state-layer
> ([`StreamingChangeProofs.md`](StreamingChangeProofs.md)). Captured here because it's the
> natural way to serve the category-D balance endpoints (and the wallet's token-balance
> reads) without running an EVM.

### The idea

The state-layer roadmap deliberately scopes the mirror to **non-executing** reads —
`eth_getBalance`, `eth_getStorageAt`, `eth_getProof`, `eth_getTransactionCount`,
`eth_getAssetBalance` — and routes `eth_call` to full nodes
([`StreamingChangeProofs.md:202`](StreamingChangeProofs.md)). But the calls the wallet
actually makes against token contracts are a tiny, fixed set of **pure getters whose return
value is a single storage slot** (or a trivial function of one). For those, an `eth_call`
is just `eth_getStorageAt` at a computed slot plus ABI framing — no interpreter, no gas, no
contract bytecode needed. We can answer them directly from the local Firewood state for an
**allowlist of known, heavy-hit contracts**, and 421-fall back everything else to the
full-node pool.

This does **not** make neve a general `eth_call` server. It special-cases a handful of
`(selector, storage-layout)` pairs on a curated contract set. Anything off the allowlist —
unknown selector, unknown contract, proxy with a non-standard layout — is misdirected
(HTTP 421), exactly as the state-layer design already does for unserveable supported calls
([`StreamingChangeProofs.md:205`](StreamingChangeProofs.md)).

### Selectors we can synthesize

| Standard | Call | Slot derivation | Notes |
| --- | --- | --- | --- |
| ERC-20 | `balanceOf(addr)` → `uint256` | `keccak256(pad32(addr) ‖ pad32(slot_balances))` | `slot_balances` is per-contract (commonly `0`, but **must be discovered**, see below). |
| ERC-20 | `totalSupply()` → `uint256` | a fixed slot | rarely the bottleneck; include for completeness. |
| ERC-20 | `decimals()`/`symbol()`/`name()` | fixed slots / not in storage at all | constant for a contract — better served from the existing `token_meta` `eth_call` cache than re-derived. |
| ERC-721 | `balanceOf(owner)` → `uint256` | `keccak256(pad32(owner) ‖ pad32(slot_balances))` | count of NFTs held. |
| ERC-721 | `ownerOf(tokenId)` → `address` | `keccak256(pad32(tokenId) ‖ pad32(slot_owners))` | |
| ERC-1155 | `balanceOf(addr, id)` → `uint256` | `keccak256(pad32(id) ‖ keccak256(pad32(addr) ‖ pad32(slot_balances)))` (nested mapping) | two-level mapping → two keccaks. |

The state layer already documents the storage-key shape this rides on:
`keccak256(addr) ‖ keccak256(slot)`, 64 bytes
([`StreamingChangeProofs.md:30`](StreamingChangeProofs.md)).

### Why an allowlist, not generic synthesis

Generic synthesis over arbitrary contracts does not work — the reasons are in
[Non-starters](#generic-eth_call-synthesis). What survives is a **curated registry** mapping
`contract → {standard, slot_balances, slot_owners, …, verified_against_node}` for the top-N
contracts by RPC hit count. Each entry is validated once at registration time by comparing
the synthesized result against a real `eth_call` on a full node across several
addresses/blocks; only exact matches get allowlisted. The "heavy-hit" framing is what makes
a hand-curated set worthwhile — a small number of contracts (USDC, USDT, WAVAX, major NFT
collections, …) covers a large share of `eth_call` balance traffic.

### Mechanics

1. **Parse** the inbound `eth_call`: `{to, data}`. Reject (421) if `to` is not in the
   registry or `data`'s 4-byte selector isn't a synthesizable getter for that entry's
   standard.
2. **Decode** the ABI args from `data` (just `address` / `uint256` words — cheap, no full
   ABI decoder needed).
3. **Derive** the storage slot per the table, using the registry's `slot_*` for that
   contract.
4. **Read** the slot from local Firewood at the requested block's state root (same path as
   `eth_getStorageAt`). Missing slot ⇒ `0x0…0`, which is the correct EVM semantic for an
   unset balance.
5. **ABI-encode** the result (left-pad to 32 bytes for `uint256`/`address`) and return it
   as the `eth_call` result.
6. **Fall back (421)** on: block tag outside Firewood's revision window, `"latest"` while
   the instance is briefly behind head, or any registry miss — identical to the state
   layer's existing misdirected-request handling.

### Caveats / open questions

- **Block-tag resolution.** Inherits the state layer's height→root limits (~100k
  revisions); historical `eth_call` beyond the window falls back to full nodes
  ([`StreamingChangeProofs.md:248`](StreamingChangeProofs.md)).
- **Registry maintenance is the real cost.** Layouts must be re-verified after any contract
  upgrade; a proxy that changes implementation can silently invalidate an entry. Periodic
  re-validation (synthesized vs node `eth_call`) should gate continued allowlisting, and a
  mismatch should auto-evict the entry to 421-fallback rather than serve a wrong balance.
- **`state`/`override` params and non-trivial calldata** are never synthesizable — any
  `eth_call` carrying state overrides or unrecognized calldata is an immediate 421.
- **Relationship to category-D `:balances` list endpoints.** Those return _all_ tokens an
  address holds, which requires the transfer index (the logs backbone) to know _which_
  contracts to probe, then a synthesized `balanceOf` per candidate contract. So this note is
  the per-contract read primitive; the list endpoints compose it over the address's
  token set from `addr_txs`.

## Open questions

- Live log/block ordering: do logs ever arrive before their block over the WS sub? Need a
  small reorder buffer keyed by blockNumber.
- `eth_getLogs` payload size per 2048-block chunk (all-topics ~80k logs) vs topic-filtered
  (~36k) — pick the filter to bound backfill bandwidth.
- `token_meta` cold-start: a page full of first-seen tokens triggers N `eth_call`s; batch or
  warm them.

---

## Reference

_The API-surface analysis, the full `listTransactionsV2` schema, the current fjall layout,
and the endpoint capability measurements that the design above rests on._

### Glacier API surface — what the wallet uses

Glacier is implemented by `data-service` in `ac-data-monorepo`, backed by ClickHouse
(`raw_blocks`, `raw_transactions`, `raw_logs`). core-mobile reaches it via
`@avalabs/glacier-sdk`, through two modules, and inspecting both published bundles gives the
complete list of `glacierSdk.*` namespaces the wallet touches:

| Module | Namespaces called | Covers |
| --- | --- | --- |
| `@avalabs/evm-module@3.8.1` | `evmTransactions` (1), `evmBalances` (4), `evmChains` (1), `nfTs` (2) | C-chain and other EVM chains |
| `@avalabs/avalanche-module` | `primaryNetworkTransactions` (1), `primaryNetworkBalances` (1) | X- and P-chains |

EVM activity flows through `evm-module`'s `getTransactionsFromGlacier` →
`glacierSdk.evmTransactions.listTransactionsV2`, with an Etherscan fallback
(`core-etherscan-sdk`) for chains Glacier doesn't index. The two modules share no Glacier
endpoints, which is why the EVM design here and the XP design in the P-chain plan can proceed
independently.

#### Categories

- **A — Have it.** Served from raw mirrored block data we already store.
- **B — Need an index.** Data is in the blocks we have, but requires a new index
  (by address or tx hash) to serve efficiently.
- **C — Need receipts + index.** Requires parsing receipts/logs (Transfer events)
  plus an index keyed by address/token.
- **D — Need state.** Requires synced state (balances, contract code, classification).
- **E — Out of scope / other.** Off-chain metadata or config.
- **F — Primary network (X/P-Chain).** A different chain and a different wallet package;
  see [XP surface](#xp-surface-x-and-p-chains).

#### Confirmed used by core-wallet

| API                                                                                             | Description                                                                                                                                                                                                                                                                            | Category                   |
| ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `evmTransactions.listTransactionsV2` — `GET /chains/{chainId}/addresses/{address}/transactions` | **The EVM activity feed.** Combined address history: `nativeTransaction` + `erc20Transfers`/`erc721Transfers`/`erc1155Transfers` nested per item. Called with `{chainId, address, pageToken, pageSize, filterSpamTokens:true}`; client filters `txStatus==="1"` and drops zero-amount. | C                          |
| `evmBalances.getNativeBalance` — `…/balances:getNative`                                         | Native balance for an address                                                                                                                                                                                                                                                          | D                          |
| `evmBalances.listErc20Balances` — `…/balances:listErc20`                                        | ERC-20 balances for an address                                                                                                                                                                                                                                                         | D                          |
| `evmBalances.listErc721Balances` — `…/balances:listErc721`                                      | ERC-721 holdings for an address                                                                                                                                                                                                                                                        | D                          |
| `evmBalances.listErc1155Balances` — `…/balances:listErc1155`                                    | ERC-1155 holdings for an address                                                                                                                                                                                                                                                       | D                          |
| `evmChains.supportedChains`                                                                     | List EVM chains Glacier indexes (used to decide Glacier vs Etherscan fallback)                                                                                                                                                                                                         | E (config)                 |
| `evmChains.listAddressChains` _(called by core-mobile directly, not evm-module)_                | Which chains an address has activity on                                                                                                                                                                                                                                                | C (cross-chain addr index) |
| `nfTs.getTokenDetails`                                                                          | ERC-721/1155 token metadata                                                                                                                                                                                                                                                            | E (off-chain metadata)     |
| `nfTs.reindexNft`                                                                               | Trigger NFT metadata re-index                                                                                                                                                                                                                                                          | E (off-chain metadata)     |
| `primaryNetworkTransactions.listLatestPrimaryNetworkTransactions` _(avalanche-module)_          | XP activity feed **and** the whole stake list. `blockchainId` path param ⇒ serves **P and X** from one handler                                                                                                                                                                         | F (primary network)        |
| `primaryNetworkBalances.getBalancesByAddresses` _(avalanche-module)_                            | P and X balances, 8-bucket decomposition; called on every account refresh                                                                                                                                                                                                              | F (primary network)        |

**F — primary network (X/P).** neve mirrors the P-chain, so these are in scope, but they are
tracked in [`p-chain-indexing-plan.md`](p-chain-indexing-plan.md) rather than here.

#### Offered by Glacier but NOT used by core-wallet

Kept for reference — these exist in `data-service` but the wallet doesn't call them.

| API                                                                | Description                                                                 | Category |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------- | -------- |
| `GET /chains/{chainId}/transactions/{txHash}`                      | Single tx by hash — wallet uses node RPC `eth_getTransactionByHash` instead | (n/a)    |
| `GET /chains/{chainId}/addresses/{address}/transactions:getNative` | Native-only address history — wallet uses combined V2 instead               | B        |
| `…/transactions:listErc20` / `:listErc721` / `:listErc1155`        | Per-standard transfer lists — folded into combined V2                       | C        |
| `GET /chains/{chainId}/tokens/{tokenAddress}/transfers`            | All transfers for a token                                                   | C        |
| `GET /chains/{chainId}/blocks/{blockId}/transactions`              | Txs in a block                                                              | A        |
| `GET /chains/{chainId}/transactions`                               | Chain-wide tx list                                                          | A        |
| `GET /chains/{chainId}/addresses/{address}`                        | Address details / contract classification                                   | D        |

### Deep dive: `listTransactionsV2`

The one endpoint that backs the entire EVM activity feed. Schemas from
`@avalabs/glacier-sdk@3.1.0-alpha.87`; semantics confirmed against the `data-service`
reference implementation in `ac-data-monorepo`.

#### Request

```text
GET /v2/chains/{chainId}/addresses/{address}/transactions
```

| Param              | In    | Notes                                                |
| ------------------ | ----- | ---------------------------------------------------- |
| `chainId`          | path  | EVM chain id (e.g. `43114`)                          |
| `address`          | path  | wallet address (0x…)                                 |
| `pageToken`        | query | opaque cursor from previous page                     |
| `pageSize`         | query | 1–100, default 10 (wallet passes its own page size)  |
| `startBlock`       | query | inclusive range start (optional)                     |
| `endBlock`         | query | exclusive range end (optional)                       |
| `filterSpamTokens` | query | wallet sends `true`                                  |
| `sortOrder`        | query | `asc`/`desc` by timestamp; wallet wants newest-first |

Wallet only ever sends `chainId, address, pageToken, pageSize, filterSpamTokens:true`.

#### Response: `ListTransactionDetailsResponseV2`

```text
{ nextPageToken?: string, transactions: TransactionDetailsV2[] }

TransactionDetailsV2 {
  nativeTransaction: NativeTransaction        // always present
  erc20Transfers?:   Erc20TransferDetailsV2[]
  erc721Transfers?:  Erc721TransferDetails[]
  erc1155Transfers?: Erc1155TransferDetails[]
  internalTransactions?: InternalTransactionDetails[]   // wallet ignores; needs traces
}
```

**Field provenance** (what neve must source each field from):

| Field group                                | Fields                                                                             | Source                                                                                         |
| ------------------------------------------ | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `NativeTransaction` block bits             | blockNumber, blockTimestamp(+Ms), blockHash, blockIndex, chainId                   | **block header / body** (have it)                                                              |
| `NativeTransaction` tx bits                | txHash, txType, nonce, value, gasLimit, gasPrice, from.address, to.address, input¹ | **tx in block body** (have it)                                                                 |
| `NativeTransaction` receipt bits           | `txStatus`, `gasUsed`                                                              | **receipt** (optional under logs-first)                                                        |
| `method`                                   | callType, methodHash, methodName                                                   | derive: methodHash = first 4 bytes of input; methodName = 4-byte signature lookup (enrichment) |
| ERC-20/721/1155 transfers                  | from, to, logIndex, value/tokenId                                                  | **decode Transfer logs**                                                                       |
| `erc20Token`/`erc721Token`/`erc1155Token`  | name, symbol, decimals (required)                                                  | token registry: read `name()`/`symbol()`/`decimals()` per contract, cache                      |
| token logoUri, price, tokenReputation      | —                                                                                  | off-chain (CoinGecko / spam list); optional, fills `filterSpamTokens`                          |
| `RichAddress` name/symbol/decimals/logoUri | —                                                                                  | optional contract-label enrichment; only `address` is required                                 |
| `internalTransactions`                     | —                                                                                  | debug traces — **not needed** (wallet ignores; reference nested view omits them)               |

¹ `NativeTransaction` (the V2 list shape) actually omits `input`; the full `input`/fee
fields live on `FullNativeTransactionDetails` used by single-tx `getTransaction`. The list
view carries `value` and `method` but not raw calldata.

#### The semantic that drives the index

Confirmed in `transactions.go:207-226` (default nested filter): the address selects
**which transactions** appear (address = `from`/`to` of the tx, **or** `from`/`to` of any
ERC-20/721/1155 Transfer log in the tx). Once a tx matches, the response embeds **all**
transfers in that tx — not only those involving the queried address (it calls
`fetchDecodedLogsForTx(txHash)` and includes every decoded transfer).

Ordering: **block number descending, then transaction index descending.**

### Endpoint capability measurements (public mainnet endpoint, 2026-06)

`https://api.avax.network/ext/bc/C/rpc` — neve's default mainnet upstream.

- `eth_getBlockReceipts` → **`-32601` not supported.** This is the measurement that rules
  out bulk receipt ingestion against the default endpoint
  ([Non-starters](#receipts-as-the-ingestion-backbone)).
- `debug_getRawBlock` → not supported.
- `eth_getTransactionReceipt` → works, ~1.7 KB/receipt JSON.
- `eth_getLogs` → works, **max range 2048 blocks/request**; ~39 logs/block all-topics,
  ~17.5 ERC `Transfer` logs/block.
- Block JSON (`eth_getBlockByNumber`, full txs): ~25 KB/block avg (7–68 KB, ~22 tx/block).
- Block rate ~1.1 s/block ⇒ ~27.5M blocks/yr; tip ≈ 86.97M; ~1 yr ago ≈ height 59–60M.
- neve stores blocks as **JSON** (`serde_json::to_vec`), which the blockstore then
  zstd-compresses (level 3) on write; blockstore is the published `blockdb` crate.

### Current fjall layout

| Keyspace                         | Key                  | Value                          | Access |
| -------------------------------- | -------------------- | ------------------------------ | ------ |
| `hash_to_height`                 | block hash (32B raw) | height (u64 **LE**)            | point  |
| `tx_to_block`                    | tx hash (32B raw)    | height (u64 LE) ‖ idx (u32 LE) | point  |
| `meta`                           | string               | string                         | point  |
| blockstore (separate, not fjall) | height               | block JSON bytes               | point  |

The LE encoding is safe **only** because every current access is a point lookup — hence the
big-endian rule for any new range-scanned key
([Non-starters](#little-endian-keys-for-anything-range-scanned)).

---

## Non-starters

Approaches that were worked out far enough to evaluate and then rejected, plus data that
cannot be served from mirrored chain data at all. Recorded so they are not re-proposed, with
what would reopen each.

### Receipts as the ingestion backbone

The obvious design for an activity feed is to ingest receipts: they carry `txStatus`,
`gasUsed`, and — since an Ethereum receipt _contains_ its `logs` array, with no separate logs
object — every Transfer event too. One fetch per block would supply all three data planes.
Three things kill it as the backbone:

- **The default public endpoint cannot serve them in bulk.** `eth_getBlockReceipts` returns
  `-32601` on `api.avax.network/ext/bc/C/rpc`, and `debug_getRawBlock` is likewise absent.
  Only per-transaction `eth_getTransactionReceipt` works (~1.7 KB each), so a receipts-first
  mirror needs an **archive node** — a hard infrastructure dependency the logs path does not
  have.
- **Backfill cost is off by three orders of magnitude.** A year of history is ~27.5M blocks.
  Via `eth_getLogs` at the measured 2048-block cap that is ~13.4k requests — request-count
  bound, ~9 minutes even at 25 req/s. Via receipts it is ~27.5M calls.
- **Storage cost is off by an order of magnitude.** Storing full receipt JSON runs about
  1 TB/yr, against ~70–90 GB/yr for the logs-derived index.

And the fidelity receipts buy is nearly invisible: `gasUsed`/`gasPrice` are never rendered by
the wallet, and `txStatus` only ever feeds a filter. Because **a reverted transaction emits no
logs**, everything discovered through a Transfer log is status-1 by construction, so the only
real loss is failed _logless_ transactions leaking into the feed as ordinary entries — the
accepted v1 divergence.

**Would reopen if:** an archive upstream is available anyway, in which case receipts become a
cheap fidelity upgrade (v2 in [Phasing](#phasing)) rather than a prerequisite — and logs can
then be lifted from the stored receipt blob instead of subscribed to.

### Generic `eth_call` synthesis

Synthesizing `eth_call` from raw storage slots works for a curated allowlist
([Serving balances from state](#serving-balances-from-state-known-contract-eth_call-synthesis))
but cannot be generalised, because every slot derivation assumes **canonical Solidity storage
layout** — sequential declaration slots, standard mapping hashing:

- **Slot numbers vary per contract.** `_balances` is slot 0 in one token and slot 3 in
  another, depending on declaration order and inheritance. There is no on-chain way to
  recover the layout from the address alone.
- **Proxies and upgradeable contracts** (OpenZeppelin transparent/UUPS, diamonds) put logic
  behind `delegatecall` and may use non-sequential or namespaced slots (ERC-7201). The
  bytecode at the address is not the storage owner.
- **Non-standard or packed layouts.** Some tokens pack balance and flags into one slot, or
  override `balanceOf` to compute it (rebasing, fee-on-transfer accounting). Reading the slot
  returns a confidently wrong number — these genuinely need execution.

The failure mode is what makes this a non-starter rather than a partial win: a wrong storage
guess does not error, it returns a plausible balance. Hence the allowlist, per-entry
validation against a real node, and auto-eviction to 421 on any mismatch.

**Would reopen if:** never in general. It narrows further only in the safe direction — more
allowlist entries, each individually verified.

### `internalTransactions`

`TransactionDetailsV2` has an `internalTransactions` array, which requires debug traces —
an execution product neve has no way to produce. The wallet ignores the field and the
reference implementation's nested view omits it, so it is simply left out.

**Would reopen if:** a consumer actually reads it, which would make this a tracing problem
rather than an indexing one.

### Off-chain token enrichment and spam filtering

`logoUri`, `price`, and `tokenReputation` on the token objects, and therefore the real
behaviour of `filterSpamTokens`, come from off-chain sources (CoinGecko, curated spam lists) —
not from chain data at any depth. v1 omits them and treats `filterSpamTokens` as a no-op.
Token `name`/`symbol`/`decimals` _are_ on-chain and come from the `token_meta` `eth_call`
cache, so the required fields are covered.

**Would reopen if:** neve grows an off-chain enrichment plane, which is a different kind of
service with its own freshness and trust story. Better fronted by the api-worker.

### Offset pagination

Glacier's reference implementation pages with a 1-indexed offset and computes
`totalCount`/`totalPages`, which is `O(offset)` — it re-walks the history to reach deep pages.
The wallet treats `pageToken` as opaque, so neve is free to encode a keyset cursor
(the last `(height, tx_index)`) and resume with a range scan, which is `O(pageSize)`
regardless of depth.

**Would reopen if:** a consumer needs `totalCount`, which keyset paging cannot produce
cheaply. No wallet path does.

### Little-endian keys for anything range-scanned

neve's existing fjall keys use `to_le_bytes()`, which is safe **only** because every current
access is a point lookup. Any key component that is range-scanned must be **big-endian** so
byte order matches numeric order; the newest-first `addr_txs` key additionally stores
`u64::MAX - height` so a forward scan yields descending heights. Reusing the LE convention on
a scanned index silently returns results in the wrong order.

**Would reopen if:** never. The existing LE point-lookup keys stay as they are; the rule
applies to new scanned indexes.
