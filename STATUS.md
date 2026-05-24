# Project status — picking this back up

## Where we are

A working C-chain block streamer + JSON-RPC server. Subscribes to `newHeads`
over WebSocket, fetches full block bodies via HTTPS, persists to blockstore
with a fjall sidecar (hash→height), and serves a small read-only subset of
the Ethereum JSON-RPC API.

This is the "block-tail half" of the lightweight mirror described in
`../avalanchego/StreamingChangeProofs.md`. The state-mirror half (Firewood
change proofs) is not started.

## What runs

```sh
cargo run                              # WS ingest + RPC server
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  http://127.0.0.1:8545
```

- WS reconnect with exponential backoff on disconnect.
- `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getBlockByHash`.
- HTTP **421** (Misdirected Request) when a hash/height isn't in our store,
  per the api-worker contract in StreamingChangeProofs.md.
- Storage tip uses `Store::height_highwater()` (added in
  ava-labs/blockstore#17, already merged) so gaps from disconnect/restart
  don't pin the reported tip below the actual tip.
- **Backfill worker** running alongside the ingester. Closes both
  within-session gaps (newHeads dropped a frame:
  `max_contiguous_height < height_highwater`) and cold-restart gaps
  (process was down: local high-water < upstream `eth_blockNumber`). Target
  each iteration is `max(local_high_water, upstream_tip)`; the worker walks
  `max_contiguous_height + 1` upward until it catches up. Logs "backfill
  starting / progress / caught up" with `contiguous`, `target`, `behind`,
  and elapsed time — fields chosen so a future ETA calculation slots in
  without restructuring.

## Layout

- `src/main.rs` — bootstrap + WebSocket ingester. `run_session` is split
  into `connect_and_subscribe`, `next_ws_event`/`classify_frame`,
  `fetch_full_block`, `persist_block`.
- `src/storage.rs` — `Storage` handle (blockstore + fjall + lazy open).
- `src/rpc.rs` — jsonrpsee `EthApi` impl.
- `src/middleware.rs` — tower layer that rewrites 200→421 on null result.

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
  *history* below the first newHead we observe; the store's anchor
  (`minimum_height`) is set on cold start to that first observed height,
  and the backfill worker only fills forward from there.
- **No ETA on long backfills yet.** Progress logs include `contiguous`,
  `target`, `behind`, and elapsed time — enough fields to derive an ETA
  later from rate, but the calculation isn't wired up.

## Next steps (from the API-tier discussion)

**Tier 1 — zero extra work, just dispatch into stored block JSON.** Roughly
an hour total:
- `eth_chainId` (hardcode 0xa86a)
- `net_version` (43114), `web3_clientVersion`, `eth_protocolVersion`
- `eth_syncing` (false, or a synthesized object)
- `eth_getBlockTransactionCountByNumber` / `ByHash`
- `eth_getTransactionByBlockNumberAndIndex` / `ByBlockHashAndIndex`
- `eth_getUncleCountByBlock*` (always 0x0 on C-chain)

**Tier 2 — one new fjall partition during ingest:**
- `eth_getTransactionByHash`: index `tx_hash → (height, tx_index)` per
  block. Two reads on lookup. ~20 LOC.

**Tier 3 — needs an extra HTTPS fetch per block:**
- `eth_getTransactionReceipt`: one `eth_getBlockReceipts(num)` call per
  block during ingest; store alongside the block. Roughly doubles
  bandwidth.
- `eth_getLogs`: needs receipts *plus* a topic/address index. Skip until
  there's a reason — the StreamingChangeProofs doc explicitly excludes
  this from the mirror's served set.

**Tier 4 — needs state mirror (Firewood change proofs):**
- `eth_getBalance`, `eth_getStorageAt`, `eth_getProof`, `eth_getCode`,
  `eth_getTransactionCount` (nonce), `eth_call`, `eth_estimateGas`. This
  is the change-proof half of the doc; out of scope here.

**Quality-of-life:**

- ETA for long backfill stretches (rate × remaining; fields already logged).
- Drop `BLOCKSTORE_DIR/index/journals/` from the git tree (already in
  `.gitignore`; verify after a fresh `rm -rf blockstore-data && cargo run`).
- Consider RLP body format if/when bootstrap interop with a Go syncer
  becomes a concrete requirement.

## Branch state

- `master` carries everything above. jj-managed, colocated with git.
- One open PR upstream: ava-labs/blockstore#17 — **merged**.
