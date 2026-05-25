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

## JSON-RPC method status

| Method | Tier |
| --- | --- |
| `eth_blockNumber` | Implemented |
| `eth_call` | 4 |
| `eth_chainId` | 0 |
| `eth_estimateGas` | 4 |
| `eth_getBalance` | 4 |
| `eth_getBlockByHash` | Implemented |
| `eth_getBlockByNumber` | Implemented |
| `eth_getBlockTransactionCountByHash` | Implemented |
| `eth_getBlockTransactionCountByNumber` | Implemented |
| `eth_getCode` | 4 |
| `eth_getLogs` | 3 (explicitly excluded by StreamingChangeProofs doc) |
| `eth_getProof` | 4 |
| `eth_getStorageAt` | 4 |
| `eth_getTransactionByBlockHashAndIndex` | Implemented |
| `eth_getTransactionByBlockNumberAndIndex` | Implemented |
| `eth_getTransactionByHash` | Implemented |
| `eth_getTransactionCount` (nonce) | 4 |
| `eth_getTransactionReceipt` | 3 |
| `eth_getUncleByBlockHashAndIndex` | 0 |
| `eth_getUncleByBlockNumberAndIndex` | 0 |
| `eth_getUncleCountByBlockHash` | 0 |
| `eth_getUncleCountByBlockNumber` | 0 |
| `eth_protocolVersion` | 0 |
| `eth_syncing` | 0 |
| `net_version` | 0 |
| `web3_clientVersion` | 0 |

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
- **Tier 3 — needs an extra HTTPS fetch per block.** One
  `eth_getBlockReceipts(num)` call per block during ingest; store
  alongside the block. Roughly doubles bandwidth. `eth_getLogs`
  additionally needs a topic/address index.
- **Tier 4 — needs state mirror (Firewood change proofs).** The
  change-proof half of the StreamingChangeProofs doc; out of scope for
  the block-tail half.

**Quality-of-life:**

- ETA for long backfill stretches (rate × remaining; fields already logged).
- Drop `BLOCKSTORE_DIR/index/journals/` from the git tree (already in
  `.gitignore`; verify after a fresh `rm -rf blockstore-data && cargo run`).
- Consider RLP body format if/when bootstrap interop with a Go syncer
  becomes a concrete requirement.

## Branch state

- `master` carries everything above. jj-managed, colocated with git.
- One open PR upstream: ava-labs/blockstore#17 — **merged**.
