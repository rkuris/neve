# blockstream-example

A small async Rust client that subscribes to Avalanche C-chain `newHeads` over
WebSocket, fetches each full block from the public HTTPS RPC, and persists it to
an [`ava-labs/blockstore`](https://github.com/ava-labs/blockstore) instance with
a [`fjall`](https://github.com/fjall-rs/fjall) sidecar index from block hash to
block height. A jsonrpsee server exposes a small read-only subset of the
Ethereum JSON-RPC API backed by that storage.

This is a sketch toward the lightweight mirror client described in
`avalanchego/StreamingChangeProofs.md` — it covers the block-tail half. State
mirroring via change proofs is not implemented here.

## Endpoints used

- WebSocket subscription: `wss://api.avax.network/ext/bc/C/ws`
- HTTPS RPC (block bodies): `https://api.avax.network/ext/bc/C/rpc`

## Storage layout

`$BLOCKSTORE_DIR` (default `./blockstore-data`):

- `blocks/` — blockstore data + index files (`blockdb.idx`, `blockdb_N.dat`).
  Keyed by `u64` height; on first run, `minimum_height` is anchored at the
  first observed block.
- `index/` — fjall keyspace with a `hash_to_height` partition mapping
  `32-byte blockHash → 8-byte u64 LE`.

Block bodies are stored as the **JSON** returned by `eth_getBlockByNumber(num, true)`.
This is debuggable and trivial to serve back; the format will need to switch to
RLP-encoded `*types.Block` (matching `graft/coreth/plugin/evm/wrapped_block.go`'s
`Bytes()`) if/when this needs to interop with a Go-side bootstrap snapshot.

## JSON-RPC methods

Listening on `$RPC_ADDR` (default `127.0.0.1:8545`).

- `eth_blockNumber` → highest stored height (hex).
- `eth_getBlockByNumber(tag, fullTx)` — supports `"latest"`, `"finalized"`,
  `"safe"`, and `0x`-prefixed hex heights. `"earliest"` and `"pending"` are
  rejected. `fullTx=false` collapses the transactions array to a list of
  hashes.
- `eth_getBlockByHash(hash, fullTx)` — fjall lookup → blockstore read.

Returns `null` (per spec) for heights/hashes not present in the local store.

## Build

The `blockstore` crate is a private GitHub repo (`ava-labs/blockstore`), so the
build needs an SSH-authenticated git fetch. `.cargo/config.toml` already sets
`net.git-fetch-with-cli = true`; you just need an SSH key registered with
GitHub.

```sh
cargo build --release
```

## Run

```sh
cargo run --release
```

Optional env vars:

- `BLOCKSTORE_DIR` — storage root (default `./blockstore-data`).
- `RPC_ADDR` — JSON-RPC listen address (default `127.0.0.1:8545`).
- `RUST_LOG` — tracing filter (default `info`).

Example queries (in another terminal):

```sh
# Current head
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  http://127.0.0.1:8545

# Block by height, tx-hashes only
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest", false]}' \
  http://127.0.0.1:8545

# Block by hash, full transactions
curl -sX POST -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["0x<hash>", true]}' \
  http://127.0.0.1:8545
```

## Inspecting the store

Install the upstream CLI:

```sh
cargo install --git ssh://git@github.com/ava-labs/blockstore.git \
  --branch main blockstore-cli \
  --config net.git-fetch-with-cli=true
```

Then:

```sh
blockstore-cli -d ./blockstore-data/blocks get --height <N>   # hex-dump a block
blockstore-cli -d ./blockstore-data/blocks copy --target <dir>  # clone the store
```

## Layout

- `src/main.rs` — bootstrap, WebSocket ingester, block-body fetcher with
  backoff retries.
- `src/storage.rs` — `Storage` handle wrapping blockstore + fjall + atomic
  high-water mark.
- `src/rpc.rs` — jsonrpsee server (`eth_*` methods).

## Known limitations

- **No reconnect.** If the WebSocket drops, the ingester exits. Resume is
  height-monotone safe (the blockstore keeps what it had), but you have to
  restart the process.
- **Best-effort fork handling.** If `eth_getBlockByNumber`'s body hash doesn't
  match the `newHeads` hash, the block is skipped. C-chain finality means this
  is rare.
- **Numeric-only block tags below ingest start return `null`.** The mirror
  doesn't backfill history; it only stores from the first `newHead` it sees.
- **JSON storage**, not RLP — see "Storage layout" above.
