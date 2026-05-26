# Streaming Change Proofs over the C-Chain WebSocket

## Goal

Expose a new `eth_subscribe` topic on the C-chain WebSocket endpoint that pushes a Firewood change proof for every accepted block. The change proof represents the state delta between the previous accepted block's state root and the new one.

The target consumer is a lightweight client (initially a Rust implementation) that does not execute EVM blocks, but mirrors state by applying authenticated deltas. With a current state mirror plus block bodies, it can answer a useful subset of JSON-RPC queries directly without running the VM:

- **Need state, no execution:** `eth_getBalance`, `eth_getStorageAt`, `eth_getProof`, `eth_getTransactionCount`, `eth_getAssetBalance`.
- **Need block bodies only:** the `eth_getBlockBy*`, `eth_getTransactionByBlock*`, and `eth_getRawTransactionByBlock*` families.

Block bodies are out of scope for the change-proof stream itself but are a natural companion (either by polling `eth_getBlockByHash` on each `newHeads` notification, or by a paired block-body subscription as a follow-up).

## Motivation

The point of this is **cost**. A lightweight mirror that consumes change proofs instead of executing blocks is cheaper to run on every axis that matters:

- **CPU.** Applying a CP is dramatically cheaper than executing the block that produced it. Block execution runs the EVM over every transaction, touches storage in unpredictable patterns, computes gas accounting, and re-derives the same state changes the producing node already computed. A CP consumer just writes the resulting K/V pairs into its own Firewood instance — no interpreter, no gas, no transaction ordering logic.
- **Latency.** Downloading and applying a CP completes well before the next block boundary on any reasonable link; full block execution doesn't necessarily. So the mirror keeps up with head with margin to spare, even on modest hardware.
- **Binary size.** A mirror client doesn't need an EVM, doesn't need precompile implementations, doesn't need consensus, doesn't need block validation, doesn't need a mempool. The Rust client is a CP applier, a trie, and a JSON-RPC server. Order-of-magnitude smaller than a full C-chain node.
- **Operational surface.** Less code → fewer bugs, fewer upgrade headaches, smaller attack surface, easier to deploy in environments (edge, embedded, browser-via-WASM) where running a full validator is impractical.

The tradeoff is the supported-methods restriction (see Goal). The cost-benefit case is that the methods we *can* support cover a large enough share of real RPC traffic to make the cheaper mirror worth deploying — see Open Issue 4.

## Why this is tractable

The C-chain Firewood state is a single flat trie (`graft/evm/firewood/base_trie.go`):

- Accounts at key `keccak256(addr)` (32 bytes) → RLP `StateAccount`.
- Storage slots at key `keccak256(addr) || keccak256(slot)` (64 bytes) → slot value.
- Account deletion is a `ffi.PrefixDelete(keccak256(addr))` covering all storage in one operation.

A single `firewood.Database.ChangeProof(prevRoot, newRoot, Nothing, Nothing, maxLength)` call therefore captures every account-level and storage-level change in one delta, with the consumer demuxing by key length. There is no separate per-account storage trie to chase.

## Change proof structure (context)

This is transparent to the Go server code — Firewood's `(*Database).ChangeProof` produces the right shape and `VerifyChangeProof` consumes it — but it's worth understanding what's actually on the wire, because it affects how the Rust client thinks about verification and how byte sizes scale.

A change proof from `R1` to `R2` comes in two shapes depending on whether it was truncated by `maxLength`:

- **Complete (untruncated) proof.** Covers the full key range. Contains *only the K/V changes themselves* — no per-key Merkle proofs. The client holds `R1`, applies the listed changes to its local Firewood, and verifies the resulting root equals the expected `R2`. The root recomputation is the verification; no separate proof material is needed because the entire delta is present.
- **Truncated (chunked) proof.** Covers only `[rangeStart, rangeEnd)` of the key range. Contains the K/V changes within that range **plus edge proofs** (boundary Merkle proofs) so the partial application can be independently authenticated against `R1` and `R2` without knowing what lies outside the range. This is what makes chunking safe: a client can apply chunk-by-chunk and verify each chunk individually.

Practical implications:

- **Byte size per entry differs between the two shapes.** Untruncated proofs are dominated by the raw change data. Truncated proofs add edge-proof overhead — a constant per-chunk cost regardless of chunk size — which means chunked delivery is somewhat less bytes-efficient than a single untruncated frame of the same total content. Worth keeping in mind when tuning `maxLength`: bias toward fewer, larger chunks rather than many small ones.
- **The verification model is the same on the consumer side either way** — call `VerifyChangeProof` (or its Rust equivalent) with the proof and the expected `R2`; the library handles the structural difference internally.
- **The Rust client doesn't need to implement two code paths.** It just calls the Firewood Rust FFI's verify function, which handles both shapes.

## Dependencies

1. **PR #5385** (`feat(firewood/syncer): implement change proof support`) lands. This wires up the Firewood change-proof API and the `*ChangeProof` wrapper type. The new subscription only needs the server-side path: `firewood.Database.ChangeProof(...)` + `(*ffi.ChangeProof).MarshalBinary()`. None of the verify/proposal/commit machinery is needed for the streaming server.
2. **Firewood v0.5.0** published, which exposes `(*Database).ChangeProof`.
3. **C-chain running on the Firewood-backed EVM** (`graft/evm`) in the target deployment.

## Wire protocol

Subscription request (JSON-RPC over WebSocket):

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "eth_subscribe",
  "params": ["changeProofs", { "version": 1 }]
}
```

The `version` param is reserved for future schema evolution. No `start`/`end` key-range filter in v1 — the canonical mirror consumer wants the full diff.

Notification frame:

```json
{
  "jsonrpc": "2.0",
  "method": "eth_subscription",
  "params": {
    "subscription": "0x...",
    "result": {
      "blockNumber": "0x...",
      "blockHash":   "0x...",
      "prevStateRoot": "0x...",
      "newStateRoot":  "0x...",
      "rangeStart":  "0x...",       // omitted for full block
      "rangeEnd":    "0x...",       // omitted for full block
      "more": false,                // true if more chunks for this block follow
      "proof": "0x..."              // hex-encoded ffi.ChangeProof.MarshalBinary()
    }
  }
}
```

Notes:

- `proof` is the raw Firewood proof bytes hex-encoded. This lets the inner format evolve without breaking the API.
- `prevStateRoot` / `newStateRoot` are the protocol — gap detection is "does `prevStateRoot` match my last-known root?" not block-number sequencing.
- The consumer **must** cross-check `newStateRoot` against the C-chain block header's `stateRoot` (obtained via paired `newHeads` subscription or by-hash lookup). This anchors trust to consensus and not to the node serving the proof.

## Server-side implementation

Three pieces, all in `graft/evm/eth/filters/`:

1. **FilterAPI method** in `api.go`, modeled on `NewHeads`:

   ```go
   func (api *FilterAPI) ChangeProofs(ctx context.Context, opts ChangeProofOpts) (*rpc.Subscription, error)
   ```

   `eth_subscribe`'s dispatch routes `"changeProofs"` to this method by name. Allocates a buffered channel for proof frames, registers with the event system, spawns a notifier goroutine that loops on the channel and calls `notifier.Notify(sub.ID, frame)` until the client unsubscribes.

2. **EventSystem fanout** in `filter_system.go`, modeled on `SubscribeNewHeads`:

   ```go
   func (es *EventSystem) SubscribeChangeProofs(ch chan *ChangeProofFrame) *Subscription
   ```

   Multiplexes a single upstream feed to all current subscribers.

3. **New internal feed at the commit site.** Wherever the EVM commit path advances the Firewood revision (somewhere downstream of `graft/evm/firewood/triedb.go`), fire a `ChainAcceptedWithChangeProofEvent { blockNumber, blockHash, prevRoot, newRoot }`. The EventSystem subscribes to this feed once, and for each event:
   - Calls `firewood.Database.ChangeProof(prevRoot, newRoot, Nothing, Nothing, maxLength)`.
   - Chunks if necessary (see below).
   - Hex-encodes and fans out to all subscribers.

   The proof is generated **once per block**, not once per subscriber. This is important — N subscribers cost N socket-writes, not N proof generations.

The new event is preferred over reconstructing `prevRoot` from `header.ParentHash` at notification time, because both roots are already in scope at the commit site.

## Bandwidth and framing

### `maxLength` selection — open issue

`maxLength` (and the corresponding bytes-per-frame target) cannot be picked accurately without a histogram of real C-chain change-proof sizes. Tracking as an open issue. The provisional design uses:

- Target frame size: **8 MB** (well under the existing `wsDefaultReadLimit = 32 MB` in `graft/evm/rpc/websocket.go:52`; friendly to browser WS clients which often default to 16 MB).
- Initial `maxLength`: `targetBytes / 128` ≈ 65k entries, assuming ~128 B per key+value+local proof overhead.
- Overshoot retry: if a generated proof exceeds the target, regenerate with `maxLength * (target / actualSize) * 0.9`. Converges in 1–2 retries because proof node overhead is sub-linear in entry count (path sharing), so the retry is conservatively under-budget.

Tuning these values is deferred until block-size data is available. Suggested follow-up: small offline script that iterates accepted roots from a synced Firewood node and calls `ChangeProof(prev, new)` for each, emitting p50/p99/p99.9/max byte and entry counts.

### Chunking

When a block's proof exceeds the target, emit multiple frames with the same `(blockNumber, prevStateRoot, newStateRoot)` and decreasing `more` flag. Each frame carries `rangeStart` / `rangeEnd` from Firewood's `FindNextKey`-style continuation — same pattern used in the existing range-proof syncer code. Consumer reassembles by tuple.

### Compression

Not pursued. Firewood's native proof format is dominated by hashes (uniform random) and keccak-hashed keys. The remaining bytes are mostly bounded-size values. Generic deflate/zstd would yield marginal gains for non-trivial CPU cost. Re-evaluate only if measurements show otherwise.

## Backpressure

The consumer model is "skip-tolerant": for `eth_getBalance("latest")` and similar use cases, being a block or two behind is acceptable, and executing the block on the consumer side is slower than downloading the deltas anyway.

The Firewood any-to-any change-proof property makes skipping graceful: a consumer that misses block N can resume at block N+1 by requesting `ChangeProof(rootN-1, rootN+1)`, which is *smaller* than the sum of the two individual proofs because keys touched in both blocks appear once with their final value. The only catastrophic failure is falling behind by more than Firewood's revision history depth (configured at ~100k revisions today — roughly two days at 2 s block cadence). That triggers a bootstrap.

Concretely:

- **Per-subscriber server-side queue**: bounded depth (start with 2–3 frames). On full queue, **drop oldest** rather than disconnect or buffer unbounded. The next frame the consumer receives will have a `prevStateRoot` that doesn't match their last-known root; they then either accept the wider implicit diff (their next received frame's `prevStateRoot` is their resume point) or explicitly resubscribe with a resume hint.
- **Node-wide subscriber cap**: hard ceiling on concurrent `changeProofs` subscribers, separate from the existing WS connection limit. Change-proof streams are an order of magnitude more expensive than `newHeads`. Default conservatively (e.g., 8); operator-tunable.

No protocol-level sequencing or gap detection is needed beyond the root chain — the roots are the protocol.

## Bootstrap and resume

A fresh consumer (or one that fell outside the history window) bootstraps via Firewood range proofs against a recently finalized root. This is already implemented and exercised by the syncer code in PR #5385. The streaming subscription itself does not handle bootstrap; the consumer's flow is:

1. Subscribe to `newHeads`, pick a recent finalized block, note its `stateRoot`.
2. Pull range proofs covering the full key space at that root via existing RPC/p2p paths.
3. Subscribe to `changeProofs`, optionally with a resume hint: the last-known root.
4. Apply incoming deltas.

For step (3), define an optional `since` field on the subscription params:

```json
{ "version": 1, "since": "0x<root>" }
```

If present, the server sends a single catch-up frame `ChangeProof(since, currentHead)` first, then begins per-block streaming. If `since` is outside the history window, the server returns `ErrInsufficientHistory` on the subscription and the consumer falls back to full bootstrap.

A paired `eth_getStateRangeProof`-style RPC for the bootstrap step is **out of scope for this feature** but worth noting as a near-term follow-up — without it, the Rust mirror needs to talk to the existing p2p sync path or to internal-only endpoints.

## Consensus anchoring

The proof frame's `newStateRoot` must equal the C-chain block header's `stateRoot` for the consumer to trust the delta. Two ways to enforce this on the consumer:

1. Subscribe to `newHeads` in parallel and join by `blockHash`.
2. After receiving a proof frame, call `eth_getBlockByHash(blockHash)` and compare.

Either is cheap. The server **does not** include the header in the proof frame — it would duplicate `newHeads` data and bind the two streams in a way that makes independent failure handling harder.

## Configuration / operator surface

New config knobs (names TBD, kept minimal):

- `enable-change-proof-stream` (bool, default off) — gates the entire feature on a node.
- `change-proof-stream-max-subscribers` (int, default 8) — concurrent subscriber cap.
- `change-proof-stream-queue-depth` (int, default 3) — per-subscriber server-side queue.
- `change-proof-stream-max-frame-bytes` (int, default 8 MB) — target frame size.

Off-by-default is intentional: the feature is high-bandwidth and operator-opt-in.

## Frontend routing (api-worker)

For the mirror to actually serve production traffic, the frontend api-worker has to be taught to recognize the supported method set (see Goal section) and route those calls to a pool of mirror instances, while continuing to route everything else (`eth_call`, `eth_sendRawTransaction`, log queries, etc.) to full nodes. This is a non-trivial piece of the overall delivery:

- **Method-aware routing.** The api-worker currently treats the C-chain endpoint as a single backend pool. Adding mirror-eligible routing means inspecting each JSON-RPC request's `method` field and dispatching to one of two pools. This is the first-line filter: methods outside the supported set never hit the mirror at all.
- **421-based fallback for unserveable requests.** Even for supported methods, the mirror may not be able to answer a specific request (numeric blockTag outside the Firewood history window, missing height→hash entry, instance briefly behind head on a `"latest"` query). The mirror responds with **HTTP 421 Misdirected Request** in those cases, and the worker retries the request against the full-node pool. RFC 9110 §15.5.20 fits this exactly: "the request was directed at a server that is unable or unwilling to produce an authoritative response for the target URI," and the spec invites the client to retry. The proxy-MUST-NOT clause doesn't bite us because the mirror is the origin server for the JSON-RPC call, not a forwarding proxy.
- **Why 421 over worker-side state tracking.** Letting the mirror decide per-request colocates the "can I serve this?" decision with the only component that actually knows the answer (current head, history window, block tail). The worker doesn't need to track per-instance mirror state to make correct routing decisions. The cost is one extra round-trip on misses, which should be rare given the method-name allowlist already filters out most ineligible traffic.
- **Optional: mirror head-height hint.** To further reduce miss-rate round-trips, the mirror can include its current head in a response header on success (e.g., `X-Mirror-Head: 0x...`). The worker caches this briefly and biases away from the mirror on `"latest"` queries when it knows the mirror is more than a block or two behind. Doesn't replace 421; just reduces how often it has to fire.
- **Pool health.** Standard liveness checks on mirror instances. A mirror that's failing health checks (process down, persistent bootstrap loop, etc.) is removed from the pool entirely; 421 is only for per-request unserveability, not instance-level failure.
- **Capacity planning.** How many mirror instances per region, behind what load balancer, with what redundancy. The cost story (Motivation section) depends on this being meaningfully cheaper than scaling the full-node pool, so capacity work needs concrete numbers.
- **Coordination cost.** This is **infra team work**, not avalanchego-side work. It requires explicit buy-in and scheduling with the infra team before the mirror can deliver real value. The avalanchego-side feature can ship and be useful for internal experimentation without it, but the production cost-savings story doesn't materialize until the api-worker routing is in place.

Treat this as a parallel workstream that needs to land before the feature is "done" from a business-value perspective.

## Rust client work

The mirror client itself is out of scope for this plan, but it's worth enumerating what the Rust team has to build so the cross-team scope is visible. The client has three lifecycle phases:

### (a) Bootstrap

Before it can answer any query, the client needs an initial copy of state and recent blocks at a known root. Concretely:

- Pick a recent finalized block, fetch its header, and treat its `stateRoot` as the target.
- Pull a full range-proof traversal of the Firewood K/V space at that root and apply it to a local Firewood instance.
- Pull a contiguous tail of recent block bodies (depth TBD; needs to cover whatever lookback the supported methods require — `eth_getBlockByNumber` with a numeric tag goes back arbitrarily far, but most live traffic is recent).

Most of the heavy lifting here is already implemented in Go and lives in this repo: the Firewood syncer (`database/merkle/firewood/syncer/`, including the change-proof work in PR #5385), the range-proof handler, and the block fetch paths. The Rust team can either:

1. Port the syncer logic to Rust against the existing Firewood Rust FFI, or
2. Run the Go syncer as a sidecar / one-shot bootstrap tool that hands a ready Firewood directory to the Rust client, then have the Rust client take over.

Option 2 is faster to ship; option 1 is a cleaner long-term product. For the block-body side specifically, [`ava-labs/blockstore`](https://github.com/ava-labs/blockstore) is a Rust implementation of the same data format and semantics as the Go blockdb in this repo, so the Rust client can read the block tail directly without going through Go.

### (b) Steady-state subscription

Once bootstrapped, the client opens a WebSocket to a participating C-chain node and subscribes to `changeProofs` (this plan's feature) plus `newHeads` (for the `blockHash` → `stateRoot` cross-check, see Consensus anchoring). For each incoming proof frame:

1. Verify `prevStateRoot` matches the client's current root. If not, request a wider diff or fall back to bootstrap.
2. Apply the proof to the local Firewood, advancing to `newStateRoot`.
3. Cross-check against the matching `newHeads` header.
4. Persist the new root as the last-known checkpoint, and record the `(blockNumber → stateRoot)` mapping.

Block bodies for new blocks come in via whatever companion mechanism is chosen (poll `eth_getBlockByHash` on each `newHeads`, or a separate subscription).

The client must maintain a persistent **height → stateRoot mapping** for every block it has observed. This is needed for:

- **Continuity checks.** When a frame arrives for block N with `prevStateRoot = R`, the client confirms `mapping[N-1] == R` before applying.
- **Resume after restart.** On startup, the client reads its last-checkpointed `(height, stateRoot)` and includes it as the `since` hint on the new subscription.
- **Historical queries.** Any supported method called with a numeric `blockTag` other than `"latest"` needs to resolve that height to a state root the client knows about. Firewood's revision history bounds how far back this can actually be served (~100k revisions today); requests outside that window fall back to the full-node pool via the api-worker.
- **Reorg detection.** Avalanche consensus means accepted blocks don't reorg, so this is a sanity check rather than a recovery mechanism — but if `mapping[N]` ever disagrees with a freshly observed header for block N, something is badly wrong and the client should bootstrap.

### (c) Readiness and JSON-RPC serving

The client exposes a JSON-RPC endpoint that the frontend api-worker routes to. Concretely:

- A readiness signal: "I have applied block N, my current root matches the chain's accepted root for block N, my block tail goes back to M." The api-worker uses this to decide whether to route traffic here.
- Handlers for the supported method set (see Goal section). Each handler reads from the local Firewood or block store, packages the answer in the standard JSON-RPC response shape.
- Behavior for blockTags: `"latest"` resolves to the mirror's current head; numeric / hash tags resolve against the block tail (or return an error if outside the tail). Coordinate with the api-worker's block-tag policy (see Frontend routing).

None of this is hard individually, but it adds up to a meaningful Rust project — likely the dominant share of the total effort once the avalanchego-side feature is in place.

## Open issues

1. **`maxLength` / frame-size sizing.** Deferred pending measurement. See the bandwidth section.
2. **Bootstrap RPC surface.** Out of scope for this PR but the Rust mirror cannot function without one; tracking as a paired follow-up.
3. **Authorization / namespacing.** Whether this lives in the public `eth_` namespace or a restricted namespace (e.g., `debug_`, `admin_`, or a new `state_`). Affects default exposure on public endpoints.
4. **RPC traffic share validation.** This whole effort assumes the methods listed in the Goal section (`eth_getBalance`, `eth_getStorageAt`, `eth_getProof`, `eth_getTransactionCount`, `eth_getAssetBalance`, the block-body families) represent a significant fraction of production RPC traffic — significant enough to justify the engineering and operational cost of a lightweight mirror client. This needs to be verified by metrics analysis of real RPC traffic on the C-chain endpoints. If the supported methods turn out to be a small share, the cost-benefit case for the mirror shrinks accordingly.

## Out of scope

- The lightweight Rust mirror client itself.
- A bootstrap RPC for initial state seeding (see open issues).
- Pushing contract bytecode over the wire (see open issues).
- Compression — re-evaluate only on measurement evidence.
- Per-subscriber key-range filtering — the canonical consumer wants the full diff.

## Effort estimate

The core feature — FilterAPI method, EventSystem fanout, new internal feed at the commit site, chunking on the size cap, queue-and-drop backpressure — is on the order of a small project, **not** an afternoon as it might first appear. The plumbing itself is small, but the work around it (framing schema, resume semantics, operator config, end-to-end testing against a real Firewood node, defining and documenting the consumer contract well enough for the Rust team to implement against without reading Go) is what pushes it from a one-day spike to a multi-week deliverable.

### Prototype / proof-of-concept path

A POC that demonstrates "you can mirror C-chain state by consuming a WS stream" is achievable in roughly **2–3 days of focused work**, by aggressively cutting scope on everything that isn't the happy path:

1. **Skip the new event feed.** Hook directly into an existing acceptance callback in `graft/evm`, snapshotting `(prevRoot, newRoot, blockHash, blockNumber)` at the latest convenient point. Refactor into a proper feed only if the POC succeeds.
2. **Hardcode the frame layout.** No `version` field, no `since` resume hint, no `rangeStart`/`rangeEnd`/`more` chunking. Just `{blockNumber, blockHash, prevStateRoot, newStateRoot, proof}`. Pick a generous `maxLength` (e.g., 500k entries) so chunking never triggers in practice on testnet; if it does, drop the frame and log loudly.
3. **No backpressure policy.** Use a small bounded channel; if it fills, disconnect the subscriber. This is wrong for production but fine for "does the wire format work?"
4. **No config knobs.** Always on, no subscriber cap. POC nodes aren't public.
5. **No resume / no bootstrap RPC.** The POC consumer starts from a known recent root that's still in Firewood's history window, or just from genesis on a fresh testnet. Defer the full bootstrap path entirely.
6. **No code streaming, no `eth_getCode` plumbing.** Pick test transactions that don't deploy or call new contracts, or hand-populate code on the consumer side. POC is about state movement, not RPC completeness.
7. **Minimal test consumer.** A small Go program in `tests/` that subscribes, applies deltas to its own Firewood instance, and verifies `newStateRoot` against `eth_getBlockByHash` on each block. Same language as the server, so no cross-language friction; the Rust client comes later.

Concrete POC sequence:

1. Wait for PR #5385 + Firewood v0.5.0.
2. Add a `ChangeProofs` method to `FilterAPI` (~50 lines) calling `firewood.Database.ChangeProof` directly inline on the acceptance hook. No `EventSystem` integration yet — one goroutine per subscriber calling Firewood directly is fine for a POC with one consumer.
3. Write the Go test consumer (~150 lines).
4. Stand up a local two-node testnet, drive transactions, watch the consumer's state root stay in lockstep with the server's.

If the POC works, the production path is the full sequence below — the POC code mostly gets thrown away or refactored into the proper feed-driven design. The point of the POC is to validate (a) the wire format carries enough information, (b) Firewood proofs apply cleanly on a separately-maintained mirror DB, and (c) bandwidth on a realistic workload is in the expected envelope.

### Production sequencing

1. Land PR #5385 and Firewood v0.5.0.
2. Build the bandwidth-measurement script (Open Issue 1). Run against mainnet replay. Pick `maxLength` / frame target.
3. Implement the core path with chunking and the new internal feed.
4. Wire the FilterAPI method and EventSystem subscription.
5. Add operator config knobs.
6. Add backpressure (queue + drop-oldest + subscriber cap).
7. Add resume / `since` semantics.
8. End-to-end test: stand up a node with the feature enabled, drive it through a mainnet replay, validate proofs against header `stateRoot` on the consumer side from a small Go test client.
9. Document the consumer contract: wire format, framing, resume protocol, failure modes.
10. Coordinate with the Rust mirror team on the bootstrap RPC follow-up.
