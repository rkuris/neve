# Benchmarking neve

How to measure neve's JSON-RPC latency and throughput, plus the baseline
numbers from the first run so future runs have something to compare against.

## TL;DR baseline (2026-05-28)

On a **t4g.small** (2 arm64 vCPU, 2 GiB RAM, **burstable — credit-throttled
during the test**), mainnet, with the whole ~1,300-block store in page cache:

- **Service time (1 connection): p50 0.83 ms, p99 2.39 ms** — sub-millisecond
  to serve a ~4.3 KB block.
- **Throughput ceiling: ~4,100 RPS**, which is the 2 vCPUs (CPU-bound, not
  network — 4,100 × 4.3 KB ≈ 140 Mbps vs a 5 Gbps NIC).
- **Operating knee: ~8 concurrent connections** — ~97 % of max throughput at
  still-low ~2 ms p50. Past that, throughput is flat and latency grows linearly
  (pure queuing).

Expect the ceiling to climb on a non-throttled / `unlimited`-credit instance;
the *shape* of the curve stays the same.

## Methodology — read this before trusting any number

1. **Run the load generator on a separate box in the same VPC/AZ**, and target
   neve's **private IP**. Driving load from a laptop measures internet RTT, not
   the server: with N connections over an R-second round trip you get about
   `N / R` requests/sec regardless of how fast neve is. Same AZ keeps network
   RTT sub-millisecond so you isolate neve's own service time.

2. **Query block heights that neve actually has.** neve is a cache of a recent
   tail; out-of-range heights return **HTTP 421** (Misdirected Request), which
   `wrk` counts under `Non-2xx or 3xx responses`. A run with a non-zero Non-2xx
   line is measuring the *reject* path, not block serving — discard it. Pull the
   valid range from `/health` (`blocks.min_height` .. `blocks.max_contiguous_height`)
   and randomize within it.

3. **Latency under a closed-loop test is `concurrency / throughput`, by
   construction** (Little's Law). At saturation, more connections do not make
   neve slower — they just lengthen the queue. To measure real per-request
   latency, test at `-c1`. To find capacity, sweep concurrency and watch where
   throughput plateaus.

4. **Mind the t4g burst credits.** Sustained load drains the CPU-credit balance;
   once it's gone the instance throttles to baseline (~40 %) and latency cliffs
   mid-test. Watch the `st` (steal) column in `top`. For a clean capacity number,
   set the instance to `unlimited` credit mode first, or keep bursts short.

## The load-generator box

The numbers above measure the *target*; they say nothing about the box running
`wrk`. Size that box so it is **never the bottleneck** — if the load generator
runs out of CPU before the target does, you are benchmarking `wrk`, not neve.

- **Instance:** `c6i.2xlarge` (8 vCPU x86, up to 12.5 Gbps) is the comfortable
  default — it out-produces both the t4g.small neve box (~4,100 RPS ceiling) and
  a much larger upstream node with margin to spare. `c6i.xlarge` (4 vCPU) is
  *probably* enough; the 2xlarge just removes all doubt. Arch is irrelevant for
  the generator (`wrk` only sends HTTP), so `c7g.2xlarge` (arm) is a cheaper
  equivalent.
- **Placement is more important than size.** The whole methodology rests on
  **sub-millisecond RTT to the target** (that is why we test against the private
  IP, same VPC/AZ). If you are comparing two targets in *different* AZs, one
  load-gen box cannot be sub-ms to both — cross-AZ adds ~1 ms+ RTT, which swamps
  a sub-ms service time. Put the load generator **in the same AZ as the
  target(s)**, and confirm with the `curl -w 'connect=%{time_connect}s'` probe
  (see "Decompose network vs server time" above) before trusting any latency
  number.

## The load script

`wrk` needs a Lua script to send POST bodies and to vary the block height so the
storage/index path is exercised (a fixed `eth_blockNumber` only tests the HTTP
front-end). The script lives next to this doc as `benchmark/randblock.lua`:

```lua
-- randblock.lua — hit random blocks within neve's stored range.
-- Set lo/hi from /health: blocks.min_height .. blocks.max_contiguous_height.
-- A non-zero "Non-2xx" line from wrk means the range is wrong (out-of-range
-- heights return HTTP 421); fix lo/hi and rerun.
math.randomseed(os.time())
local lo, hi = 86631564, 86632800   -- EXAMPLE — refresh from /health each session
request = function()
  local h = math.random(lo, hi)
  local body = string.format(
    '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x%x",false]}', h)
  return wrk.format("POST", "/", {["Content-Type"] = "application/json"}, body)
end
```

Get the current valid range first:

```sh
curl -s http://<priv-ip>:8545/health \
  | jq '{lo: .blocks.min_height, hi: .blocks.max_contiguous_height}'
```

## Running it

Install wrk on the load-gen box: `sudo apt-get install -y wrk`.

**Service time (no queue):**

```sh
wrk -t1 -c1 -d20s --latency -s randblock.lua http://<priv-ip>:8545/
```

**Capacity sweep — find the knee:**

```sh
for c in 2 4 8 16 32; do
  echo "=== -c$c ==="
  wrk -t1 -c$c -d15s --latency -s randblock.lua http://<priv-ip>:8545/
done
```

**Decompose network vs. server time** (ICMP/`ping` is blocked by the EC2
security group by default — use the open TCP port instead):

```sh
curl -o /dev/null -s \
  -w 'connect=%{time_connect}s ttfb=%{time_starttransfer}s total=%{time_total}s\n' \
  http://<priv-ip>:8545/health
# time_connect ≈ network RTT; ttfb - connect ≈ neve's think time.
```

While a test runs, watch the box: `top` (neve %CPU, and `st` for throttling),
`journalctl -u neve -f`.

## Comparing neve against the upstream avalanchego node

To get a fair node-vs-neve number, drive **both targets from the same load-gen
box, over the same block range, with the same script** — only the path/port and
the `lo/hi` differ. avalanchego's C-chain RPC is `POST /ext/bc/C/rpc` on port
**9650** (not neve's `POST /` on 8545), so there is a sibling script,
`randblock-node.lua`, that differs only in the request path.

1. **Find each target's range and take the overlap.** neve holds only a recent
   tail; the node holds `[lowest_available .. tip]`. Benchmark the *intersection*
   or the two aren't serving the same work.

   ```sh
   # neve's range:
   curl -s http://<neve-priv-ip>:8545/health \
     | jq '{lo: .blocks.min_height, hi: .blocks.max_contiguous_height}'
   # node's range: probe it (binary-search the floor; tip from eth_blockNumber).
   ```

   Set the **same** `lo/hi` (the overlap) in both `randblock.lua` and
   `randblock-node.lua`.

2. **Confirm sub-ms RTT to both** from the load-gen box (private IPs):

   ```sh
   for ip_port in <neve-priv-ip>:8545 <node-priv-ip>:9650; do
     curl -o /dev/null -s -w "$ip_port connect=%{time_connect}s\n" "http://$ip_port/"
   done
   ```

3. **Run the identical sweep against each**, swapping only script + target:

   ```sh
   # neve
   wrk -t1 -c1  -d20s --latency -s randblock.lua      http://<neve-priv-ip>:8545/
   # avalanchego C-chain
   wrk -t1 -c1  -d20s --latency -s randblock-node.lua http://<node-priv-ip>:9650/ext/bc/C/rpc
   ```

   Then sweep `-c2 4 8 16 32` against each, as below.

**Caveat — the reject path differs.** neve returns HTTP 421 for out-of-range
heights (so a non-zero `Non-2xx` line flags a bad range, as noted above). The
node instead answers an out-of-range height with a JSON `null` result at HTTP
200 — `wrk` will *not* flag it, so a wrong `lo/hi` silently benchmarks cheap
null reads. Keep `lo/hi` strictly inside the overlap.

## Same-hardware head-to-head: neve vs avalanchego (2026-05-31)

The cleanest comparison: both targets on the **same instance type**, same AZ,
driven by the same load box, over the same blocks — so the only variable is the
software.

- **Hardware:** neve and the avalanchego node each on a **c6i.2xlarge** (8 vCPU
  x86, *not* burstable — no credit throttling on either), both in us-east-1a.
  avalanchego **v1.14.2**, state-synced, **~244 GB** on disk.
- **Load box:** a third c6i.2xlarge in us-east-1a. Confirmed sub-ms, **matched**
  RTT to both (`connect` ≈ 0.22 ms to neve, 0.18 ms to the node) — so network
  cancels out of the comparison.
- **Workload:** `eth_getBlockByNumber(<height>, false)` over the overlap range
  `[86703873 .. 86881651]`, identical `randblock.lua` / `randblock-node.lua`.
- **neve** ran in `--mirror-from` mode (mirrored the production tail; same block
  bytes it would serve in prod). The **node** is an *unstaked* validator — it
  tracks and executes mainnet blocks but isn't consensus-sampled, so RPC isn't
  competing with consensus voting (a fair, if slightly generous-to-the-node,
  read of its serving capacity).

### avalanchego C-chain node (`POST /ext/bc/C/rpc`)

| conns | RPS    | p50      | p99      |
|------:|-------:|---------:|---------:|
| 1     | 780    | 1.24 ms  | 2.56 ms  |
| 2     | 1,294  | 1.61 ms  | 2.66 ms  |
| 4     | 2,333  | 1.69 ms  | 2.97 ms  |
| 8     | 4,902  | 1.65 ms  | 4.36 ms  |
| 16    | 10,297 | 1.45 ms  | 11.44 ms |
| 32    | 15,355 | 1.90 ms  | 7.99 ms  |
| 64    | **18,430** | 3.17 ms | 14.15 ms |
| 128   | 17,092 | 6.05 ms  | 34.10 ms |
| 256   | 16,215 | 12.46 ms | 77.59 ms |

Peak **~18,430 RPS at c64**. `mpstat -P ALL` during the run showed all 8 cores
~97 % busy — a genuine CPU-bound ceiling (and the load box sat ~93 % idle, so it
wasn't the limiter). Throughput then *declines* ~12 % past the knee (c128/c256):
Go-runtime contention under heavy concurrency.

### neve (`POST /`)

| conns | RPS    | p50      | p99      |
|------:|-------:|---------:|---------:|
| 1     | 3,889  | 0.212 ms | 0.803 ms |
| 2     | 7,128  | 0.231 ms | 0.87 ms  |
| 4     | 11,942 | 0.276 ms | 1.12 ms  |
| 8     | 17,539 | 0.377 ms | 1.51 ms  |
| 16    | 22,523 | 0.635 ms | 1.97 ms  |
| 32    | 23,089 | 1.33 ms  | 2.89 ms  |
| 64    | 22,988 | 2.71 ms  | 5.50 ms  |
| 128   | 23,342 | 5.36 ms  | 11.29 ms |
| 256   | **23,600** | 10.49 ms | 24.96 ms |

Peak **~23,600 RPS**, with the knee around **c16 (22,523 RPS at 0.64 ms p50)**.
`mpstat` at c256 showed **97.7 % busy** — a genuine CPU-bound ceiling. Unlike the
node, throughput **holds flat** under overload (23.1k → 23.3k → 23.6k from c32 to
c256) rather than degrading.

### Verdict

On identical 8-vCPU hardware, same AZ, same blocks, same load tool:

- **Peak throughput: neve ~23,600 RPS vs node ~18,430 → ≈ +28 %.**
- **Per-request latency: ~5.9× lower** — c1 p50 **0.212 ms vs 1.24 ms**.
- **neve reaches the node's *entire* peak (18.4k) by ~c16–c32**, at a fraction of
  the latency; the node needs c64 to get there.
- **Overload manners:** neve holds a flat plateau to c256; the node sheds ~12 %.
- **Footprint:** neve served this from a **1.6 GB** block tail at **~0.4 GiB
  RSS**; on the same box avalanchego carries its **~244 GB** state DB and sat at
  **~8.8 GiB resident (peak ~13.4 GiB)** — ~22× the memory, and why it needs a
  16 GiB host while neve fits on a 2 GiB one. (But see the cost caveats below:
  neve doesn't serve state *yet* — that 244 GB is the job neve hasn't taken on.)

Both ceilings are genuine CPU saturation, confirmed with `mpstat -P ALL` on each
target during the run (load box idle throughout) — so these are real per-box
limits, not load-generator artifacts.

### Cost (deployed)

The benchmark put both on a c6i.2xlarge for a clean comparison, but neve doesn't
*need* that hardware to carry the volume — that's the deployment win. At
us-east-1 list price, serving the projected load:

- **neve:** ~**$339.94/mo** — on-demand t4g.small + ~4 TB gp3 EBS.
- **full node:** ~**$575.88/mo** — c6i.2xlarge + the same 4 TB, its 8 vCPU
  largely spent on the consensus and execution neve doesn't run.

With storage held equal at 4 TB, the ~$236/mo difference is compute you stop
paying for. (neve's actual blockstore is ~1.6 GB; the 4 TB is just to keep the
comparison apples-to-apples.)

**Caveats — this is *today's* scope, not feature parity.** Read the cost gap as
"what block-reads cost on each," not "what a full read API costs":

- **neve doesn't serve state yet.** It answers the read-only *block-tail* subset
  — not `eth_getBalance`, `eth_call`, `eth_getStorageAt`, nonces, etc., which a
  full node does. This is the reads neve answers, not feature parity.
- **The 244 GB is a *state-synced* node; production is far bigger.** This
  benchmark node was state-synced (recent state only), hence the modest 244 GB.
  A production API node is typically **archival from block 1 — ~4 TB and up**, and
  bigger once you account for full state history. So the 4 TB held equal above is
  realistic for the node, if anything conservative — storage is not where neve's
  advantage lives.
- **Adding state to neve is a substantial undertaking, not a small addition.** The
  planned [firewood](https://github.com/ava-labs/firewood)-backed state layer
  will add real CPU, memory, and storage on neve's side and narrow this gap — see
  the future-direction note below. The durable advantages are **latency, memory,
  and operational simplicity**, not disk. Don't read today's delta as the
  steady-state cost once neve serves state.

### Future direction: firewood and state

The numbers above are for **block-tail reads only**. The next milestone is a
[firewood](https://github.com/ava-labs/firewood)-backed state layer, synced via
change proofs ([`docs/StreamingChangeProofs.md`](../docs/StreamingChangeProofs.md)),
extending the same sync-and-serve model to non-executing state reads
(`eth_getBalance`, `eth_getCode`, `eth_getStorageAt`, nonces). What that means for
the comparisons above:

- **It's a significant undertaking**, not a thin shim — state sync, change-proof
  verification, and a state store are each substantial pieces.
- **Storage and memory grow.** A served state trie is large (the archival node's
  ~4 TB is mostly state history); neve's ~1.6 GB / ~320 MiB footprint rises
  materially once it holds state.
- **The cost and footprint gaps narrow.** As neve takes on the expensive part,
  the compute/memory/storage delta shrinks. The advantages expected to *persist*
  are the ones rooted in not executing or running consensus: lower latency, a
  smaller resident set per unit of served data, and operational simplicity.
- **Executing methods stay out of scope** — `eth_call` and friends still need a
  full node.

So read the cost and footprint numbers above as the *block-serving* phase, with
this expansion explicitly ahead of them.

## Baseline sweep (t4g.small, throttled, mainnet)

| conns | RPS   | p50     | p99      |
|-------|-------|---------|----------|
| 1     | 1,088 | 0.83 ms | 2.39 ms  |
| 2     | 2,021 | 0.88 ms | 2.85 ms  |
| 4     | 3,207 | 1.14 ms | 2.96 ms  |
| 8     | 3,964 | 1.96 ms | 4.35 ms  |
| 16    | 4,043 | 3.92 ms | 8.25 ms  |
| 32    | 4,099 | 7.81 ms | 13.56 ms |

Throughput scales nearly linearly to ~c4, reaches ~97 % of ceiling by c8, and is
pegged at ~4,100 RPS from c16 on — c32 buys 1 % more throughput than c16 for 2×
the latency. That plateau is the 2 (throttled) vCPUs: ~2,050 RPS/core.

### Extreme overload (`-t4 -c200`) — plateau and Little's Law hold

A separate `-t4 -c200 -d60s` run far past the knee confirms the curve doesn't
misbehave under heavy concurrency:

```text
Latency    50.29ms   17.44ms 283.92ms   68.52%
50%   51.74ms   75%   62.21ms   90%   71.02ms   99%   86.05ms
238467 requests in 1.00m, 0.96GB read
Requests/sec:   3972.71
```

Two things to note. **Throughput is still ~3,970 RPS** — 6× the connections of
the c32 row buys nothing, exactly as a CPU-bound plateau predicts; it doesn't
collapse under overload. And **latency is pure queuing**: Little's Law says
`concurrency / throughput = 200 / 3972 ≈ 50.4 ms`, which lands right on the
measured 50.29 ms average. So the extra connections only lengthen the queue —
the server's per-request service time is unchanged (still the ~0.83 ms from the
c1 row). This is the textbook signature of a saturated closed-loop system, not
a regression.

## Notes / caveats

- All measurements above had `wa: 0` (no I/O wait) — the entire blockstore fit
  in page cache, so this is the **hot-path best case**. Once the dataset outgrows
  RAM, cold-block reads from EBS will add latency; re-benchmark with
  `sync && echo 3 | sudo tee /proc/sys/vm/drop_caches` between runs, or with a
  store larger than RAM, to measure that path.
- The 200→421 middleware buffers and re-parses every response body as JSON to
  decide the status code — a small per-request cost on the hot path, not yet
  optimized.
