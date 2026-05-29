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

## The load script

`wrk` needs a Lua script to send POST bodies and to vary the block height so the
storage/index path is exercised (a fixed `eth_blockNumber` only tests the HTTP
front-end). Save as `randblock.lua`:

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
