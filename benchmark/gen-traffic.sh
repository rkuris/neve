#!/usr/bin/env bash
# Generate a mix of read-only JSON-RPC traffic against a neve instance so the
# dashboards have something to show (served throughput / latency / etc).
#
# Not a benchmark — just demo filler. Needs only curl + jq.
#
#   bash benchmark/gen-traffic.sh [URL]      # URL defaults to http://127.0.0.1:8545
#   WORKERS=40 bash benchmark/gen-traffic.sh # crank concurrency for more RPS
#   DURATION=120 bash benchmark/gen-traffic.sh   # stop after N seconds (0 = until Ctrl-C)
#
# Throughput is concurrency/RTT-bound from your laptop (each worker is a serial
# curl, so ~WORKERS / round-trip-time req/s) — bump WORKERS for more. The
# served-latency panel stays accurate regardless: that histogram is recorded
# *inside* neve (service time), so it reflects neve's real sub-ms handling, not
# your network RTT to the box.
set -u

URL="${1:-http://127.0.0.1:8545}"
WORKERS="${WORKERS:-8}"
DURATION="${DURATION:-0}"

# Valid block range from /health, so requests stay in-range (no HTTP 421s).
read -r LO HI < <(curl -fsS "$URL/health" \
  | jq -r '"\(.blocks.min_height) \(.blocks.max_contiguous_height)"')
if [ -z "${LO:-}" ] || [ "$LO" = null ]; then
  echo "could not read block range from $URL/health — is neve up?" >&2
  exit 1
fi
echo "traffic → $URL  range ${LO}..${HI}  workers=${WORKERS}  (Ctrl-C to stop)"

req() { curl -fsS -o /dev/null -X POST "$URL" -H 'content-type: application/json' -d "$1"; }

worker() {
  while :; do
    # 30-bit random spread across the whole retained range.
    h=$(( LO + ((RANDOM << 15 | RANDOM) % (HI - LO + 1)) ))
    hx=$(printf '0x%x' "$h")
    case $((RANDOM % 6)) in
      0) req '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' ;;
      1) req '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' ;;
      2) req "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$hx\",false]}" ;;
      3) req "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$hx\",true]}" ;;
      4) req '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}' ;;
      5) req "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockTransactionCountByNumber\",\"params\":[\"$hx\"]}" ;;
    esac
  done
}

pids=()
for _ in $(seq 1 "$WORKERS"); do worker & pids+=("$!"); done

# Stop all workers and exit cleanly (0), whether we hit the duration or Ctrl-C —
# kill the tracked worker PIDs rather than `kill 0`, which would SIGTERM this
# script itself and exit non-zero.
shutdown() { kill "${pids[@]}" 2>/dev/null; wait "${pids[@]}" 2>/dev/null; }
trap 'shutdown; exit 0' INT TERM

if [ "$DURATION" -gt 0 ]; then sleep "$DURATION"; shutdown; else wait; fi
exit 0   # `wait` on signalled workers reports 143; the run itself succeeded
