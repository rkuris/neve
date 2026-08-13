//! C-chain backfill worker: closes gaps between the contiguous frontier and the
//! upstream tip (and, in mirror mode, fills the whole retained range from the
//! anchored floor up). Block and log fetching is reused from
//! [`crate::eth::ingest`]; the progress/ETA reporting is shared with every other
//! chain in [`crate::progress`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use futures_util::stream;
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::chain::{Chain, IngestCfg};
use crate::eth::ingest::{decode_hash, extract_tx_hashes, fetch_full_block, fetch_logs};
use crate::metrics;
use crate::progress::BackfillProgress;
use crate::record;
use crate::storage::Storage;
// tokio's clock rather than `std`'s: identical in production, but it lets the TTL
// be tested by advancing virtual time instead of doing arithmetic on an `Instant`
// (the pattern already used in `conn.rs` and `upstream.rs`).
use tokio::time::Instant;

/// How long a fetched upstream tip stays usable before backfill re-reads it.
///
/// Caching it matters because the alternative — re-reading the tip per backfilled
/// block — costs a second upstream request per block, half the entire request
/// budget, to learn something that changes by ~1 block/s.
///
/// A time-based TTL rather than every-N-blocks because it self-tunes: it costs a
/// fixed fraction of the request budget regardless of how fast backfill is
/// running (~0.4% at 25 blocks/s), where a block count would poll too often when
/// caught up and too rarely at speed.
///
/// 10s rather than something tighter because **the local high-water usually *is*
/// the tip** — newHeads keeps it there, and `target` is `hw.max(this)` — so this
/// poll only matters when the subscription is stalled or has not yet delivered.
/// `ws_idle_timeout` (2m by default) is what actually recovers that case, so a
/// worst-case 10s lag in noticing new heights is far inside the window that
/// already exists. It also only skews the reported `behind` by ~10 blocks against
/// a figure in the millions.
const TIP_TTL: Duration = Duration::from_secs(10);

/// How long the backfill task naps once it has caught up to the tip. This is
/// the dominant term in the steady-state lag: newHeads delivers a *sparse* set
/// of heads (upstream coalesces frames the serial ingester can't drain fast
/// enough), so the contiguous frontier only advances when backfill fills the
/// holes. At ~1 block/s a 5s nap left us ~5 behind; 1s keeps us ~1 behind at
/// the cost of one extra `eth_blockNumber` per second while idle.
const BACKFILL_CAUGHT_UP_POLL: Duration = Duration::from_secs(1);

/// Backfill task. Closes both gap sources: (1) within-session holes between
/// `max_contiguous_height` and `height_highwater` when newHeads drops frames,
/// and (2) the cold-restart gap between local high-water and the upstream tip.
///
/// The target is `max(local_high_water, upstream_tip)`. newHeads keeps
/// advancing `high_water` concurrently, so the target chases the moving tip
/// without any explicit handoff between this task and the ingester.
/// Upper bound on a single `eth_getLogs` range — the upstream's ~2048-block cap
/// (see `avalanche-public-endpoint-quirks`). The window spans `[from, from+N-1]`,
/// so the request's block span stays under the limit.
const LOGS_WINDOW: u64 = 2048;

/// One `eth_getLogs` window's worth of logs, fetched once and looked up per
/// height as the backfill loop walks the range. Backfill controls both fetches,
/// so it joins logs to blocks window-locally — the live [`crate::join`] buffer
/// is not involved here.
#[derive(Default)]
struct LogWindow {
    /// Logs bucketed by block height; heights with no logs are absent.
    by_height: HashMap<u64, Vec<Value>>,
}

/// Upstream tip, cached for [`TIP_TTL`]. See that constant for why.
struct TipCache {
    height: u64,
    fetched: Option<Instant>,
}

impl TipCache {
    const fn new() -> Self {
        Self {
            height: 0,
            fetched: None,
        }
    }

    /// The tip, re-reading it upstream only once the cached value has aged past
    /// [`TIP_TTL`]. Use this whenever a stale answer cannot change the decision
    /// being made.
    async fn get(&mut self, http: &reqwest::Client, cfg: &IngestCfg) -> u64 {
        if self.fetched.is_some_and(|at| at.elapsed() < TIP_TTL) {
            return self.height;
        }
        self.get_uncached(http, cfg).await
    }

    /// The tip, always read upstream. Use this where a stale answer would be
    /// wrong — the caught-up confirmation — rather than merely imprecise.
    ///
    /// A failed read keeps the previous value rather than collapsing to 0: the
    /// caller maxes it with the local high-water anyway, so a miss costs freshness
    /// and never correctness. The attempt is still recorded, so a hard-down
    /// upstream cannot spin this into a request per pass.
    async fn get_uncached(&mut self, http: &reqwest::Client, cfg: &IngestCfg) -> u64 {
        cfg.pace().await;
        if let Some(tip) = upstream_block_number(http, cfg).await {
            self.height = tip;
        }
        self.fetched = Some(Instant::now());
        self.height
    }
}

impl LogWindow {
    /// Replace the window with a freshly-fetched logs array, bucketed by height.
    /// Clearing first is load-bearing: a run serves every height from whatever
    /// this holds, so a leftover bucket from the previous run would attach the
    /// wrong logs to a height that had none.
    fn load(&mut self, logs: Value) {
        self.by_height.clear();
        if let Value::Array(items) = logs {
            for log in items {
                if let Some(height) = block_number_of(&log) {
                    self.by_height.entry(height).or_default().push(log);
                }
            }
        }
    }

    /// This height's logs as a serialized JSON array (`[]` if none), with the
    /// entry count (for metrics).
    fn serialized(&self, height: u64) -> (Vec<u8>, usize) {
        match self.by_height.get(&height) {
            Some(logs) => (
                serde_json::to_vec(logs).unwrap_or_else(|_| b"[]".to_vec()),
                logs.len(),
            ),
            None => (b"[]".to_vec(), 0),
        }
    }

    /// Fetch `[from, to]`'s logs in one `eth_getLogs` and bucket them, replacing
    /// whatever the window held. `None` on a fetch failure, which cuts the run
    /// short so no height in it is persisted with a logs half we never fetched.
    ///
    /// Loaded for a whole run up front rather than lazily per height, because
    /// the run's blocks are fetched concurrently and a lazily-shared window
    /// would need locking to serve them — while buying nothing: one request
    /// covers the run either way.
    async fn load_range(
        &mut self,
        http: &reqwest::Client,
        cfg: &IngestCfg,
        from: u64,
        to: u64,
    ) -> Option<()> {
        cfg.pace().await;
        let logs = fetch_logs(http, cfg, from, to).await?;
        debug!(
            from,
            to,
            count = logs.as_array().map_or(0, Vec::len),
            "fetched logs window"
        );
        self.load(logs);
        Some(())
    }
}

/// One height's outcome from the concurrent fetch stage.
enum Fetched {
    /// newHeads filled this slot while the run was in flight.
    Skip,
    Block(u64, Value),
}

/// The loop's mutable state, carried across runs: progress reporting, the lag
/// gauge `/health` and `/metrics` read, and the current logs window.
struct RunState {
    progress: BackfillProgress,
    behind_tip: Arc<AtomicU64>,
    logs: LogWindow,
}

/// The `blockNumber` of a log object as a `u64`, if present and well-formed.
fn block_number_of(log: &Value) -> Option<u64> {
    let s = log.get("blockNumber")?.as_str()?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

pub(crate) async fn backfill_loop(
    storage: Storage,
    http: reqwest::Client,
    cfg: IngestCfg,
    backfill_count: Arc<AtomicU64>,
    behind_tip: Arc<AtomicU64>,
) {
    wait_for_bootstrap(&cfg).await;
    let mut run = RunState {
        progress: BackfillProgress::new(Chain::C, cfg.progress_period),
        behind_tip,
        logs: LogWindow::default(),
    };
    let mut tip = TipCache::new();
    loop {
        let hw = storage.high_water().await;
        // Cold start: normally we wait until newHeads anchors the store
        // (minimum_height) before backfilling — backfilling from genesis is
        // out of scope. In mirror mode the floor is known up front (from the
        // upstream's /health), so we start immediately without waiting for a
        // newHead.
        if hw == 0 && cfg.backfill_floor.is_none() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        // Effective floor for "behind"/next accounting. Before the first block
        // is written, the store reports max_contiguous_height = 0; the mirror
        // floor tells us the real baseline so progress isn't off by the floor.
        let floor = cfg.backfill_floor.unwrap_or(0);
        let raw_contiguous = storage.max_contiguous_height().await;
        let contiguous = raw_contiguous.max(floor.saturating_sub(1));
        // A fresh tip is only ever *needed* at the moment caught-up gets decided.
        // Below anything already known — the local high-water, or the last tip
        // read — there is work to do whatever a new poll would say, so a cached
        // value cannot change the outcome and the request is pure waste. Once the
        // frontier reaches everything known, confirm against upstream before
        // declaring caught-up, since that decision is the one a stale tip could
        // get wrong.
        //
        // Both inputs are free: `hw` and `contiguous` are local reads and
        // `tip.height` is the cached value. Including the cached tip is what keeps
        // this at one request per block when newHeads is stalled and `hw` alone
        // would sit at the frontier.
        let known = hw.max(tip.height);
        let upstream_tip = if contiguous < known {
            tip.get(&http, &cfg).await
        } else {
            tip.get_uncached(&http, &cfg).await
        };
        let target = hw.max(upstream_tip);
        let behind = target.saturating_sub(contiguous);
        // Re-read the stored tip adjacent to the contiguous read and clamp to it:
        // `hw` above predates the upstream round-trip, during which live ingestion
        // can lift the store past it, inverting the head/contiguous gauges.
        metrics::ingest_heights(
            Chain::C,
            storage.high_water().await.max(contiguous),
            contiguous,
            behind,
        );
        if contiguous >= target {
            run.behind_tip.store(0, Ordering::Relaxed);
            run.progress.caught_up(contiguous);
            tokio::time::sleep(BACKFILL_CAUGHT_UP_POLL).await;
            continue;
        }
        run.behind_tip.store(behind, Ordering::Relaxed);
        if run.progress.observe(contiguous, target, behind) {
            backfill_count.fetch_add(1, Ordering::Relaxed);
        }
        // Fill a bounded run before re-measuring. Sized at LOGS_WINDOW so the
        // run needs exactly one `eth_getLogs`, which also bounds how stale the
        // tip and `behind` can get: one re-read per 2048 blocks.
        let next = contiguous.saturating_add(1);
        let end = target.min(next.saturating_add(LOGS_WINDOW.saturating_sub(1)));
        if !fill_range(&storage, &http, &cfg, next..=end, target, &mut run).await {
            // Fetch or persist failed; back off so a persistent problem doesn't
            // become a hot loop. The outer loop re-measures, so the retry
            // resumes at the contiguous frontier.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Fetch and persist an inclusive run of heights, keeping
/// `cfg.fetch_concurrency` of them in flight. Returns `false` if the run was cut
/// short and the caller should re-measure and retry.
///
/// `buffered` yields results **in height order** while fetching ahead, so writes
/// stay sequential and the contiguous frontier only ever advances by one. That
/// ordering is what makes a mid-run failure cheap: everything before it is
/// already durable, and the outer loop simply resumes from the frontier.
///
/// Concurrency buys back round-trip *latency*, not request rate — every fetch
/// still claims a pacer slot, so a run against the public endpoint issues the
/// same ~25 req/s it always did, just without idling between them. That is the
/// whole win: the serial path spent one full round-trip per block doing nothing.
///
/// The run's logs are fetched **once, up front**, which is why the caller sizes
/// a run at [`LOGS_WINDOW`]: one `eth_getLogs` per run, the same amortization
/// the serial path got from its lazy window. Only the blocks parallelize.
async fn fill_range(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    heights: std::ops::RangeInclusive<u64>,
    tip: u64,
    run: &mut RunState,
) -> bool {
    let (from, to) = (*heights.start(), *heights.end());
    // Before any block is fetched: a height must never be persisted with an
    // empty logs half we never actually fetched, since `[]` on disk means "this
    // height emitted none" (see `crate::record`).
    if cfg.ingest_logs && run.logs.load_range(http, cfg, from, to).await.is_none() {
        return false;
    }
    let mut in_flight = stream::iter(heights)
        .map(|height| fetch_one(storage, http, cfg, height))
        .buffered(cfg.fetch_concurrency.max(1));

    while let Some(fetched) = in_flight.next().await {
        let (height, block) = match fetched {
            None => return false,
            Some(Fetched::Skip) => continue,
            Some(Fetched::Block(height, block)) => (height, block),
        };
        let (logs_bytes, log_count) = if cfg.ingest_logs {
            run.logs.serialized(height)
        } else {
            (record::EMPTY_ARRAY.to_vec(), 0)
        };
        if let Err(e) = persist_backfilled(storage, height, &block, &logs_bytes).await {
            warn!(height, error = %e, "backfill persist failed");
            return false;
        }
        if cfg.ingest_logs {
            metrics::logs_persisted(Chain::C, metrics::BlockSource::Backfill, log_count as u64);
        }
        // Per height, not per run: a run is up to 2048 heights, long enough that
        // `/health` and the `summary` line would otherwise quote a gap minutes
        // out of date during a long fill.
        let behind = tip.saturating_sub(height);
        run.behind_tip.store(behind, Ordering::Relaxed);
        run.progress.observe(height, tip, behind);
    }
    true
}

/// One height's block, or [`Fetched::Skip`] when newHeads already filled the
/// slot. `None` is a fetch failure, which cuts the run short.
///
/// No trailing nap: pacing is claimed per *request*, before the call. A trailing
/// sleep would make the period `work + interval` and let the achieved rate sag
/// as latency rises, which is how the previous ~25 req/s intent silently ran at
/// ~11.7 blocks/s.
async fn fetch_one(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    height: u64,
) -> Option<Fetched> {
    // Race guard: newHead may have just filled this slot.
    if matches!(storage.get_by_height(height).await, Ok(Some(_))) {
        return Some(Fetched::Skip);
    }
    cfg.pace().await;
    let block = fetch_full_block(http, height, cfg, None).await?;
    Some(Fetched::Block(height, block))
}

/// In mirror mode, block until the `oldBlocks` bootstrap signals completion so
/// backfill doesn't race its ascending frontier with redundant per-block HTTPS
/// fetches. Afterward backfill settles into its steady-state job: filling the
/// holes dropped live frames leave below the contiguous tip. `notify_one`
/// stores a permit if the bootstrap finishes first, so this never deadlocks.
/// No-op (returns immediately) outside mirror mode.
async fn wait_for_bootstrap(cfg: &IngestCfg) {
    if cfg.subscribe_blocks {
        cfg.bootstrap_done.notified().await;
    }
}

/// Ask upstream HTTPS RPC for its current tip. Used to seed the backfill
/// target after a cold restart, before newHeads have caught us up.
async fn upstream_block_number(http: &reqwest::Client, cfg: &IngestCfg) -> Option<u64> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": [],
    });
    let resp = http.post(&cfg.rpc_url).json(&body).send().await.ok()?;
    let v = resp.json::<Value>().await.ok()?;
    let s = v.get("result")?.as_str()?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

/// Persist a block fetched by the backfill path (or streamed by the mirror's
/// `oldBlocks` bootstrap). Unlike `persist_block`, there is no newHead hash to
/// compare against, so we trust the body's reported hash, and we do NOT
/// republish to the live broadcast — these are historical fills, not "new"
/// blocks, so a downstream mirror's `newBlocks` feed must not see them.
pub(crate) async fn persist_backfilled(
    storage: &Storage,
    height: u64,
    block: &Value,
    logs: &[u8],
) -> Result<()> {
    let body_hash = block
        .get("hash")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("backfilled block missing hash"))?;
    let hash_bytes = decode_hash(body_hash)?;
    let tx_hashes = extract_tx_hashes(block);
    let bytes = serde_json::to_vec(block)?;
    let block_len = bytes.len();
    storage
        .put(height, hash_bytes, &tx_hashes, &[&bytes, logs])
        .await?;
    metrics::block_persisted(Chain::C, metrics::BlockSource::Backfill);
    debug!(
        height,
        bytes = block_len,
        txs = tx_hashes.len(),
        "backfilled block",
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// `reqwest` is built with `rustls-no-provider`, so a `Client` cannot be
    /// constructed until a provider is installed — `main` does this at startup.
    /// Idempotent via `ok()`: several tests in one binary share the process.
    fn http() -> reqwest::Client {
        let _ = rustls::crypto::ring::default_provider().install_default();
        reqwest::Client::new()
    }

    /// An [`IngestCfg`] whose `rpc_url` cannot be reached, so any tip refresh
    /// fails. Unpaced, so the tests don't sleep.
    fn unreachable_cfg() -> IngestCfg {
        let (blocks, _) = tokio::sync::broadcast::channel(1);
        IngestCfg {
            chain: Chain::C,
            max_wait: Duration::ZERO,
            ws_idle_timeout: Duration::ZERO,
            ws_url: String::new(),
            // Port 1 with nothing listening: connect fails fast.
            rpc_url: "http://127.0.0.1:1".to_owned(),
            poll_interval: Duration::ZERO,
            blocks,
            subscribe_blocks: false,
            backfill_inter_fetch: Duration::ZERO,
            pacer: Arc::new(crate::upstream::Pacer::new(Duration::ZERO)),
            host_pacer: None,
            fetch_concurrency: 1,
            backfill_floor: None,
            prefetch_delay_cap: Duration::ZERO,
            progress_period: Duration::from_secs(60),
            fatal: Arc::new(tokio::sync::Notify::new()),
            bootstrap_done: Arc::new(tokio::sync::Notify::new()),
            ingest_logs: false,
        }
    }

    /// A fresh cache must not be consulted before it has ever been filled —
    /// otherwise the first pass would treat height 0 as the tip.
    #[tokio::test]
    async fn tip_cache_refreshes_when_never_fetched() {
        let mut tip = TipCache::new();
        assert!(tip.fetched.is_none());
        // Refresh fails (unreachable), but the attempt is what matters here.
        let got = tip.get(&http(), &unreachable_cfg()).await;
        assert_eq!(got, 0);
        assert!(
            tip.fetched.is_some(),
            "a failed refresh still marks the attempt"
        );
    }

    /// A failed refresh keeps the previous value instead of collapsing to 0. The
    /// caller maxes the result with the local high-water, so losing the cached tip
    /// would only ever lose freshness — but reporting 0 would make `behind` and the
    /// caught-up decision briefly nonsense.
    #[tokio::test(start_paused = true)]
    async fn tip_cache_keeps_stale_value_when_refresh_fails() {
        let mut tip = TipCache::new();
        tip.height = 500;
        tip.fetched = Some(Instant::now());
        // Age it past the TTL on the virtual clock, so `get` must refresh — and
        // the refresh fails against the unreachable upstream.
        tokio::time::advance(TIP_TTL + Duration::from_secs(1)).await;
        let got = tip.get(&http(), &unreachable_cfg()).await;
        assert_eq!(got, 500, "stale tip retained across a failed refresh");
    }

    /// Within the TTL and permitted to cache, no request is made — the whole point
    /// of the cache. `fetched` is the witness: a refresh attempt restamps it, a
    /// cache hit leaves it alone.
    #[tokio::test(start_paused = true)]
    async fn tip_cache_serves_fresh_value_without_a_request() {
        let mut tip = TipCache::new();
        tip.height = 700;
        let stamped = Instant::now();
        tip.fetched = Some(stamped);
        // Nudge the clock so a restamp would be visible — but stay inside the TTL.
        tokio::time::advance(Duration::from_millis(1)).await;
        let got = tip.get(&http(), &unreachable_cfg()).await;
        assert_eq!(got, 700);
        assert_eq!(
            tip.fetched,
            Some(stamped),
            "cache hit must not touch upstream"
        );
    }

    /// About to declare caught-up, the loop calls `get_uncached`, which reads
    /// upstream even though the cache is fresh — that decision is the one a stale
    /// tip could get wrong. Contrast with the test above: identical fresh cache, the
    /// only difference is which method is called.
    #[tokio::test(start_paused = true)]
    async fn tip_cache_is_bypassed_when_confirming_caught_up() {
        let mut tip = TipCache::new();
        tip.height = 900;
        let stamped = Instant::now();
        tip.fetched = Some(stamped);
        // Same nudge as the test above, and still well inside the TTL, so `get`
        // would have been a cache hit here.
        tokio::time::advance(Duration::from_millis(1)).await;
        let got = tip.get_uncached(&http(), &unreachable_cfg()).await;
        assert_eq!(got, 900, "failed refresh retains the value");
        assert_ne!(
            tip.fetched,
            Some(stamped),
            "a fresh cache must still be bypassed when confirming caught-up"
        );
    }

    /// The branch the loop takes, stated directly: `get` is enough whenever the
    /// frontier is below anything already known, and only the caught-up
    /// confirmation needs `get_uncached`. `known` folds in the cached tip so a
    /// stalled newHeads — where high-water sits at the frontier — still costs one
    /// request per block rather than two.
    #[test]
    fn cached_tip_suffices_until_the_frontier_reaches_what_is_known() {
        let suffices = |hw: u64, cached_tip: u64, contiguous: u64| contiguous < hw.max(cached_tip);

        // Deep backfill: frontier far below the local high-water.
        assert!(suffices(92_570_000, 92_570_000, 89_700_000));
        // Small gap, e.g. a brief WS drop — still no reason to re-read the tip.
        assert!(suffices(92_570_000, 92_570_000, 92_569_500));
        // Frontier has reached everything known: confirm before declaring caught-up.
        assert!(!suffices(92_570_000, 92_570_000, 92_570_000));
        // Stalled newHeads: hw stuck at the frontier, but a previously-read tip
        // still shows work — so the cached value stays usable.
        assert!(suffices(89_700_000, 92_570_000, 89_700_000));
        // Cold start, nothing known yet: must poll.
        assert!(!suffices(0, 0, 0));
    }

    /// A run serves every one of its heights from whatever the window holds, so
    /// a load must not leave the previous run's buckets behind — that would
    /// attach one run's logs to another run's height.
    #[test]
    fn log_window_load_replaces_previous_contents() {
        let mut w = LogWindow::default();
        w.load(json!([{"blockNumber": "0x10", "logIndex": "0x0"}]));
        assert_eq!(w.serialized(0x10).1, 1);

        w.load(json!([{"blockNumber": "0x20", "logIndex": "0x0"}]));
        assert_eq!(w.serialized(0x20).1, 1);
        // The earlier run's height is now empty, not still carrying its logs.
        assert_eq!(w.serialized(0x10), (b"[]".to_vec(), 0));
    }

    #[test]
    fn log_window_buckets_logs_by_height() {
        let mut w = LogWindow::default();
        w.load(json!([
            {"blockNumber": "0x10", "logIndex": "0x0"},
            {"blockNumber": "0x10", "logIndex": "0x1"},
            {"blockNumber": "0x12", "logIndex": "0x0"},
        ]));

        let (h10, n10) = w.serialized(0x10);
        assert_eq!(n10, 2);
        let parsed: Value = serde_json::from_slice(&h10).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 2);

        // A gap height inside the window serializes to an explicit empty array.
        let (h11, n11) = w.serialized(0x11);
        assert_eq!(n11, 0);
        assert_eq!(h11, b"[]");

        assert_eq!(w.serialized(0x12).1, 1);
    }

    #[test]
    fn log_window_serialized_preserves_log_objects() {
        let mut w = LogWindow::default();
        w.load(json!([{"blockNumber": "0x1", "address": "0xabc"}]));
        let (bytes, _) = w.serialized(1);
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed[0]["address"], "0xabc");
    }
}
