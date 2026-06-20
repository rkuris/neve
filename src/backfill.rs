//! Backfill worker + periodic summary. The backfill loop closes gaps between
//! the contiguous frontier and the upstream tip (and, in mirror mode, fills
//! the whole retained range from the anchored floor up); the summary loop is
//! the operator-visible heartbeat. Block fetching is reused from `subscribe`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use tracing::{Level, debug, info, warn};

use crate::IngestCfg;
use crate::metrics;
use crate::storage::Storage;
use crate::subscribe::{decode_hash, extract_tx_hashes, fetch_full_block};

/// Emit a tracing event at a level chosen at runtime. tracing's own macros bake
/// the level into static callsite metadata, so they require a const level; this
/// fans out to the level-specific macros once. `behind_level` only yields DEBUG,
/// INFO, or WARN, so the catch-all maps to `info!`.
macro_rules! event_at {
    ($level:expr, $($args:tt)*) => {
        match $level {
            Level::DEBUG => debug!($($args)*),
            Level::WARN => warn!($($args)*),
            _ => info!($($args)*),
        }
    };
}

/// Mutable progress state for the backfill task. Held in one struct so adding
/// an ETA calculation later is local: the start fields already capture the
/// reference point a rate calculation needs.
#[derive(Debug, Default)]
struct BackfillProgress {
    /// Height at which the current "behind" stretch began. `None` when caught up
    /// (the `Default`).
    start_height: Option<u64>,
    /// Wall-clock when the current "behind" stretch began.
    start_time: Option<std::time::Instant>,
    /// Last height at which a progress line was emitted (to throttle logs).
    last_logged: u64,
    /// `behind` at the start of the stretch — used to pick the severity for
    /// the matching "caught up" line.
    start_behind: u64,
}

impl BackfillProgress {
    /// State for a freshly-entered "behind" stretch: anchor the start height and
    /// clock (the reference point for rate/ETA), seed the log throttle at the
    /// current height, and record the initial gap for the "caught up" severity.
    fn new(contiguous: u64, behind: u64) -> Self {
        Self {
            start_height: Some(contiguous),
            start_time: Some(std::time::Instant::now()),
            last_logged: contiguous,
            start_behind: behind,
        }
    }

    /// Record one observation while behind the tip. Starts a new stretch when
    /// none is active — logging the "starting" line and returning `true` so the
    /// caller can count it — otherwise emits a throttled progress line. Returns
    /// `false` when no new stretch began.
    fn observe(&mut self, contiguous: u64, target: u64, behind: u64) -> bool {
        if self.start_height.is_none() {
            *self = Self::new(contiguous, behind);
            event_at!(
                behind_level(behind),
                contiguous,
                target,
                behind,
                "backfill starting"
            );
            return true;
        }
        if contiguous.saturating_sub(self.last_logged) >= BACKFILL_LOG_EVERY {
            self.last_logged = contiguous;
            let (rate, eta) = self.eta(contiguous, behind);
            info!(
                contiguous,
                target,
                behind,
                bps = format_args!("{rate:.2}"),
                eta = %eta.map_or_else(|| "?".to_owned(), format_secs),
                "backfill progress",
            );
        }
        false
    }

    /// Close out the active stretch (if any): log the "caught up" line, then
    /// reset to the idle `Default`. A no-op when no stretch was running.
    fn caught_up(&mut self, contiguous: u64) {
        if let (Some(start_h), Some(start_t)) = (self.start_height, self.start_time) {
            event_at!(
                behind_level(self.start_behind),
                blocks = contiguous.saturating_sub(start_h),
                elapsed = %format_secs(start_t.elapsed().as_secs()),
                "backfill caught up",
            );
        }
        *self = Self::default();
    }

    /// Compute `(blocks_per_sec, eta)` for the active stretch. Rate is blocks
    /// filled since the stretch began over elapsed wall-clock; ETA is remaining
    /// `behind` over that rate. ETA is `None` without enough signal yet (zero
    /// elapsed or no progress).
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    fn eta(&self, contiguous: u64, behind: u64) -> (f64, Option<u64>) {
        let (Some(start_h), Some(start_t)) = (self.start_height, self.start_time) else {
            return (0.0, None);
        };
        let elapsed = start_t.elapsed().as_secs_f64();
        let filled = contiguous.saturating_sub(start_h);
        if elapsed <= 0.0 || filled == 0 {
            return (0.0, None);
        }
        let rate = filled as f64 / elapsed;
        let eta = (behind as f64 / rate).round() as u64;
        (rate, Some(eta))
    }
}

/// Pick a log level from how far behind the tip we are. Small gaps (1-2) are
/// debug noise; moderate gaps (3-20) are info; large gaps (>20) are warn.
const fn behind_level(behind: u64) -> Level {
    match behind {
        0..=2 => Level::DEBUG,
        3..=20 => Level::INFO,
        _ => Level::WARN,
    }
}

/// Heights between progress lines during a long backfill stretch. At the
/// observed steady-state rate of ~4 blocks/sec this yields one line per
/// minute, which is enough signal without spamming the log.
const BACKFILL_LOG_EVERY: u64 = 300;

/// First periodic summary fires this soon after startup so the operator
/// sees confirmation that ingest is running without waiting a full period.
const SUMMARY_FIRST_DELAY: Duration = Duration::from_secs(5);

/// Emit a single INFO line at startup and then every `period`, reporting
/// `block`, `contiguous`, `behind`, new blocks ingested in the period, rate,
/// and how many backfill stretches started since the last summary.
/// Steady-state per-block events live at DEBUG; this is the operator-visible
/// heartbeat.
pub(crate) async fn summary_loop(
    storage: Storage,
    period: Duration,
    backfill_count: Arc<AtomicU64>,
) {
    let mut delay = SUMMARY_FIRST_DELAY;
    let mut prev: Option<(u64, std::time::Instant)> = None;
    loop {
        tokio::time::sleep(delay).await;
        delay = period;
        let hw = storage.high_water().await;
        let mc = storage.max_contiguous_height().await;
        let now = std::time::Instant::now();
        let backfills = backfill_count.swap(0, Ordering::Relaxed);
        // Derive `behind` from the same snapshot as `block`/`contiguous` rather
        // than the `behind_tip` atomic, which the backfill task updates on its
        // own cadence and would otherwise contradict the heights on this line.
        let behind = hw.saturating_sub(mc);
        match prev {
            None => {
                // First tick is a heartbeat — rate has no meaning yet because
                // we haven't sampled an interval.
                info!(
                    block = hw,
                    contiguous = mc,
                    behind,
                    backfill = backfills,
                    "summary (startup)",
                );
            }
            Some((prev_hw, prev_t)) => {
                let elapsed = now.duration_since(prev_t).as_secs_f64();
                let added = hw.saturating_sub(prev_hw);
                #[allow(clippy::cast_precision_loss)]
                let rate = if elapsed > 0.0 {
                    added as f64 / elapsed
                } else {
                    0.0
                };
                info!(
                    block = hw,
                    contiguous = mc,
                    behind,
                    new = added,
                    bps = format_args!("{rate:.2}"),
                    backfill = backfills,
                    "summary",
                );
            }
        }
        prev = Some((hw, now));
    }
}

/// Format a seconds count as e.g. `3h12m`, `45m`, `12s`, rendering 0 as `<1s`
/// (a genuine sub-second duration). Compact for log lines. "Unknown" is not this
/// function's concern — the ETA call site maps its own no-signal sentinel to `?`.
fn format_secs(s: u64) -> String {
    if s == 0 {
        return "<1s".to_owned();
    }
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let sec = s % 60;
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{sec:02}s")
    } else {
        format!("{sec}s")
    }
}

/// Minimum delay between backfill block fetches. Caps the worker at ~25 req/s
/// against Cloudflare's rate limit on the public Avalanche endpoint. The
/// newHead ingester is unaffected — it fetches at chain pace. `--mirror-from`
/// overrides this to zero (the upstream is another neve, no rate limit).
pub(crate) const BACKFILL_INTER_FETCH_MS: u64 = 40;

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
pub(crate) async fn backfill_loop(
    storage: Storage,
    http: reqwest::Client,
    cfg: IngestCfg,
    backfill_count: Arc<AtomicU64>,
    behind_tip: Arc<AtomicU64>,
) {
    wait_for_bootstrap(&cfg).await;
    let mut progress = BackfillProgress::default();
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
        let upstream = upstream_block_number(&http, &cfg).await.unwrap_or(0);
        let target = hw.max(upstream);
        // Effective floor for "behind"/next accounting. Before the first block
        // is written, the store reports max_contiguous_height = 0; the mirror
        // floor tells us the real baseline so progress isn't off by the floor.
        let floor = cfg.backfill_floor.unwrap_or(0);
        let raw_contiguous = storage.max_contiguous_height().await;
        let contiguous = raw_contiguous.max(floor.saturating_sub(1));
        let behind = target.saturating_sub(contiguous);
        // Re-read the stored tip adjacent to the contiguous read and clamp to it:
        // `hw` above predates the upstream round-trip, during which live ingestion
        // can lift the store past it, inverting the head/contiguous gauges.
        metrics::ingest_heights(
            storage.high_water().await.max(contiguous),
            contiguous,
            behind,
        );
        if contiguous >= target {
            behind_tip.store(0, Ordering::Relaxed);
            progress.caught_up(contiguous);
            tokio::time::sleep(BACKFILL_CAUGHT_UP_POLL).await;
            continue;
        }
        behind_tip.store(behind, Ordering::Relaxed);
        if progress.observe(contiguous, target, behind) {
            backfill_count.fetch_add(1, Ordering::Relaxed);
        }
        backfill_next_block(&storage, &http, &cfg, contiguous.saturating_add(1)).await;
    }
}

/// Fetch block `next` and persist it. Skips
/// silently when newHeads already filled the slot. On any miss or error it naps
/// briefly and returns so the caller re-measures and retries; on success it
/// applies the inter-fetch rate-limit nap.
async fn backfill_next_block(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    next: u64,
) {
    // Race guard: newHead may have just filled this slot.
    if matches!(storage.get_by_height(next).await, Ok(Some(_))) {
        return;
    }
    let Some(block) = fetch_full_block(http, next, cfg, None).await else {
        tokio::time::sleep(Duration::from_secs(1)).await;
        return;
    };
    if let Err(e) = persist_backfilled(storage, next, &block).await {
        warn!(height = next, error = %e, "backfill persist failed");
        tokio::time::sleep(Duration::from_secs(1)).await;
        return;
    }
    if !cfg.backfill_inter_fetch.is_zero() {
        tokio::time::sleep(cfg.backfill_inter_fetch).await;
    }
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
        .put(
            height,
            hash_bytes,
            &tx_hashes,
            &bytes,
            crate::record::EMPTY_LOGS,
        )
        .await?;
    metrics::block_persisted(metrics::BlockSource::Backfill);
    debug!(
        height,
        bytes = block_len,
        txs = tx_hashes.len(),
        "backfilled block",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_secs_buckets() {
        assert_eq!(format_secs(0), "<1s");
        assert_eq!(format_secs(5), "5s");
        assert_eq!(format_secs(59), "59s");
        assert_eq!(format_secs(60), "1m00s");
        assert_eq!(format_secs(125), "2m05s");
        assert_eq!(format_secs(3600), "1h00m");
        assert_eq!(format_secs(3 * 3600 + 12 * 60 + 7), "3h12m");
    }

    #[test]
    fn eta_idle_when_no_progress() {
        let p = BackfillProgress::default();
        let (rate, eta) = p.eta(100, 50);
        assert!(rate.abs() < f64::EPSILON, "rate {rate} should be 0");
        assert_eq!(eta, None, "no progress yet → ETA unknown");
    }

    #[test]
    fn eta_math_from_known_rate() {
        // Stretch started 2 seconds ago at height 1000; we've filled 20 blocks
        // (now at 1020) and 80 remain. Rate 10 blk/s → ETA 8 s.
        let start_time = std::time::Instant::now()
            .checked_sub(Duration::from_secs(2))
            .expect("clock can subtract 2s");
        let p = BackfillProgress {
            start_height: Some(1000),
            start_time: Some(start_time),
            last_logged: 0,
            start_behind: 0,
        };
        let (rate, eta) = p.eta(1020, 80);
        let eta = eta.expect("known rate yields a concrete ETA");
        // Allow some wiggle for the clock since the test started.
        assert!((rate - 10.0).abs() < 1.5, "rate {rate} not near 10");
        assert!((6..=10).contains(&eta), "eta {eta} not near 8");
    }
}
