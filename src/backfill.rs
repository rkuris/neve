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
use crate::subscribe::{decode_hash, extract_tx_hashes, fetch_block_receipts, fetch_full_block};

/// Mutable progress state for the backfill task. Held in one struct so adding
/// an ETA calculation later is local: the start fields already capture the
/// reference point a rate calculation needs.
#[derive(Debug)]
struct BackfillProgress {
    /// Height at which the current "behind" stretch began. `None` when caught up.
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
    const fn new() -> Self {
        Self {
            start_height: None,
            start_time: None,
            last_logged: 0,
            start_behind: 0,
        }
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

/// Compute `(blocks_per_sec, eta_secs)` from a `BackfillProgress` snapshot. Rate is
/// blocks filled since the stretch began divided by elapsed wall-clock; ETA is
/// remaining `behind` divided by that rate. Returns `(0.0, 0)` when there's
/// not enough signal yet (e.g. zero elapsed or no progress).
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]
fn eta_from_progress(p: &BackfillProgress, contiguous: u64, behind: u64) -> (f64, u64) {
    let (Some(start_h), Some(start_t)) = (p.start_height, p.start_time) else {
        return (0.0, 0);
    };
    let elapsed = start_t.elapsed().as_secs_f64();
    let filled = contiguous.saturating_sub(start_h);
    if elapsed <= 0.0 || filled == 0 {
        return (0.0, 0);
    }
    let rate = filled as f64 / elapsed;
    let eta = (behind as f64 / rate).round() as u64;
    (rate, eta)
}

/// Format a seconds count as e.g. `3h12m`, `45m`, `12s`. Compact for log lines.
fn format_secs(s: u64) -> String {
    if s == 0 {
        return "?".to_owned();
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
    let mut progress = BackfillProgress::new();
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
        metrics::ingest_heights(hw, contiguous, behind);
        if contiguous >= target {
            behind_tip.store(0, Ordering::Relaxed);
            if let (Some(start_h), Some(start_t)) = (progress.start_height, progress.start_time) {
                let filled = contiguous.saturating_sub(start_h);
                let elapsed = start_t.elapsed().as_secs();
                // `format_secs` renders 0 as "?" (unknown ETA); here 0 just
                // means the stretch closed in under a second.
                let elapsed_str = if elapsed == 0 {
                    "<1s".to_owned()
                } else {
                    format_secs(elapsed)
                };
                match behind_level(progress.start_behind) {
                    Level::DEBUG => debug!(
                        blocks = filled,
                        elapsed = %elapsed_str,
                        "backfill caught up",
                    ),
                    Level::WARN => warn!(
                        blocks = filled,
                        elapsed = %elapsed_str,
                        "backfill caught up",
                    ),
                    _ => info!(
                        blocks = filled,
                        elapsed = %elapsed_str,
                        "backfill caught up",
                    ),
                }
                progress = BackfillProgress::new();
            }
            tokio::time::sleep(BACKFILL_CAUGHT_UP_POLL).await;
            continue;
        }
        behind_tip.store(behind, Ordering::Relaxed);
        // Entering a "behind" stretch (or first iteration of one).
        if progress.start_height.is_none() {
            progress.start_height = Some(contiguous);
            progress.start_time = Some(std::time::Instant::now());
            progress.last_logged = contiguous;
            progress.start_behind = behind;
            backfill_count.fetch_add(1, Ordering::Relaxed);
            match behind_level(behind) {
                Level::DEBUG => debug!(contiguous, target, behind, "backfill starting"),
                Level::WARN => warn!(contiguous, target, behind, "backfill starting"),
                _ => info!(contiguous, target, behind, "backfill starting"),
            }
        } else if contiguous.saturating_sub(progress.last_logged) >= BACKFILL_LOG_EVERY {
            progress.last_logged = contiguous;
            let (rate, eta_secs) = eta_from_progress(&progress, contiguous, behind);
            info!(
                contiguous,
                target,
                behind,
                bps = format_args!("{rate:.2}"),
                eta = %format_secs(eta_secs),
                "backfill progress",
            );
        }
        let next = contiguous.saturating_add(1);
        // Race guard: newHead may have just filled this slot.
        if matches!(storage.get_by_height(next).await, Ok(Some(_))) {
            continue;
        }
        let number_hex = format!("0x{next:x}");
        let Some(block) = fetch_full_block(&http, &number_hex, next, &cfg).await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let receipts_value = if cfg.receipts {
            let Some(r) = fetch_block_receipts(&http, &number_hex, next, &cfg).await else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            Some(r)
        } else {
            None
        };
        if let Err(e) = persist_backfilled(&storage, next, &block, receipts_value.as_ref()).await {
            warn!(height = next, error = %e, "backfill persist failed");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        if !cfg.backfill_inter_fetch.is_zero() {
            tokio::time::sleep(cfg.backfill_inter_fetch).await;
        }
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
    receipts: Option<&Value>,
) -> Result<()> {
    let body_hash = block
        .get("hash")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("backfilled block missing hash"))?;
    let hash_bytes = decode_hash(body_hash)?;
    let tx_hashes = extract_tx_hashes(block);
    let bytes = serde_json::to_vec(block)?;
    let receipts_bytes = receipts.map(serde_json::to_vec).transpose()?;
    let block_len = bytes.len();
    let receipts_len = receipts_bytes.as_ref().map_or(0, Vec::len);
    storage
        .put(height, hash_bytes, &tx_hashes, bytes, receipts_bytes)
        .await?;
    metrics::block_persisted(metrics::BlockSource::Backfill);
    debug!(
        height,
        bytes = block_len,
        receipts_bytes = receipts_len,
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
        assert_eq!(format_secs(0), "?");
        assert_eq!(format_secs(5), "5s");
        assert_eq!(format_secs(59), "59s");
        assert_eq!(format_secs(60), "1m00s");
        assert_eq!(format_secs(125), "2m05s");
        assert_eq!(format_secs(3600), "1h00m");
        assert_eq!(format_secs(3 * 3600 + 12 * 60 + 7), "3h12m");
    }

    #[test]
    fn eta_idle_when_no_progress() {
        let p = BackfillProgress::new();
        let (rate, eta) = eta_from_progress(&p, 100, 50);
        assert!(rate.abs() < f64::EPSILON, "rate {rate} should be 0");
        assert_eq!(eta, 0);
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
        let (rate, eta) = eta_from_progress(&p, 1020, 80);
        // Allow some wiggle for the clock since the test started.
        assert!((rate - 10.0).abs() < 1.5, "rate {rate} not near 10");
        assert!((6..=10).contains(&eta), "eta {eta} not near 8");
    }
}
