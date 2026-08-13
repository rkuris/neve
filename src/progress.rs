//! Operator-visible progress reporting, shared by every chain's ingest
//! pipeline: the per-stretch backfill progress/ETA lines and the periodic
//! `summary` heartbeat.
//!
//! Nothing here is chain-specific — heights and rates read the same on any
//! chain — so each instance gets its own tracker and tags its lines with the
//! chain label.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{Level, debug, info, warn};

use crate::chain::Chain;
use crate::storage::Storage;

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

/// Mutable progress state for one chain's backfill task.
#[derive(Debug)]
pub(crate) struct BackfillProgress {
    /// Which chain's lines this tracker emits.
    chain: Chain,
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
    /// An idle tracker for `chain`: no stretch running.
    pub(crate) const fn new(chain: Chain) -> Self {
        Self {
            chain,
            start_height: None,
            start_time: None,
            last_logged: 0,
            start_behind: 0,
        }
    }

    /// Reset to idle, keeping the chain.
    const fn reset(&mut self) {
        *self = Self::new(self.chain);
    }

    /// Enter a "behind" stretch: anchor the start height and clock (the
    /// reference point for rate/ETA), seed the log throttle at the current
    /// height, and record the initial gap for the "caught up" severity.
    fn begin(&mut self, contiguous: u64, behind: u64) {
        self.start_height = Some(contiguous);
        self.start_time = Some(std::time::Instant::now());
        self.last_logged = contiguous;
        self.start_behind = behind;
    }

    /// Record one observation while behind the tip. Starts a new stretch when
    /// none is active — logging the "starting" line and returning `true` so the
    /// caller can count it — otherwise emits a throttled progress line. Returns
    /// `false` when no new stretch began.
    pub(crate) fn observe(&mut self, contiguous: u64, target: u64, behind: u64) -> bool {
        if self.start_height.is_none() {
            self.begin(contiguous, behind);
            event_at!(
                behind_level(behind),
                chain = self.chain.as_str(),
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
                chain = self.chain.as_str(),
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
    /// reset to idle. A no-op when no stretch was running.
    pub(crate) fn caught_up(&mut self, contiguous: u64) {
        if let (Some(start_h), Some(start_t)) = (self.start_height, self.start_time) {
            event_at!(
                behind_level(self.start_behind),
                chain = self.chain.as_str(),
                blocks = contiguous.saturating_sub(start_h),
                elapsed = %format_secs(start_t.elapsed().as_secs()),
                "backfill caught up",
            );
        }
        self.reset();
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

/// How far behind the summary should claim to be, given the stored high-water
/// mark, the contiguous frontier, and the ingest task's distance to the upstream
/// tip. Two different gaps, and the operator wants the worse one:
///
/// - `hw - mc` is the *internal* gap — heights already stored above an unfilled
///   hole. That is what "behind" means on the C-chain, where newHeads keeps
///   writing at the tip while backfill closes holes underneath it.
/// - `behind_tip` is the distance to the *upstream* tip, and is the only one that
///   says anything during a forward fill from genesis: that fill is gapless, so
///   `hw == mc` and the internal gap reads 0 while the chain is still millions of
///   blocks away.
const fn behind_of(hw: u64, mc: u64, behind_tip: u64) -> u64 {
    let internal = hw.saturating_sub(mc);
    if internal > behind_tip {
        internal
    } else {
        behind_tip
    }
}

/// Emit a single INFO line at startup and then every `period` for one chain,
/// reporting `block`, `contiguous`, `behind`, new blocks ingested in the period,
/// rate, and how many backfill stretches started since the last summary.
/// Steady-state per-block events live at DEBUG; this is the operator-visible
/// heartbeat. One task per chain instance, each tagging its own `chain` label.
pub(crate) async fn summary_loop(
    storage: Storage,
    period: Duration,
    backfill_count: Arc<AtomicU64>,
    behind_tip: Arc<AtomicU64>,
) {
    let chain = storage.chain().as_str();
    let mut delay = SUMMARY_FIRST_DELAY;
    let mut prev: Option<(u64, std::time::Instant)> = None;
    loop {
        tokio::time::sleep(delay).await;
        delay = period;
        let hw = storage.high_water().await;
        let mc = storage.max_contiguous_height().await;
        let now = std::time::Instant::now();
        let backfills = backfill_count.swap(0, Ordering::Relaxed);
        let behind = behind_of(hw, mc, behind_tip.load(Ordering::Relaxed));
        match prev {
            None => {
                // First tick is a heartbeat — rate has no meaning yet because
                // we haven't sampled an interval.
                info!(
                    chain,
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
                    chain,
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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

    /// The regression this exists for: a P-chain fill from genesis is gapless, so
    /// the internal gap is 0 and only `behind_tip` knows the chain is 21M behind.
    #[test]
    fn behind_prefers_the_worse_gap() {
        assert_eq!(behind_of(3_372_390, 3_372_390, 21_984_910), 21_984_910);
        // C-chain shape: newHeads is 500 ahead of the frontier and the backfill
        // task hasn't stored a fresh gap yet.
        assert_eq!(behind_of(1_500, 1_000, 0), 500);
        assert_eq!(behind_of(1_000, 1_000, 0), 0);
    }

    #[test]
    fn eta_idle_when_no_progress() {
        let p = BackfillProgress::new(Chain::C);
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
            chain: Chain::C,
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

    /// A stretch that starts and then catches up returns the tracker to idle, so
    /// the next stretch re-anchors instead of reporting a stale rate.
    #[test]
    fn caught_up_returns_tracker_to_idle() {
        let mut p = BackfillProgress::new(Chain::P);
        assert!(
            p.observe(100, 200, 100),
            "first observation starts a stretch"
        );
        assert!(!p.observe(101, 200, 99), "second does not start another");
        p.caught_up(200);
        assert_eq!(p.start_height, None);
        // The chain label survives the reset.
        assert_eq!(p.chain, Chain::P);
        assert!(p.observe(200, 300, 100), "a new stretch can start again");
    }
}
