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
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::chain::{Chain, IngestCfg};
use crate::eth::ingest::{decode_hash, extract_tx_hashes, fetch_full_block, fetch_logs};
use crate::metrics;
use crate::progress::BackfillProgress;
use crate::record;
use crate::storage::Storage;

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
    /// Inclusive `[start, end]` currently cached, if any.
    range: Option<(u64, u64)>,
    /// Logs bucketed by block height; heights with no logs are absent.
    by_height: HashMap<u64, Vec<Value>>,
}

impl LogWindow {
    const fn covers(&self, height: u64) -> bool {
        match self.range {
            Some((start, end)) => height >= start && height <= end,
            None => false,
        }
    }

    /// Bucket a freshly-fetched `[from, to]` logs array by block height.
    fn load(&mut self, from: u64, to: u64, logs: Value) {
        self.by_height.clear();
        if let Value::Array(items) = logs {
            for log in items {
                if let Some(height) = block_number_of(&log) {
                    self.by_height.entry(height).or_default().push(log);
                }
            }
        }
        self.range = Some((from, to));
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

    /// This height's logs, loading the covering window via one `eth_getLogs` call
    /// when needed; `tip` caps the window's upper bound. `None` only on a fetch
    /// failure (the caller retries the height).
    async fn logs_for(
        &mut self,
        http: &reqwest::Client,
        cfg: &IngestCfg,
        height: u64,
        tip: u64,
    ) -> Option<(Vec<u8>, usize)> {
        if !self.covers(height) {
            let from = height;
            let to = height
                .saturating_add(LOGS_WINDOW.saturating_sub(1))
                .min(tip);
            let logs = fetch_logs(http, cfg, from, to).await?;
            debug!(
                from,
                to,
                count = logs.as_array().map_or(0, Vec::len),
                "fetched logs window"
            );
            self.load(from, to, logs);
        }
        Some(self.serialized(height))
    }
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
    let mut progress = BackfillProgress::new(Chain::C);
    let mut logs = LogWindow::default();
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
            Chain::C,
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
        backfill_next_block(
            &storage,
            &http,
            &cfg,
            contiguous.saturating_add(1),
            target,
            &mut logs,
        )
        .await;
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
    tip: u64,
    logs: &mut LogWindow,
) {
    // Race guard: newHead may have just filled this slot.
    if matches!(storage.get_by_height(next).await, Ok(Some(_))) {
        return;
    }
    let Some(block) = fetch_full_block(http, next, cfg, None).await else {
        tokio::time::sleep(Duration::from_secs(1)).await;
        return;
    };
    // Join this height's logs window-locally when log ingestion is on; otherwise
    // store an empty logs half (the live-logs milestone fills the tip later). On
    // a logs-fetch failure, retry the whole height rather than persist a
    // block-only record.
    let (logs_bytes, log_count) = if cfg.ingest_logs {
        let Some(result) = logs.logs_for(http, cfg, next, tip).await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            return;
        };
        result
    } else {
        (record::EMPTY_LOGS.to_vec(), 0)
    };
    if let Err(e) = persist_backfilled(storage, next, &block, &logs_bytes).await {
        warn!(height = next, error = %e, "backfill persist failed");
        tokio::time::sleep(Duration::from_secs(1)).await;
        return;
    }
    if cfg.ingest_logs {
        metrics::logs_persisted(Chain::C, metrics::BlockSource::Backfill, log_count as u64);
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
        .put(height, hash_bytes, &tx_hashes, &bytes, logs)
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

    #[test]
    fn log_window_covers_only_loaded_range() {
        let mut w = LogWindow::default();
        assert!(!w.covers(5));
        w.load(10, 20, json!([]));
        assert!(w.covers(10) && w.covers(15) && w.covers(20));
        assert!(!w.covers(9));
        assert!(!w.covers(21));
    }

    #[test]
    fn log_window_buckets_logs_by_height() {
        let mut w = LogWindow::default();
        w.load(
            0x10,
            0x12,
            json!([
                {"blockNumber": "0x10", "logIndex": "0x0"},
                {"blockNumber": "0x10", "logIndex": "0x1"},
                {"blockNumber": "0x12", "logIndex": "0x0"},
            ]),
        );

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
        w.load(1, 1, json!([{"blockNumber": "0x1", "address": "0xabc"}]));
        let (bytes, _) = w.serialized(1);
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed[0]["address"], "0xabc");
    }
}
