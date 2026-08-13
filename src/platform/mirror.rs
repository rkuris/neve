//! P-chain mirror mode: fill a store from another neve instead of from a node.
//!
//! avalanchego has no push mechanism for P-chain blocks, so a neve-P instance is
//! the only thing that can stream them — which makes neve→neve the *only*
//! streaming replication path this chain has. It also sidesteps the public
//! endpoint's rate limit entirely (measured: ~14 req/s draws a one-hour 429),
//! which is what makes deep history practical at all.
//!
//! The stream carries whole **records**, not blocks: element 1 holds the
//! canonical bytes, and without them a mirror could serve neither the hex
//! encodings nor verify a block ID. Each arriving record goes through exactly
//! the same `sha256(bytes) == blockID` check as a directly-fetched one, so a
//! mirror is no more trusting of its upstream than of a node.
//!
//! Shape: one `oldRecords` bootstrap over the historical range, then a live
//! `newRecords` subscription — mirroring the C-chain's bootstrap-then-follow
//! flow, minus the per-block fetch it never needs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::chain::{Chain, IngestCfg};
use crate::metrics::{self, BlockSource};
use crate::platform::ingest;
use crate::progress::BackfillProgress;
use crate::storage::Storage;
use crate::upstream;

/// Reconnect backoff cap. Same ladder as the C-chain's session loop.
const MAX_BACKOFF_MS: u64 = 30_000;

/// Mirror loop: bootstrap the historical range, then follow the tip, reconnecting
/// with backoff. Runs until the process shuts down.
pub(crate) async fn mirror(
    storage: Storage,
    http: reqwest::Client,
    cfg: IngestCfg,
    backfill_count: Arc<AtomicU64>,
    behind_tip: Arc<AtomicU64>,
) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        match session(&storage, &http, &cfg, &backfill_count, &behind_tip).await {
            Ok(()) => {
                info!(chain = "p", "mirror session ended cleanly, reconnecting");
                attempt = 0;
            }
            Err(e) => {
                warn!(chain = "p", error = ?e, attempt, "mirror session failed");
                attempt = attempt.saturating_add(1);
            }
        }
        metrics::ws_reconnect(Chain::P);
        let backoff_ms = 500u64
            .saturating_mul(1u64 << attempt.min(6))
            .min(MAX_BACKOFF_MS);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }
}

/// One connection's worth of work: catch up on history, then follow the tip.
async fn session(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    backfill_count: &Arc<AtomicU64>,
    behind_tip: &Arc<AtomicU64>,
) -> Result<()> {
    bootstrap(storage, http, cfg, backfill_count, behind_tip).await?;
    follow_tip(storage, cfg, behind_tip).await
}

/// Stream the historical range from the upstream neve over one `oldRecords`
/// subscription.
///
/// Runs to a fixed target — the upstream's contiguous tip, read once from
/// `/health` — so completion is self-determined (we know we're done when we've
/// persisted that height) rather than relying on detecting a server-side
/// subscription close, which the raw frame reader can't see cleanly. Requesting
/// exactly the contiguous tip also guarantees the server accepts the range.
async fn bootstrap(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    backfill_count: &Arc<AtomicU64>,
    behind_tip: &Arc<AtomicU64>,
) -> Result<()> {
    let target = fetch_upstream_contiguous(http, &cfg.rpc_url).await?;
    // First height we lack: the configured floor on a cold start, otherwise one
    // past what we already hold contiguously.
    let from = if storage.is_empty().await {
        cfg.backfill_floor.unwrap_or(0)
    } else {
        storage.max_contiguous_height().await.saturating_add(1)
    };
    if from > target {
        info!(
            chain = "p",
            from, target, "mirror bootstrap: already current with upstream"
        );
        return Ok(());
    }
    let count = target.saturating_sub(from).saturating_add(1);
    info!(
        chain = "p",
        from,
        to = target,
        count,
        "mirror bootstrap: streaming historical records",
    );
    backfill_count.fetch_add(1, Ordering::Relaxed);
    let mut progress = BackfillProgress::new(Chain::P, cfg.progress_period);
    progress.observe(from.saturating_sub(1), target, count);

    let (mut tx, mut rx) = upstream::connect_ws(cfg).await?;
    upstream::send_request(
        &mut tx,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "platform.subscribe",
            "params": { "kind": "oldRecords", "from": from, "to": target },
        }),
    )
    .await?;

    loop {
        // A stalled stream (or an upstream that rejected the subscription)
        // shouldn't hang startup forever.
        let frame =
            match tokio::time::timeout(cfg.ws_idle_timeout, upstream::next_frame(&mut tx, &mut rx))
                .await
            {
                Ok(Some(f)) => f,
                Ok(None) => bail!("oldRecords stream ended before reaching target {target}"),
                Err(_elapsed) => {
                    metrics::ws_idle_timeout(Chain::P);
                    bail!(
                        "mirror bootstrap idle for {}s before reaching target {target}",
                        cfg.ws_idle_timeout.as_secs(),
                    );
                }
            };
        let Some(record) = notification_payload(&frame) else {
            continue;
        };
        let Some(height) = persist_record(storage, record, BlockSource::Backfill).await else {
            // A record that fails verification is a real problem with the
            // upstream; don't silently skip a height and leave a gap.
            bail!("mirror bootstrap: upstream sent a record that failed verification");
        };
        behind_tip.store(target.saturating_sub(height), Ordering::Relaxed);
        progress.observe(height, target, target.saturating_sub(height));
        if height >= target {
            progress.caught_up(height);
            info!(chain = "p", target, "mirror bootstrap complete");
            return Ok(());
        }
    }
}

/// Follow the upstream's tip over a live `newRecords` subscription.
async fn follow_tip(storage: &Storage, cfg: &IngestCfg, behind_tip: &Arc<AtomicU64>) -> Result<()> {
    let (mut tx, mut rx) = upstream::connect_ws(cfg).await?;
    upstream::send_request(
        &mut tx,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "platform.subscribe",
            "params": { "kind": "newRecords" },
        }),
    )
    .await?;
    metrics::upstream_connected(Chain::P);
    info!(chain = "p", "mirror following upstream tip via newRecords");

    loop {
        // The idle watchdog is the only thing that notices a silently-dead
        // socket: P-chain blocks are demand-driven and bursty, so a long quiet
        // stretch is normal — hence reconnecting rather than treating it as
        // fatal.
        let frame =
            match tokio::time::timeout(cfg.ws_idle_timeout, upstream::next_frame(&mut tx, &mut rx))
                .await
            {
                Ok(Some(f)) => f,
                Ok(None) => return Ok(()),
                Err(_elapsed) => {
                    metrics::ws_idle_timeout(Chain::P);
                    return Err(anyhow!(
                        "no P-chain records within {}s idle timeout; reconnecting",
                        cfg.ws_idle_timeout.as_secs(),
                    ));
                }
            };
        let Some(record) = notification_payload(&frame) else {
            continue;
        };
        if let Some(height) = persist_record(storage, record, BlockSource::Live).await {
            behind_tip.store(0, Ordering::Relaxed);
            debug!(chain = "p", height, "mirrored live record");
        }
    }
}

/// The `params.result` of a `platform.subscription` notification, or `None` for
/// any other frame (the subscription ack, an error, a stray response).
fn notification_payload(v: &Value) -> Option<&Value> {
    if v.get("method").and_then(Value::as_str)? != "platform.subscription" {
        // The ack carries the subscription id; log it once and move on.
        if let Some(result) = v.get("result")
            && v.get("id").is_some()
        {
            info!(chain = "p", sub = %result, "subscribed");
        } else if let Some(err) = v.get("error") {
            warn!(chain = "p", error = %err, "upstream rejected the subscription");
        }
        return None;
    }
    v.get("params")?.get("result")
}

/// Verify and store one streamed record. Returns its height on success.
///
/// The record arrives as the same JSON array that is on disk upstream, so the
/// halves are split back out and put through the identical cross-check the
/// direct-fetch path uses — a mirror trusts its upstream exactly as much as it
/// would trust a node, which is to say only as far as the bytes verify.
async fn persist_record(storage: &Storage, record: &Value, source: BlockSource) -> Option<u64> {
    let Some(parts) = record.as_array() else {
        warn!(chain = "p", "streamed record is not an array");
        return None;
    };
    let want = crate::record::arity(Chain::P);
    if parts.len() != want {
        warn!(
            chain = "p",
            got = parts.len(),
            want,
            "streamed record has the wrong element count",
        );
        return None;
    }
    // Index-free destructuring so a shape change can't silently mis-bind.
    let [block_json, bytes_json, rewards] = parts.as_slice() else {
        return None;
    };
    let fetched = ingest::verify_record(bytes_json, block_json)?;
    let rewards_bytes = serde_json::to_vec(rewards).ok()?;
    ingest::store_block(storage, &fetched, &rewards_bytes, source).await
}

/// Read the upstream neve's contiguous P-chain tip from `/health`. Used as the
/// fixed end for the bootstrap: it's a height the upstream can serve gaplessly,
/// which makes bootstrap completion self-determined.
async fn fetch_upstream_contiguous(http: &reqwest::Client, base: &str) -> Result<u64> {
    let url = format!("{}/health", base.trim_end_matches('/'));
    let resp = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {}", crate::upstream::redact_url(&url)))?;
    if !resp.status().is_success() {
        bail!("upstream /health returned HTTP {}", resp.status());
    }
    let v: Value = resp.json().await.context("decode /health body")?;
    crate::health::upstream_blocks_field(&v, Chain::P, "max_contiguous_height").ok_or_else(|| {
        anyhow!("/health has no P-chain blocks.max_contiguous_height (is the upstream mirroring the P-chain?)")
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// A streamed record has to be a P-chain record — right arity, halves that
    /// agree. Anything else is refused rather than written, so a mirror trusts
    /// its upstream no further than a node.
    #[tokio::test]
    async fn persist_record_refuses_malformed_or_inconsistent_records() {
        use crate::platform::codec;
        use crate::test_support::{chain_serve, unique_temp_dir};

        let dir = unique_temp_dir("mirror-persist");
        let c = chain_serve(Chain::P, &dir);
        let bytes = b"canonical-block-bytes".to_vec();
        let hexnc = codec::Encoding::Hexnc.render_bytes(&bytes).unwrap();
        let good_block = json!({ "id": codec::block_id_of(&bytes), "height": 7, "txs": [] });

        // Not an array; wrong arity; halves that disagree.
        for bad in [
            json!({ "height": 7 }),
            json!([good_block.clone(), hexnc.clone()]),
            json!([
                json!({ "id": codec::cb58_encode(&[9; 32]), "height": 7 }),
                hexnc.clone(),
                []
            ]),
        ] {
            assert!(
                persist_record(&c.storage, &bad, BlockSource::Backfill)
                    .await
                    .is_none(),
                "should have refused {bad}",
            );
            assert!(c.storage.is_empty().await, "nothing may reach the store");
        }

        // The well-formed record does land, at the height the record reports.
        let good = json!([good_block, hexnc, []]);
        assert_eq!(
            persist_record(&c.storage, &good, BlockSource::Backfill).await,
            Some(7),
        );
        assert!(c.storage.get_by_height(7).await.unwrap().is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn notification_payload_extracts_only_subscription_frames() {
        let notif = json!({
            "jsonrpc": "2.0",
            "method": "platform.subscription",
            "params": { "subscription": "0x1", "result": [{"height": 7}, "0x00", []] },
        });
        let got = notification_payload(&notif).unwrap();
        assert_eq!(got[0]["height"], 7);

        // The subscribe ack, an error, and a stray response are all skipped.
        assert!(notification_payload(&json!({"id": 1, "result": "0xsub"})).is_none());
        assert!(notification_payload(&json!({"id": 1, "error": {"code": -32000}})).is_none());
        assert!(notification_payload(&json!({"method": "eth_subscription"})).is_none());
    }
}
