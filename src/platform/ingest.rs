//! P-chain ingest: a single polling loop.
//!
//! Unlike the C-chain there is no live/backfill split, because there is no push
//! mechanism to split from — no `eth_subscribe` analog exists for P-chain
//! blocks, and the old X-chain pubsub was removed in avalanchego v1.11.13. So
//! this one loop reads the tip from `platform.getHeight` on a timer and walks the
//! contiguous frontier up to it, which is also all the recovery logic the chain
//! needs: accepted P-chain blocks are final and heights are contiguous, so there
//! is no reorg to detect and no hole that isn't simply "not fetched yet".
//!
//! Each height costs two upstream calls, `hexnc` and `json`, and both are stored
//! verbatim so either encoding serves without a codec parser. Before writing,
//! `sha256(bytes)` is checked against the JSON's block ID: the two halves must
//! describe the same block, or the height is refused.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::chain::{Chain, IngestCfg};
use crate::metrics::{self, BlockSource};
use crate::platform::codec;
use crate::progress::BackfillProgress;
use crate::record;
use crate::storage::Storage;
use crate::upstream::{handle_throttle, retry_after_secs};

/// The pieces of one height needed to store it: the canonical bytes, the JSON
/// exactly as upstream returned it, the block ID, and its tx IDs.
struct FetchedBlock {
    /// `platform.getBlockByHeight(.., "hexnc")`, verbatim (a `0x…` JSON string).
    bytes_json: Vec<u8>,
    /// `platform.getBlockByHeight(.., "json")`, verbatim.
    json: Vec<u8>,
    /// 32-byte block ID, decoded from the JSON's CB58 `id` and cross-checked
    /// against `sha256(bytes)`.
    id: [u8; 32],
    tx_ids: Vec<[u8; 32]>,
    /// The block's `time` (unix seconds), for the freshness gauge.
    timestamp: Option<u64>,
}

/// One-shot startup handshake: the genesis block's ID, which is the P-chain's
/// per-network fingerprint and the analog of `eth_chainId`.
///
/// The P-chain has no chain-ID method, so the store is bound to the network by
/// something equally immutable and equally cheap to check: the ID of height 0.
/// Deriving it from the fetched bytes rather than trusting the reported `id`
/// means this doubles as a proof that the endpoint really speaks P-chain.
pub(crate) async fn fetch_genesis_id(
    http: &reqwest::Client,
    rpc_url: &str,
    max_wait: Duration,
) -> Result<String> {
    let cfg = handshake_cfg(rpc_url, max_wait);
    let bytes_json = fetch_block_encoding(http, &cfg, 0, codec::Encoding::Hexnc)
        .await
        .ok_or_else(|| {
            anyhow!(
                "could not fetch P-chain block 0 from {rpc_url}; \
                 is it a P-chain endpoint?"
            )
        })?;
    let hex = bytes_json
        .as_str()
        .ok_or_else(|| anyhow!("P-chain block 0 'block' field is not a string"))?;
    let bytes = codec::hexnc_decode(hex).context("decoding P-chain genesis bytes")?;
    Ok(codec::block_id_of(&bytes))
}

/// A throwaway [`IngestCfg`] for the startup handshake, before the real one
/// exists. Only `rpc_url` and `max_wait` are consulted on that path.
fn handshake_cfg(rpc_url: &str, max_wait: Duration) -> IngestCfg {
    let (blocks, _) = broadcast::channel::<Value>(1);
    IngestCfg {
        chain: Chain::P,
        max_wait,
        ws_idle_timeout: Duration::ZERO,
        ws_url: String::new(),
        rpc_url: rpc_url.to_owned(),
        poll_interval: Duration::from_secs(1),
        blocks,
        subscribe_blocks: false,
        backfill_inter_fetch: Duration::ZERO,
        backfill_floor: None,
        prefetch_delay_cap: Duration::ZERO,
        fatal: Arc::new(tokio::sync::Notify::new()),
        bootstrap_done: Arc::new(tokio::sync::Notify::new()),
        ingest_logs: false,
    }
}

/// The P-chain ingest loop. Runs until the process shuts down.
pub(crate) async fn ingest(
    storage: Storage,
    http: reqwest::Client,
    cfg: IngestCfg,
    backfill_count: Arc<AtomicU64>,
    behind_tip: Arc<AtomicU64>,
) -> Result<()> {
    let mut progress = BackfillProgress::new(Chain::P);
    // The tip is re-read on a timer rather than per height: `getHeight` is
    // served from a short CDN cache, so polling it per block would spend a
    // request to learn nothing.
    let mut tip = 0u64;
    let mut tip_read_at: Option<Instant> = None;

    loop {
        let stale = tip_read_at.is_none_or(|t| t.elapsed() >= cfg.poll_interval);
        if stale {
            let polled = fetch_height(&http, &cfg).await;
            tip_read_at = Some(Instant::now());
            let Some(h) = polled else {
                // Transient upstream trouble; `fetch_rpc` already logged and paid
                // its own backoff. Keep the last known tip and retry.
                tokio::time::sleep(cfg.poll_interval).await;
                continue;
            };
            if h > tip {
                debug!(chain = "p", tip = h, "upstream tip advanced");
            }
            tip = h;
        }

        let contiguous = storage.max_contiguous_height().await;
        let next = next_height(storage.is_empty().await, contiguous, &cfg, tip);
        let behind = tip
            .saturating_sub(next)
            .saturating_add(u64::from(next <= tip));
        metrics::ingest_heights(
            Chain::P,
            storage.high_water().await.max(contiguous),
            contiguous,
            behind,
        );

        if next > tip {
            behind_tip.store(0, Ordering::Relaxed);
            progress.caught_up(contiguous);
            tokio::time::sleep(cfg.poll_interval).await;
            continue;
        }
        behind_tip.store(behind, Ordering::Relaxed);
        if progress.observe(contiguous, tip, behind) {
            backfill_count.fetch_add(1, Ordering::Relaxed);
        }

        // A height at the tip is the closest thing this chain has to a live
        // block: it is what a downstream mirror wants pushed, and the only kind
        // whose timestamp should move the freshness gauge.
        let at_tip = next >= tip;
        if !fill_height(&storage, &http, &cfg, next, at_tip).await {
            // Fetch or verification failed; back off before retrying the height
            // so a persistent problem doesn't become a hot loop.
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        if !cfg.backfill_inter_fetch.is_zero() {
            tokio::time::sleep(cfg.backfill_inter_fetch).await;
        }
    }
}

/// The next height to fetch.
///
/// A store that has never been written anchors at the configured floor, or at
/// the current tip when there is none — matching the C-chain's "anchor where you
/// came in, grow forward" default. Otherwise it is one past the contiguous
/// frontier. Kept separate from the loop (and from the C-chain's
/// `floor.saturating_sub(1)` arithmetic) so a floor of 0 means height 0 rather
/// than silently skipping genesis.
fn next_height(is_empty: bool, contiguous: u64, cfg: &IngestCfg, tip: u64) -> u64 {
    if is_empty {
        cfg.backfill_floor.unwrap_or(tip)
    } else {
        contiguous.saturating_add(1)
    }
}

/// Fetch, verify and persist one height. Returns `false` if the height should be
/// retried (fetch failure, or a block that failed verification).
async fn fill_height(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    height: u64,
    at_tip: bool,
) -> bool {
    let Some(fetched) = fetch_block(http, cfg, height).await else {
        return false;
    };
    let source = if at_tip {
        BlockSource::Live
    } else {
        BlockSource::Backfill
    };
    // Only a tip block's timestamp may move the freshness gauge; a backfilled
    // block's older timestamp would drag it backward.
    if at_tip && let Some(ts) = fetched.timestamp {
        metrics::last_block_timestamp(Chain::P, ts);
    }
    // Rewards are the Phase-1 feed (fetch-at-ingest on a committed
    // RewardValidatorTx); until then every height stores an explicit empty
    // element, which is why adding that feed needs no migration.
    let elements: [&[u8]; 3] = [&fetched.json, &fetched.bytes_json, record::EMPTY_ARRAY];
    if let Err(e) = storage
        .put(height, fetched.id, &fetched.tx_ids, &elements)
        .await
    {
        warn!(chain = "p", height, error = %e, "P-chain persist failed");
        return false;
    }
    metrics::block_persisted(Chain::P, source);
    debug!(
        chain = "p",
        height,
        txs = fetched.tx_ids.len(),
        bytes = fetched.bytes_json.len(),
        json = fetched.json.len(),
        "stored P-chain block",
    );
    if at_tip {
        announce(&cfg.blocks, &fetched.json);
    }
    true
}

/// Publish a freshly-ingested tip block to subscribers. Skips the parse+clone
/// entirely when nobody is listening, which is the common case.
fn announce(blocks: &broadcast::Sender<Value>, json: &[u8]) {
    if blocks.receiver_count() == 0 {
        return;
    }
    match serde_json::from_slice::<Value>(json) {
        Ok(v) => {
            let _ = blocks.send(v);
        }
        Err(e) => debug!(chain = "p", error = %e, "could not parse block for fan-out"),
    }
}

/// Fetch both encodings of one height and cross-check them.
///
/// The two calls are independent upstream reads of the same block, so agreement
/// is not free: `sha256(bytes)` must reproduce the CB58 ID the JSON reports, and
/// the JSON's `height` must be the height we asked for. Either mismatch means
/// something between us and the chain is wrong, and storing it would put a
/// record on disk whose halves disagree forever.
async fn fetch_block(http: &reqwest::Client, cfg: &IngestCfg, height: u64) -> Option<FetchedBlock> {
    let bytes_value = fetch_block_encoding(http, cfg, height, codec::Encoding::Hexnc).await?;
    let json_value = fetch_block_encoding(http, cfg, height, codec::Encoding::Json).await?;

    let hex = bytes_value.as_str()?;
    let bytes = match codec::hexnc_decode(hex) {
        Ok(b) => b,
        Err(e) => {
            reject(height, "bad_hex", &e.to_string());
            return None;
        }
    };

    let reported_id = json_value.get("id").and_then(Value::as_str).unwrap_or("");
    let derived_id = codec::block_id_of(&bytes);
    if reported_id != derived_id {
        reject(
            height,
            "id_mismatch",
            &format!("json reports {reported_id}, bytes hash to {derived_id}"),
        );
        return None;
    }
    let reported_height = json_value.get("height").and_then(Value::as_u64);
    if reported_height != Some(height) {
        reject(
            height,
            "height_mismatch",
            &format!("json reports height {reported_height:?}"),
        );
        return None;
    }
    let id = match codec::cb58_decode(&derived_id) {
        Ok(id) => id,
        Err(e) => {
            reject(height, "bad_id", &e.to_string());
            return None;
        }
    };

    // Serialize the two halves for storage. The `hexnc` half is stored as the
    // JSON string upstream sent, so `platform.getBlock`'s byte encodings are
    // served back verbatim.
    let bytes_json = serde_json::to_vec(&bytes_value).ok()?;
    let json = serde_json::to_vec(&json_value).ok()?;
    Some(FetchedBlock {
        bytes_json,
        json,
        id,
        tx_ids: extract_tx_ids(&json_value),
        timestamp: json_value.get("time").and_then(Value::as_u64),
    })
}

/// Count and log a height refused before it reached the store. A nonzero
/// `neve_ingest_rejected_total` means the ingest path protected the store from
/// something inconsistent — always worth looking at.
fn reject(height: u64, reason: &'static str, detail: &str) {
    metrics::block_rejected(Chain::P, reason);
    warn!(
        chain = "p",
        height, reason, detail, "refusing P-chain block; will retry",
    );
}

/// The tx IDs in a P-chain block, CB58-decoded, for every wire shape (see
/// [`crate::platform::block_txs`]). Malformed entries are skipped rather than
/// fatal: a tx type this build has never seen must flow through to the store
/// untouched rather than stall ingest — which is the whole point of storing the
/// block verbatim.
fn extract_tx_ids(block: &Value) -> Vec<[u8; 32]> {
    crate::platform::block_txs(block)
        .into_iter()
        .filter_map(|tx| tx.get("id").and_then(Value::as_str))
        .filter_map(|s| match codec::cb58_decode(s) {
            Ok(id) => Some(id),
            Err(e) => {
                debug!(chain = "p", tx = s, error = %e, "skipping unparseable tx id");
                None
            }
        })
        .collect()
}

/// `platform.getHeight` — the upstream's accepted tip. Served from a short CDN
/// cache on the public endpoint, so it can lag the true tip by a few seconds.
async fn fetch_height(http: &reqwest::Client, cfg: &IngestCfg) -> Option<u64> {
    let result = fetch_rpc(http, cfg, 0, "platform.getHeight", json!({})).await?;
    // avalanchego serializes numbers as strings.
    let raw = result.get("height")?;
    raw.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| raw.as_u64())
}

/// `platform.getBlockByHeight` in one encoding, returning the `block` field
/// verbatim: a JSON string for the hex encodings, the block object for `json`.
async fn fetch_block_encoding(
    http: &reqwest::Client,
    cfg: &IngestCfg,
    height: u64,
    encoding: codec::Encoding,
) -> Option<Value> {
    let mut result = fetch_rpc(
        http,
        cfg,
        height,
        "platform.getBlockByHeight",
        json!({ "height": height, "encoding": encoding.as_str() }),
    )
    .await?;
    result.get_mut("block").map(Value::take)
}

/// Attempts a single [`fetch_rpc`] call makes before giving up. P-chain blocks
/// below the reported tip are final and already there, so a miss is upstream
/// trouble rather than propagation lag — a short budget, and the caller retries
/// the height on the next pass.
const RPC_MAX_ATTEMPTS: u32 = 3;
/// Initial retry backoff; doubles each attempt.
const RPC_RETRY_BACKOFF_MS: u64 = 50;

/// One `platform.*` round-trip, with the same retry / `Retry-After` / fatal
/// plumbing the C-chain fetch path uses.
///
/// The P-chain dialect differs from eth in two ways this has to respect: params
/// are a **named object**, not a positional array, and a height that doesn't
/// exist comes back as a JSON-RPC *error* rather than a null result.
async fn fetch_rpc(
    http: &reqwest::Client,
    cfg: &IngestCfg,
    height: u64,
    method: &str,
    params: Value,
) -> Option<Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    for attempt in 0..RPC_MAX_ATTEMPTS {
        let started = Instant::now();
        let resp = match http.post(&cfg.rpc_url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                metrics::upstream_request(
                    Chain::P,
                    metrics::UpstreamOutcome::Error,
                    started.elapsed().as_secs_f64(),
                );
                warn!(chain = "p", error = %e, height, method, "rpc request failed");
                return None;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            metrics::upstream_request(Chain::P, status, started.elapsed().as_secs_f64());
            let retry_after = retry_after_secs(&resp).unwrap_or(5);
            handle_throttle(cfg, method, retry_after, status.as_u16()).await;
            continue;
        }
        match resp.json::<Value>().await {
            Ok(mut parsed) => {
                let result = parsed
                    .get_mut("result")
                    .map(Value::take)
                    .filter(|r| !r.is_null());
                let outcome = if !status.is_success() {
                    metrics::UpstreamOutcome::Error
                } else if result.is_some() {
                    metrics::UpstreamOutcome::Ok
                } else {
                    // The P-chain reports a missing height as an error object,
                    // so an absent result is "upstream declined", not "not yet
                    // propagated" — but it is the same `empty` signal for rates.
                    metrics::UpstreamOutcome::Empty
                };
                metrics::upstream_request(Chain::P, outcome, started.elapsed().as_secs_f64());
                if let Some(result) = result {
                    return Some(result);
                }
                if let Some(err) = parsed.get("error") {
                    debug!(chain = "p", height, method, error = %err, "upstream returned an error");
                }
            }
            Err(e) => {
                metrics::upstream_request(
                    Chain::P,
                    metrics::UpstreamOutcome::Error,
                    started.elapsed().as_secs_f64(),
                );
                warn!(chain = "p", error = %e, height, method, "decode rpc response");
                return None;
            }
        }
        let backoff = RPC_RETRY_BACKOFF_MS.saturating_mul(1u64 << attempt.min(10));
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
    debug!(
        chain = "p",
        height, method, "not available within retry budget; will retry",
    );
    None
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn cfg_with_floor(floor: Option<u64>) -> IngestCfg {
        let mut cfg = handshake_cfg("http://unused", Duration::from_secs(1));
        cfg.backfill_floor = floor;
        cfg
    }

    /// A fresh store with no floor anchors at the current tip and grows forward
    /// — the P-chain analog of the C-chain's "anchor at the first newHead".
    #[test]
    fn fresh_store_without_a_floor_anchors_at_the_tip() {
        let cfg = cfg_with_floor(None);
        assert_eq!(next_height(true, 0, &cfg, 500), 500);
    }

    /// An explicit floor wins on a fresh store — including floor 0, which must
    /// mean genesis rather than skipping it.
    #[test]
    fn fresh_store_with_a_floor_starts_there_including_genesis() {
        assert_eq!(next_height(true, 0, &cfg_with_floor(Some(100)), 500), 100);
        assert_eq!(
            next_height(true, 0, &cfg_with_floor(Some(0)), 500),
            0,
            "a floor of 0 must fetch height 0, not skip it",
        );
    }

    /// Once anything is stored, the floor is history: the frontier drives.
    #[test]
    fn populated_store_walks_the_contiguous_frontier() {
        let cfg = cfg_with_floor(Some(100));
        assert_eq!(next_height(false, 120, &cfg, 500), 121);
        // Caught up: `next` passes the tip, which the loop reads as idle.
        assert_eq!(next_height(false, 500, &cfg, 500), 501);
    }

    /// Commit and abort blocks carry no transactions at all, and an
    /// unknown/malformed tx ID must be skipped rather than stall ingest — the
    /// forward-compatibility property the verbatim store exists to provide.
    #[test]
    fn tx_ids_tolerate_missing_and_unparseable_entries() {
        assert!(extract_tx_ids(&json!({"height": 5})).is_empty());
        assert!(extract_tx_ids(&json!({"height": 5, "txs": []})).is_empty());

        let good = codec::cb58_encode(&[7u8; 32]);
        let block = json!({
            "txs": [
                { "id": good },
                { "id": "not-valid-cb58!!" },
                { "unsignedTx": {} },
            ],
        });
        assert_eq!(extract_tx_ids(&block), vec![[7u8; 32]]);
    }

    /// Regression: Apricot-era blocks carry a single `tx` object rather than a
    /// `txs` array. Indexing only `txs` silently skipped every pre-Banff
    /// transaction, so a from-genesis mirror served 421 for all of them.
    #[test]
    fn tx_ids_index_the_apricot_single_tx_shape() {
        let id = [0x5a; 32];
        let block = json!({
            "height": 1,
            "tx": { "id": codec::cb58_encode(&id), "unsignedTx": {} },
        });
        assert_eq!(extract_tx_ids(&block), vec![id]);
    }
}
