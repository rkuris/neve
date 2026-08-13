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
use futures_util::StreamExt;
use futures_util::stream;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::chain::{Chain, IngestCfg, LogsSource};
use crate::metrics::{self, BlockSource};
use crate::platform::codec;
use crate::progress::BackfillProgress;
use crate::record;
use crate::storage::Storage;
use crate::subscribe::{LiveTx, LiveUpdate};
use crate::upstream::{handle_throttle, retry_after_secs};

/// The pieces of one height needed to store it: the canonical bytes, the JSON
/// exactly as upstream returned it, the block ID, and its tx IDs.
pub(crate) struct FetchedBlock {
    /// The block's height, as the JSON reports it.
    height: u64,
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
                "could not fetch P-chain block 0 from {}; \
                 is it a P-chain endpoint?",
                crate::upstream::redact_url(rpc_url)
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
    let (blocks, _) = broadcast::channel(1);
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
        pacer: Arc::new(crate::upstream::Pacer::new(Duration::ZERO)),
        host_pacer: None,
        fetch_concurrency: 1,
        backfill_floor: None,
        prefetch_delay_cap: Duration::ZERO,
        progress_period: Duration::from_secs(60),
        fatal: Arc::new(tokio::sync::Notify::new()),
        bootstrap_done: Arc::new(tokio::sync::Notify::new()),
        ingest_logs: false,
        logs_source: LogsSource::GetLogs,
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
    let mut progress = BackfillProgress::new(Chain::P, cfg.progress_period);
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

        // Fill a bounded run of heights before re-reading the tip: long enough
        // that the pipeline stays full, short enough that `behind` and the tip
        // stay fresh during a long fill.
        let end = tip.min(next.saturating_add(FILL_CHUNK.saturating_sub(1)));
        if !fill_range(
            &storage,
            &http,
            &cfg,
            next..=end,
            tip,
            &mut progress,
            &behind_tip,
        )
        .await
        {
            // Fetch or verification failed; back off before retrying so a
            // persistent problem doesn't become a hot loop. The outer loop
            // re-measures, so the retry resumes at the contiguous frontier.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Heights fetched per pass before the tip is re-read. One extra
/// `platform.getHeight` per chunk is negligible next to the chunk's own cost.
const FILL_CHUNK: u64 = 8192;

/// Fetch and store an inclusive run of heights, keeping
/// `cfg.fetch_concurrency` of them in flight. Returns `false` if the run was cut
/// short and should be retried.
///
/// `buffered` yields results **in height order** while fetching ahead, so writes
/// stay sequential and the contiguous frontier only ever advances by one. That
/// ordering is what makes a mid-run failure cheap to recover from: everything
/// before the failure is already durable, and the outer loop simply resumes from
/// the frontier.
async fn fill_range(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    heights: std::ops::RangeInclusive<u64>,
    tip: u64,
    progress: &mut BackfillProgress,
    behind_tip: &AtomicU64,
) -> bool {
    let mut in_flight = stream::iter(heights)
        .map(|height| fetch_block(http, cfg, height))
        .buffered(cfg.fetch_concurrency.max(1));

    while let Some(result) = in_flight.next().await {
        let Some(fetched) = result else {
            return false;
        };
        // A height at the tip is the closest thing this chain has to a live
        // block: it is what a downstream mirror wants pushed, and the only kind
        // whose timestamp should move the freshness gauge.
        let at_tip = fetched.height >= tip;
        let source = if at_tip {
            BlockSource::Live
        } else {
            BlockSource::Backfill
        };
        // Rewards are the next phase's feed (fetch-at-ingest on a committed
        // RewardValidatorTx); until then every height stores an explicit empty
        // element, which is why adding that feed needs no migration.
        if store_block(storage, &fetched, record::EMPTY_ARRAY, source)
            .await
            .is_none()
        {
            return false;
        }
        // Per height, not per chunk: a chunk is 8192 heights, long enough that
        // `/health` and the `summary` line would otherwise quote a gap minutes out
        // of date during a long fill.
        let behind = tip.saturating_sub(fetched.height);
        behind_tip.store(behind, Ordering::Relaxed);
        progress.observe(fetched.height, tip, behind);
        if at_tip {
            if let Some(ts) = fetched.timestamp {
                metrics::last_block_timestamp(Chain::P, ts);
            }
            let elements: [&[u8]; 3] = [&fetched.json, &fetched.bytes_json, record::EMPTY_ARRAY];
            announce(&cfg.blocks, &fetched.json, &elements);
        }
    }
    true
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

/// Write one verified block, with `rewards` as its trailing element. Returns the
/// height on success. Shared by the direct-fetch and mirror paths so both write
/// the same record layout and count the same metrics.
pub(crate) async fn store_block(
    storage: &Storage,
    fetched: &FetchedBlock,
    rewards: &[u8],
    source: BlockSource,
) -> Option<u64> {
    let height = fetched.height;
    let elements: [&[u8]; 3] = [&fetched.json, &fetched.bytes_json, rewards];
    if let Err(e) = storage
        .put(height, fetched.id, &fetched.tx_ids, &elements)
        .await
    {
        warn!(chain = "p", height, error = %e, "P-chain persist failed");
        return None;
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
    Some(height)
}

/// Verify a record whose height we learn from the record itself — the mirror
/// path, where the stream chooses the height rather than us asking for one.
pub(crate) fn verify_record(bytes_value: &Value, json_value: &Value) -> Option<FetchedBlock> {
    verify(bytes_value, json_value, None)
}

/// Publish a freshly-ingested tip block to subscribers, carrying **both** the
/// block and the whole record: the P-chain writes the complete record before
/// announcing, so `newRecords` (what a downstream mirror subscribes to) can be
/// served live. Skips the parse+clone entirely when nobody is listening, which
/// is the common case.
fn announce(blocks: &LiveTx, block_json: &[u8], record: &[&[u8]]) {
    if blocks.receiver_count() == 0 {
        return;
    }
    let block = match serde_json::from_slice::<Value>(block_json) {
        Ok(v) => v,
        Err(e) => {
            debug!(chain = "p", error = %e, "could not parse block for fan-out");
            return;
        }
    };
    // The record goes out as the same array that is on disk, rebuilt from the
    // elements already in hand rather than re-read from storage.
    let encoded = crate::record::encode(record);
    let record = serde_json::from_slice::<Value>(&encoded).ok();
    let _ = blocks.send(Arc::new(LiveUpdate { block, record }));
}

/// Fetch both encodings of one height and cross-check them.
async fn fetch_block(http: &reqwest::Client, cfg: &IngestCfg, height: u64) -> Option<FetchedBlock> {
    // The two encodings are independent reads, so issue them together rather
    // than back-to-back: one round-trip of latency per height instead of two.
    // They still take a pacer slot each, so this costs no extra request rate.
    let (bytes_value, json_value) = tokio::join!(
        fetch_block_encoding(http, cfg, height, codec::Encoding::Hexnc),
        fetch_block_encoding(http, cfg, height, codec::Encoding::Json),
    );
    verify(&bytes_value?, &json_value?, Some(height))
}

/// Cross-check a record's two halves and decode what the store needs.
///
/// The halves are independent readings of the same block — two upstream calls on
/// the direct path, two array elements on the mirror path — so agreement is not
/// free: `sha256(bytes)` must reproduce the CB58 ID the JSON reports. When
/// `expect_height` is given (the direct path asked for a specific height) the
/// JSON's height must match it too. Either mismatch means something between us
/// and the chain is wrong, and storing it would put a record on disk whose
/// halves disagree forever.
pub(crate) fn verify(
    bytes_value: &Value,
    json_value: &Value,
    expect_height: Option<u64>,
) -> Option<FetchedBlock> {
    // Height is only for logging until it's validated below.
    let reported_height = json_value.get("height").and_then(Value::as_u64);
    let log_height = expect_height.or(reported_height).unwrap_or_default();

    let Some(hex) = bytes_value.as_str() else {
        reject(log_height, "bad_hex", "block bytes element is not a string");
        return None;
    };
    let bytes = match codec::hexnc_decode(hex) {
        Ok(b) => b,
        Err(e) => {
            reject(log_height, "bad_hex", &e.to_string());
            return None;
        }
    };

    let reported_id = json_value.get("id").and_then(Value::as_str).unwrap_or("");
    let derived_id = codec::block_id_of(&bytes);
    if reported_id != derived_id {
        reject(
            log_height,
            "id_mismatch",
            &format!("json reports {reported_id}, bytes hash to {derived_id}"),
        );
        return None;
    }
    let Some(height) = reported_height else {
        reject(log_height, "no_height", "json has no usable 'height'");
        return None;
    };
    if expect_height.is_some_and(|want| want != height) {
        reject(
            log_height,
            "height_mismatch",
            &format!("json reports height {height}"),
        );
        return None;
    }
    let id = match codec::cb58_decode(&derived_id) {
        Ok(id) => id,
        Err(e) => {
            reject(log_height, "bad_id", &e.to_string());
            return None;
        }
    };

    // Serialize the two halves for storage. The `hexnc` half is stored as the
    // JSON string upstream sent, so `platform.getBlock`'s byte encodings are
    // served back verbatim.
    let bytes_json = serde_json::to_vec(bytes_value).ok()?;
    let json = serde_json::to_vec(json_value).ok()?;
    Some(FetchedBlock {
        height,
        bytes_json,
        json,
        id,
        tx_ids: extract_tx_ids(json_value),
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
        // Every attempt is a request, so it takes a slot — retries included.
        // This is the single choke point that keeps the configured rate honest
        // no matter how many fetches are in flight.
        cfg.pace().await;
        let started = Instant::now();
        let resp = match http.post(&cfg.rpc_url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                metrics::upstream_request(
                    Chain::P,
                    metrics::UpstreamOutcome::Error,
                    started.elapsed().as_secs_f64(),
                );
                warn!(chain = "p", error = %e.without_url(), height, method, "rpc request failed");
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
                warn!(chain = "p", error = %e.without_url(), height, method, "decode rpc response");
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

    /// A well-formed pair verifies and yields the height, ID and tx IDs the
    /// store needs.
    #[test]
    fn verify_accepts_a_consistent_pair() {
        let bytes = b"canonical-block-bytes".to_vec();
        let id = codec::block_id_of(&bytes);
        let tx = [0x5a; 32];
        let json = json!({
            "id": id,
            "height": 42,
            "time": 1_786_114_324u64,
            "tx": { "id": codec::cb58_encode(&tx) },
        });
        let hexnc = Value::String(codec::Encoding::Hexnc.render_bytes(&bytes).unwrap());

        let got = verify(&hexnc, &json, Some(42)).unwrap();
        assert_eq!(got.height, 42);
        assert_eq!(got.id, codec::cb58_decode(&id).unwrap());
        assert_eq!(got.tx_ids, vec![tx]);
        assert_eq!(got.timestamp, Some(1_786_114_324));

        // Learning the height from the record (the mirror path) works too.
        assert_eq!(verify(&hexnc, &json, None).unwrap().height, 42);
    }

    /// The integrity check that makes the record self-verifying: bytes that
    /// don't hash to the reported ID are refused, so neither a broken upstream
    /// nor a tampering mirror can put disagreeing halves on disk.
    #[test]
    fn verify_refuses_halves_that_disagree() {
        let bytes = b"canonical-block-bytes".to_vec();
        let hexnc = Value::String(codec::Encoding::Hexnc.render_bytes(&bytes).unwrap());

        // Right shape, wrong ID.
        let wrong_id = json!({ "id": codec::cb58_encode(&[0xff; 32]), "height": 42 });
        assert!(verify(&hexnc, &wrong_id, Some(42)).is_none());

        // Right ID, but for different bytes than the ones supplied.
        let other = Value::String(
            codec::Encoding::Hexnc
                .render_bytes(b"different-bytes")
                .unwrap(),
        );
        let good = json!({ "id": codec::block_id_of(&bytes), "height": 42 });
        assert!(verify(&other, &good, Some(42)).is_none());

        // Consistent halves, but not the height we asked for.
        assert!(verify(&hexnc, &good, Some(43)).is_none());

        // Unusable inputs.
        assert!(verify(&json!(7), &good, None).is_none());
        assert!(verify(&hexnc, &json!({ "id": codec::block_id_of(&bytes) }), None).is_none());
    }

    /// The P-chain live path publishes the **whole record**, not just the block
    /// — that is what lets a downstream mirror subscribe to `newRecords` and
    /// still be able to serve the hex encodings.
    #[tokio::test]
    async fn announce_publishes_the_block_and_the_whole_record() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(4);
        let block = br#"{"height":42,"id":"abc"}"#;
        let bytes = br#""0xdeadbeef""#;
        announce(&tx, block, &[block, bytes, record::EMPTY_ARRAY]);

        let update = rx.recv().await.unwrap();
        assert_eq!(update.block["height"], 42);
        let rec = update.record.as_ref().expect("P-chain publishes records");
        let parts = rec.as_array().unwrap();
        assert_eq!(parts.len(), record::arity(Chain::P));
        assert_eq!(parts[0]["height"], 42);
        assert_eq!(parts[1], "0xdeadbeef");
        assert!(parts[2].as_array().unwrap().is_empty());
    }

    /// Nobody listening means no work: the parse and the record rebuild are both
    /// skipped, which is the steady state on an instance with no subscribers.
    #[tokio::test]
    async fn announce_is_a_noop_without_subscribers() {
        let (tx, rx) = tokio::sync::broadcast::channel(4);
        drop(rx);
        announce(&tx, b"not even valid json", &[b"x"]);
        assert_eq!(tx.receiver_count(), 0);
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
