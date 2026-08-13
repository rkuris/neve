//! Upstream subscription + fetch side: connect to an upstream's `newHeads`
//! WebSocket, fetch each full block over HTTPS RPC,
//! and persist it — plus the one-shot `eth_chainId` handshake. The backfill
//! worker reuses the block-fetch helpers here.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::chain::{Chain, IngestCfg, LogsSource};
use crate::eth::backfill::persist_backfilled;
use crate::join::{JoinBuffer, JoinOutcome};
use crate::metrics::{self, UpstreamOutcome};
use crate::record;
use crate::storage::Storage;
use crate::subscribe::{LiveTx, LiveUpdate};
use crate::upstream::{self, WsRx, WsTx, connect_ws, handle_throttle, retry_after_secs};

/// The decoded pieces needed to store a live block: `(hash, tx_hashes, bytes)`.
type PreparedBlock = ([u8; 32], Vec<[u8; 32]>, Vec<u8>);

/// Interesting event emitted by the WebSocket session loop.
#[derive(Debug)]
enum WsEvent {
    Subscribed,
    /// A `newHeads` notification: header only, so we still fetch the full block.
    NewHead {
        height: u64,
        hash: String,
    },
    /// A `newBlocks` notification (neve extension): the whole block arrived on
    /// the socket, so we persist it directly with no follow-up fetch.
    NewBlock {
        height: u64,
        hash: String,
        block: Value,
    },
}

pub(crate) async fn ingest(
    storage: Storage,
    http: reqwest::Client,
    cfg: IngestCfg,
    join: Option<JoinBuffer>,
) -> Result<()> {
    // Mirror mode: stream the historical range over a single `oldBlocks`
    // subscription before going live. This replaces the per-block HTTPS
    // backfill for the cold-start (or catch-up) bulk — whole blocks arrive on
    // the socket. The backfill loop waits on `bootstrap_done`, so signal it
    // even when bootstrap fails, letting backfill take over as the fallback.
    if cfg.subscribe_blocks {
        if let Err(e) = bootstrap_via_oldblocks(&storage, &http, &cfg).await {
            warn!(error = ?e, "oldBlocks bootstrap incomplete; backfill will fill the remainder");
        }
        cfg.bootstrap_done.notify_one();
    }
    // Persists across reconnects so the learned pre-fetch delay isn't relearned
    // from zero each time the socket drops. A zero cap (the default) keeps it inert.
    let mut aimd = AimdDelay::new(cfg.prefetch_delay_cap);
    let mut attempt: u32 = 0;
    loop {
        match run_session(&storage, &http, &cfg, &mut aimd, join.as_ref()).await {
            Ok(()) => {
                info!("websocket session ended cleanly, reconnecting");
                attempt = 0;
            }
            Err(e) => {
                warn!(error = ?e, attempt, "websocket session failed");
                attempt = attempt.saturating_add(1);
            }
        }
        // Every loop past the first connect is a reconnect (clean end or failure).
        metrics::ws_reconnect(Chain::C);
        // Exponential backoff: 500ms, 1s, 2s, 4s, 8s; cap at 30s.
        let backoff_ms = 500u64.saturating_mul(1u64 << attempt.min(6)).min(30_000);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }
}

async fn run_session(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    aimd: &mut AimdDelay,
    join: Option<&JoinBuffer>,
) -> Result<()> {
    let (mut tx, mut rx) = connect_and_subscribe(cfg).await?;
    loop {
        // Idle watchdog: `next_ws_event` only returns on a newHead (or the
        // one-time subscription ack); pings are handled internally and don't
        // return. So a timeout here means no new blocks within the window —
        // surface it as an error so `ingest` reconnects with backoff rather
        // than blocking forever on a silently-dead socket.
        let event = match tokio::time::timeout(cfg.ws_idle_timeout, next_ws_event(&mut tx, &mut rx))
            .await
        {
            Ok(Some(event)) => event,
            Ok(None) => break,
            Err(_elapsed) => {
                metrics::ws_idle_timeout(Chain::C);
                return Err(anyhow!(
                    "no newHeads within {}s idle timeout; reconnecting",
                    cfg.ws_idle_timeout.as_secs(),
                ));
            }
        };
        // newBlocks delivers the whole block on the socket — persist it
        // directly, no eth_getBlockByNumber round-trip. newHeads delivers a
        // header, so we still fetch the body.
        let (height, hash, block) = match event {
            WsEvent::NewBlock {
                height,
                hash,
                block,
            } => {
                debug!(height, %hash, "new block (full)");
                (height, hash, block)
            }
            WsEvent::NewHead { height, hash } => {
                debug!(height, %hash, "new head");
                let Some(block) = fetch_full_block(http, height, cfg, Some(aimd)).await else {
                    continue;
                };
                (height, hash, block)
            }
            WsEvent::Subscribed => continue,
        };
        match join {
            // Log ingestion on: buffer the block (serveable from memory at once),
            // then pull its logs and complete the join into a [block, logs] write.
            Some(buf) => {
                persist_block_logs(buf, http, cfg, height, &hash, &block, &cfg.blocks).await?;
            }
            // Off: write the block immediately with an empty logs half.
            None => persist_block(storage, height, &hash, &block, &cfg.blocks).await?,
        }
    }
    Ok(())
}

async fn connect_and_subscribe(cfg: &IngestCfg) -> Result<(WsTx, WsRx)> {
    let (mut tx, rx) = connect_ws(cfg).await?;
    // newBlocks (whole block, no follow-up fetch) when mirroring a neve;
    // newHeads (header, then fetch) against the public endpoint.
    let kind = if cfg.subscribe_blocks {
        "newBlocks"
    } else {
        "newHeads"
    };
    upstream::send_request(
        &mut tx,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [kind],
        }),
    )
    .await?;
    // Live session established; mark connected-since so the upstream connection
    // age is trackable (and resets on each reconnect).
    metrics::upstream_connected(Chain::C);
    Ok((tx, rx))
}

/// Mirror cold-start / catch-up: stream the historical block range from the
/// upstream neve over a single `oldBlocks` subscription, before going live.
/// Whole blocks arrive on the socket, so this is far cheaper than the per-block
/// `eth_getBlockByNumber` backfill it replaces for the bulk.
///
/// Runs to a fixed target — the upstream's contiguous tip, read once from
/// `/health` — so completion is self-determined (we know we're done when we've
/// persisted that height) rather than relying on detecting a server-side
/// subscription close, which our raw frame reader can't see cleanly. Requesting
/// exactly the contiguous tip also guarantees the server accepts the range. On
/// any error we return so the caller can fall back to backfill.
async fn bootstrap_via_oldblocks(
    storage: &Storage,
    http: &reqwest::Client,
    cfg: &IngestCfg,
) -> Result<()> {
    // First height we lack: the mirror floor on a cold start, otherwise one
    // past what we already hold contiguously.
    let floor = cfg.backfill_floor.unwrap_or(0);
    let have = storage.max_contiguous_height().await;
    let from = floor.max(have.saturating_add(1));
    let target = fetch_upstream_contiguous(http, &cfg.rpc_url).await?;
    if from > target {
        info!(
            from,
            target, "oldBlocks bootstrap: already current with upstream"
        );
        return Ok(());
    }
    info!(
        from,
        to = target,
        count = target.saturating_sub(from).saturating_add(1),
        "oldBlocks bootstrap: streaming historical range",
    );
    let (mut tx, mut rx) = connect_ws(cfg).await?;
    upstream::send_request(
        &mut tx,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["oldBlocks", format!("0x{from:x}"), format!("0x{target:x}")],
        }),
    )
    .await?;
    loop {
        // Idle watchdog: a stalled stream (or an upstream that rejected the
        // subscription) shouldn't hang startup forever — bail and let backfill
        // take over.
        let event = match tokio::time::timeout(cfg.ws_idle_timeout, next_ws_event(&mut tx, &mut rx))
            .await
        {
            Ok(Some(event)) => event,
            Ok(None) => bail!("oldBlocks stream ended before reaching target {target}"),
            Err(_elapsed) => {
                metrics::ws_idle_timeout(Chain::C);
                bail!(
                    "oldBlocks bootstrap idle for {}s before reaching target {target}",
                    cfg.ws_idle_timeout.as_secs(),
                );
            }
        };
        let WsEvent::NewBlock { height, block, .. } = event else {
            // A correct upstream only emits full blocks for oldBlocks; ignore a
            // stray ack or header.
            continue;
        };
        // Mirror bootstrap streams blocks only; logs arrive via `oldIndex` in a
        // later milestone, so store an empty logs half for now.
        persist_backfilled(storage, height, &block, crate::record::EMPTY_ARRAY).await?;
        if height >= target {
            info!(target, "oldBlocks bootstrap complete");
            return Ok(());
        }
    }
}

/// Read the upstream neve's contiguous tip (`blocks.max_contiguous_height`)
/// from `/health`. Used as the fixed end for the `oldBlocks` bootstrap: it's a
/// height the upstream can serve gaplessly, and it makes bootstrap completion
/// self-determined.
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
    crate::health::upstream_blocks_field(&v, Chain::C, "max_contiguous_height").ok_or_else(|| {
        anyhow!("/health missing blocks.max_contiguous_height (is the upstream a neve?)")
    })
}

/// Pull the next interesting event from the WebSocket, skipping frames this
/// dialect doesn't care about. Returns `None` when the stream ends or breaks.
async fn next_ws_event(tx: &mut WsTx, rx: &mut WsRx) -> Option<WsEvent> {
    loop {
        let v = upstream::next_frame(tx, rx).await?;
        if let Some(event) = classify_frame(&v) {
            return Some(event);
        }
    }
}

/// Identify a JSON-RPC frame as either a subscription ack, a newHead
/// notification, or something we don't care about (returns `None`).
fn classify_frame(v: &Value) -> Option<WsEvent> {
    if let Some(result) = v.get("result")
        && v.get("id").is_some()
        && v.get("method").is_none()
    {
        info!(sub = %result, "subscribed");
        return Some(WsEvent::Subscribed);
    }
    if v.get("method").and_then(Value::as_str) != Some("eth_subscription") {
        return None;
    }
    let head = v.get("params").and_then(|p| p.get("result"))?;
    let number_hex = head.get("number").and_then(Value::as_str)?;
    let hash = head.get("hash").and_then(Value::as_str)?.to_owned();
    let height = u64::from_str_radix(number_hex.trim_start_matches("0x"), 16).ok()?;
    // A `newBlocks` payload is a full block (transactions array present); a
    // `newHeads` payload is a header (transactions stripped). The presence of
    // the field tells the two apart without threading the subscription kind in.
    if head.get("transactions").is_some() {
        return Some(WsEvent::NewBlock {
            height,
            hash,
            block: head.clone(),
        });
    }
    Some(WsEvent::NewHead { height, hash })
}

/// Additive-increase / additive-decrease controller for the live `newHeads`
/// pre-fetch delay.
///
/// A `newHeads` notification can beat the block's availability on the HTTPS RPC
/// backend (propagation lag), so an *immediate* fetch comes back `empty` and
/// burns a retry. Parking a short delay `d` before the first fetch lets the
/// block land; we adapt `d` toward the smallest value that keeps the first-try
/// empty rate low. Each first-try `empty` nudges `d` up by a large step; each
/// first-try `ok` eases it down by a small step. The asymmetry parks `d` just
/// under the real lag at a steady-state first-try empty rate of
/// `DEC / (INC + DEC)` (~9% with the constants below) — *provided* that rate is
/// reachable within `max`.
///
/// `max` is the operator-set cap (`prefetch_delay_cap`) and **defaults to
/// zero, which disables the pre-delay entirely** — the right call against the
/// public Avalanche endpoint, whose propagation tail is heavy enough that the
/// controller just pegs at any sane cap and pays full freshness cost on every
/// block to cut a now-cheap (25ms-retry) problem. It earns its keep against a
/// fast private full node that serves `newHeads`: there empties are rare, so
/// the controller parks `d` low and trims wasted requests with little freshness
/// cost. (Against a neve upstream the live path uses `newBlocks` and never
/// fetches, so the controller is inert regardless.)
///
/// Live `newHeads` path only — backfill fetches old blocks that always exist
/// (never `empty`) and must not be slowed, so it leaves the controller unset.
pub(crate) struct AimdDelay {
    delay: Duration,
    /// Upper bound on `delay`; `0` disables the pre-delay. From `prefetch_delay_cap`.
    max: Duration,
}

impl AimdDelay {
    /// Increase step on a first-try `empty` (additive increase).
    const INC: Duration = Duration::from_millis(10);
    /// Decrease step on a first-try `ok` (additive decrease). Smaller than `INC`
    /// so the delay creeps back down only as fast as it's safe to.
    const DEC: Duration = Duration::from_millis(1);

    const fn new(max: Duration) -> Self {
        Self {
            delay: Duration::ZERO,
            max,
        }
    }

    const fn current(&self) -> Duration {
        self.delay
    }

    /// Feed back the first clean (2xx) fetch outcome for one head. With a zero
    /// `max` the delay can never leave zero, so the controller stays inert.
    fn record(&mut self, first_try_ok: bool) {
        self.delay = if first_try_ok {
            self.delay.saturating_sub(Self::DEC)
        } else {
            self.delay.saturating_add(Self::INC).min(self.max)
        };
    }
}

/// Fetch the full block (with transactions) from HTTPS RPC. Pass `Some(aimd)`
/// on the live `newHeads` path to apply (and adapt) the pre-fetch delay; pass
/// `None` for backfill, which fetches blocks that already exist.
pub(crate) async fn fetch_full_block(
    http: &reqwest::Client,
    height: u64,
    cfg: &IngestCfg,
    aimd: Option<&mut AimdDelay>,
) -> Option<Value> {
    fetch_rpc(
        http,
        height,
        "eth_getBlockByNumber",
        json!([format!("0x{height:x}"), true]),
        cfg,
        aimd,
    )
    .await
}

/// Fetch every log in the inclusive block range `[from, to]` via `eth_getLogs`
/// over the HTTPS RPC, returning the raw logs array (or `None` if the call can't
/// succeed within the retry budget). The caller chunks the range to the upstream
/// `eth_getLogs` block cap (~2048). A future optimization issues this over the
/// WebSocket socket instead, avoiding a per-request connection setup.
pub(crate) async fn fetch_logs(
    http: &reqwest::Client,
    cfg: &IngestCfg,
    from: u64,
    to: u64,
) -> Option<Value> {
    fetch_rpc(
        http,
        from,
        "eth_getLogs",
        json!([{ "fromBlock": format!("0x{from:x}"), "toBlock": format!("0x{to:x}") }]),
        cfg,
        None,
    )
    .await
}

/// This height's logs, from whichever source the startup probe settled on.
///
/// Both spellings return the same thing — a flat array of log objects in
/// ascending `logIndex` — so the stored record's logs half is byte-comparable
/// whichever endpoint produced it. That matters because an operator fills from
/// their own node (receipts) and then repoints at the public endpoint
/// (`eth_getLogs`) for the live tail: the two halves of one store must not
/// disagree about shape.
pub(crate) async fn fetch_logs_for_height(
    http: &reqwest::Client,
    cfg: &IngestCfg,
    height: u64,
) -> Option<Value> {
    match cfg.logs_source {
        LogsSource::Receipts => {
            let receipts = fetch_block_receipts(http, cfg, height).await?;
            Some(logs_from_receipts(&receipts))
        }
        LogsSource::GetLogs => fetch_logs(http, cfg, height, height).await,
    }
}

/// Every receipt in one block, via `eth_getBlockReceipts`.
pub(crate) async fn fetch_block_receipts(
    http: &reqwest::Client,
    cfg: &IngestCfg,
    height: u64,
) -> Option<Value> {
    fetch_rpc(
        http,
        height,
        "eth_getBlockReceipts",
        json!([format!("0x{height:x}")]),
        cfg,
        None,
    )
    .await
}

/// Flatten a receipts array into the logs array `eth_getLogs` would have
/// returned for that block.
///
/// Receipts arrive in transaction order and each one's `logs` are already in
/// ascending `logIndex`, so concatenating in order reproduces `eth_getLogs`'
/// block-wide ordering without sorting. A receipt log carries the same fields
/// as a standalone one (`blockNumber`, `blockHash`, `transactionHash`,
/// `transactionIndex`, `logIndex`, `removed`), which is what makes the two
/// sources interchangeable at rest.
pub(crate) fn logs_from_receipts(receipts: &Value) -> Value {
    let Some(items) = receipts.as_array() else {
        return Value::Array(Vec::new());
    };
    let logs: Vec<Value> = items
        .iter()
        .filter_map(|r| r.get("logs").and_then(Value::as_array))
        .flat_map(|logs| logs.iter().cloned())
        .collect();
    Value::Array(logs)
}

/// Decide at startup whether this upstream serves `eth_getBlockReceipts`.
///
/// A probe rather than a config key because it is a fact about the endpoint,
/// and because the endpoint is expected to change under a deployment: the
/// from-genesis fill runs against an operator's own node, and steady state
/// often runs against the public one, which answers `-32601`.
///
/// Anything other than a well-formed array — method missing, transport error,
/// a proxy rewriting the response — falls back to `eth_getLogs`. Falling back
/// costs speed; guessing wrong the other way would persist heights with a logs
/// half we never actually got.
pub(crate) async fn probe_logs_source(
    http: &reqwest::Client,
    rpc_url: &str,
) -> (LogsSource, Option<String>) {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockReceipts",
        // `latest` rather than a height: the probe runs before anything has
        // established what heights this endpoint holds, and every endpoint that
        // implements the method accepts the tag.
        "params": ["latest"],
    });
    let resp = match http.post(rpc_url).json(&body).send().await {
        Ok(r) => r,
        Err(e) => return (LogsSource::GetLogs, Some(e.to_string())),
    };
    match resp.json::<Value>().await {
        Ok(v) => classify_probe(&v),
        Err(e) => (LogsSource::GetLogs, Some(e.to_string())),
    }
}

/// Read a probe response as a verdict. Split out from the request so the
/// interesting half — which shapes count as "supported" — is testable without a
/// server.
fn classify_probe(value: &Value) -> (LogsSource, Option<String>) {
    if let Some(err) = value.get("error") {
        let msg = err
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        let code = err.get("code").and_then(Value::as_i64);
        return (
            LogsSource::GetLogs,
            Some(match code {
                Some(c) => format!("{msg} (code {c})"),
                None => msg.to_owned(),
            }),
        );
    }
    match value.get("result") {
        // An empty array is a pass: a block with no receipts still proves the
        // method exists, which is the only thing being asked.
        Some(Value::Array(_)) => (LogsSource::Receipts, None),
        _ => (
            LogsSource::GetLogs,
            Some("response had no result array".to_owned()),
        ),
    }
}

/// One round-trip to the HTTPS RPC, with retry/backoff for unfinalized blocks
/// and `Retry-After`-aware handling of 429 / 503 (capped to 60s) so heavy
/// backfill stretches don't trip Cloudflare's rate limiter in front of the
/// public Avalanche endpoint. Returns `None` if the call cannot succeed
/// within the retry budget.
/// Attempts a single `fetch_rpc` call makes before giving up. A `null` result
/// means the block hasn't propagated to the answering RPC backend yet; for a
/// just-produced newHead that's the common case. We keep the retry budget short
/// (backoff 25ms, 50ms, 100ms ≈ 175ms total) so the *serial* newHeads ingester
/// isn't head-of-line blocked retrying the tip while later heads pile up in the
/// WS buffer (and get dropped upstream). The live path also parks an adaptive
/// pre-fetch delay (see `AimdDelay`) so most first attempts land after the block
/// has propagated, sidestepping the retry entirely. Any block missed this way is
/// filled by the backfill task, which fetches older heights the pool already has.
const RPC_MAX_ATTEMPTS: u32 = 3;
/// Initial retry backoff after an `empty`; doubles each attempt. Sized to the
/// real propagation lag, which is tens of ms — a coarser backoff would both add
/// ingest latency and hide the true lag from the metrics.
const RPC_RETRY_BACKOFF_MS: u64 = 25;

async fn fetch_rpc(
    http: &reqwest::Client,
    height: u64,
    method: &str,
    params: Value,
    cfg: &IngestCfg,
    mut aimd: Option<&mut AimdDelay>,
) -> Option<Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    // Live path only: park the adaptive pre-fetch delay so the block has time to
    // propagate to the HTTPS backend before the first attempt. Backfill passes
    // `None` (old blocks already exist) and fetches immediately.
    if let Some(aimd) = aimd.as_deref() {
        let delay = aimd.current();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
    // Whether we've fed the first clean (2xx) outcome back to the controller yet
    // — throttles (429/503) and transport errors aren't propagation signals.
    let mut recorded = false;
    for attempt in 0..RPC_MAX_ATTEMPTS {
        // Per-attempt latency: the request round-trip including body decode,
        // measured up to whichever outcome this attempt reaches (excludes the
        // retry backoff sleep below).
        let started = Instant::now();
        let resp = match http.post(&cfg.rpc_url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                metrics::upstream_request(
                    Chain::C,
                    UpstreamOutcome::Error,
                    started.elapsed().as_secs_f64(),
                );
                warn!(error = %e.without_url(), height, "rpc request failed");
                return None;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            metrics::upstream_request(Chain::C, status, started.elapsed().as_secs_f64());
            let retry_after = retry_after_secs(&resp).unwrap_or(5);
            handle_throttle(cfg, method, retry_after, status.as_u16()).await;
            continue;
        }
        match resp.json::<Value>().await {
            Ok(mut parsed) => {
                // Move the result out rather than cloning the (large) block JSON;
                // `parsed` is dropped at the end of this scope anyway.
                let result = parsed
                    .get_mut("result")
                    .map(Value::take)
                    .filter(|r| !r.is_null());
                // 2xx with a usable result is `ok`; 2xx with a null result is
                // `empty` (block not propagated yet); a non-2xx body is `error`.
                let outcome = if !status.is_success() {
                    UpstreamOutcome::Error
                } else if result.is_some() {
                    UpstreamOutcome::Ok
                } else {
                    UpstreamOutcome::Empty
                };
                let first_try_ok = matches!(outcome, UpstreamOutcome::Ok);
                metrics::upstream_request(Chain::C, outcome, started.elapsed().as_secs_f64());
                // Feed the first clean outcome (ok vs empty) back to the live
                // controller so it can adapt the pre-fetch delay toward the lag.
                if !recorded {
                    if let Some(aimd) = aimd.as_deref_mut() {
                        aimd.record(first_try_ok);
                        metrics::upstream_first_attempt(
                            Chain::C,
                            first_try_ok,
                            aimd.current().as_secs_f64(),
                        );
                    }
                    recorded = true;
                }
                if let Some(result) = result {
                    return Some(result);
                }
            }
            Err(e) => {
                metrics::upstream_request(
                    Chain::C,
                    UpstreamOutcome::Error,
                    started.elapsed().as_secs_f64(),
                );
                warn!(error = %e.without_url(), height, "decode rpc response");
                return None;
            }
        }
        let backoff = RPC_RETRY_BACKOFF_MS.saturating_mul(1u64 << attempt.min(10));
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
    // Expected for a just-arrived newHead the HTTP pool hasn't caught up on yet:
    // we gave up within the short budget and the backfill task (no head-of-line
    // cost) will fill it. Genuine gaps surface via the summary's `behind` /
    // contiguity, not here, so this is debug rather than a scary WARN.
    debug!(
        height,
        method, "block not available within retry budget; leaving for backfill"
    );
    None
}

/// Validate a live body against the head hash and decode the pieces needed to
/// store it: `(hash, tx_hashes, block_bytes)`. `None` on a fork mismatch (the WS
/// feed disagreeing with the load-balanced RPC pool) or a bad hash — the caller
/// skips the height.
fn prepare_block(height: u64, expected_hash: &str, block: &Value) -> Option<PreparedBlock> {
    let body_hash = block.get("hash").and_then(Value::as_str).unwrap_or("");
    if body_hash != expected_hash {
        warn!(height, head = %expected_hash, body = %body_hash, "hash mismatch (fork?)");
        return None;
    }
    let hash_bytes = match decode_hash(expected_hash) {
        Ok(h) => h,
        Err(e) => {
            warn!(error = %e, "bad hash on newHead");
            return None;
        }
    };
    let bytes = match serde_json::to_vec(block) {
        Ok(b) => b,
        Err(e) => {
            warn!(height, error = %e, "block re-serialize failed");
            return None;
        }
    };
    Some((hash_bytes, extract_tx_hashes(block), bytes))
}

/// Announce a freshly-available live block to subscribers and bump the freshness
/// gauge. Publishes the *full* block; each subscriber projects it (newHeads
/// strips transactions, newBlocks keeps them), so chains of mirrors propagate.
/// Skips the clone when nobody is listening (the hot path).
///
/// No `record` is published: this fires before the block's logs are joined, on
/// purpose, so reads don't wait on the logs round-trip. `newRecords` is
/// therefore not offered on the C-chain (`oldRecords` still is).
fn announce_block(blocks: &LiveTx, block: &Value) {
    // Live blocks arrive tip-first, so the freshness gauge only advances; a
    // malformed/missing timestamp just skips.
    if let Some(ts) = block
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
    {
        metrics::last_block_timestamp(Chain::C, ts);
    }
    if blocks.receiver_count() > 0 {
        let _ = blocks.send(Arc::new(LiveUpdate {
            block: block.clone(),
            record: None,
        }));
    }
}

/// Live path with log ingestion **off**: store the block immediately with an
/// empty logs half.
async fn persist_block(
    storage: &Storage,
    height: u64,
    expected_hash: &str,
    block: &Value,
    blocks: &LiveTx,
) -> Result<()> {
    let Some((hash_bytes, tx_hashes, bytes)) = prepare_block(height, expected_hash, block) else {
        return Ok(());
    };
    storage
        .put(
            height,
            hash_bytes,
            &tx_hashes,
            &[&bytes, record::EMPTY_ARRAY],
        )
        .await?;
    metrics::block_persisted(Chain::C, metrics::BlockSource::Live);
    announce_block(blocks, block);
    debug!(height, txs = tx_hashes.len(), "stored block");
    Ok(())
}

/// Live path with log ingestion **on**: buffer the block (immediately serveable
/// from the join buffer), announce it, then pull its logs (one `eth_getLogs` or
/// one `eth_getBlockReceipts`, whichever the startup probe settled on) and
/// complete the join into a durable `[block, logs]` write.
///
/// `getBlockByNumber` doesn't wait on the logs round-trip: the block is queryable
/// from the buffer the instant it arrives. If the logs fetch fails the block
/// stays buffered (still serveable) and is re-derived by backfill if the stall
/// persists; if the cap was hit the block was flushed, so it isn't announced.
async fn persist_block_logs(
    buf: &JoinBuffer,
    http: &reqwest::Client,
    cfg: &IngestCfg,
    height: u64,
    expected_hash: &str,
    block: &Value,
    blocks: &LiveTx,
) -> Result<()> {
    let Some((hash_bytes, tx_hashes, bytes)) = prepare_block(height, expected_hash, block) else {
        return Ok(());
    };
    if buf.on_block(height, hash_bytes, tx_hashes, bytes).await? == JoinOutcome::Deferred {
        return Ok(());
    }
    announce_block(blocks, block);
    let Some(logs) = fetch_logs_for_height(http, cfg, height).await else {
        debug!(
            height,
            "live getLogs failed; block left buffered for backfill"
        );
        return Ok(());
    };
    let count = logs.as_array().map_or(0, Vec::len);
    debug!(height, logs = count, "fetched live logs");
    let logs_bytes = serde_json::to_vec(&logs).unwrap_or_else(|_| record::EMPTY_ARRAY.to_vec());
    if buf.on_logs(height, logs_bytes).await? == JoinOutcome::Completed {
        metrics::block_persisted(Chain::C, metrics::BlockSource::Live);
        metrics::logs_persisted(Chain::C, metrics::BlockSource::Live, count as u64);
        debug!(height, logs = count, "stored block with logs");
    }
    Ok(())
}

/// Pull the per-tx hashes out of a full block returned by
/// `eth_getBlockByNumber(.., true)`. Malformed or missing entries are skipped
/// silently — a degenerate block JSON shouldn't take down ingest.
pub(crate) fn extract_tx_hashes(block: &Value) -> Vec<[u8; 32]> {
    let Some(txs) = block.get("transactions").and_then(Value::as_array) else {
        return Vec::new();
    };
    txs.iter()
        .filter_map(|tx| tx.get("hash").and_then(Value::as_str))
        .filter_map(|s| decode_hash(s).ok())
        .collect()
}

/// One-shot startup query for the upstream chain ID. Used to stamp/verify
/// the on-disk store and catch cross-network pollution even when the user
/// has overridden `chains.c.rpc_url`. Errors propagate so we refuse to start
/// rather than guess. Honors `max_wait` for Retry-After on 429 / 503:
/// shorter than `max_wait` → sleep and retry; longer → bail out loudly.
pub(crate) async fn fetch_chain_id(
    http: &reqwest::Client,
    rpc_url: &str,
    max_wait: Duration,
) -> Result<u64> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_chainId",
        "params": [],
    });
    loop {
        let resp = http
            .post(rpc_url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("eth_chainId request to {rpc_url} failed"))?;
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            let retry_after = retry_after_secs(&resp).unwrap_or(5);
            let wait = Duration::from_secs(retry_after);
            if wait > max_wait {
                bail!(
                    "eth_chainId throttled by upstream (status {status}, \
                     retry_after {retry_after}s exceeds max_wait {}s); \
                     not waiting",
                    max_wait.as_secs(),
                );
            }
            warn!(%status, retry_after, "eth_chainId throttled, sleeping");
            tokio::time::sleep(wait).await;
            continue;
        }
        if !status.is_success() {
            bail!("eth_chainId returned HTTP {status}");
        }
        let v: Value = resp.json().await.context("eth_chainId response decode")?;
        let s = v
            .get("result")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("eth_chainId: missing 'result' string"))?;
        let id = u64::from_str_radix(s.trim_start_matches("0x"), 16)
            .context("eth_chainId: malformed hex")?;
        return Ok(id);
    }
}

/// Decode a `0x`-prefixed (or bare) 32-byte hex hash. Shared by the live and
/// backfill persist paths.
pub(crate) fn decode_hash(s: &str) -> Result<[u8; 32]> {
    let raw = hex::decode(s.trim_start_matches("0x"))?;
    raw.as_slice()
        .try_into()
        .map_err(|_| anyhow!("hash must be 32 bytes"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const TEST_CAP: Duration = Duration::from_millis(200);

    /// `reqwest` is built with `rustls-no-provider`, so a `Client` cannot be
    /// constructed until a provider is installed — `main` does this at startup.
    /// Idempotent via `ok()`: several tests in one binary share the process.
    fn http() -> reqwest::Client {
        let _ = rustls::crypto::ring::default_provider().install_default();
        reqwest::Client::new()
    }

    /// An unpaced [`IngestCfg`] pointed at `url`, for the logs-source paths.
    /// Only `rpc_url`, `logs_source`, `ingest_logs` and the pacers are consulted
    /// there.
    fn probe_cfg(url: &str, logs_source: LogsSource) -> IngestCfg {
        let (blocks, _) = tokio::sync::broadcast::channel(1);
        IngestCfg {
            chain: Chain::C,
            max_wait: Duration::ZERO,
            ws_idle_timeout: Duration::ZERO,
            ws_url: String::new(),
            rpc_url: url.to_owned(),
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
            ingest_logs: true,
            logs_source,
        }
    }

    #[test]
    fn aimd_starts_at_zero_and_floors_at_zero() {
        let mut a = AimdDelay::new(TEST_CAP);
        assert_eq!(a.current(), Duration::ZERO);
        // `ok` while already at zero must not underflow.
        a.record(true);
        assert_eq!(a.current(), Duration::ZERO);
    }

    #[test]
    fn aimd_increases_on_empty_decreases_on_ok() {
        let mut a = AimdDelay::new(TEST_CAP);
        a.record(false); // empty: +INC
        assert_eq!(a.current(), AimdDelay::INC);
        a.record(false); // empty: +INC again
        assert_eq!(a.current(), AimdDelay::INC * 2);
        a.record(true); // ok: -DEC
        assert_eq!(
            a.current(),
            (AimdDelay::INC * 2).saturating_sub(AimdDelay::DEC)
        );
    }

    #[test]
    fn aimd_clamps_at_cap() {
        let mut a = AimdDelay::new(TEST_CAP);
        // Far more empties than it takes to reach the cap; must saturate, not exceed.
        for _ in 0..1000 {
            a.record(false);
        }
        assert_eq!(a.current(), TEST_CAP);
    }

    #[test]
    fn aimd_zero_cap_stays_inert() {
        let mut a = AimdDelay::new(Duration::ZERO);
        // Even a long run of empties can't lift the delay off zero when disabled.
        for _ in 0..100 {
            a.record(false);
        }
        assert_eq!(a.current(), Duration::ZERO);
    }

    /// The public Avalanche endpoint's actual answer: the method is absent, so
    /// the probe must fall back rather than treat the error as a transport
    /// blip. This is the case the WARN exists for.
    #[test]
    fn probe_falls_back_when_the_method_is_missing() {
        let (source, reason) = classify_probe(&json!({
            "jsonrpc": "2.0", "id": 1,
            "error": {"code": -32601, "message": "the method eth_getBlockReceipts does not exist"},
        }));
        assert_eq!(source, LogsSource::GetLogs);
        assert!(reason.expect("a reason to warn with").contains("-32601"));
    }

    /// An empty array still proves the method exists — a block can legitimately
    /// have no receipts, and that is not evidence against support.
    #[test]
    fn probe_accepts_an_empty_receipts_array() {
        let (source, reason) = classify_probe(&json!({"jsonrpc": "2.0", "id": 1, "result": []}));
        assert_eq!(source, LogsSource::Receipts);
        assert!(reason.is_none());
    }

    #[test]
    fn probe_accepts_a_populated_receipts_array() {
        let (source, _) = classify_probe(&json!({"result": [{"logs": []}]}));
        assert_eq!(source, LogsSource::Receipts);
    }

    /// Anything that isn't a result array falls back. Guessing "supported" from
    /// an unrecognized shape would persist heights whose logs half we never got.
    #[test]
    fn probe_falls_back_on_unrecognized_shapes() {
        for body in [
            json!({"result": null}),
            json!({"result": "0x1"}),
            json!({"jsonrpc": "2.0", "id": 1}),
        ] {
            let (source, reason) = classify_probe(&body);
            assert_eq!(source, LogsSource::GetLogs, "body: {body}");
            assert!(reason.is_some());
        }
    }

    /// Receipts arrive in transaction order and each one's logs are already in
    /// ascending `logIndex`, so a plain concatenation reproduces what
    /// `eth_getLogs` returns for the block — no sort needed. This is what lets a
    /// store filled from a node and followed from the public endpoint hold one
    /// consistent shape.
    #[test]
    fn receipts_flatten_to_getlogs_order() {
        let receipts = json!([
            {"transactionIndex": "0x0", "logs": [
                {"logIndex": "0x0", "address": "0xa"},
                {"logIndex": "0x1", "address": "0xb"},
            ]},
            {"transactionIndex": "0x1", "logs": []},
            {"transactionIndex": "0x2", "logs": [{"logIndex": "0x2", "address": "0xc"}]},
        ]);
        let logs = logs_from_receipts(&receipts);
        let got: Vec<&str> = logs
            .as_array()
            .expect("flattened logs are an array")
            .iter()
            .filter_map(|l| l.get("logIndex").and_then(Value::as_str))
            .collect();
        assert_eq!(got, ["0x0", "0x1", "0x2"]);
    }

    /// A block whose transactions emitted nothing yields an empty array, which
    /// `crate::record` stores as an explicit "this height emitted none" — not as
    /// the absent element that would mean "never ingested".
    #[test]
    fn receipts_with_no_logs_flatten_to_empty() {
        let logs = logs_from_receipts(&json!([{"logs": []}, {"logs": []}]));
        assert_eq!(logs, json!([]));
    }

    /// A receipt missing `logs` entirely is skipped rather than panicking: the
    /// flattener runs on whatever an upstream sent, not on a validated shape.
    #[test]
    fn receipts_tolerate_missing_or_malformed_logs() {
        assert_eq!(logs_from_receipts(&json!([{"status": "0x1"}])), json!([]));
        assert_eq!(logs_from_receipts(&json!({"not": "an array"})), json!([]));
        assert_eq!(logs_from_receipts(&Value::Null), json!([]));
    }

    /// The whole receipts path against a server that actually answers: the probe
    /// must recognize support, and the fetch must reach `eth_getBlockReceipts`
    /// with a shape it accepts and flatten the reply. Unit tests cover the
    /// classification and the flattening; this covers the wiring between them,
    /// which is what runs for days against an operator's archive node.
    #[tokio::test]
    async fn receipts_source_probes_and_fetches_end_to_end() {
        let url = crate::test_support::mock_rpc(HashMap::from([(
            "eth_getBlockReceipts".to_owned(),
            json!([
                {"transactionIndex": "0x0", "logs": [{"logIndex": "0x0", "address": "0xa"}]},
                {"transactionIndex": "0x1", "logs": [{"logIndex": "0x1", "address": "0xb"}]},
            ]),
        )]))
        .await;

        let (source, reason) = probe_logs_source(&http(), &url).await;
        assert_eq!(source, LogsSource::Receipts, "reason: {reason:?}");

        let cfg = probe_cfg(&url, LogsSource::Receipts);
        let logs = fetch_logs_for_height(&http(), &cfg, 0x10)
            .await
            .expect("receipts fetch");
        assert_eq!(logs.as_array().expect("array").len(), 2);
    }

    /// An upstream that serves `eth_getLogs` but not `eth_getBlockReceipts` —
    /// the public endpoint — falls back, and the fallback then actually serves.
    #[tokio::test]
    async fn getlogs_fallback_probes_and_fetches_end_to_end() {
        let url = crate::test_support::mock_rpc(HashMap::from([(
            "eth_getLogs".to_owned(),
            json!([{"logIndex": "0x0", "address": "0xa"}]),
        )]))
        .await;

        let (source, reason) = probe_logs_source(&http(), &url).await;
        assert_eq!(source, LogsSource::GetLogs);
        assert!(reason.expect("a reason to warn with").contains("-32601"));

        let cfg = probe_cfg(&url, LogsSource::GetLogs);
        let logs = fetch_logs_for_height(&http(), &cfg, 0x10)
            .await
            .expect("getLogs fetch");
        assert_eq!(logs.as_array().expect("array").len(), 1);
    }
}
