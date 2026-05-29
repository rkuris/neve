//! Upstream subscription + fetch side: connect to an upstream's `newHeads`
//! WebSocket, fetch each full block (and optionally receipts) over HTTPS RPC,
//! and persist it — plus the one-shot `eth_chainId` handshake. The backfill
//! worker reuses the block-fetch helpers here.

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as TungError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, warn};

use crate::IngestCfg;
use crate::backfill::persist_backfilled;
use crate::metrics::{self, UpstreamOutcome};
use crate::storage::Storage;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsTx = SplitSink<WsStream, Message>;
type WsRx = SplitStream<WsStream>;

/// Sent on the WS handshake and every HTTPS RPC request. The Cloudflare
/// `Human Rate Limit Bypass` WAF rule requires a non-empty UA that doesn't
/// match any known-automation substring; a real-browser UA from a non-
/// datacenter ASN is the cheapest way into that bypass. TLS JA3 fingerprint
/// still comes from rustls and is *not* impersonated here.
pub(crate) const BROWSER_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/// Interesting event emitted by the WebSocket session loop.
#[derive(Debug)]
enum WsEvent {
    Subscribed,
    /// A `newHeads` notification: header only, so we still fetch the full block.
    NewHead {
        number_hex: String,
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

pub(crate) async fn ingest(storage: Storage, http: reqwest::Client, cfg: IngestCfg) -> Result<()> {
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
    let mut attempt: u32 = 0;
    loop {
        match run_session(&storage, &http, &cfg).await {
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
        metrics::ws_reconnect();
        // Exponential backoff: 500ms, 1s, 2s, 4s, 8s; cap at 30s.
        let backoff_ms = 500u64.saturating_mul(1u64 << attempt.min(6)).min(30_000);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }
}

async fn run_session(storage: &Storage, http: &reqwest::Client, cfg: &IngestCfg) -> Result<()> {
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
                metrics::ws_idle_timeout();
                return Err(anyhow!(
                    "no newHeads within {}s idle timeout; reconnecting",
                    cfg.ws_idle_timeout.as_secs(),
                ));
            }
        };
        // newBlocks delivers the whole block on the socket — persist it
        // directly, no eth_getBlockByNumber round-trip. newHeads delivers a
        // header, so we still fetch the body.
        let (number_hex, height, hash, block) = match event {
            WsEvent::NewBlock {
                height,
                hash,
                block,
            } => {
                debug!(height, %hash, "new block (full)");
                (format!("0x{height:x}"), height, hash, block)
            }
            WsEvent::NewHead {
                number_hex,
                height,
                hash,
            } => {
                debug!(height, %hash, "new head");
                let Some(block) = fetch_full_block(http, &number_hex, height, cfg).await else {
                    continue;
                };
                (number_hex, height, hash, block)
            }
            WsEvent::Subscribed => continue,
        };
        // Receipts aren't carried by newBlocks (they're a separate index), so
        // fetch them here when enabled, regardless of subscription kind.
        let receipts_value = if cfg.receipts {
            let Some(r) = fetch_block_receipts(http, &number_hex, height, cfg).await else {
                warn!(height, "skipping block: receipts fetch failed");
                continue;
            };
            Some(r)
        } else {
            None
        };
        persist_block(
            storage,
            height,
            &hash,
            &block,
            receipts_value.as_ref(),
            &cfg.blocks,
        )
        .await?;
    }
    Ok(())
}

/// Open the upstream WebSocket (browser UA for the WAF bypass) and split it
/// into a read/write pair. A 429 / 503 on the handshake is surfaced as a
/// transient error after honoring `Retry-After`, so the caller's reconnect
/// path retries with backoff. No subscription is sent here — the caller picks
/// the kind (`connect_and_subscribe` for the live feed, the bootstrap for
/// `oldBlocks`).
async fn connect_ws(cfg: &IngestCfg) -> Result<(WsTx, WsRx)> {
    info!(url = %cfg.ws_url, "connecting websocket");
    let mut req = cfg.ws_url.as_str().into_client_request()?;
    req.headers_mut().insert(
        "User-Agent",
        BROWSER_UA
            .parse()
            .context("BROWSER_UA is not a valid header value")?,
    );
    let ws = match connect_async(req).await {
        Ok((ws, _)) => ws,
        Err(TungError::Http(resp))
            if resp.status() == http::StatusCode::TOO_MANY_REQUESTS
                || resp.status() == http::StatusCode::SERVICE_UNAVAILABLE =>
        {
            let retry_after = retry_after_from_headers(resp.headers()).unwrap_or(5);
            handle_throttle(
                cfg,
                "websocket connect",
                retry_after,
                resp.status().as_u16(),
            )
            .await;
            // handle_throttle returns only if we slept; loop the caller by surfacing
            // a transient error so the reconnect path takes over with its backoff.
            return Err(anyhow!("ws throttled (slept {retry_after}s, retrying)"));
        }
        Err(e) => return Err(anyhow::Error::from(e).context("connecting websocket")),
    };
    Ok(ws.split())
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
    tx.send(Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [kind],
        })
        .to_string()
        .into(),
    ))
    .await?;
    // Live session established; mark connected-since so the upstream connection
    // age is trackable (and resets on each reconnect).
    metrics::upstream_connected();
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
    tx.send(Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["oldBlocks", format!("0x{from:x}"), format!("0x{target:x}")],
        })
        .to_string()
        .into(),
    ))
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
                metrics::ws_idle_timeout();
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
        // oldBlocks carries the block but not receipts (a separate index). Fetch
        // them when enabled, exactly as the live path does. On failure, skip the
        // block entirely so backfill re-fetches block + receipts together — a
        // block persisted without its receipts would be skipped by backfill's
        // presence check, stranding the receipts.
        let receipts_value = if cfg.receipts {
            let number_hex = format!("0x{height:x}");
            let Some(r) = fetch_block_receipts(http, &number_hex, height, cfg).await else {
                warn!(
                    height,
                    "oldBlocks bootstrap: receipts fetch failed; leaving for backfill"
                );
                continue;
            };
            Some(r)
        } else {
            None
        };
        persist_backfilled(storage, height, &block, receipts_value.as_ref()).await?;
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
        .with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        bail!("upstream /health returned HTTP {}", resp.status());
    }
    let v: Value = resp.json().await.context("decode /health body")?;
    v.get("blocks")
        .and_then(|b| b.get("max_contiguous_height"))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            anyhow!("/health missing blocks.max_contiguous_height (is the upstream a neve?)")
        })
}

/// Pull the next interesting event from the WebSocket. Internally handles
/// pings, close frames, parse errors, and any frame that isn't a subscription
/// notification. Returns `None` when the stream ends or breaks.
async fn next_ws_event(tx: &mut WsTx, rx: &mut WsRx) -> Option<WsEvent> {
    while let Some(msg) = rx.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "websocket error");
                return None;
            }
        };
        let text = match msg {
            // tungstenite 0.26+ holds text as Utf8Bytes and binary/ping as Bytes;
            // normalize to an owned String so the JSON parse below is unchanged.
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            Message::Ping(p) => {
                tx.send(Message::Pong(p)).await.ok();
                continue;
            }
            Message::Close(_) => {
                info!("server closed connection");
                return None;
            }
            _ => continue,
        };
        let Ok(v) = serde_json::from_str::<Value>(&text) else {
            warn!("bad json");
            continue;
        };
        if let Some(event) = classify_frame(&v) {
            return Some(event);
        }
    }
    None
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
    let number_hex = head.get("number").and_then(Value::as_str)?.to_owned();
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
    Some(WsEvent::NewHead {
        number_hex,
        height,
        hash,
    })
}

/// Fetch the full block (with transactions) from HTTPS RPC.
pub(crate) async fn fetch_full_block(
    http: &reqwest::Client,
    number_hex: &str,
    height: u64,
    cfg: &IngestCfg,
) -> Option<Value> {
    fetch_rpc(
        http,
        height,
        "eth_getBlockByNumber",
        json!([number_hex, true]),
        cfg,
    )
    .await
}

/// Fetch the array of `eth_getBlockReceipts` for a block height. Returns the
/// raw `result` value (a JSON array) so callers can store it verbatim.
pub(crate) async fn fetch_block_receipts(
    http: &reqwest::Client,
    number_hex: &str,
    height: u64,
    cfg: &IngestCfg,
) -> Option<Value> {
    fetch_rpc(
        http,
        height,
        "eth_getBlockReceipts",
        json!([number_hex]),
        cfg,
    )
    .await
}

/// One round-trip to the HTTPS RPC, with retry/backoff for unfinalized blocks
/// and `Retry-After`-aware handling of 429 / 503 (capped to 60s) so heavy
/// backfill stretches don't trip Cloudflare's rate limiter in front of the
/// public Avalanche endpoint. Returns `None` if the call cannot succeed
/// within the retry budget.
/// Attempts a single `fetch_rpc` call makes before giving up. A `null` result
/// means the block hasn't propagated to the answering RPC backend yet; for a
/// just-produced newHead that's the common case. We keep the budget short
/// (backoff 250ms, 500ms, 1s ≈ 1.75s total) so the *serial* newHeads ingester
/// isn't head-of-line blocked retrying the tip while later heads pile up in the
/// WS buffer (and get dropped upstream). Any block missed this way is filled by
/// the backfill task, which fetches older heights that the pool already has.
const RPC_MAX_ATTEMPTS: u32 = 3;

async fn fetch_rpc(
    http: &reqwest::Client,
    height: u64,
    method: &str,
    params: Value,
    cfg: &IngestCfg,
) -> Option<Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    for attempt in 0..RPC_MAX_ATTEMPTS {
        let resp = match http.post(&cfg.rpc_url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                metrics::upstream_request(UpstreamOutcome::Error);
                warn!(error = %e, height, "rpc request failed");
                return None;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            metrics::upstream_request(status);
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
                metrics::upstream_request(outcome);
                if let Some(result) = result {
                    return Some(result);
                }
            }
            Err(e) => {
                metrics::upstream_request(UpstreamOutcome::Error);
                warn!(error = %e, height, "decode rpc response");
                return None;
            }
        }
        let backoff = 250u64.saturating_mul(1u64 << attempt.min(10));
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

/// Handle a 429 / 503 response with a `Retry-After` value. If the wait is
/// within `cfg.max_wait`, just sleep and return (caller will retry). If it's
/// longer than `cfg.max_wait`, log an ERROR, signal the fatal channel, and
/// park forever — main's select! will pick up the notify and exit with an
/// error. Parking avoids racing the caller into more requests.
async fn handle_throttle(cfg: &IngestCfg, what: &str, retry_after: u64, status: u16) {
    metrics::upstream_retry_after(retry_after);
    let wait = Duration::from_secs(retry_after);
    if wait > cfg.max_wait {
        error!(
            what,
            status,
            retry_after,
            max_wait_secs = cfg.max_wait.as_secs(),
            "upstream throttled longer than --max-wait; shutting down",
        );
        cfg.fatal.notify_one();
        std::future::pending::<()>().await;
        return;
    }
    warn!(what, status, retry_after, "throttled by upstream, sleeping");
    tokio::time::sleep(wait).await;
}

/// Parse a `Retry-After` header. Supports the integer-seconds form; the
/// HTTP-date form is rarer and not worth a chrono dependency to handle.
fn retry_after_secs(resp: &reqwest::Response) -> Option<u64> {
    retry_after_from_headers(resp.headers())
}

fn retry_after_from_headers(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
}

/// Validate the fetched body against the head hash and persist it. Mismatches
/// (fork between the WS feed and the load-balanced RPC pool) are skipped.
async fn persist_block(
    storage: &Storage,
    height: u64,
    expected_hash: &str,
    block: &Value,
    receipts: Option<&Value>,
    blocks: &broadcast::Sender<Value>,
) -> Result<()> {
    let body_hash = block.get("hash").and_then(Value::as_str).unwrap_or("");
    if body_hash != expected_hash {
        warn!(height, head = %expected_hash, body = %body_hash, "hash mismatch (fork?)");
        return Ok(());
    }
    let hash_bytes = match decode_hash(expected_hash) {
        Ok(h) => h,
        Err(e) => {
            warn!(error = %e, "bad hash on newHead");
            return Ok(());
        }
    };
    let tx_hashes = extract_tx_hashes(block);
    let bytes = serde_json::to_vec(block)?;
    let receipts_bytes = receipts.map(serde_json::to_vec).transpose()?;
    let block_len = bytes.len();
    let receipts_len = receipts_bytes.as_ref().map_or(0, Vec::len);
    storage
        .put(height, hash_bytes, &tx_hashes, bytes, receipts_bytes)
        .await?;
    metrics::block_persisted(metrics::BlockSource::Live);
    debug!(
        height,
        bytes = block_len,
        receipts_bytes = receipts_len,
        txs = tx_hashes.len(),
        "stored block",
    );
    // Announce to live subscribers. We publish the *full* block; each
    // subscriber projects it (newHeads strips transactions, newBlocks keeps
    // them). This also means a mirror re-serves what it received, so chains of
    // mirrors propagate. Skip the clone entirely when nobody is listening —
    // send() would just return Err on zero receivers, and this is the hot path.
    if blocks.receiver_count() > 0 {
        let _ = blocks.send(block.clone());
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
/// has overridden `--rpc-url`. Errors propagate so we refuse to start
/// rather than guess. Honors `--max-wait` for Retry-After on 429 / 503:
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
                     retry_after {retry_after}s exceeds --max-wait {}s); \
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
