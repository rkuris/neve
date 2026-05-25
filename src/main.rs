mod middleware;
mod rpc;
mod storage;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Parser;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::error::Error as TungError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{error, info, warn};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsTx = SplitSink<WsStream, Message>;
type WsRx = SplitStream<WsStream>;

/// Interesting event emitted by the WebSocket session loop.
#[derive(Debug)]
enum WsEvent {
    Subscribed,
    NewHead {
        number_hex: String,
        height: u64,
        hash: String,
    },
}

use crate::storage::Storage;

const MAINNET_WS_URL: &str = "wss://api.avax.network/ext/bc/C/ws";
const MAINNET_RPC_URL: &str = "https://api.avax.network/ext/bc/C/rpc";
const TESTNET_WS_URL: &str = "wss://api.avax-test.network/ext/bc/C/ws";
const TESTNET_RPC_URL: &str = "https://api.avax-test.network/ext/bc/C/rpc";

#[derive(Debug, Parser)]
#[command(about = "Avalanche C-chain block streamer + JSON-RPC mirror")]
struct Cli {
    /// Crank logging up to DEBUG (overridden by `RUST_LOG` if set).
    #[arg(long)]
    debug: bool,

    /// Stop after the given duration (e.g. `30s`, `5m`, `1h`). Parsed via
    /// the `parse_duration` crate. Useful for short test runs.
    #[arg(long, value_parser = parse_stop_time)]
    stop_time: Option<Duration>,

    /// Fetch and store per-block receipts so `eth_getTransactionReceipt`
    /// works. Doubles upstream bandwidth — off by default to be polite to
    /// Cloudflare in front of the public Avalanche endpoint.
    #[arg(long)]
    receipts: bool,

    /// Maximum time to wait when upstream sends `Retry-After` (e.g. `30s`,
    /// `10m`, `1h`). If the server asks us to wait longer than this, we log
    /// an error and shut down rather than silently sleep. Default: 10m.
    #[arg(long, value_parser = parse_stop_time, default_value = "10m")]
    max_wait: Duration,

    /// WebSocket endpoint for `newHeads` subscription. Defaults to mainnet
    /// (or testnet when `--testnet` is set).
    #[arg(long)]
    ws_url: Option<String>,

    /// HTTPS JSON-RPC endpoint for block / receipt fetches. Defaults to
    /// mainnet (or testnet when `--testnet` is set). An explicit `--rpc-url`
    /// wins over `--testnet`.
    #[arg(long)]
    rpc_url: Option<String>,

    /// Shortcut: use the testnet (`api.avax-test.network`) endpoints. Far
    /// more permissive rate limits than mainnet — recommended for dev work.
    /// Overridden by an explicit `--ws-url` / `--rpc-url`.
    #[arg(long)]
    testnet: bool,
}

/// Runtime knobs that need to be available deep in the ingest/backfill paths.
#[derive(Clone)]
struct IngestCfg {
    receipts: bool,
    max_wait: Duration,
    ws_url: String,
    rpc_url: String,
    /// Notified when something fatal happens (e.g. upstream throttle exceeds
    /// `--max-wait`). main's select! awaits this and exits with an error.
    fatal: Arc<Notify>,
}

fn parse_stop_time(s: &str) -> Result<Duration, String> {
    // Plain integer → seconds, so `--stop-time 6` works without a unit suffix.
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }
    parse_duration::parse(s).map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("install rustls crypto provider"))?;
    let default_level = if cli.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
        )
        .init();

    let data_dir = PathBuf::from(
        std::env::var("BLOCKSTORE_DIR").unwrap_or_else(|_| "./blockstore-data".to_owned()),
    );
    std::fs::create_dir_all(&data_dir)?;
    let storage = Storage::open(&data_dir)?;
    info!(path = %data_dir.display(), high_water = storage.high_water().await, "storage opened");

    let rpc_addr: std::net::SocketAddr = std::env::var("RPC_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8545".to_owned())
        .parse()?;
    let _rpc_handle = rpc::serve(rpc_addr, storage.clone()).await?;

    let http = reqwest::Client::builder().build()?;
    if cli.receipts {
        info!("--receipts enabled: will fetch eth_getBlockReceipts per block");
    }
    let (default_ws, default_rpc) = if cli.testnet {
        (TESTNET_WS_URL, TESTNET_RPC_URL)
    } else {
        (MAINNET_WS_URL, MAINNET_RPC_URL)
    };
    let cfg = IngestCfg {
        receipts: cli.receipts,
        max_wait: cli.max_wait,
        ws_url: cli.ws_url.unwrap_or_else(|| default_ws.to_owned()),
        rpc_url: cli.rpc_url.unwrap_or_else(|| default_rpc.to_owned()),
        fatal: Arc::new(Notify::new()),
    };
    info!(
        max_wait_secs = cfg.max_wait.as_secs(),
        ws_url = %cfg.ws_url,
        rpc_url = %cfg.rpc_url,
        "ingest config",
    );
    tokio::spawn(backfill_loop(storage.clone(), http.clone(), cfg.clone()));

    let fatal = cfg.fatal.clone();
    let ingest_fut = ingest(storage, http, cfg);
    if let Some(stop) = cli.stop_time {
        info!(?stop, "stop-time set, will exit after this duration");
    }
    tokio::select! {
        r = ingest_fut => r,
        () = sleep_or_pending(cli.stop_time) => {
            info!("stop-time reached, shutting down");
            Ok(())
        }
        sig = wait_for_signal() => {
            info!(signal = sig, "signal received, shutting down");
            Ok(())
        }
        () = fatal.notified() => {
            Err(anyhow!("fatal upstream condition; see prior ERROR log"))
        }
    }
    // When this function returns, tokio shuts the runtime down. That cancels
    // the backfill task and drops Storage — blockstore's Drop checkpoints
    // and fjall's Drop flushes, so on-disk state is consistent.
}

/// Resolve immediately if `stop` is `None` (never fire), otherwise sleep for
/// the duration. Lets the main select! arm uniformly without a conditional.
async fn sleep_or_pending(stop: Option<Duration>) {
    match stop {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending::<()>().await,
    }
}

/// Wait for any of SIGINT / SIGTERM / SIGQUIT and return its name. Unix only.
async fn wait_for_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigquit = signal(SignalKind::quit()).expect("install SIGQUIT handler");
    tokio::select! {
        _ = sigint.recv() => "SIGINT",
        _ = sigterm.recv() => "SIGTERM",
        _ = sigquit.recv() => "SIGQUIT",
    }
}

async fn ingest(storage: Storage, http: reqwest::Client, cfg: IngestCfg) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        match run_session(&storage, &http, &cfg).await {
            Ok(()) => {
                info!("websocket session ended cleanly, reconnecting");
                attempt = 0;
            }
            Err(e) => {
                warn!(error = %e, attempt, "websocket session failed");
                attempt = attempt.saturating_add(1);
            }
        }
        // Exponential backoff: 500ms, 1s, 2s, 4s, 8s; cap at 30s.
        let backoff_ms = 500u64.saturating_mul(1u64 << attempt.min(6)).min(30_000);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }
}

async fn run_session(storage: &Storage, http: &reqwest::Client, cfg: &IngestCfg) -> Result<()> {
    let (mut tx, mut rx) = connect_and_subscribe(cfg).await?;
    while let Some(event) = next_ws_event(&mut tx, &mut rx).await {
        let WsEvent::NewHead {
            number_hex,
            height,
            hash,
        } = event
        else {
            continue;
        };
        info!(height, %hash, "new head");
        let Some(block) = fetch_full_block(http, &number_hex, height, cfg).await else {
            continue;
        };
        let receipts_value = if cfg.receipts {
            let Some(r) = fetch_block_receipts(http, &number_hex, height, cfg).await else {
                warn!(height, "skipping block: receipts fetch failed");
                continue;
            };
            Some(r)
        } else {
            None
        };
        persist_block(storage, height, &hash, &block, receipts_value.as_ref()).await?;
    }
    Ok(())
}

async fn connect_and_subscribe(cfg: &IngestCfg) -> Result<(WsTx, WsRx)> {
    info!(url = %cfg.ws_url, "connecting websocket");
    let ws = match connect_async(&cfg.ws_url).await {
        Ok((ws, _)) => ws,
        Err(TungError::Http(resp))
            if resp.status() == http::StatusCode::TOO_MANY_REQUESTS
                || resp.status() == http::StatusCode::SERVICE_UNAVAILABLE =>
        {
            let retry_after = retry_after_from_headers(resp.headers()).unwrap_or(5);
            handle_throttle(cfg, "websocket connect", retry_after, resp.status().as_u16()).await;
            // handle_throttle returns only if we slept; loop the caller by surfacing
            // a transient error so the reconnect path takes over with its backoff.
            return Err(anyhow!("ws throttled (slept {retry_after}s, retrying)"));
        }
        Err(e) => return Err(anyhow::Error::from(e).context("connecting websocket")),
    };
    let (mut tx, rx) = ws.split();
    tx.send(Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"],
        })
        .to_string(),
    ))
    .await?;
    Ok((tx, rx))
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
            Message::Text(t) => t,
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
    Some(WsEvent::NewHead {
        number_hex,
        height,
        hash,
    })
}

/// Fetch the full block (with transactions) from HTTPS RPC.
async fn fetch_full_block(
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
async fn fetch_block_receipts(
    http: &reqwest::Client,
    number_hex: &str,
    height: u64,
    cfg: &IngestCfg,
) -> Option<Value> {
    fetch_rpc(http, height, "eth_getBlockReceipts", json!([number_hex]), cfg).await
}

/// One round-trip to the HTTPS RPC, with retry/backoff for unfinalized blocks
/// and `Retry-After`-aware handling of 429 / 503 (capped to 60s) so heavy
/// backfill stretches don't trip Cloudflare's rate limiter in front of the
/// public Avalanche endpoint. Returns `None` if the call cannot succeed
/// within the retry budget.
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
    for attempt in 0..5u32 {
        let resp = match http.post(&cfg.rpc_url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, height, "rpc request failed");
                return None;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            let retry_after = retry_after_secs(&resp).unwrap_or(5);
            handle_throttle(cfg, method, retry_after, status.as_u16()).await;
            continue;
        }
        match resp.json::<Value>().await {
            Ok(parsed) => {
                if let Some(result) = parsed.get("result")
                    && !result.is_null()
                {
                    return Some(result.clone());
                }
            }
            Err(e) => {
                warn!(error = %e, height, "decode rpc response");
                return None;
            }
        }
        let backoff = 250u64.saturating_mul(1u64 << attempt.min(10));
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
    warn!(height, method, "rpc call still failing after retries");
    None
}

/// Handle a 429 / 503 response with a `Retry-After` value. If the wait is
/// within `cfg.max_wait`, just sleep and return (caller will retry). If it's
/// longer than `cfg.max_wait`, log an ERROR, signal the fatal channel, and
/// park forever — main's select! will pick up the notify and exit with an
/// error. Parking avoids racing the caller into more requests.
async fn handle_throttle(cfg: &IngestCfg, what: &str, retry_after: u64, status: u16) {
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
    headers.get(http::header::RETRY_AFTER)?.to_str().ok()?.parse::<u64>().ok()
}

/// Validate the fetched body against the head hash and persist it. Mismatches
/// (fork between the WS feed and the load-balanced RPC pool) are skipped.
async fn persist_block(
    storage: &Storage,
    height: u64,
    expected_hash: &str,
    block: &Value,
    receipts: Option<&Value>,
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
    info!(
        height,
        bytes = block_len,
        receipts_bytes = receipts_len,
        txs = tx_hashes.len(),
        "stored block",
    );
    Ok(())
}

/// Pull the per-tx hashes out of a full block returned by
/// `eth_getBlockByNumber(.., true)`. Malformed or missing entries are skipped
/// silently — a degenerate block JSON shouldn't take down ingest.
fn extract_tx_hashes(block: &Value) -> Vec<[u8; 32]> {
    let Some(txs) = block.get("transactions").and_then(Value::as_array) else {
        return Vec::new();
    };
    txs.iter()
        .filter_map(|tx| tx.get("hash").and_then(Value::as_str))
        .filter_map(|s| decode_hash(s).ok())
        .collect()
}

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
}

impl BackfillProgress {
    const fn new() -> Self {
        Self { start_height: None, start_time: None, last_logged: 0 }
    }
}

/// Heights of progress logging during a long backfill stretch.
const BACKFILL_LOG_EVERY: u64 = 100;

/// Minimum delay between backfill block fetches. Caps the worker at ~25 req/s
/// against Cloudflare's rate limit on the public Avalanche endpoint. The
/// newHead ingester is unaffected — it fetches at chain pace.
const BACKFILL_INTER_FETCH_MS: u64 = 40;

/// Backfill task. Closes both gap sources: (1) within-session holes between
/// `max_contiguous_height` and `height_highwater` when newHeads drops frames,
/// and (2) the cold-restart gap between local high-water and the upstream tip.
///
/// The target is `max(local_high_water, upstream_tip)`. newHeads keeps
/// advancing `high_water` concurrently, so the target chases the moving tip
/// without any explicit handoff between this task and the ingester.
async fn backfill_loop(storage: Storage, http: reqwest::Client, cfg: IngestCfg) {
    let mut progress = BackfillProgress::new();
    loop {
        let hw = storage.high_water().await;
        // Cold start: wait until newHeads anchors the store (minimum_height).
        // Backfilling from genesis is out of scope.
        if hw == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        let upstream = upstream_block_number(&http, &cfg).await.unwrap_or(0);
        let target = hw.max(upstream);
        let contiguous = storage.max_contiguous_height().await;
        if contiguous >= target {
            if let (Some(start_h), Some(start_t)) = (progress.start_height, progress.start_time) {
                let filled = contiguous.saturating_sub(start_h);
                info!(
                    blocks = filled,
                    elapsed_secs = start_t.elapsed().as_secs(),
                    "backfill caught up"
                );
                progress = BackfillProgress::new();
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        let behind = target.saturating_sub(contiguous);
        // Entering a "behind" stretch (or first iteration of one).
        if progress.start_height.is_none() {
            progress.start_height = Some(contiguous);
            progress.start_time = Some(std::time::Instant::now());
            progress.last_logged = contiguous;
            info!(contiguous, target, behind, "backfill starting");
        } else if contiguous.saturating_sub(progress.last_logged) >= BACKFILL_LOG_EVERY {
            progress.last_logged = contiguous;
            info!(contiguous, target, behind, "backfill progress");
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
        tokio::time::sleep(Duration::from_millis(BACKFILL_INTER_FETCH_MS)).await;
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

/// Persist a block fetched by the backfill path. Unlike `persist_block`, there
/// is no newHead hash to compare against, so we trust the body's reported hash.
async fn persist_backfilled(
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
    info!(
        height,
        bytes = block_len,
        receipts_bytes = receipts_len,
        txs = tx_hashes.len(),
        "backfilled block",
    );
    Ok(())
}

fn decode_hash(s: &str) -> Result<[u8; 32]> {
    let raw = hex::decode(s.trim_start_matches("0x"))?;
    raw.as_slice()
        .try_into()
        .map_err(|_| anyhow!("hash must be 32 bytes"))
}
