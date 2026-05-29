mod health;
mod middleware;
mod rpc;
mod storage;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::error::Error as TungError;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{Level, debug, error, info, warn};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsTx = SplitSink<WsStream, Message>;
type WsRx = SplitStream<WsStream>;

/// Sent on the WS handshake and every HTTPS RPC request. The Cloudflare
/// `Human Rate Limit Bypass` WAF rule requires a non-empty UA that doesn't
/// match any known-automation substring; a real-browser UA from a non-
/// datacenter ASN is the cheapest way into that bypass. TLS JA3 fingerprint
/// still comes from rustls and is *not* impersonated here.
const BROWSER_UA: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

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

#[derive(Debug, Clone, Copy, ValueEnum)]
#[clap(rename_all = "lower")]
enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    const fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://api.avax.network/ext/bc/C/ws",
            Self::Testnet => "wss://api.avax-test.network/ext/bc/C/ws",
        }
    }
    const fn rpc_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://api.avax.network/ext/bc/C/rpc",
            Self::Testnet => "https://api.avax-test.network/ext/bc/C/rpc",
        }
    }
    const fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }
    fn default_data_dir(self) -> PathBuf {
        PathBuf::from(format!("./blockstore-data-{}", self.as_str()))
    }
}

const CLI_EXAMPLES: &str = "\
EXAMPLES:
  # Dev quick start — use the permissive testnet endpoints.
  neve --network testnet

  # Mainnet ingest including receipts (eth_getTransactionReceipt support).
  neve --receipts

  # Bounded test run, debug logging, custom data dir.
  neve --network testnet --stop-time 30 --log-level debug --data-dir /tmp/bs
";

#[derive(Debug, Clone, Copy, ValueEnum)]
#[clap(rename_all = "lower")]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Avalanche C-chain block streamer + JSON-RPC mirror",
    after_help = CLI_EXAMPLES,
)]
struct Cli {
    /// Logging verbosity. Overridden by `RUST_LOG` if set.
    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    log_level: LogLevel,

    /// Stop after the given duration (e.g. `30s`, `5m`, `1h`). Parsed via
    /// the `parse_duration` crate. Useful for short test runs.
    #[arg(long, value_parser = parse_human_duration)]
    stop_time: Option<Duration>,

    /// Fetch and store per-block receipts so `eth_getTransactionReceipt`
    /// works. Doubles upstream bandwidth — off by default to be polite to
    /// Cloudflare in front of the public Avalanche endpoint.
    #[arg(long)]
    receipts: bool,

    /// Maximum time to wait when upstream sends `Retry-After` (e.g. `30s`,
    /// `10m`, `1h`). If the server asks us to wait longer than this, we log
    /// an error and shut down rather than silently sleep. Default: 10m.
    #[arg(long, value_parser = parse_human_duration, default_value = "10m")]
    max_wait: Duration,

    /// Drop and reconnect the WebSocket if no `newHeads` arrive within this
    /// window (e.g. `30s`, `2m`). Guards against a silently-dead socket — a
    /// half-open TCP connection or a stalled subscription that never errors,
    /// where the read would otherwise block forever. Default: 2m.
    #[arg(long, value_parser = parse_human_duration, default_value = "2m")]
    ws_idle_timeout: Duration,

    /// WebSocket endpoint for `newHeads` subscription. Defaults to the URL
    /// for the configured `--network`. An explicit `--ws-url` wins.
    #[arg(long)]
    ws_url: Option<String>,

    /// HTTPS JSON-RPC endpoint for block / receipt fetches. Defaults to the
    /// URL for the configured `--network`. An explicit `--rpc-url` wins.
    #[arg(long)]
    rpc_url: Option<String>,

    /// Which Avalanche network to target. Picks the default WS / RPC URLs
    /// and the default `--data-dir`. Testnet has much more permissive rate
    /// limits and is recommended for dev work.
    #[arg(long, value_enum, default_value_t = Network::Mainnet)]
    network: Network,

    /// Directory holding the blockstore + fjall index. Created if missing.
    /// Defaults to `./blockstore-data-<network>` so swapping networks
    /// doesn't cross-pollinate stores. A `NETWORK` stamp file is written
    /// on first open and verified on subsequent opens.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Socket address for the JSON-RPC server.
    #[arg(long, default_value = "127.0.0.1:8545")]
    rpc_addr: std::net::SocketAddr,

    /// Maximum concurrent JSON-RPC connections. Excess connections are
    /// rejected with HTTP 429. jsonrpsee's own default is only 100, which a
    /// public/wallet-facing endpoint blows past easily.
    #[arg(long, default_value_t = 1024)]
    max_connections: u32,

    /// Cadence for the periodic `summary` INFO log line (e.g. `30s`, `5m`,
    /// `1h`). The first summary fires shortly after startup regardless.
    #[arg(long, value_parser = parse_human_duration, default_value = "5m")]
    summary_period: Duration,
}

/// Runtime knobs that need to be available deep in the ingest/backfill paths.
#[derive(Clone)]
struct IngestCfg {
    receipts: bool,
    max_wait: Duration,
    /// Reconnect the WebSocket if no `newHeads` arrive within this window.
    ws_idle_timeout: Duration,
    ws_url: String,
    rpc_url: String,
    /// Notified when something fatal happens (e.g. upstream throttle exceeds
    /// `--max-wait`). main's select! awaits this and exits with an error.
    fatal: Arc<Notify>,
}

fn parse_human_duration(s: &str) -> Result<Duration, String> {
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(cli.log_level.as_str())),
        )
        .init();

    let http = reqwest::Client::builder().user_agent(BROWSER_UA).build()?;
    let ws_url = cli
        .ws_url
        .clone()
        .unwrap_or_else(|| cli.network.ws_url().to_owned());
    let rpc_url = cli
        .rpc_url
        .clone()
        .unwrap_or_else(|| cli.network.rpc_url().to_owned());
    let chain_id = fetch_chain_id(&http, &rpc_url, cli.max_wait).await?;
    info!(chain_id, rpc_url = %rpc_url, "queried upstream chain_id");

    let data_dir = cli
        .data_dir
        .clone()
        .unwrap_or_else(|| cli.network.default_data_dir());
    std::fs::create_dir_all(&data_dir)?;
    let storage = Storage::open(&data_dir, chain_id)?;
    info!(
        path = %data_dir.display(),
        chain_id,
        high_water = storage.high_water().await,
        "storage opened",
    );

    let backfill_count = Arc::new(AtomicU64::new(0));
    let behind_tip = Arc::new(AtomicU64::new(0));

    let _rpc_handle = rpc::serve(
        cli.rpc_addr,
        storage.clone(),
        data_dir.clone(),
        chain_id,
        cli.max_connections,
        behind_tip.clone(),
    )
    .await?;
    if cli.receipts {
        info!("--receipts enabled: will fetch eth_getBlockReceipts per block");
    }
    let cfg = IngestCfg {
        receipts: cli.receipts,
        max_wait: cli.max_wait,
        ws_idle_timeout: cli.ws_idle_timeout,
        ws_url,
        rpc_url,
        fatal: Arc::new(Notify::new()),
    };
    info!(
        max_wait_secs = cfg.max_wait.as_secs(),
        ws_idle_timeout_secs = cfg.ws_idle_timeout.as_secs(),
        ws_url = %cfg.ws_url,
        rpc_url = %cfg.rpc_url,
        "ingest config",
    );
    tokio::spawn(backfill_loop(
        storage.clone(),
        http.clone(),
        cfg.clone(),
        backfill_count.clone(),
        behind_tip.clone(),
    ));
    tokio::spawn(summary_loop(
        storage.clone(),
        cli.summary_period,
        backfill_count,
    ));

    let fatal = cfg.fatal.clone();
    let storage_close = storage.clone();
    let ingest_fut = ingest(storage, http, cfg);
    if let Some(stop) = cli.stop_time {
        info!(?stop, "stop-time set, will exit after this duration");
    }
    let outcome = tokio::select! {
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
    };
    // Graceful flush. Returning drops the runtime, which cancels the spawned
    // tasks and drops Storage — the blockstore checkpoints in its own Drop, and
    // fjall's journal (a WAL) survives a clean exit in the page cache. But
    // steady-state writes use PersistMode::Buffer, so a *power failure* right
    // after exit could lose the un-synced tail; fsync it explicitly here. The
    // "Recovering keyspace" lines on the next startup are fjall's normal open
    // path (it always recovers when the marker file exists), not a dirty close.
    info!("flushing storage to disk");
    if let Err(e) = storage_close.persist().await {
        warn!(error = %e, "storage flush on shutdown failed");
    }
    outcome
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
                warn!(error = ?e, attempt, "websocket session failed");
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
    loop {
        // Idle watchdog: `next_ws_event` only returns on a newHead (or the
        // one-time subscription ack); pings are handled internally and don't
        // return. So a timeout here means no new blocks within the window —
        // surface it as an error so `ingest` reconnects with backoff rather
        // than blocking forever on a silently-dead socket.
        let event = match tokio::time::timeout(
            cfg.ws_idle_timeout,
            next_ws_event(&mut tx, &mut rx),
        )
        .await
        {
            Ok(Some(event)) => event,
            Ok(None) => break,
            Err(_elapsed) => {
                return Err(anyhow!(
                    "no newHeads within {}s idle timeout; reconnecting",
                    cfg.ws_idle_timeout.as_secs(),
                ));
            }
        };
        let WsEvent::NewHead {
            number_hex,
            height,
            hash,
        } = event
        else {
            continue;
        };
        debug!(height, %hash, "new head");
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
    let mut req = cfg.ws_url.as_str().into_client_request()?;
    req.headers_mut().insert(
        "User-Agent",
        BROWSER_UA.parse().context("BROWSER_UA is not a valid header value")?,
    );
    let ws = match connect_async(req).await {
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
    debug!(
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
    /// `behind` at the start of the stretch — used to pick the severity for
    /// the matching "caught up" line.
    start_behind: u64,
}

impl BackfillProgress {
    const fn new() -> Self {
        Self { start_height: None, start_time: None, last_logged: 0, start_behind: 0 }
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
async fn summary_loop(
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
                let rate = if elapsed > 0.0 { added as f64 / elapsed } else { 0.0 };
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
#[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
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
/// newHead ingester is unaffected — it fetches at chain pace.
const BACKFILL_INTER_FETCH_MS: u64 = 40;

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
async fn backfill_loop(
    storage: Storage,
    http: reqwest::Client,
    cfg: IngestCfg,
    backfill_count: Arc<AtomicU64>,
    behind_tip: Arc<AtomicU64>,
) {
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
            behind_tip.store(0, Ordering::Relaxed);
            if let (Some(start_h), Some(start_t)) = (progress.start_height, progress.start_time) {
                let filled = contiguous.saturating_sub(start_h);
                let elapsed = start_t.elapsed().as_secs();
                // `format_secs` renders 0 as "?" (unknown ETA); here 0 just
                // means the stretch closed in under a second.
                let elapsed_str =
                    if elapsed == 0 { "<1s".to_owned() } else { format_secs(elapsed) };
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
        let behind = target.saturating_sub(contiguous);
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
        tokio::time::sleep(Duration::from_millis(BACKFILL_INTER_FETCH_MS)).await;
    }
}

/// One-shot startup query for the upstream chain ID. Used to stamp/verify
/// the on-disk store and catch cross-network pollution even when the user
/// has overridden `--rpc-url`. Errors propagate so we refuse to start
/// rather than guess. Honors `--max-wait` for Retry-After on 429 / 503:
/// shorter than `max_wait` → sleep and retry; longer → bail out loudly.
async fn fetch_chain_id(http: &reqwest::Client, rpc_url: &str, max_wait: Duration) -> Result<u64> {
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
    debug!(
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
