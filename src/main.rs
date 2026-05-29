mod backfill;
mod health;
mod metrics;
mod middleware;
mod rpc;
mod storage;
mod subscribe;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use serde_json::Value;
use tokio::sync::{Notify, broadcast};
use tracing::{info, warn};

use crate::backfill::{BACKFILL_INTER_FETCH_MS, backfill_loop, summary_loop};
use crate::storage::Storage;
use crate::subscribe::{BROWSER_UA, fetch_chain_id, ingest};

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

    /// Mirror another neve instance from a single endpoint. neve serves
    /// JSON-RPC, the `newHeads` WebSocket, and `/health` on one socket, so
    /// this one URL yields all three: the WS and RPC endpoints are derived
    /// from it (`http`→`ws`, `https`→`wss`), overriding `--network` /
    /// `--ws-url` / `--rpc-url`. When the local store is empty, the
    /// upstream's `/health` is queried for its earliest retained block and
    /// the store is anchored there so backfill reproduces the upstream's
    /// whole retained range (not just forward from the tip). Backfill runs
    /// unthrottled in this mode — there's no public-endpoint rate limit to
    /// be polite to. Example: `--mirror-from http://10.0.0.5:8545`.
    #[arg(long, value_name = "URL")]
    mirror_from: Option<String>,

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
    /// Publishes each freshly-persisted **full** block to subscribers (the
    /// fan-out source for `newHeads` and `newBlocks`). Only the WS-driven
    /// path feeds this; backfill does not (those aren't "new"). Clone is
    /// cheap — it's a `broadcast::Sender` handle.
    blocks: broadcast::Sender<Value>,
    /// Subscribe to `newBlocks` (whole block, no follow-up fetch) instead of
    /// `newHeads` (header, then fetch). `true` in `--mirror-from` mode, where
    /// the upstream is a neve that serves the extension; `false` against the
    /// public endpoint, which only offers `newHeads`.
    subscribe_blocks: bool,
    /// Minimum delay between backfill block fetches. `40ms` (~25 req/s) by
    /// default to stay under Cloudflare on the public endpoint; `0` in
    /// `--mirror-from` mode, where the upstream is another neve with no such
    /// limit.
    backfill_inter_fetch: Duration,
    /// Lowest height backfill should fill down to. `Some(floor)` in mirror
    /// mode (the upstream's earliest retained height) lets backfill begin
    /// from `floor` without waiting for a `newHead` to anchor the store.
    /// `None` keeps the original "anchor at first newHead, fill forward only"
    /// behavior.
    backfill_floor: Option<u64>,
    /// Notified when something fatal happens (e.g. upstream throttle exceeds
    /// `--max-wait`). main's select! awaits this and exits with an error.
    fatal: Arc<Notify>,
    /// Notified once the mirror's `oldBlocks` bootstrap has finished streaming
    /// the historical range (or given up). The backfill loop waits on this in
    /// mirror mode so it doesn't race the bootstrap's ascending frontier with
    /// redundant HTTPS fetches. Unused (never awaited) outside mirror mode.
    bootstrap_done: Arc<Notify>,
}

impl IngestCfg {
    /// Assemble the ingest knobs from the CLI plus the already-resolved
    /// WebSocket / RPC endpoints, with a fresh `fatal` notifier.
    fn new(
        cli: &Cli,
        ws_url: String,
        rpc_url: String,
        blocks: broadcast::Sender<Value>,
        backfill_floor: Option<u64>,
    ) -> Self {
        // Mirror mode targets another neve: backfill unthrottled, and use the
        // newBlocks extension to skip the per-block fetch round-trip.
        let mirror = cli.mirror_from.is_some();
        let backfill_inter_fetch = if mirror {
            Duration::ZERO
        } else {
            Duration::from_millis(BACKFILL_INTER_FETCH_MS)
        };
        Self {
            receipts: cli.receipts,
            max_wait: cli.max_wait,
            ws_idle_timeout: cli.ws_idle_timeout,
            ws_url,
            rpc_url,
            blocks,
            subscribe_blocks: mirror,
            backfill_inter_fetch,
            backfill_floor,
            fatal: Arc::new(Notify::new()),
            bootstrap_done: Arc::new(Notify::new()),
        }
    }
}

fn parse_human_duration(s: &str) -> Result<Duration, String> {
    // Plain integer → seconds, so `--stop-time 6` works without a unit suffix.
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }
    parse_duration::parse(s).map_err(|e| e.to_string())
}

/// Configure tracing output for the run's destination. An interactive terminal
/// gets ANSI colors and a timestamp; under systemd/journald (no TTY) both are
/// dropped — ANSI would be stored as literal `^[[2m…` escapes, and journald
/// already stamps every line, so neve's own timestamp would just be a duplicate.
fn init_tracing(default_level: &str) {
    let interactive = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let builder = tracing_subscriber::fmt()
        .with_ansi(interactive)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
        );
    if interactive {
        builder.init();
    } else {
        builder.without_time().init();
    }
}

/// Drive `PrometheusHandle::run_upkeep` and refresh the `process_*` collector on
/// a fixed cadence. Upkeep drains histogram buckets and clears idle metrics so
/// the renderer's memory stays bounded over long runs; the collector re-reads
/// process CPU/memory/fd stats. 5s is frequent enough without measurable cost.
fn spawn_metrics_upkeep(handle: metrics_exporter_prometheus::PrometheusHandle) {
    tokio::spawn(async move {
        let collector = metrics::process_collector();
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            handle.run_upkeep();
            collector.collect();
        }
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("install rustls crypto provider"))?;
    init_tracing(cli.log_level.as_str());

    // Install the global metrics recorder before anything records; the handle
    // renders the `/metrics` payload and drives periodic upkeep.
    let metrics_handle = metrics::install()?;
    spawn_metrics_upkeep(metrics_handle.clone());

    let http = reqwest::Client::builder().user_agent(BROWSER_UA).build()?;
    let (ws_url, rpc_url) = resolve_endpoints(&cli)?;
    let chain_id = fetch_chain_id(&http, &rpc_url, cli.max_wait).await?;
    info!(chain_id, rpc_url = %rpc_url, "queried upstream chain_id");

    let data_dir = cli
        .data_dir
        .clone()
        .unwrap_or_else(|| cli.network.default_data_dir());
    std::fs::create_dir_all(&data_dir)?;

    let anchor_floor = mirror_anchor_floor(&http, &cli, &data_dir).await;
    let storage = Storage::open(&data_dir, chain_id, anchor_floor)?;
    info!(
        path = %data_dir.display(),
        chain_id,
        high_water = storage.high_water().await,
        "storage opened",
    );

    let backfill_count = Arc::new(AtomicU64::new(0));
    let behind_tip = Arc::new(AtomicU64::new(0));
    // Full-block fan-out for eth_subscribe (newHeads / newBlocks). Capacity
    // 1024 ≈ minutes of tail at C-chain block rate; a subscriber slower than
    // that gets Lagged and resumes from the tip rather than back-pressuring
    // ingest.
    let (block_tx, _) = broadcast::channel::<Value>(1024);

    let _rpc_handle = rpc::serve(
        cli.rpc_addr,
        storage.clone(),
        data_dir.clone(),
        chain_id,
        cli.max_connections,
        behind_tip.clone(),
        block_tx.clone(),
        metrics_handle,
    )
    .await?;
    if cli.receipts {
        info!("--receipts enabled: will fetch eth_getBlockReceipts per block");
    }
    let cfg = IngestCfg::new(&cli, ws_url, rpc_url, block_tx, anchor_floor);
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
    // Box::pin: this future transitively holds the large `ingest` state machine.
    Box::pin(run_until_shutdown(
        ingest_fut,
        fatal,
        cli.stop_time,
        storage_close,
    ))
    .await
}

/// Drive `ingest_fut` until the first shutdown trigger fires — ingest returning,
/// the optional stop-time elapsing, an OS signal, or a fatal upstream condition
/// — then flush storage to disk and return the run's outcome.
async fn run_until_shutdown(
    ingest_fut: impl std::future::Future<Output = Result<()>>,
    fatal: Arc<Notify>,
    stop_time: Option<Duration>,
    storage_close: Storage,
) -> Result<()> {
    let outcome = tokio::select! {
        r = ingest_fut => r,
        () = sleep_or_pending(stop_time) => {
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

/// Resolve the `(ws_url, rpc_url)` pair. `--mirror-from <url>` derives both
/// from one neve endpoint (neve serves RPC + WS + `/health` on one socket),
/// overriding `--network` / `--ws-url` / `--rpc-url`. Otherwise an explicit
/// `--ws-url` / `--rpc-url` wins, falling back to the `--network` defaults.
fn resolve_endpoints(cli: &Cli) -> Result<(String, String)> {
    if let Some(base) = cli.mirror_from.as_deref() {
        let base = base.trim_end_matches('/').to_owned();
        let ws = derive_ws_url(&base)?;
        info!(rpc = %base, ws = %ws, "mirror mode: derived endpoints from --mirror-from");
        return Ok((ws, base));
    }
    Ok((
        cli.ws_url
            .clone()
            .unwrap_or_else(|| cli.network.ws_url().to_owned()),
        cli.rpc_url
            .clone()
            .unwrap_or_else(|| cli.network.rpc_url().to_owned()),
    ))
}

/// In `--mirror-from` mode with an empty local store, learn the upstream's
/// earliest retained block from `/health` and return it as the anchor floor
/// so backfill reproduces the whole upstream range. An existing store already
/// has its floor baked in (skip the probe and resume); not mirroring → `None`.
async fn mirror_anchor_floor(
    http: &reqwest::Client,
    cli: &Cli,
    data_dir: &std::path::Path,
) -> Option<u64> {
    let base = cli.mirror_from.as_deref()?;
    if data_dir.join("blocks").join("blockdb.idx").exists() {
        info!("mirror: local store already exists, resuming with its anchored floor");
        return None;
    }
    match fetch_upstream_min_height(http, base).await {
        Ok(min_h) => {
            info!(
                min_height = min_h,
                "mirror: anchoring backfill floor at upstream's earliest retained block",
            );
            Some(min_h)
        }
        Err(e) => {
            warn!(error = %e, "mirror: /health probe failed; falling back to forward-only from tip");
            None
        }
    }
}

/// Derive a WebSocket URL from an HTTP(S) base, preserving host/port/path.
/// neve serves the `newHeads` WebSocket on the same socket as its HTTP
/// JSON-RPC, so mirroring needs only the one endpoint. `ws://` / `wss://`
/// inputs pass through unchanged.
fn derive_ws_url(base: &str) -> Result<String> {
    if let Some(rest) = base.strip_prefix("https://") {
        Ok(format!("wss://{rest}"))
    } else if let Some(rest) = base.strip_prefix("http://") {
        Ok(format!("ws://{rest}"))
    } else if base.starts_with("ws://") || base.starts_with("wss://") {
        Ok(base.to_owned())
    } else {
        bail!("--mirror-from must be an http(s):// (or ws(s)://) URL, got: {base}")
    }
}

/// Probe a neve upstream's `/health` for its earliest retained block height
/// (`blocks.min_height`). Used to anchor a fresh mirror store's floor so
/// backfill reproduces the upstream's whole retained range rather than only
/// growing forward from the current tip.
async fn fetch_upstream_min_height(http: &reqwest::Client, base: &str) -> Result<u64> {
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
        .and_then(|b| b.get("min_height"))
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("/health missing blocks.min_height (is the upstream a neve?)"))
}
