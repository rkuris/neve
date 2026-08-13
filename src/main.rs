mod bulk;
mod chain;
mod conn;
mod eth;
mod health;
mod join;
mod metrics;
mod middleware;
mod platform;
mod progress;
mod record;
mod rpc;
mod storage;
mod subscribe;
#[cfg(test)]
mod test_support;
mod upstream;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use serde_json::Value;
use tokio::sync::{Notify, broadcast};
use tracing::{info, warn};

use crate::chain::{Chain, IngestCfg, Network, normalize_chains};
use crate::eth::ingest::fetch_chain_id;
use crate::join::JoinBuffer;
use crate::progress::summary_loop;
use crate::rpc::ChainServe;
use crate::storage::Storage;
use crate::upstream::{Pacer, USER_AGENT, redact_url};

/// jemalloc instead of the system allocator, on by default.
///
/// neve's memory is nearly all allocator-held: on the production host under an
/// active backfill, 440 MiB of anonymous memory, every page of it private and
/// dirty, so none of it can be reclaimed under pressure. glibc's per-thread
/// arenas retain freed chunks and seldom hand them back; jemalloc purges on a
/// decay timer. Half that figure also sat on transparent huge pages, which
/// jemalloc does not request by default, so the 2 MiB rounding should go too.
///
/// Deliberately left at jemalloc's defaults. `dirty_decay_ms`/`muzzy_decay_ms`
/// and `background_thread` are the knobs to reach for if the default 10 s decay
/// proves too lazy, but they should be turned with a measurement in hand rather
/// than on principle.
///
/// The `msvc` guard is because jemalloc does not build there; the feature exists
/// so any other unsupported platform can opt out with `--no-default-features`.
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const CLI_EXAMPLES: &str = "\
EXAMPLES:
  # Dev quick start — use the permissive testnet endpoints.
  neve --network testnet

  # Bounded test run, debug logging, custom data dir.
  neve --network testnet --stop-time 30 --log-level debug --data-dir /tmp/bs

  # Backfill deep history into a fresh store (here: the whole chain from
  # genesis). Anchored at creation; stays throttled against the public endpoint.
  neve --backfill-floor 0

  # Mirror the C-chain and the P-chain from one process, on one socket.
  # eth_* answers from the C store, platform.* from the P store.
  neve --chains c,p

  # P-chain only, whole chain from genesis. Against the public endpoint this
  # is slow on purpose (see --p-request-interval); point --p-rpc-url at your
  # own node and set --p-request-interval 0 to fill it quickly.
  neve --chains p --p-backfill-floor 0
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
    // Include the build's commit, not just the crate version. It was already
    // compiled in for `/health` and `neve_build_info`, but both need a *running*
    // instance to read — so an archived or hand-installed binary on disk could not
    // be identified at all. Rust string literals are not NUL-terminated, so the
    // SHA is fused into one rodata blob and `strings` cannot isolate it either.
    // Surfacing it here means any binary can be asked what it is:
    //   $ neve --version
    //   neve 0.2.2 (abc1234)
    // which is what deploy/rollback.sh uses to label archives from the binaries
    // themselves rather than trusting the filename it wrote.
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("NEVE_GIT_COMMIT"), ")"),
    about = "Avalanche block streamer + JSON-RPC mirror (C-chain, P-chain, or both)",
    after_help = CLI_EXAMPLES,
)]
struct Cli {
    /// Which chains to mirror, comma-separated: `c` (EVM C-chain), `p`
    /// (platform P-chain), or `c,p` for both in one process. Each chain gets its
    /// own store, its own upstream connection, and its own `chain=` metric
    /// label; they share one listening socket, and a request picks its chain by
    /// method namespace (`eth_*` vs `platform.*`).
    #[arg(
        long,
        value_name = "LIST",
        value_enum,
        value_delimiter = ',',
        default_value = "c"
    )]
    chains: Vec<Chain>,

    /// Logging verbosity. Overridden by `RUST_LOG` if set.
    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    log_level: LogLevel,

    /// Stop after the given duration (e.g. `30s`, `5m`, `1h`). Parsed via
    /// the `parse_duration` crate. Useful for short test runs.
    #[arg(long, value_parser = parse_human_duration)]
    stop_time: Option<Duration>,

    /// Maximum time to wait when upstream sends `Retry-After` (e.g. `30s`,
    /// `10m`, `1h`). Within it, neve logs a WARN and sleeps; beyond it, it logs an
    /// ERROR and shuts down rather than sleep indefinitely.
    ///
    /// The default 65m is sized to absorb the value this endpoint actually sends.
    /// A throttled Avalanche public endpoint answers `Retry-After: 3600`, and the
    /// previous 10m default turned that into a shutdown — which under a
    /// `Restart=always` unit (as deploy/neve.service ships) becomes an hour-long
    /// crash loop, each cycle re-paying store recovery, with RPC unavailable
    /// throughout. Sleeping keeps serving up while backfill waits out the hour, and
    /// it is not silent: the WARN and `neve_upstream_retry_after_seconds` both fire.
    /// Lower it if you would rather the process exit and let an orchestrator decide.
    #[arg(long, value_parser = parse_human_duration, default_value = "65m")]
    max_wait: Duration,

    /// Drop and reconnect the C-chain WebSocket if no `newHeads` arrive within
    /// this window (e.g. `30s`, `2m`). Guards against a silently-dead socket — a
    /// half-open TCP connection or a stalled subscription that never errors,
    /// where the read would otherwise block forever. Default: 2m.
    #[arg(long, value_parser = parse_human_duration, default_value = "2m")]
    ws_idle_timeout: Duration,

    /// C-chain WebSocket endpoint for the `newHeads` subscription. Defaults to
    /// the URL for the configured `--network`. An explicit `--ws-url` wins.
    ///
    /// Prefer `NEVE_WS_URL` over the flag when the URL carries a credential:
    /// see the note on `--p-rpc-url`.
    #[arg(long, env = "NEVE_WS_URL", hide_env_values = true)]
    ws_url: Option<String>,

    /// C-chain HTTPS JSON-RPC endpoint for block fetches. Defaults to the
    /// URL for the configured `--network`. An explicit `--rpc-url` wins.
    ///
    /// Prefer `NEVE_RPC_URL` over the flag when the URL carries a credential:
    /// see the note on `--p-rpc-url`.
    #[arg(long, env = "NEVE_RPC_URL", hide_env_values = true)]
    rpc_url: Option<String>,

    /// P-chain HTTPS JSON-RPC endpoint (`platform.*`). Defaults to the
    /// `/ext/bc/P` URL for the configured `--network`. The P-chain has no
    /// upstream push mechanism, so there is no `--p-ws-url`: it polls
    /// `platform.getHeight` at `--p-poll-interval` instead.
    ///
    /// **Set this through `NEVE_P_RPC_URL`, not the flag, if the URL carries a
    /// rate-limit bypass token** (`?token=…`). Command-line arguments are
    /// world-readable through `/proc/<pid>/cmdline`, so a token passed as a flag
    /// is visible to every local user; a process's environment is readable only
    /// by its own user and root. neve redacts URL query strings from its logs
    /// either way.
    #[arg(long, env = "NEVE_P_RPC_URL", hide_env_values = true)]
    p_rpc_url: Option<String>,

    /// How long the P-chain tip poller waits between `platform.getHeight`
    /// calls. The public endpoint serves that method from a short cache, so
    /// polling much faster buys nothing. Default: 1s.
    #[arg(long, value_parser = parse_human_duration, default_value = "1s")]
    p_poll_interval: Duration,

    /// Minimum spacing between *individual* C-chain backfill upstream requests.
    ///
    /// A true rate cap, not a nap appended to each block: enforced globally
    /// through one pacer, so it bounds requests per second regardless of upstream
    /// latency. Backfill costs one `eth_getBlockByNumber` per block, plus one
    /// `eth_getLogs` per ~2048-block window under `--ingest-logs`, plus an
    /// occasional `eth_blockNumber` for the tip.
    ///
    /// The default 40ms is ~25 req/s, the rate this endpoint has long been
    /// documented as tolerating, and now measured rather than assumed: the mainnet
    /// instance sustained 24.75 blocks/s with no 429. The evidence is stronger than
    /// that run alone, because the pre-cache code was already issuing ~23.4 req/s
    /// continuously for days — two requests per block at 11.7 blocks/s — so this is
    /// ~7% above a long-proven rate rather than a step into the unknown.
    ///
    /// Raise it (a larger delay) if you see HTTP 429. The only hard throttle anyone
    /// has observed was ~14 req/s of `platform.*` against `api.avax-test.network`,
    /// which suggests the P-chain path or testnet is stricter than mainnet's
    /// C-chain. Ignored in `--mirror-from` mode, which is unthrottled.
    #[arg(long, value_parser = parse_human_duration, default_value = "40ms")]
    request_interval: Duration,

    /// Minimum delay between *individual* P-chain upstream requests while
    /// filling history. Each height costs two calls (`hexnc` + `json`), so this
    /// paces requests rather than heights.
    ///
    /// The default is deliberately far politer than the C-chain's ~25 req/s:
    /// measured on 2026-08-10, `api.avax-test.network` answered a sustained
    /// ~14 req/s of `platform.*` with HTTP 429 and `Retry-After: 3600`, and the
    /// limit applies to the whole host per IP — a hard P-chain backfill will
    /// throttle a C-chain instance sharing the address. Filling deep history at
    /// this rate takes a very long time, so point `--p-rpc-url` at your own node
    /// (or a neve mirror) and set this to `0` for that. Ignored in
    /// `--mirror-from` mode, which is already unthrottled.
    #[arg(long, value_parser = parse_human_duration, default_value = "200ms")]
    p_request_interval: Duration,

    /// How many P-chain heights to fetch concurrently while filling history.
    ///
    /// Each height costs two upstream round-trips, and issuing them serially
    /// caps the fill at roughly `1/(2 x RTT)` — a few hundred heights/s against
    /// a real node, which is hours for a from-genesis mainnet fill. Fetching
    /// ahead hides that latency.
    ///
    /// This can only recover time spent *waiting*: `--p-request-interval` is
    /// enforced globally across every in-flight request, so raising this against
    /// the public endpoint changes nothing. Raise it when pointed at your own
    /// node (with `--p-request-interval 0`), where round-trip latency is the
    /// whole cost.
    #[arg(long, default_value_t = 8)]
    p_concurrency: usize,

    /// Which Avalanche network to target. Picks the default endpoint URLs
    /// and the default `--data-dir`, for every selected chain. Testnet has much
    /// more permissive rate limits and is recommended for dev work.
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

    /// Base directory holding each chain's blockstore + fjall index. Created if
    /// missing. Defaults to `./blockstore-data-<network>` so swapping networks
    /// doesn't cross-pollinate stores. The C-chain store sits at this path
    /// directly (unchanged from before multi-chain support, so existing data
    /// dirs need no migration); other chains get a subdirectory — the P-chain's
    /// is `<data-dir>/p`, overridable with `--p-data-dir`. Each store is stamped
    /// with its chain and network on first open and verified on every open.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Directory holding the P-chain store, overriding the default
    /// `<data-dir>/p`.
    #[arg(long)]
    p_data_dir: Option<PathBuf>,

    /// Socket address for the JSON-RPC server. One socket serves every selected
    /// chain.
    #[arg(long, default_value = "127.0.0.1:8545")]
    rpc_addr: std::net::SocketAddr,

    /// Maximum concurrent JSON-RPC connections. Excess connections are
    /// rejected with HTTP 429. jsonrpsee's own default is only 100, which a
    /// public/wallet-facing endpoint blows past easily.
    #[arg(long, default_value_t = 1024)]
    max_connections: u32,

    /// Cadence for the periodic `summary` INFO log line (e.g. `30s`, `5m`,
    /// `1h`), one line per chain. The first summary fires shortly after startup
    /// regardless.
    #[arg(long, value_parser = parse_human_duration, default_value = "5m")]
    summary_period: Duration,

    /// Lowest C-chain block height backfill should fill down to, anchored when
    /// the store is first created. Without it, neve anchors at the first
    /// `newHead` it receives and only fills *forward* from there; set it to
    /// retain deep history — e.g. `--backfill-floor 0` to mirror the whole
    /// chain. Ignored if the store already exists (the floor is baked in at
    /// creation; truncate the data dir to re-anchor). Against the public
    /// endpoint backfill stays throttled to ~25 req/s, so a deep floor takes a
    /// long time to fill. Overrides the `--mirror-from` auto-floor when both are
    /// given.
    #[arg(long, value_name = "HEIGHT")]
    backfill_floor: Option<u64>,

    /// Lowest P-chain height to fill down to. Same semantics as
    /// `--backfill-floor`, for the P store.
    #[arg(long, value_name = "HEIGHT")]
    p_backfill_floor: Option<u64>,

    /// Cap on the adaptive pre-fetch delay parked before the first live
    /// C-chain `newHeads` block fetch (e.g. `50ms`, `100ms`). A `newHeads` event
    /// can outrun the block's availability on the HTTPS backend; an AIMD
    /// controller learns a short delay that lets it land, cutting wasted `empty`
    /// fetches. Default `0s` disables it: the public Avalanche endpoint's
    /// propagation tail is heavy enough that any cap just pegs and adds latency
    /// to every block, while the cheap 25ms retry already covers the misses. Set
    /// a small cap only against a fast private full node that serves `newHeads`.
    /// No effect in `--mirror-from` mode (that uses `newBlocks`, no fetch).
    #[arg(long, value_parser = parse_human_duration, default_value = "0s")]
    prefetch_delay_cap: Duration,

    /// Close a JSON-RPC connection that has had no read or write activity for
    /// this long (e.g. `60s`, `2m`). Defends against slowloris and the leaked
    /// idle-keep-alive fd growth jsonrpsee can't reap on its own. `0` disables
    /// the reaping entirely (connections may then linger until `--max-connections`).
    #[arg(long, value_parser = parse_human_duration, default_value = "60s")]
    idle_timeout: Duration,

    /// Maximum number of blocks a single `GET /blocks?from=&to=` bulk-export
    /// request may return; larger ranges are rejected with HTTP 400. Split a
    /// bigger download into successive windows. `?chain=` picks which chain's
    /// store to export from.
    #[arg(long, default_value_t = 10_000)]
    max_blocks_per_request: u64,

    /// Ingest C-chain event logs alongside blocks: backfill fetches each
    /// ~2048-block window's logs via `eth_getLogs`, and the live path fetches
    /// each tip block's logs and joins them into the combined `[block, logs]`
    /// record. Off by default until the feed is proven.
    #[arg(long)]
    ingest_logs: bool,

    /// Max pending heights in a live join buffer before it flushes (and defers
    /// those heights to backfill). Only used with `--ingest-logs`.
    #[arg(long, default_value_t = 8192)]
    join_buffer_cap: usize,
}

impl Cli {
    /// Base directory every chain's store hangs off.
    fn base_data_dir(&self) -> PathBuf {
        self.data_dir
            .clone()
            .unwrap_or_else(|| self.network.default_data_dir())
    }

    /// Where `chain`'s store lives: the explicit per-chain override when given,
    /// else the chain's default spot under the base.
    fn chain_data_dir(&self, chain: Chain) -> PathBuf {
        match chain {
            Chain::C => self.base_data_dir(),
            Chain::P => self
                .p_data_dir
                .clone()
                .unwrap_or_else(|| chain.data_dir(&self.base_data_dir())),
        }
    }

    /// The floor flag for `chain`.
    const fn chain_backfill_floor(&self, chain: Chain) -> Option<u64> {
        match chain {
            Chain::C => self.backfill_floor,
            Chain::P => self.p_backfill_floor,
        }
    }

    /// Resolve `chain`'s `(ws_url, rpc_url)`. `--mirror-from <url>` points every
    /// chain at one neve endpoint (neve serves RPC + WS + `/health` on one
    /// socket, and chains are told apart by method namespace), overriding
    /// `--network` and the per-chain URL flags. Otherwise an explicit per-chain
    /// URL wins, falling back to the `--network` defaults. The returned `ws_url`
    /// is empty for a chain with no upstream push mechanism.
    fn chain_endpoints(&self, chain: Chain) -> Result<(String, String)> {
        if let Some(base) = self.mirror_from.as_deref() {
            let base = base.trim_end_matches('/').to_owned();
            let ws = derive_ws_url(&base)?;
            info!(
                chain = chain.as_str(),
                rpc = %redact_url(&base), ws = %redact_url(&ws),
                "mirror mode: derived endpoints from --mirror-from",
            );
            return Ok((ws, base));
        }
        let (ws_flag, rpc_flag) = match chain {
            Chain::C => (self.ws_url.clone(), self.rpc_url.clone()),
            Chain::P => (None, self.p_rpc_url.clone()),
        };
        let ws = ws_flag
            .or_else(|| chain.default_ws_url(self.network))
            .unwrap_or_default();
        let rpc = rpc_flag.unwrap_or_else(|| chain.default_rpc_url(self.network));
        Ok((ws, rpc))
    }

    /// Assemble one chain's ingest knobs from the CLI plus its already-resolved
    /// endpoints. `fatal` is shared across chains so any chain's unrecoverable
    /// condition shuts the whole process down.
    fn ingest_cfg(
        &self,
        chain: Chain,
        ws_url: String,
        rpc_url: String,
        blocks: subscribe::LiveTx,
        backfill_floor: Option<u64>,
        fatal: Arc<Notify>,
    ) -> IngestCfg {
        // Mirror mode targets another neve: backfill unthrottled, and use the
        // newBlocks extension to skip the per-block fetch round-trip.
        let mirror = self.mirror_from.is_some();
        let backfill_inter_fetch = match (mirror, chain) {
            (true, _) => Duration::ZERO,
            (false, Chain::C) => self.request_interval,
            (false, Chain::P) => self.p_request_interval,
        };
        IngestCfg {
            chain,
            max_wait: self.max_wait,
            pacer: Arc::new(Pacer::new(backfill_inter_fetch)),
            fetch_concurrency: self.p_concurrency.max(1),
            ws_idle_timeout: self.ws_idle_timeout,
            ws_url,
            rpc_url,
            blocks,
            poll_interval: self.p_poll_interval,
            subscribe_blocks: mirror,
            backfill_inter_fetch,
            backfill_floor,
            prefetch_delay_cap: self.prefetch_delay_cap,
            fatal,
            bootstrap_done: Arc::new(Notify::new()),
            ingest_logs: self.ingest_logs,
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
    // `--log-level` is neve's own verbosity. Scope debug/trace to neve's crate so
    // chatty dependencies (hyper's per-request "pooling idle connection", fjall,
    // rustls, …) aren't dragged down with it — they stay at info. `RUST_LOG`, when
    // set, overrides this entirely.
    let scoped = match default_level {
        "debug" | "trace" => format!("info,neve={default_level}"),
        level => level.to_owned(),
    };
    let builder = tracing_subscriber::fmt()
        .with_ansi(interactive)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| scoped.into()),
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

/// One fully-wired chain instance: what the serving layer needs (`serve`) and
/// what its ingest pipeline needs (`cfg`).
struct Instance {
    serve: ChainServe,
    cfg: IngestCfg,
}

/// Stand up one chain: query the upstream for its network identity, open (or
/// verify) the store, wire the live fan-out and optional join buffer, and build
/// both halves of the instance.
async fn build_instance(
    cli: &Cli,
    chain: Chain,
    http: &reqwest::Client,
    fatal: Arc<Notify>,
) -> Result<Instance> {
    let (ws_url, rpc_url) = cli.chain_endpoints(chain)?;
    let identity = fetch_identity(chain, http, &rpc_url, cli.max_wait).await?;
    info!(
        chain = chain.as_str(),
        identity = %identity,
        rpc_url = %redact_url(&rpc_url),
        "queried upstream network identity",
    );

    let data_dir = cli.chain_data_dir(chain);
    std::fs::create_dir_all(&data_dir)?;
    let anchor_floor = resolve_anchor_floor(http, cli, chain, &data_dir).await;
    let storage = Storage::open(&data_dir, chain, &identity, anchor_floor)?;
    info!(
        chain = chain.as_str(),
        path = %data_dir.display(),
        high_water = storage.high_water().await,
        "storage opened",
    );

    // Live join buffer, only when this chain's derived-data ingestion is on.
    // Block reads consult it so an in-flight tip record (buffered while its
    // second half is fetched) is serveable from memory; a periodic tick
    // refreshes its gauges.
    // C-chain only for now: the P-chain's second half (reward UTXOs) is fetched
    // in a later phase, so nothing joins at its tip yet.
    let join = (chain == Chain::C && cli.ingest_logs)
        .then(|| JoinBuffer::new(storage.clone(), cli.join_buffer_cap));
    if let Some(buf) = join.clone() {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                buf.sample();
            }
        });
    }

    // Live fan-out for subscriptions.
    let (blocks, _) = broadcast::channel(subscribe::LIVE_CHANNEL_CAP);
    let behind_tip = Arc::new(AtomicU64::new(0));
    let cfg = cli.ingest_cfg(chain, ws_url, rpc_url, blocks.clone(), anchor_floor, fatal);
    info!(
        chain = chain.as_str(),
        max_wait_secs = cfg.max_wait.as_secs(),
        ws_idle_timeout_secs = cfg.ws_idle_timeout.as_secs(),
        request_interval_ms = cfg.backfill_inter_fetch.as_millis(),
        fetch_concurrency = cfg.fetch_concurrency,
        ws_url = %redact_url(&cfg.ws_url),
        rpc_url = %redact_url(&cfg.rpc_url),
        "ingest config",
    );

    Ok(Instance {
        serve: ChainServe {
            chain,
            storage,
            data_dir,
            identity,
            behind_tip,
            blocks,
            join,
            ingests_logs: chain == Chain::C && cli.ingest_logs,
        },
        cfg,
    })
}

/// Query the upstream for the opaque fingerprint that binds a store to one
/// network. The C-chain uses `eth_chainId` in decimal, which is what
/// pre-multi-chain stores are already stamped with.
async fn fetch_identity(
    chain: Chain,
    http: &reqwest::Client,
    rpc_url: &str,
    max_wait: Duration,
) -> Result<String> {
    match chain {
        Chain::C => Ok(fetch_chain_id(http, rpc_url, max_wait).await?.to_string()),
        // The P-chain has no chain-ID method, so bind the store to the network
        // by the genesis block's ID — equally immutable, and derived from the
        // fetched bytes so it doubles as proof the endpoint speaks P-chain.
        Chain::P => platform::ingest::fetch_genesis_id(http, rpc_url, max_wait).await,
    }
}

/// Spawn `chain`'s ingest pipeline, returning the future that drives its live
/// path. Background loops (backfill, summary) are spawned here; the returned
/// future is what `main` awaits.
fn spawn_pipeline(
    inst: &Instance,
    http: &reqwest::Client,
    summary_period: Duration,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
    let Instance { serve, cfg } = inst;
    let backfill_count = Arc::new(AtomicU64::new(0));
    tokio::spawn(summary_loop(
        serve.storage.clone(),
        summary_period,
        backfill_count.clone(),
        serve.behind_tip.clone(),
    ));
    match serve.chain {
        // The C-chain splits push (live `newHeads`) from pull (gap-closing
        // backfill), so it runs two loops.
        Chain::C => {
            tokio::spawn(eth::backfill::backfill_loop(
                serve.storage.clone(),
                http.clone(),
                cfg.clone(),
                backfill_count,
                serve.behind_tip.clone(),
            ));
            Box::pin(eth::ingest::ingest(
                serve.storage.clone(),
                http.clone(),
                cfg.clone(),
                serve.join.clone(),
            ))
        }
        // The P-chain has no push mechanism to split from, so one loop both
        // follows the tip and closes gaps — polling a node, or streaming whole
        // records from an upstream neve when mirroring.
        Chain::P if cfg.subscribe_blocks => Box::pin(platform::mirror::mirror(
            serve.storage.clone(),
            http.clone(),
            cfg.clone(),
            backfill_count,
            serve.behind_tip.clone(),
        )),
        Chain::P => Box::pin(platform::ingest::ingest(
            serve.storage.clone(),
            http.clone(),
            cfg.clone(),
            backfill_count,
            serve.behind_tip.clone(),
        )),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let chains = normalize_chains(&cli.chains)?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("install rustls crypto provider"))?;
    init_tracing(cli.log_level.as_str());

    // Install the global metrics recorder before anything records; the handle
    // renders the `/metrics` payload and drives periodic upkeep.
    let metrics_handle = metrics::install()?;
    spawn_metrics_upkeep(metrics_handle.clone());

    let http = reqwest::Client::builder().user_agent(USER_AGENT).build()?;
    // Shared across chains: whichever pipeline hits an unrecoverable upstream
    // condition takes the process down, rather than leaving a half-serving mirror.
    let fatal = Arc::new(Notify::new());

    info!(
        chains = %chains.iter().copied().map(Chain::as_str).collect::<Vec<_>>().join(","),
        network = cli.network.as_str(),
        "starting",
    );
    let mut instances = Vec::with_capacity(chains.len());
    for &chain in &chains {
        instances.push(build_instance(&cli, chain, &http, fatal.clone()).await?);
    }

    // `--idle-timeout 0` disables the connection reaper; a positive value enables
    // it. (`Option` rather than a magic-zero `Duration` past this boundary.)
    let idle_timeout = (cli.idle_timeout > Duration::ZERO).then_some(cli.idle_timeout);
    let serve_cfg = rpc::ServeConfig {
        addr: cli.rpc_addr,
        max_connections: cli.max_connections,
        idle_timeout,
        max_blocks_per_request: cli.max_blocks_per_request,
    };
    let _rpc_handle = rpc::serve(
        serve_cfg,
        instances.iter().map(|i| i.serve.clone()).collect(),
        metrics_handle,
    )
    .await?;

    let ingest_futs: Vec<_> = instances
        .iter()
        .map(|inst| spawn_pipeline(inst, &http, cli.summary_period))
        .collect();
    let stores: Vec<Storage> = instances.iter().map(|i| i.serve.storage.clone()).collect();

    if let Some(stop) = cli.stop_time {
        info!(?stop, "stop-time set, will exit after this duration");
    }
    // Box::pin: this future transitively holds the large per-chain `ingest` state
    // machines.
    Box::pin(run_until_shutdown(
        futures_util::future::try_join_all(ingest_futs),
        fatal,
        cli.stop_time,
        stores,
    ))
    .await
}

/// Drive every chain's live-ingest future until the first shutdown trigger fires
/// — any ingest returning, the optional stop-time elapsing, an OS signal, or a
/// fatal upstream condition — then flush every store to disk and return the
/// run's outcome.
async fn run_until_shutdown<T>(
    ingest_fut: impl std::future::Future<Output = Result<T>>,
    fatal: Arc<Notify>,
    stop_time: Option<Duration>,
    stores: Vec<Storage>,
) -> Result<()> {
    let outcome = tokio::select! {
        r = ingest_fut => r.map(|_| ()),
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
    for store in &stores {
        if let Err(e) = store.persist().await {
            warn!(chain = store.chain().as_str(), error = %e, "storage flush on shutdown failed");
        }
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

/// Resolve the height at which to anchor `chain`'s freshly-created store floor,
/// so backfill fills *down* to it rather than only forward from the first live
/// block. Sources, in priority order:
///
/// 1. An explicit per-chain floor flag (wins even in mirror mode).
/// 2. `--mirror-from` mode: the upstream's earliest retained block for this
///    chain, learned from its `/health`, so backfill reproduces the whole
///    upstream range.
///
/// In every case an *existing* store already has its floor baked in at
/// creation, so we skip the work and return `None` (resume as-is). Neither
/// flag nor probe can lower the floor of a store that already exists.
async fn resolve_anchor_floor(
    http: &reqwest::Client,
    cli: &Cli,
    chain: Chain,
    data_dir: &Path,
) -> Option<u64> {
    let store_exists = data_dir.join("blocks").join("blockdb.idx").exists();

    // An explicit floor is the most specific intent and applies in any mode,
    // including against the public endpoint.
    if let Some(floor) = cli.chain_backfill_floor(chain) {
        if store_exists {
            info!(
                chain = chain.as_str(),
                floor,
                "backfill floor flag ignored: store already exists, resuming with its baked-in floor",
            );
            return None;
        }
        info!(chain = chain.as_str(), floor, "anchoring backfill floor");
        return Some(floor);
    }

    let base = cli.mirror_from.as_deref()?;
    if store_exists {
        info!(
            chain = chain.as_str(),
            "mirror: local store already exists, resuming with its anchored floor",
        );
        return None;
    }
    match fetch_upstream_min_height(http, base, chain).await {
        Ok(min_h) => {
            info!(
                chain = chain.as_str(),
                min_height = min_h,
                "mirror: anchoring backfill floor at upstream's earliest retained block",
            );
            Some(min_h)
        }
        Err(e) => {
            warn!(
                chain = chain.as_str(),
                error = %e,
                "mirror: /health probe failed; falling back to forward-only from tip",
            );
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

/// Probe a neve upstream's `/health` for `chain`'s earliest retained block
/// height. Used to anchor a fresh mirror store's floor so backfill reproduces
/// the upstream's whole retained range rather than only growing forward from the
/// current tip.
async fn fetch_upstream_min_height(
    http: &reqwest::Client,
    base: &str,
    chain: Chain,
) -> Result<u64> {
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
    health::upstream_blocks_field(&v, chain, "min_height").ok_or_else(|| {
        anyhow!(
            "/health has no {} blocks.min_height (is the upstream a neve serving this chain?)",
            chain.as_str()
        )
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Parse a CLI from arguments, as a user would type them.
    fn cli(args: &[&str]) -> Cli {
        Cli::parse_from(std::iter::once("neve").chain(args.iter().copied()))
    }

    /// The default is C-chain only — an existing invocation keeps its behavior.
    #[test]
    fn default_selects_only_the_c_chain() {
        assert_eq!(cli(&[]).chains, vec![Chain::C]);
    }

    #[test]
    fn chains_flag_accepts_a_list() {
        assert_eq!(cli(&["--chains", "p"]).chains, vec![Chain::P]);
        assert_eq!(cli(&["--chains", "c,p"]).chains, vec![Chain::C, Chain::P]);
    }

    /// The C-chain store stays at `--data-dir` itself; the P-chain nests under
    /// it, and `--p-data-dir` overrides that.
    #[test]
    fn per_chain_data_dirs() {
        let c = cli(&["--data-dir", "/srv/neve"]);
        assert_eq!(c.chain_data_dir(Chain::C), PathBuf::from("/srv/neve"));
        assert_eq!(c.chain_data_dir(Chain::P), PathBuf::from("/srv/neve/p"));

        let c = cli(&["--data-dir", "/srv/neve", "--p-data-dir", "/mnt/big/p"]);
        assert_eq!(c.chain_data_dir(Chain::C), PathBuf::from("/srv/neve"));
        assert_eq!(c.chain_data_dir(Chain::P), PathBuf::from("/mnt/big/p"));
    }

    /// Each chain resolves its own endpoints: the unprefixed URL flags stay
    /// C-scoped, `--p-rpc-url` is the P-chain's, and the P-chain never gets a
    /// WebSocket (it has no upstream push mechanism to subscribe to).
    #[test]
    fn per_chain_endpoints() {
        let c = cli(&["--network", "testnet"]);
        let (ws, rpc) = c.chain_endpoints(Chain::C).unwrap();
        assert_eq!(rpc, "https://api.avax-test.network/ext/bc/C/rpc");
        assert_eq!(ws, "wss://api.avax-test.network/ext/bc/C/ws");
        let (p_ws, p_rpc) = c.chain_endpoints(Chain::P).unwrap();
        assert_eq!(p_rpc, "https://api.avax-test.network/ext/bc/P");
        assert!(p_ws.is_empty(), "the P-chain has no upstream WebSocket");

        let c = cli(&[
            "--rpc-url",
            "http://c.local",
            "--p-rpc-url",
            "http://p.local",
        ]);
        assert_eq!(c.chain_endpoints(Chain::C).unwrap().1, "http://c.local");
        assert_eq!(c.chain_endpoints(Chain::P).unwrap().1, "http://p.local");
    }

    /// `--mirror-from` points every chain at the one upstream neve endpoint,
    /// overriding the per-chain URL flags — chains are told apart there by
    /// method namespace, not by URL.
    #[test]
    fn mirror_from_overrides_every_chain() {
        let c = cli(&[
            "--chains",
            "c,p",
            "--mirror-from",
            "http://10.0.0.5:8545/",
            "--rpc-url",
            "http://ignored",
        ]);
        for chain in [Chain::C, Chain::P] {
            let (ws, rpc) = c.chain_endpoints(chain).unwrap();
            assert_eq!(rpc, "http://10.0.0.5:8545");
            assert_eq!(ws, "ws://10.0.0.5:8545");
        }
    }

    #[test]
    fn per_chain_backfill_floors() {
        let c = cli(&["--backfill-floor", "10", "--p-backfill-floor", "20"]);
        assert_eq!(c.chain_backfill_floor(Chain::C), Some(10));
        assert_eq!(c.chain_backfill_floor(Chain::P), Some(20));
        let c = cli(&["--backfill-floor", "10"]);
        assert_eq!(c.chain_backfill_floor(Chain::P), None);
    }

    #[test]
    fn derive_ws_url_maps_schemes() {
        assert_eq!(derive_ws_url("https://h:1").unwrap(), "wss://h:1");
        assert_eq!(derive_ws_url("http://h:1").unwrap(), "ws://h:1");
        assert_eq!(derive_ws_url("wss://h:1").unwrap(), "wss://h:1");
        assert!(derive_ws_url("h:1").is_err());
    }
}
