mod bulk;
mod chain;
mod config;
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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde_json::Value;
use tokio::sync::{Notify, broadcast};
use tracing::{info, warn};

use crate::chain::{Chain, IngestCfg, LogsSource};
use crate::config::{ChainCfg, Cli, EXAMPLE_CONFIG, Notice, PrintMode, Upstream, UpstreamKind};
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
///
/// Everything chain-specific arrives in `chain_cfg`, already merged from file,
/// environment and command line. `upstream` supplies only what is shared by the
/// whole host — the token-bearing base URL, and the one pacer that holds every
/// chain to the endpoint's per-IP budget between them.
async fn build_instance(
    chain_cfg: &ChainCfg,
    upstream: &Upstream,
    http: &reqwest::Client,
    fatal: Arc<Notify>,
) -> Result<Instance> {
    let chain = chain_cfg.chain;
    let identity = fetch_identity(chain, http, &chain_cfg.rpc_url, chain_cfg.max_wait).await?;
    info!(
        chain = chain.as_str(),
        identity = %identity,
        rpc_url = %redact_url(&chain_cfg.rpc_url),
        "queried upstream network identity",
    );

    let data_dir = chain_cfg.data_dir.clone();
    std::fs::create_dir_all(&data_dir)?;
    let anchor_floor = resolve_anchor_floor(http, chain_cfg, upstream).await;
    let storage = Storage::open(&data_dir, chain, &identity, anchor_floor)?;
    info!(
        chain = chain.as_str(),
        path = %data_dir.display(),
        high_water = storage.high_water().await,
        "storage opened",
    );

    // Live fan-out for subscriptions.
    let (blocks, _) = broadcast::channel(subscribe::LIVE_CHANNEL_CAP);
    let behind_tip = Arc::new(AtomicU64::new(0));
    let logs_source = resolve_logs_source(chain_cfg, http).await;
    let cfg = ingest_cfg(chain_cfg, blocks.clone(), anchor_floor, fatal, logs_source);
    info!(
        chain = chain.as_str(),
        max_wait_secs = cfg.max_wait.as_secs(),
        ws_idle_timeout_secs = cfg.ws_idle_timeout.as_secs(),
        request_interval_ms = cfg.backfill_inter_fetch.as_millis(),
        fetch_concurrency = cfg.fetch_concurrency,
        summary_period_secs = cfg.progress_period.as_secs(),
        ingest_logs = cfg.ingest_logs,
        ws_url = %redact_url(&cfg.ws_url),
        rpc_url = %redact_url(&cfg.rpc_url),
        "ingest config",
    );

    // Live join buffer, only when this chain's derived-data ingestion is on —
    // `cfg.ingest_logs`, which has already narrowed the setting to the chains
    // that have logs at all. Block reads consult the buffer so an in-flight tip
    // record (buffered while its second half is fetched) is serveable from
    // memory; a periodic tick refreshes its gauges.
    let join = cfg
        .ingest_logs
        .then(|| JoinBuffer::new(storage.clone(), chain_cfg.join_buffer_cap));
    if let Some(buf) = join.clone() {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                buf.sample();
            }
        });
    }

    Ok(Instance {
        serve: ChainServe {
            chain,
            storage,
            data_dir,
            identity,
            behind_tip,
            blocks,
            join,
            ingests_logs: cfg.ingest_logs,
        },
        cfg,
    })
}

/// Assemble one chain's ingest knobs from its resolved configuration.
///
/// `fatal` is the one field deliberately not copied from a config key: it is
/// shared across chains, so any chain's unrecoverable condition shuts the whole
/// process down rather than leaving a half-serving mirror. `host_pacer` is one
/// object shared by every chain reading from the upstream host, because that
/// endpoint's rate limit is per-IP for the whole host and a budget held per
/// chain would be exceeded by their sum (see `IngestCfg::pace`).
fn ingest_cfg(
    chain_cfg: &ChainCfg,
    blocks: subscribe::LiveTx,
    backfill_floor: Option<u64>,
    fatal: Arc<Notify>,
    logs_source: LogsSource,
) -> IngestCfg {
    IngestCfg {
        chain: chain_cfg.chain,
        max_wait: chain_cfg.max_wait,
        pacer: Arc::new(Pacer::new(chain_cfg.request_interval)),
        host_pacer: chain_cfg.host_pacer.clone(),
        fetch_concurrency: chain_cfg.concurrency,
        ws_idle_timeout: chain_cfg.ws_idle_timeout,
        ws_url: chain_cfg.ws_url.clone(),
        rpc_url: chain_cfg.rpc_url.clone(),
        blocks,
        poll_interval: chain_cfg.poll_interval,
        subscribe_blocks: chain_cfg.subscribe_blocks,
        backfill_inter_fetch: chain_cfg.request_interval,
        backfill_floor,
        prefetch_delay_cap: chain_cfg.prefetch_delay_cap,
        // One knob paces both operator-visible lines; see `ChainCfg`.
        progress_period: chain_cfg.summary_period,
        fatal,
        bootstrap_done: Arc::new(Notify::new()),
        ingest_logs: chain_cfg.ingest_logs,
        logs_source,
    }
}

/// Settle where this chain's logs will come from, and say so once at startup.
///
/// Probed rather than configured because it is a property of the endpoint, not
/// a preference — and the endpoint is expected to change under one deployment:
/// a from-genesis fill runs against an operator's own node, which serves
/// `eth_getBlockReceipts`, and steady state often runs against the public one,
/// which answers `-32601`. Repointing `rpc_url` is then the only step.
///
/// Not fatal when the method is missing: `eth_getLogs` serves the same logs
/// more slowly, and the identity handshake has already proven the endpoint is
/// alive. Skipped entirely when this chain isn't ingesting logs, so a P-chain
/// or a blocks-only C-chain spends no request on it.
async fn resolve_logs_source(chain_cfg: &ChainCfg, http: &reqwest::Client) -> LogsSource {
    if !chain_cfg.ingest_logs {
        return LogsSource::GetLogs;
    }
    match eth::ingest::probe_logs_source(http, &chain_cfg.rpc_url).await {
        (LogsSource::Receipts, _) => {
            info!(
                chain = chain_cfg.chain.as_str(),
                "upstream serves eth_getBlockReceipts; fetching logs per block alongside the block",
            );
            LogsSource::Receipts
        }
        (LogsSource::GetLogs, reason) => {
            warn!(
                chain = chain_cfg.chain.as_str(),
                reason = reason.as_deref().unwrap_or("unknown"),
                "upstream does not serve eth_getBlockReceipts; falling back to eth_getLogs. \
                 Backfill will be slower: logs come one range window per 2048 blocks, which \
                 cannot overlap the block fetches and returns a response that grows with log \
                 density. Point rpc_url at a node that serves eth_getBlockReceipts to avoid it",
            );
            LogsSource::GetLogs
        }
    }
}

/// Query the upstream for the opaque fingerprint that binds a store to one
/// network. The C-chain's is `eth_chainId` in decimal, which is the spelling
/// C-chain stores in the field carry, so it has to stay exactly that.
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
///
/// The `match` below stays: it picks between genuinely different ingest
/// *dialects*, not between two spellings of one knob.
fn spawn_pipeline(
    inst: &Instance,
    http: &reqwest::Client,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
    let Instance { serve, cfg } = inst;
    let backfill_count = Arc::new(AtomicU64::new(0));
    tokio::spawn(summary_loop(
        serve.storage.clone(),
        // Per chain, like every other knob: `progress_period` is this chain's
        // `summary_period`, which paces the backfill line off the same clock.
        cfg.progress_period,
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
    // Ahead of `resolve`, so a config file that fails to parse cannot block the
    // one command whose whole job is to show a file that parses.
    if cli.print_config_example {
        print!("{EXAMPLE_CONFIG}");
        return Ok(());
    }
    let resolved = cli.resolve()?;
    // Also ahead of `init_tracing`, because the subscriber writes to stdout: an
    // INFO line interleaved into the output would make
    // `neve --print-config > config.toml` write a file that doesn't parse. The
    // deferred notices still have to reach the operator — `--print-config` is
    // precisely where one looks to find out which settings are deprecated — so
    // they go to stderr by hand instead.
    if let Some(mode) = resolved.print {
        for notice in &resolved.notices {
            match notice {
                Notice::Info(m) => eprintln!("note: {m}"),
                Notice::Warn(m) => eprintln!("warning: {m}"),
            }
        }
        match mode {
            PrintMode::Example => print!("{EXAMPLE_CONFIG}"),
            PrintMode::Config => print!("{}", resolved.to_redacted_toml()?),
        }
        return Ok(());
    }

    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("install rustls crypto provider"))?;
    init_tracing(resolved.log_level.as_str());
    // Resolution runs before there is a subscriber — it is what decides the log
    // level — so its deprecation warnings and the effective host rate cap were
    // held back until now.
    resolved.emit_notices();

    // Install the global metrics recorder before anything records; the handle
    // renders the `/metrics` payload and drives periodic upkeep.
    let metrics_handle = metrics::install()?;
    spawn_metrics_upkeep(metrics_handle.clone());

    let http = reqwest::Client::builder().user_agent(USER_AGENT).build()?;
    // Shared across chains: whichever pipeline hits an unrecoverable upstream
    // condition takes the process down, rather than leaving a half-serving mirror.
    let fatal = Arc::new(Notify::new());

    info!(
        chains = %resolved.chains.keys().copied().map(Chain::as_str).collect::<Vec<_>>().join(","),
        network = resolved.network.as_str(),
        upstream = %redact_url(&resolved.upstream.base),
        "starting",
    );
    let mut instances = Vec::with_capacity(resolved.chains.len());
    for chain_cfg in resolved.chains.values() {
        instances.push(build_instance(chain_cfg, &resolved.upstream, &http, fatal.clone()).await?);
    }

    let serve_cfg = rpc::ServeConfig {
        addr: resolved.server.addr,
        max_connections: resolved.server.max_connections,
        idle_timeout: resolved.server.idle_timeout,
        max_blocks_per_request: resolved.server.max_blocks_per_request,
    };
    let _rpc_handle = rpc::serve(
        serve_cfg,
        instances.iter().map(|i| i.serve.clone()).collect(),
        metrics_handle,
    )
    .await?;

    let ingest_futs: Vec<_> = instances
        .iter()
        .map(|inst| spawn_pipeline(inst, &http))
        .collect();
    let stores: Vec<Storage> = instances.iter().map(|i| i.serve.storage.clone()).collect();

    if let Some(stop) = resolved.stop_time {
        info!(?stop, "stop-time set, will exit after this duration");
    }
    // Box::pin: this future transitively holds the large per-chain `ingest` state
    // machines.
    Box::pin(run_until_shutdown(
        futures_util::future::try_join_all(ingest_futs),
        fatal,
        resolved.stop_time,
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
/// 1. A configured floor (`chains.<x>.backfill_floor`, which for the P-chain
///    defaults to genesis). Wins even against a neve upstream.
/// 2. A `"tip"` floor against a neve upstream: that chain's earliest retained
///    block, learned from the upstream's `/health`, so backfill reproduces the
///    whole upstream range rather than only growing forward from its tip.
///
/// A `"tip"` floor against anything else is the plain forward-only anchor, which
/// is what a `None` return means here.
///
/// In every case an *existing* store already has its floor baked in at
/// creation, so we skip the work and return `None` (resume as-is). Neither the
/// setting nor the probe can lower the floor of a store that already exists.
async fn resolve_anchor_floor(
    http: &reqwest::Client,
    chain_cfg: &ChainCfg,
    upstream: &Upstream,
) -> Option<u64> {
    let chain = chain_cfg.chain;
    let store_exists = chain_cfg
        .data_dir
        .join("blocks")
        .join("blockdb.idx")
        .exists();

    // An explicit floor is the most specific intent and applies against any
    // upstream, including the public endpoint.
    if let Some(floor) = chain_cfg.backfill_floor {
        if store_exists {
            info!(
                chain = chain.as_str(),
                floor,
                "configured backfill floor ignored: store already exists, resuming with its baked-in floor",
            );
            return None;
        }
        info!(chain = chain.as_str(), floor, "anchoring backfill floor");
        return Some(floor);
    }

    // No floor set. Against another neve that is not "start at the tip" but
    // "start where the upstream starts" — the probe below.
    if upstream.kind != UpstreamKind::Neve {
        return None;
    }
    if store_exists {
        info!(
            chain = chain.as_str(),
            "mirror: local store already exists, resuming with its anchored floor",
        );
        return None;
    }
    // `upstream.base` rather than the chain's `rpc_url`: `/health` is a path on
    // the mirrored neve, and the per-chain URL may carry a query the path would
    // land behind.
    match fetch_upstream_min_height(http, &upstream.base, chain).await {
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
