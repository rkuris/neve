//! Configuration: a TOML file, the environment, and the command line, merged
//! into one `Resolved` value that the rest of the process reads.
//!
//! # Why there is a file at all
//!
//! neve's knobs are almost all *per chain*, and there is no upper bound on how
//! many chains it mirrors. Keying them by chain — `[chains.c]`, `[chains.p]` —
//! is what keeps the surface flat as chains are added, and what lets a value be
//! stated once in `[defaults]` and overridden where it differs.
//!
//! # The layers, lowest precedence first
//!
//! 1. **Built-in per-chain defaults** (`Builtin::for_chain`).
//! 2. **`[defaults]`** — one table applying to every enabled chain.
//! 3. **`[chains.<x>]`** — that chain's own table.
//! 4. **Environment** — `NEVE_UPSTREAM_TOKEN`, plus the deprecated
//!    `NEVE_RPC_URL` / `NEVE_WS_URL` / `NEVE_P_RPC_URL`.
//! 5. **Command line** — the visible flags, the hidden deprecated aliases, and
//!    `--set <dotted.key>=<value>` last of all.
//!
//! Layers 4 and 5 are applied by *mutating the parsed `toml::Value` tree* before
//! it is deserialized, rather than by a second merge pass over typed structs.
//! That is what gives `--set` its key validation for free: every override lands
//! in the same document the file did, and the same `deny_unknown_fields`
//! deserialize that rejects a typo in the file rejects a typo in a `--set`. The
//! document is re-deserialized after each individual override, so the error can
//! name the flag or the `--set` argument that introduced it rather than
//! surfacing anonymously at the end.
//!
//! # Secrets
//!
//! The upstream rate-limit bypass token is a `Token`, whose `Debug` *and*
//! `Display` render `<redacted>`. It is appended to every upstream URL, so
//! `--print-config` runs those URLs through `crate::upstream::redact_url` on the
//! way out. Nothing in this module ever formats the token itself.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize, de};
use toml::{Table, Value};
use tracing::{info, warn};

use crate::chain::{Chain, Network, normalize_chains};
use crate::upstream::{Pacer, redact_url};

/// Config file consulted when neither `--config` nor `NEVE_CONFIG` names one.
///
/// Read only if it exists — a missing file here is not an error, whereas a
/// missing *explicitly requested* file is. That asymmetry is deliberate: a
/// packaged install drops its config here and needs no flag, while a dev run in
/// a checkout must not silently inherit the host's production settings simply
/// because the daemon happens to be installed on the same machine.
const DEFAULT_CONFIG_PATH: &str = "/etc/neve/config.toml";

/// Every environment variable this module reads. Listed so `Env::from_process`
/// snapshots exactly these and tests can supply the same set by hand — reading
/// the live environment from a test is both racy under `cargo test`'s thread
/// pool and, since edition 2024, `unsafe` to write.
const ENV_VARS: &[&str] = &[
    "NEVE_CONFIG",
    "NEVE_UPSTREAM_TOKEN",
    "NEVE_RPC_URL",
    "NEVE_WS_URL",
    "NEVE_P_RPC_URL",
];

/// Host-wide request cap applied to an untokened public endpoint, in req/s.
///
/// The public endpoint's rate limit is enforced **per IP for the whole host**,
/// not per chain path, so this budget is shared by every chain in the process
/// (see `Upstream::host_pacer`).
const PUBLIC_MAX_RPS: f64 = 25.0;

const CLI_EXAMPLES: &str = "\
EXAMPLES:
  # Dev quick start — use the permissive testnet endpoints.
  neve --network testnet

  # Bounded test run, debug logging, custom data dir.
  neve --network testnet --stop-time 30 --log-level debug --data-dir /tmp/bs

  # Run from a config file, and see what it actually resolved to.
  neve --config /etc/neve/config.toml --print-config

  # Every key, its built-in default, and the reasoning behind it. A reference,
  # not a starting point: a real config file sets only what it changes.
  neve --print-config-example

  # P-chain only, whole chain from genesis (its default floor). Against the
  # public endpoint this is slow on purpose; point chains.p.rpc_url at your own
  # node and set chains.p.request_interval to 0 to fill it quickly.
  neve --chains p

  # Override any config key ad hoc; the key is validated like a file key.
  neve --set chains.c.backfill_floor=0 --set upstream.max_rps=50

  # Mirror another neve instance: one URL yields RPC, WebSocket, and /health.
  neve --mirror-from http://10.0.0.5:8545
";

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// The upstream rate-limit bypass token.
///
/// A newtype rather than a `String` because a token has exactly one legitimate
/// destination — the query string of an upstream request — and a great many
/// illegitimate ones. `Debug` and `Display` both render `<redacted>`, so no
/// `{}`, `{:?}`, `info!(token)`, or panic message can leak it; the value comes
/// out only through `expose`, which is named to be greppable.
#[derive(Clone, PartialEq, Eq)]
pub struct Token(String);

impl Token {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The token in the clear. Every call site is a place to check.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

// ---------------------------------------------------------------------------
// Small shared enums
// ---------------------------------------------------------------------------

/// Logging verbosity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[clap(rename_all = "lower")]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// What kind of server the upstream is, which decides how per-chain endpoints
/// are derived from `upstream.base` and what the polite defaults are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UpstreamKind {
    /// An avalanchego node (or the public endpoint in front of a fleet of them).
    /// Chains are distinguished by URL path, so one `base` re-points them all.
    #[default]
    Avalanchego,
    /// Another neve instance. It serves JSON-RPC, the WebSocket, and `/health`
    /// on one socket for every chain it mirrors, so `base` *is* the endpoint and
    /// chains are told apart by method namespace rather than by path.
    Neve,
}

/// What `--print-config` / `--print-config-example` asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrintMode {
    /// The fully-resolved configuration, as TOML, secrets redacted.
    Config,
    /// The annotated template (`EXAMPLE_CONFIG`).
    Example,
}

/// A startup log line held back until the caller has initialized tracing.
///
/// `resolve` runs *before* `init_tracing` — it is what decides the log level —
/// so anything it logs directly would be swallowed by the absent subscriber.
/// Deprecation warnings and the effective host rate cap are exactly the lines an
/// operator must see, so they ride out here and are emitted by `emit_notices`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notice {
    Info(String),
    Warn(String),
}

// ---------------------------------------------------------------------------
// The resolved configuration
// ---------------------------------------------------------------------------

/// Everything the process needs, with every layer already merged and validated.
#[derive(Debug)]
pub struct Resolved {
    pub network: Network,
    pub log_level: LogLevel,
    pub stop_time: Option<Duration>,
    pub upstream: Upstream,
    pub server: Server,
    /// Enabled chains only, in `Chain` order so startup logs, `/health`, and
    /// instance construction all agree on a deterministic sequence.
    pub chains: BTreeMap<Chain, ChainCfg>,
    pub print: Option<PrintMode>,
    /// Deferred startup log lines; see `Notice` and `emit_notices`.
    pub notices: Vec<Notice>,
}

/// The upstream this instance mirrors, shared by every chain.
///
/// `Debug` is hand-written: `base` may itself carry a `?token=…` query, so the
/// `Token` newtype alone would not stop a `{:?}` from spelling the credential
/// out. See `ChainCfg` for the same reasoning applied to the derived URLs.
#[derive(Clone)]
pub struct Upstream {
    pub kind: UpstreamKind,
    /// Origin (avalanchego) or full endpoint (neve), with any trailing `/`
    /// trimmed. Per-chain URLs are derived from it unless overridden.
    pub base: String,
    pub token: Option<Token>,
    /// Query parameter the token rides in. `token` on the public endpoint.
    pub token_param: String,
    /// Where the token was read from, for `--print-config`. `None` when the
    /// token came from `NEVE_UPSTREAM_TOKEN` or there is no token.
    pub token_file: Option<PathBuf>,
    /// Effective host-wide request cap in req/s, or `None` for uncapped.
    pub max_rps: Option<f64>,
    /// **One** pacer for the whole host, shared by every chain.
    ///
    /// The public endpoint's rate limit is per-IP for the entire host rather
    /// than per chain path: a hard P-chain backfill will throttle a C-chain
    /// instance at the same address. Per-chain pacers therefore cannot express
    /// the limit — two chains each politely holding to 25 req/s jointly issue
    /// 50. A fetch waits on this pacer *and* on its chain's own
    /// `request_interval` pacer, so the host cap bounds the sum while the
    /// per-chain interval still bounds each chain individually.
    pub host_pacer: Option<Arc<Pacer>>,
}

impl fmt::Debug for Upstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upstream")
            .field("kind", &self.kind)
            .field("base", &redact_url(&self.base))
            .field("token", &self.token)
            .field("token_param", &self.token_param)
            .field("token_file", &self.token_file)
            .field("max_rps", &self.max_rps)
            .field("host_pacer", &self.host_pacer)
            .finish()
    }
}

/// The one serving socket, shared by every chain.
#[derive(Debug, Clone)]
pub struct Server {
    pub addr: SocketAddr,
    pub max_connections: u32,
    /// `None` when configured to `0`, which disables the connection reaper
    /// entirely (an `Option` rather than a magic-zero `Duration` past this
    /// boundary).
    pub idle_timeout: Option<Duration>,
    pub max_blocks_per_request: u64,
}

/// One chain's fully-resolved knobs.
///
/// The prose justifying each default lives on the corresponding `ChainFile`
/// field, which is the key an operator actually writes; this struct is the
/// merged result.
///
/// `Debug` is hand-written for one reason: by this point the token has been
/// appended to `rpc_url` and `ws_url`, so a derived `Debug` would undo the
/// `Token` newtype the moment anything dumped a config. `crate::chain::IngestCfg`
/// redacts the same two fields for the same reason.
#[derive(Clone)]
pub struct ChainCfg {
    pub chain: Chain,
    /// Upstream JSON-RPC endpoint, token already appended.
    pub rpc_url: String,
    /// Upstream WebSocket endpoint with the token appended, or empty for a chain
    /// with no upstream push mechanism (the P-chain, against avalanchego).
    pub ws_url: String,
    pub data_dir: PathBuf,
    /// `None` is the `"tip"` setting: anchor at the first live block and fill
    /// forward only.
    ///
    /// In `kind = "neve"` mode a `None` here additionally means "probe the
    /// upstream's `/health` for its earliest retained block and anchor there" —
    /// the mirror's floor belongs to the upstream's retained range, not to a
    /// built-in default. An explicit floor still wins over that probe.
    pub backfill_floor: Option<u64>,
    pub request_interval: Duration,
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub max_wait: Duration,
    pub ws_idle_timeout: Duration,
    pub prefetch_delay_cap: Duration,
    pub ingest_logs: bool,
    pub join_buffer_cap: usize,
    pub summary_period: Duration,
    /// Subscribe to `newBlocks` (whole block, no follow-up fetch) instead of
    /// `newHeads` (header, then fetch). True exactly when the upstream is
    /// another neve, which is the only server that offers the extension.
    pub subscribe_blocks: bool,
    /// The `upstream.max_rps` pacer, shared with every other chain reading from
    /// the same host, or `None` when this chain reads from somewhere else (or
    /// when there is no cap). See `host_pacer_for`.
    pub host_pacer: Option<Arc<Pacer>>,
}

impl fmt::Debug for ChainCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChainCfg")
            .field("chain", &self.chain)
            .field("rpc_url", &redact_url(&self.rpc_url))
            .field("ws_url", &redact_url(&self.ws_url))
            .field("data_dir", &self.data_dir)
            .field("backfill_floor", &self.backfill_floor)
            .field("request_interval", &self.request_interval)
            .field("concurrency", &self.concurrency)
            .field("poll_interval", &self.poll_interval)
            .field("max_wait", &self.max_wait)
            .field("ws_idle_timeout", &self.ws_idle_timeout)
            .field("prefetch_delay_cap", &self.prefetch_delay_cap)
            .field("ingest_logs", &self.ingest_logs)
            .field("join_buffer_cap", &self.join_buffer_cap)
            .field("summary_period", &self.summary_period)
            .field("subscribe_blocks", &self.subscribe_blocks)
            .field("host_paced", &self.host_pacer.is_some())
            .finish()
    }
}

impl Resolved {
    /// Emit the deferred startup lines. Call once, immediately after tracing is
    /// initialized — before that, these events have nowhere to go.
    pub fn emit_notices(&self) {
        for notice in &self.notices {
            match notice {
                Notice::Info(m) => info!("{m}"),
                Notice::Warn(m) => warn!("{m}"),
            }
        }
    }

    /// The whole resolved configuration as TOML, in the shape of the file that
    /// would produce it, with secrets redacted (`--print-config`).
    ///
    /// The output stays a *parseable* file: the one key the schema would reject
    /// — the placeholder standing in for a token that came from the environment
    /// — is emitted commented out. What it does not do is round-trip to the same
    /// run, because every URL has its query string replaced: the token is
    /// appended there, and printing URLs verbatim would defeat the `Token`
    /// newtype at the last step.
    pub fn to_redacted_toml(&self) -> Result<String> {
        let mut out = Table::new();
        out.insert("network".to_owned(), value_of(&self.network)?);
        out.insert("log_level".to_owned(), value_of(&self.log_level)?);
        out.insert("upstream".to_owned(), value_of(&self.redacted_upstream())?);
        out.insert("server".to_owned(), value_of(&self.redacted_server())?);

        let mut chains = Table::new();
        for (chain, cfg) in &self.chains {
            chains.insert(
                chain.as_str().to_owned(),
                value_of(&RedactedChain::of(cfg))?,
            );
        }
        out.insert("chains".to_owned(), Value::Table(chains));
        let rendered = toml::to_string_pretty(&out).context("render resolved config as TOML")?;
        // `token` is not a key the schema accepts — the real one never reaches
        // this function — so comment the placeholder out rather than emit a file
        // that cannot be fed back in.
        Ok(rendered.replace("\ntoken = \"", "\n#token = \""))
    }

    fn redacted_upstream(&self) -> RedactedUpstream {
        let up = &self.upstream;
        RedactedUpstream {
            kind: up.kind,
            base: redact_url(&up.base).into_owned(),
            token_param: up.token_param.clone(),
            max_rps: up.max_rps,
            token_file: up
                .token_file
                .as_ref()
                .map(|p| p.display().to_string())
                .map(Value::String),
            // A token with no file behind it came from the environment; say so
            // without saying what it is.
            token: (up.token.is_some() && up.token_file.is_none())
                .then(|| "<redacted, from NEVE_UPSTREAM_TOKEN>".to_owned()),
        }
    }

    fn redacted_server(&self) -> RedactedServer {
        let srv = &self.server;
        RedactedServer {
            addr: srv.addr.to_string(),
            max_connections: srv.max_connections,
            idle_timeout: fmt_duration(srv.idle_timeout.unwrap_or(Duration::ZERO)),
            max_blocks_per_request: srv.max_blocks_per_request,
        }
    }
}

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

/// The command line.
///
/// Only knobs a human genuinely types ad hoc are visible; everything else lives
/// in the config file and is reachable from the command line through `--set`,
/// which validates its key against the same schema the file is parsed with. The
/// hidden flags at the bottom are deprecated aliases, kept working for one
/// release so a deployment can migrate its unit file separately from its binary.
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
pub struct Cli {
    /// Configuration file (TOML). Defaults to `/etc/neve/config.toml` when that
    /// exists; a path given here (or in `NEVE_CONFIG`) must exist. Every key in
    /// it is optional — `--print-config-example` writes an annotated template.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Which Avalanche network to target. Picks the default upstream host and
    /// the default `--data-dir`, for every selected chain. Testnet has much more
    /// permissive rate limits and is recommended for dev work.
    #[arg(long, value_enum)]
    pub network: Option<Network>,

    /// Base directory holding each chain's blockstore + fjall index. Created if
    /// missing. Defaults to `./blockstore-data-<network>` so swapping networks
    /// doesn't cross-pollinate stores. The C-chain store sits at this path
    /// directly, which is where C-chain stores in the field live; other chains
    /// get a subdirectory — the P-chain's is `<data-dir>/p`, overridable with
    /// `chains.p.data_dir`. Each store is
    /// stamped with its chain and network on first open and verified on every
    /// open.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Socket address for the JSON-RPC server. One socket serves every selected
    /// chain. Config key: `server.addr`.
    #[arg(long, value_name = "ADDR")]
    pub rpc_addr: Option<SocketAddr>,

    /// Which chains to mirror, comma-separated: `c` (EVM C-chain), `p`
    /// (platform P-chain), or `c,p` for both in one process. Each chain gets its
    /// own store, its own upstream connection, and its own `chain=` metric
    /// label; they share one listening socket, and a request picks its chain by
    /// method namespace (`eth_*` vs `platform.*`).
    ///
    /// This is a *selector*, not a definition: it restricts the run to the
    /// chains listed, and any of them the config file doesn't mention runs on
    /// pure defaults. Without it, the enabled set is whatever `[chains.<x>]`
    /// tables the file declares — or both chains, if it declares none.
    #[arg(long, value_name = "LIST", value_enum, value_delimiter = ',')]
    pub chains: Vec<Chain>,

    /// Mirror another neve instance from a single endpoint. neve serves
    /// JSON-RPC, the WebSocket, and `/health` on one socket, so this one URL
    /// yields all three: the WS and RPC endpoints are derived from it
    /// (`http`→`ws`, `https`→`wss`) for every chain. When the local store is
    /// empty, the upstream's `/health` is queried for its earliest retained
    /// block and the store is anchored there so backfill reproduces the
    /// upstream's whole retained range (not just forward from the tip).
    /// Backfill runs unthrottled in this mode — there's no public-endpoint rate
    /// limit to be polite to. Sugar for `upstream.kind = "neve"` plus
    /// `upstream.base = <URL>`. Example: `--mirror-from http://10.0.0.5:8545`.
    #[arg(long, value_name = "URL")]
    pub mirror_from: Option<String>,

    /// Stop after the given duration (e.g. `30s`, `5m`, `1h`). A bare integer
    /// means seconds. Useful for short test runs; this is the *only* correct way
    /// to bound one, because the exit path fsyncs the fjall journal and
    /// checkpoints the blockstore.
    #[arg(long, value_name = "DUR", value_parser = parse_human_duration)]
    pub stop_time: Option<Duration>,

    /// Logging verbosity. Overridden by `RUST_LOG` if set.
    #[arg(long, value_enum)]
    pub log_level: Option<LogLevel>,

    /// Ingest event logs alongside blocks, on every enabled chain that has them.
    /// Config key: `defaults.ingest_logs` (or per chain).
    #[arg(long)]
    pub ingest_logs: bool,

    /// Override one config key: `--set chains.p.request_interval=0`. Repeatable.
    /// The key is a dotted path into the config file and is validated against
    /// the same schema, so a typo is a startup error naming the key rather than
    /// a setting that silently does nothing. The value is parsed as a TOML
    /// scalar (`12`, `true`, `1.5`, `[1, 2]`) and otherwise taken as a string,
    /// so `--set upstream.base=https://h` needs no quoting.
    #[arg(long, value_name = "KEY=VALUE")]
    pub set: Vec<String>,

    /// Print the fully-resolved configuration as TOML (secrets redacted) and
    /// exit. The answer to "what is this process actually running with".
    #[arg(long)]
    pub print_config: bool,

    /// Print an annotated example configuration file and exit.
    #[arg(long, conflicts_with = "print_config")]
    pub print_config_example: bool,

    // -----------------------------------------------------------------------
    // Deprecated aliases. Each maps onto a config key and warns once when used;
    // all of them go away next release.
    // -----------------------------------------------------------------------
    /// Deprecated: `chains.c.ws_url`.
    #[arg(long, hide = true)]
    pub ws_url: Option<String>,

    /// Deprecated: `chains.c.rpc_url`.
    #[arg(long, hide = true)]
    pub rpc_url: Option<String>,

    /// Deprecated: `chains.p.rpc_url`.
    #[arg(long, hide = true)]
    pub p_rpc_url: Option<String>,

    /// Deprecated: `chains.p.poll_interval`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub p_poll_interval: Option<String>,

    /// Deprecated: `chains.c.request_interval`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub request_interval: Option<String>,

    /// Deprecated: `chains.p.request_interval`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub p_request_interval: Option<String>,

    /// Deprecated: `chains.p.concurrency`.
    #[arg(long, hide = true)]
    pub p_concurrency: Option<usize>,

    /// Deprecated: `chains.p.data_dir`.
    #[arg(long, hide = true, value_name = "PATH")]
    pub p_data_dir: Option<PathBuf>,

    /// Deprecated: `chains.c.backfill_floor`.
    #[arg(long, hide = true, value_name = "HEIGHT")]
    pub backfill_floor: Option<u64>,

    /// Deprecated: `chains.p.backfill_floor`.
    #[arg(long, hide = true, value_name = "HEIGHT")]
    pub p_backfill_floor: Option<u64>,

    /// Deprecated: `defaults.max_wait`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub max_wait: Option<String>,

    /// Deprecated: `defaults.ws_idle_timeout`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub ws_idle_timeout: Option<String>,

    /// Deprecated: `defaults.prefetch_delay_cap`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub prefetch_delay_cap: Option<String>,

    /// Deprecated: `server.idle_timeout`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub idle_timeout: Option<String>,

    /// Deprecated: `server.max_connections`.
    #[arg(long, hide = true)]
    pub max_connections: Option<u32>,

    /// Deprecated: `server.max_blocks_per_request`.
    #[arg(long, hide = true)]
    pub max_blocks_per_request: Option<u64>,

    /// Deprecated: `defaults.summary_period`.
    #[arg(long, hide = true, value_name = "DUR")]
    pub summary_period: Option<String>,

    /// Deprecated: `defaults.join_buffer_cap`.
    #[arg(long, hide = true)]
    pub join_buffer_cap: Option<usize>,
}

// ---------------------------------------------------------------------------
// The file schema
// ---------------------------------------------------------------------------

/// The config file, verbatim. Every field is optional, and unknown keys are
/// rejected: silently ignoring a misspelled key is how a deployment ends up
/// running with a setting the operator believes they changed.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    network: Option<Network>,
    log_level: Option<LogLevel>,
    upstream: Option<UpstreamFile>,
    server: Option<ServerFile>,
    /// Per-chain keys applying to every enabled chain.
    defaults: Option<ChainFile>,
    /// The presence of a `[chains.<x>]` table enables chain `<x>`. With no
    /// `[chains]` table at all, every chain neve knows about is enabled.
    chains: Option<BTreeMap<Chain, ChainFile>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpstreamFile {
    /// `avalanchego` (default) or `neve`. Decides how per-chain URLs are derived
    /// from `base`, and shifts several defaults: a neve upstream has no public
    /// rate limit (`request_interval` defaults to 0), serves the `newBlocks`
    /// extension (so the live path skips the per-block fetch), and reports its
    /// earliest retained block on `/health` (which anchors a fresh store).
    kind: Option<UpstreamKind>,

    /// Upstream origin, e.g. `https://api.avax.network`, defaulting to the
    /// public endpoint for the configured `network`. For `kind = "avalanchego"`
    /// each chain's path is appended to it; for `kind = "neve"` it is the
    /// endpoint itself, for every chain.
    ///
    /// **Prefer `token_file` to embedding a `?token=…` here.** Either works —
    /// the token is appended only if the URL does not already carry the
    /// parameter — but a token in the file is a token in every backup of the
    /// file, and a token passed as `--set upstream.base=…` is world-readable
    /// through `/proc/<pid>/cmdline`.
    base: Option<String>,

    /// File holding the upstream rate-limit bypass token, whitespace trimmed.
    /// Takes precedence over `NEVE_UPSTREAM_TOKEN`. The token is appended to
    /// every upstream URL as `?<token_param>=<token>`.
    token_file: Option<PathBuf>,

    /// Query parameter the token rides in. Default `token`.
    token_param: Option<String>,

    /// Host-wide cap on upstream requests per second, shared by **every** chain.
    ///
    /// The public endpoint's rate limit is per-IP for the whole host rather than
    /// per chain path, so this is the knob that actually corresponds to it; the
    /// per-chain `request_interval` bounds one chain in isolation. Defaults to
    /// 25 req/s against an untokened public endpoint, and to no cap when a token
    /// is configured (bypassing the limit is the reason to have one) or when the
    /// upstream is another neve. `0` also means no cap.
    max_rps: Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerFile {
    /// Socket address for the JSON-RPC server. One socket serves every selected
    /// chain. Default `127.0.0.1:8545`.
    addr: Option<SocketAddr>,

    /// Maximum concurrent JSON-RPC connections. Excess connections are rejected
    /// with HTTP 429. jsonrpsee's own default is only 100, which a public /
    /// wallet-facing endpoint blows past easily; neve's default is 1024.
    max_connections: Option<u32>,

    /// Close a JSON-RPC connection that has had no read or write activity for
    /// this long (e.g. `60s`, `2m`). Defends against slowloris and the leaked
    /// idle-keep-alive fd growth jsonrpsee can't reap on its own. `0` disables
    /// the reaping entirely (connections may then linger until
    /// `max_connections`). Default `60s`.
    idle_timeout: Option<HumanDuration>,

    /// Maximum number of blocks a single `GET /blocks?from=&to=` bulk-export
    /// request may return; larger ranges are rejected with HTTP 400. Split a
    /// bigger download into successive windows. `?chain=` picks which chain's
    /// store to export from. Default 10000.
    max_blocks_per_request: Option<u64>,
}

/// The per-chain knobs. Every one of these is valid in both `[defaults]` and
/// `[chains.<x>]`; the latter wins.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainFile {
    /// Whether to serve this chain. Defaults to `true`, so a `[chains.<x>]`
    /// table is on by definition.
    ///
    /// There are three ways to turn a chain off, for three situations. Omitting
    /// its table is the plain one: once a `[chains]` table exists it is the whole
    /// set, so a file naming only `[chains.c]` serves only the C-chain. Setting
    /// `enabled = false` turns a chain off while keeping its settings, which is
    /// what you want when the block is tuned and you would rather not retype it.
    /// `--chains <list>` overrides both for one run — a chain named there is
    /// served even if the file disables it, because a command-line selector that
    /// a config file could veto would be useless in an incident.
    ///
    /// Only valid in `[chains.<x>]`. In `[defaults]` it would have to mean
    /// "disable everything", which is what not running neve means.
    enabled: Option<bool>,

    /// HTTPS JSON-RPC endpoint for this chain, overriding the URL derived from
    /// `upstream.base`. The C-chain's default is `<base>/ext/bc/C/rpc`; the
    /// P-chain's (`platform.*`) is `<base>/ext/bc/P`.
    ///
    /// **If the URL carries a rate-limit bypass token** (`?token=…`), put the
    /// token in `upstream.token_file` instead of writing it here, and never pass
    /// it as a command-line flag: command-line arguments are world-readable
    /// through `/proc/<pid>/cmdline`, so a token passed as a flag is visible to
    /// every local user. neve redacts URL query strings from its logs either
    /// way.
    rpc_url: Option<String>,

    /// WebSocket endpoint for this chain's live subscription, overriding the URL
    /// derived from `upstream.base`.
    ///
    /// Empty for the P-chain against an avalanchego upstream: the P-chain has no
    /// upstream push mechanism — no `eth_subscribe` analog, and the old X-chain
    /// pubsub was removed in avalanchego v1.11.13 — so a P instance polls
    /// `platform.getHeight` at `poll_interval` instead. Against a neve upstream
    /// every chain has a socket, because neve serves the `newBlocks` extension
    /// for all of them.
    ws_url: Option<String>,

    /// Where this chain's store lives.
    ///
    /// In `[chains.<x>]` this is that chain's directory, exactly. In
    /// `[defaults]` (and from `--data-dir`) it is instead the **base** every
    /// chain hangs off — two chains cannot share one store directory, so a
    /// literal shared value would be meaningless. The C-chain store sits at the
    /// base itself and every other chain gets a subdirectory (`<base>/p`). That
    /// asymmetry is load-bearing: C-chain stores in the field live at the base,
    /// and moving them would mean a resync.
    data_dir: Option<PathBuf>,

    /// Lowest height backfill should fill down to, anchored when the store is
    /// first created. `"tip"` anchors at the first live block and fills *forward
    /// only*; an integer retains deep history — `0` mirrors the whole chain.
    /// (TOML has no null, hence the string.)
    ///
    /// Ignored if the store already exists: the floor is baked in at creation,
    /// so truncate the data dir to re-anchor. Against the public endpoint
    /// backfill stays throttled, so a deep floor takes a long time to fill.
    ///
    /// **The C-chain defaults to `"tip"` and the P-chain to `0`.** That split is
    /// deliberate: a P-chain store is only useful with its history — the
    /// wallet-facing queries it answers are historical by nature — and a
    /// from-genesis P fill is the expected shape of a fresh install, whose ETA
    /// the backfill and summary progress lines already report.
    ///
    /// Against a neve upstream both chains default to `"tip"`, which there means
    /// something more specific: the upstream's `/health` is probed for its
    /// earliest retained block and the store is anchored at *that*, so the
    /// mirror reproduces the upstream's whole retained range. An explicit floor
    /// overrides the probe.
    backfill_floor: Option<Floor>,

    /// Minimum spacing between *individual* upstream requests for this chain
    /// while filling history.
    ///
    /// A true rate cap, not a nap appended to each block: enforced globally
    /// through one pacer, so it bounds requests per second regardless of
    /// upstream latency. C-chain backfill costs one `eth_getBlockByNumber` per
    /// block, plus one `eth_getLogs` per ~2048-block window under `ingest_logs`,
    /// plus an occasional `eth_blockNumber` for the tip. Each P-chain height
    /// costs two calls (`hexnc` + `json`), so this paces requests rather than
    /// heights.
    ///
    /// The C-chain default of 40ms is ~25 req/s, the rate this endpoint has long
    /// been documented as tolerating, and now measured rather than assumed: the
    /// mainnet instance sustained 24.75 blocks/s with no 429. The evidence is
    /// stronger than that run alone, because the pre-cache code was already
    /// issuing ~23.4 req/s continuously for days — two requests per block at
    /// 11.7 blocks/s — so this is ~7% above a long-proven rate rather than a step
    /// into the unknown.
    ///
    /// The P-chain default of 200ms is deliberately far politer. Measured on
    /// 2026-08-10, `api.avax-test.network` answered a sustained ~14 req/s of
    /// `platform.*` with HTTP 429 and `Retry-After: 3600`, and the limit applies
    /// to the whole host per IP — a hard P-chain backfill will throttle a
    /// C-chain instance sharing the address. (That host-wide limit is what
    /// `upstream.max_rps` exists for; this key bounds one chain, that one bounds
    /// their sum.) Filling deep history at this rate takes a very long time, so
    /// point `chains.p.rpc_url` at your own node and set this to `0` for that.
    ///
    /// Raise it (a larger delay) if you see HTTP 429. Defaults to `0` for every
    /// chain in `kind = "neve"` mode, which is unthrottled.
    request_interval: Option<HumanDuration>,

    /// How many heights the fill keeps in flight at once. Default 8.
    ///
    /// Each P-chain height costs two upstream round-trips, and issuing them
    /// serially caps the fill at roughly `1/(2 x RTT)` — a few hundred heights/s
    /// against a real node, which is hours for a from-genesis mainnet fill.
    /// Fetching ahead hides that latency.
    ///
    /// This can only recover time spent *waiting*: `request_interval` is
    /// enforced globally across every in-flight request, so raising this against
    /// the public endpoint changes nothing. Raise it when pointed at your own
    /// node (with `request_interval = 0`), where round-trip latency is the whole
    /// cost.
    concurrency: Option<usize>,

    /// How long a tip poller waits between `platform.getHeight` calls. The
    /// public endpoint serves that method from a short cache, so polling much
    /// faster buys nothing. Default 1s. Unused on the C-chain, which is
    /// push-driven.
    poll_interval: Option<HumanDuration>,

    /// Maximum time to wait when upstream sends `Retry-After` (e.g. `30s`,
    /// `10m`, `1h`). Within it, neve logs a WARN and sleeps; beyond it, it logs
    /// an ERROR and shuts down rather than sleep indefinitely.
    ///
    /// The default 65m is sized to absorb the value this endpoint actually
    /// sends. A throttled Avalanche public endpoint answers `Retry-After: 3600`,
    /// and any cap below that turns the throttle into a shutdown — which under a
    /// `Restart=always` unit (as deploy/neve.service ships) becomes an hour-long
    /// crash loop, each cycle re-paying store recovery, with RPC unavailable
    /// throughout. Sleeping keeps serving up while backfill waits out the hour,
    /// and it is not silent: the WARN and `neve_upstream_retry_after_seconds`
    /// both fire. Lower it if you would rather the process exit and let an
    /// orchestrator decide.
    max_wait: Option<HumanDuration>,

    /// Drop and reconnect this chain's WebSocket if nothing arrives within this
    /// window (e.g. `30s`, `2m`). Guards against a silently-dead socket — a
    /// half-open TCP connection or a stalled subscription that never errors,
    /// where the read would otherwise block forever. Default 2m.
    ws_idle_timeout: Option<HumanDuration>,

    /// Cap on the adaptive pre-fetch delay parked before the first live
    /// `newHeads` block fetch (e.g. `50ms`, `100ms`). A `newHeads` event can
    /// outrun the block's availability on the HTTPS backend; an AIMD controller
    /// learns a short delay that lets it land, cutting wasted `empty` fetches.
    /// Default `0s` disables it: the public Avalanche endpoint's propagation
    /// tail is heavy enough that any cap just pegs and adds latency to every
    /// block, while the cheap 25ms retry already covers the misses. Set a small
    /// cap only against a fast private full node that serves `newHeads`. No
    /// effect against a neve upstream (that uses `newBlocks`, no fetch).
    prefetch_delay_cap: Option<HumanDuration>,

    /// Ingest event logs alongside blocks: backfill fetches each ~2048-block
    /// window's logs via `eth_getLogs`, and the live path fetches each tip
    /// block's logs and joins them into the combined `[block, logs]` record. Off
    /// by default until the feed is proven. C-chain only.
    ingest_logs: Option<bool>,

    /// Max pending heights in a live join buffer before it flushes (and defers
    /// those heights to backfill). Only used with `ingest_logs`. Default 8192.
    join_buffer_cap: Option<usize>,

    /// Cadence for the periodic `summary` INFO log line (e.g. `30s`, `5m`,
    /// `1h`), one line per chain. The first summary fires shortly after startup
    /// regardless. Also paces the `backfill` progress line, so a long fill logs
    /// one of each per period instead of burying the summary. Default 5m.
    summary_period: Option<HumanDuration>,
}

impl ChainFile {
    /// Overlay `over` onto `self`: every key `over` sets wins, every key it
    /// leaves unset is inherited.
    fn overlay(&mut self, over: &Self) {
        macro_rules! take {
            ($($field:ident),* $(,)?) => {
                $(if over.$field.is_some() { self.$field = over.$field.clone(); })*
            };
        }
        take!(
            rpc_url,
            ws_url,
            data_dir,
            backfill_floor,
            request_interval,
            concurrency,
            poll_interval,
            max_wait,
            ws_idle_timeout,
            prefetch_delay_cap,
            ingest_logs,
            join_buffer_cap,
            summary_period,
        );
    }
}

// ---------------------------------------------------------------------------
// Scalar types needing custom deserialization
// ---------------------------------------------------------------------------

/// A duration written either as a string (`"40ms"`, `"1m"`, `"1h2m50ms"` — units
/// compose, largest first) or as a bare TOML integer, which means seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HumanDuration(Duration);

impl HumanDuration {
    const fn get(self) -> Duration {
        self.0
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl de::Visitor<'_> for V {
            type Value = HumanDuration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration string such as \"40ms\", \"5m\" or \"1h\", or an integer number of seconds")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                parse_human_duration(v)
                    .map(HumanDuration)
                    .map_err(de::Error::custom)
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                let secs = u64::try_from(v)
                    .map_err(|_| de::Error::custom(format!("negative duration: {v}")))?;
                Ok(HumanDuration(Duration::from_secs(secs)))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(HumanDuration(Duration::from_secs(v)))
            }
        }
        d.deserialize_any(V)
    }
}

/// A backfill floor: a height, or `"tip"` for "anchor at the first live block
/// and fill forward only". TOML has no null, so the sentinel is a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Floor {
    Tip,
    At(u64),
}

impl Floor {
    const fn height(self) -> Option<u64> {
        match self {
            Self::Tip => None,
            Self::At(h) => Some(h),
        }
    }
}

impl<'de> Deserialize<'de> for Floor {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl de::Visitor<'_> for V {
            type Value = Floor;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an integer height, or the string \"tip\"")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if v.eq_ignore_ascii_case("tip") {
                    return Ok(Floor::Tip);
                }
                Err(de::Error::custom(format!(
                    "expected an integer height or \"tip\", got {v:?}"
                )))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                let h = u64::try_from(v)
                    .map_err(|_| de::Error::custom(format!("negative height: {v}")))?;
                Ok(Floor::At(h))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Floor::At(v))
            }
        }
        d.deserialize_any(V)
    }
}

/// Parse a human duration: a bare integer means seconds, anything else goes to
/// the `parse_duration` crate (`40ms`, `5m`, `1h`, `1h 5m`).
pub(crate) fn parse_human_duration(s: &str) -> Result<Duration, String> {
    // Plain integer → seconds, so `--stop-time 6` works without a unit suffix.
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }
    parse_duration::parse(s).map_err(|e| e.to_string())
}

/// Render a duration back into a form `parse_human_duration` accepts, preferring
/// the largest exact unit. Used only by `--print-config`.
fn fmt_duration(d: Duration) -> String {
    let millis = d.as_millis();
    if millis == 0 {
        return "0s".to_owned();
    }
    if !millis.is_multiple_of(1000) {
        return format!("{millis}ms");
    }
    let secs = d.as_secs();
    if secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

// ---------------------------------------------------------------------------
// Built-in defaults
// ---------------------------------------------------------------------------

/// The bottom layer: what a chain gets with no file, no environment, no flags.
///
/// These are exactly the values the pre-config-file clap flags defaulted to,
/// with the one deliberate change documented on `ChainFile::backfill_floor`.
#[derive(Debug, Clone, Copy)]
struct Builtin {
    request_interval: Duration,
    backfill_floor: Floor,
    concurrency: usize,
    poll_interval: Duration,
    max_wait: Duration,
    ws_idle_timeout: Duration,
    prefetch_delay_cap: Duration,
    ingest_logs: bool,
    join_buffer_cap: usize,
    summary_period: Duration,
}

impl Builtin {
    const fn for_chain(chain: Chain, kind: UpstreamKind) -> Self {
        // A neve upstream has no public rate limit to be polite to, and its
        // retained range — not a built-in number — is what a fresh mirror should
        // anchor to, so both of the per-chain-varying defaults collapse there.
        let mirror = matches!(kind, UpstreamKind::Neve);
        Self {
            request_interval: match (mirror, chain) {
                (true, _) => Duration::ZERO,
                (false, Chain::C) => Duration::from_millis(40),
                (false, Chain::P) => Duration::from_millis(200),
            },
            backfill_floor: match (mirror, chain) {
                (false, Chain::P) => Floor::At(0),
                _ => Floor::Tip,
            },
            concurrency: 8,
            poll_interval: Duration::from_secs(1),
            // 65 minutes: one `Retry-After: 3600` plus slack. See
            // `ChainFile::max_wait`.
            max_wait: Duration::from_secs(3900),
            ws_idle_timeout: Duration::from_secs(120),
            prefetch_delay_cap: Duration::ZERO,
            ingest_logs: false,
            join_buffer_cap: 8192,
            summary_period: Duration::from_secs(300),
        }
    }
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

/// Everything `resolve` reads that is neither argv nor the config file.
///
/// Snapshotted into a struct rather than read through `std::env` at the point of
/// use so tests can drive the whole resolution with a synthetic environment:
/// mutating the process environment is racy across `cargo test`'s threads and,
/// since edition 2024, `unsafe` — which this crate denies outright.
#[derive(Debug, Default, Clone)]
struct Env {
    vars: BTreeMap<String, String>,
    /// Config file consulted when neither `--config` nor `NEVE_CONFIG` names
    /// one, and only if it exists. `None` in tests, so a config file installed
    /// on the machine running them cannot change their outcome.
    default_config: Option<PathBuf>,
}

impl Env {
    fn from_process() -> Self {
        Self {
            vars: ENV_VARS
                .iter()
                .filter_map(|key| {
                    std::env::var(key)
                        .ok()
                        .map(|value| ((*key).to_owned(), value))
                })
                .collect(),
            default_config: Some(PathBuf::from(DEFAULT_CONFIG_PATH)),
        }
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.vars.get(key).map(String::as_str)
    }
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

impl Cli {
    /// Merge every layer and validate. Errors name the offending key.
    pub fn resolve(&self) -> Result<Resolved> {
        self.resolve_with(&Env::from_process())
    }

    fn resolve_with(&self, env: &Env) -> Result<Resolved> {
        let mut notices = Vec::new();
        let mut tree = self.load_tree(env)?;

        // Chain selection is settled before any override lands, because most
        // overrides address a chain by name: `--rpc-url` writes
        // `chains.c.rpc_url`, materializing a `[chains.c]` table, and that must
        // not silently become a chain selector that disables the P-chain. Only
        // `--chains` and `enabled` select, so `select_chains` reads those two
        // out of the command line itself rather than waiting for the merge.
        let enabled = self.select_chains(&tree)?;
        self.reject_sets_for_idle_chains(&enabled)?;

        self.apply_env(&mut tree, env, &mut notices)?;
        self.apply_deprecated_flags(&mut tree, &enabled, &mut notices)?;
        self.apply_flags(&mut tree, &enabled)?;
        self.apply_sets(&mut tree)?;

        let file = deserialize_tree(&tree)?;
        build(self, env, file, &enabled, notices)
    }

    /// Read the config file into a mutable document, or start from an empty one.
    fn load_tree(&self, env: &Env) -> Result<Table> {
        let (path, required) = match (self.config.as_deref(), env.get("NEVE_CONFIG")) {
            (Some(p), _) => (Some(p.to_path_buf()), true),
            (None, Some(p)) => (Some(PathBuf::from(p)), true),
            (None, None) => (env.default_config.clone(), false),
        };
        let Some(path) = path else {
            return Ok(Table::new());
        };
        if !required && !path.exists() {
            return Ok(Table::new());
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("read config file {}", path.display()))?;
        // Deserialize the typed schema straight from the text, purely for the
        // error message: toml's errors carry line/column spans, and those are
        // lost the moment the document becomes a `Value` tree.
        let _: File = toml::from_str(&text)
            .with_context(|| format!("invalid config file {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parse config file {}", path.display()))
    }

    /// Which chains this run serves.
    ///
    /// `--chains` is the explicit selector and outranks everything, including an
    /// `enabled = false` in the file. Otherwise the set is the file's
    /// `[chains.<x>]` tables — or every chain neve knows about, when the file
    /// declares none — less any chain whose `enabled` key turns it off.
    ///
    /// `enabled` is read straight out of the file and the `--set` arguments
    /// rather than from the merged document, because selection has to be settled
    /// before the merge (see `resolve_with`).
    fn select_chains(&self, tree: &Table) -> Result<Vec<Chain>> {
        if !self.chains.is_empty() {
            return normalize_chains(&self.chains);
        }
        let declared = declared_chains(tree)?;
        if declared.as_ref().is_some_and(Vec::is_empty) {
            bail!("the `[chains]` table is empty; there would be nothing to serve");
        }
        let mut chains = Vec::with_capacity(Chain::ALL.len());
        for &chain in Chain::ALL {
            let on = match self.set_enabled(chain)? {
                Some(explicit) => explicit,
                None => {
                    declared.as_ref().is_none_or(|d| d.contains(&chain))
                        && file_enabled(tree, chain)?
                }
            };
            if on {
                chains.push(chain);
            }
        }
        if chains.is_empty() {
            bail!(
                "every chain is disabled; enable one with `--chains <list>` or by setting `enabled = true`",
            );
        }
        normalize_chains(&chains)
    }

    /// `chains.<x>.enabled` as given by `--set`, the one override layer that can
    /// change the chain set. Last occurrence wins, matching every other key.
    fn set_enabled(&self, chain: Chain) -> Result<Option<bool>> {
        let wanted = format!("chains.{}.enabled", chain.as_str());
        let mut found = None;
        for arg in &self.set {
            let Some((key, raw)) = arg.split_once('=') else {
                continue;
            };
            if key.trim() == wanted {
                found =
                    Some(raw.parse::<bool>().map_err(|_| {
                        anyhow!("`--set {wanted}={raw}`: expected `true` or `false`")
                    })?);
            }
        }
        Ok(found)
    }

    /// Reject a `--set` aimed at a chain this run does not serve.
    ///
    /// The chain set is fixed before the overrides merge, so such a key would
    /// deserialize, validate, and then do nothing at all. Refusing it keeps the
    /// promise that a `--set` which is accepted is a `--set` that took effect.
    fn reject_sets_for_idle_chains(&self, enabled: &[Chain]) -> Result<()> {
        for arg in &self.set {
            let Some((key, _)) = arg.split_once('=') else {
                continue;
            };
            let mut parts = key.trim().split('.');
            if parts.next() != Some("chains") {
                continue;
            }
            let (Some(name), Some(field)) = (parts.next(), parts.next()) else {
                continue;
            };
            // An unknown chain name is the schema's error to report, not ours.
            let Ok(chain) = Chain::deserialize(Value::String(name.to_owned())) else {
                continue;
            };
            if field == "enabled" || enabled.contains(&chain) {
                continue;
            }
            bail!(
                "`--set {key}=…` configures the {name}-chain, which this run does not serve; add `--chains {name}` or `--set chains.{name}.enabled=true`",
            );
        }
        Ok(())
    }

    /// Layer 4: the environment.
    fn apply_env(&self, tree: &mut Table, env: &Env, notices: &mut Vec<Notice>) -> Result<()> {
        // The token is deliberately *not* routed through the document tree: it
        // would then be one `--print-config` away from a terminal.
        for (var, key) in [
            ("NEVE_RPC_URL", "chains.c.rpc_url"),
            ("NEVE_WS_URL", "chains.c.ws_url"),
            ("NEVE_P_RPC_URL", "chains.p.rpc_url"),
        ] {
            let Some(value) = env.get(var) else { continue };
            notices.push(Notice::Warn(format!(
                "`{var}` is deprecated and will be removed in the next release; set `{key}` in the config file instead — and if the URL carries a credential, move that to `upstream.token_file`, which is what these variables existed to allow"
            )));
            set_checked(
                tree,
                key,
                Value::String(value.to_owned()),
                &format!("environment variable `{var}`"),
            )?;
        }
        Ok(())
    }

    /// Layer 5a: the hidden deprecated flags.
    ///
    /// One flat table of flag → config key, so the whole compatibility surface
    /// is legible at a glance and deleting it next release is a single edit.
    /// The duration-valued flags are carried as raw strings rather than parsed
    /// `Duration`s: the file layer already knows how to read `"40ms"`, and
    /// round-tripping a parsed duration back into a string to hand it over would
    /// be a lossy detour.
    fn apply_deprecated_flags(
        &self,
        tree: &mut Table,
        enabled: &[Chain],
        notices: &mut Vec<Notice>,
    ) -> Result<()> {
        let text = |v: &Option<String>| v.clone().map(Value::String);
        let path = |v: &Option<PathBuf>| v.as_ref().map(|p| Value::String(p.display().to_string()));
        let pending = [
            ("--ws-url", "chains.c.ws_url", text(&self.ws_url)),
            ("--rpc-url", "chains.c.rpc_url", text(&self.rpc_url)),
            ("--p-rpc-url", "chains.p.rpc_url", text(&self.p_rpc_url)),
            (
                "--p-poll-interval",
                "chains.p.poll_interval",
                text(&self.p_poll_interval),
            ),
            (
                "--request-interval",
                "chains.c.request_interval",
                text(&self.request_interval),
            ),
            (
                "--p-request-interval",
                "chains.p.request_interval",
                text(&self.p_request_interval),
            ),
            ("--max-wait", "chains.*.max_wait", text(&self.max_wait)),
            (
                "--ws-idle-timeout",
                "chains.*.ws_idle_timeout",
                text(&self.ws_idle_timeout),
            ),
            (
                "--prefetch-delay-cap",
                "chains.*.prefetch_delay_cap",
                text(&self.prefetch_delay_cap),
            ),
            (
                "--idle-timeout",
                "server.idle_timeout",
                text(&self.idle_timeout),
            ),
            (
                "--summary-period",
                "chains.*.summary_period",
                text(&self.summary_period),
            ),
            ("--p-data-dir", "chains.p.data_dir", path(&self.p_data_dir)),
            (
                "--p-concurrency",
                "chains.p.concurrency",
                int(self.p_concurrency, "--p-concurrency")?,
            ),
            (
                "--join-buffer-cap",
                "chains.*.join_buffer_cap",
                int(self.join_buffer_cap, "--join-buffer-cap")?,
            ),
            (
                "--backfill-floor",
                "chains.c.backfill_floor",
                int(self.backfill_floor, "--backfill-floor")?,
            ),
            (
                "--p-backfill-floor",
                "chains.p.backfill_floor",
                int(self.p_backfill_floor, "--p-backfill-floor")?,
            ),
            (
                "--max-connections",
                "server.max_connections",
                int(self.max_connections, "--max-connections")?,
            ),
            (
                "--max-blocks-per-request",
                "server.max_blocks_per_request",
                int(self.max_blocks_per_request, "--max-blocks-per-request")?,
            ),
        ];

        for (flag, key, value) in pending {
            let Some(value) = value else { continue };
            // A `chains.*.<name>` flag has no chain of its own, so point the
            // operator at `[defaults]`, where one value covers every chain.
            let suggest = key
                .strip_prefix("chains.*.")
                .map_or_else(|| key.to_owned(), |name| format!("defaults.{name}"));
            notices.push(Notice::Warn(format!(
                "`{flag}` is deprecated and will be removed in the next release; use `{suggest}` in the config file (or `--set {suggest}=…`)"
            )));
            apply_key(tree, key, &value, &format!("flag `{flag}`"), enabled)?;
        }
        Ok(())
    }

    /// Layer 5b: the visible flags.
    fn apply_flags(&self, tree: &mut Table, enabled: &[Chain]) -> Result<()> {
        if let Some(network) = self.network {
            set_checked(tree, "network", value_of(&network)?, "flag `--network`")?;
        }
        if let Some(level) = self.log_level {
            set_checked(tree, "log_level", value_of(&level)?, "flag `--log-level`")?;
        }
        if let Some(dir) = &self.data_dir {
            // The *base* every chain hangs off, not one chain's directory; see
            // `ChainFile::data_dir`.
            set_checked(
                tree,
                "defaults.data_dir",
                Value::String(dir.display().to_string()),
                "flag `--data-dir`",
            )?;
        }
        if let Some(addr) = self.rpc_addr {
            set_checked(
                tree,
                "server.addr",
                Value::String(addr.to_string()),
                "flag `--rpc-addr`",
            )?;
        }
        if self.ingest_logs {
            apply_key(
                tree,
                "chains.*.ingest_logs",
                &Value::Boolean(true),
                "flag `--ingest-logs`",
                enabled,
            )?;
        }
        if let Some(base) = &self.mirror_from {
            // Sugar for the two keys that together mean "mirror that neve".
            set_checked(
                tree,
                "upstream.kind",
                value_of(&UpstreamKind::Neve)?,
                "flag `--mirror-from`",
            )?;
            set_checked(
                tree,
                "upstream.base",
                Value::String(base.clone()),
                "flag `--mirror-from`",
            )?;
        }
        Ok(())
    }

    /// Layer 5c: `--set`, applied last so it can override anything.
    fn apply_sets(&self, tree: &mut Table) -> Result<()> {
        for arg in &self.set {
            let (key, raw) = arg
                .split_once('=')
                .ok_or_else(|| anyhow!("`--set` expects KEY=VALUE, got `{arg}`"))?;
            set_checked(
                tree,
                key,
                parse_scalar(raw),
                &format!("`--set {key}={raw}`"),
            )?;
        }
        Ok(())
    }
}

/// The chains the file declares a `[chains.<x>]` table for, or `None` when it
/// has no `[chains]` table at all — which means every chain.
fn declared_chains(tree: &Table) -> Result<Option<Vec<Chain>>> {
    let Some(chains) = tree.get("chains") else {
        return Ok(None);
    };
    let chains = chains
        .as_table()
        .ok_or_else(|| anyhow!("`chains` must be a table of per-chain tables"))?;
    chains
        .keys()
        .map(|key| {
            Chain::deserialize(Value::String(key.clone()))
                .with_context(|| format!("unknown chain `chains.{key}`"))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

/// `chains.<x>.enabled` as the file spells it. Absent means enabled: a chain
/// with a table is a chain you asked for.
fn file_enabled(tree: &Table, chain: Chain) -> Result<bool> {
    let Some(value) = tree
        .get("chains")
        .and_then(Value::as_table)
        .and_then(|t| t.get(chain.as_str()))
        .and_then(Value::as_table)
        .and_then(|t| t.get("enabled"))
    else {
        return Ok(true);
    };
    value
        .as_bool()
        .ok_or_else(|| anyhow!("`chains.{}.enabled` must be a boolean", chain.as_str()))
}

/// Write one override into the document.
///
/// A key of the form `chains.*.<name>` fans out to every chain this run serves,
/// which is what a flag naming no chain has to mean. Writing it to
/// `defaults.<name>` instead would leave it *below* `[chains.<x>]` in the
/// precedence order, so a chain that named the same key in the file would
/// silently outrank the command line.
fn apply_key(
    tree: &mut Table,
    key: &str,
    value: &Value,
    origin: &str,
    enabled: &[Chain],
) -> Result<()> {
    let Some(name) = key.strip_prefix("chains.*.") else {
        return set_checked(tree, key, value.clone(), origin);
    };
    for chain in enabled {
        set_checked(
            tree,
            &format!("chains.{}.{name}", chain.as_str()),
            value.clone(),
            origin,
        )?;
    }
    Ok(())
}

/// Convert an integer-valued flag into a TOML integer (which is an `i64`),
/// naming the flag if the value somehow does not fit.
fn int<T: TryInto<i64> + fmt::Display + Copy>(v: Option<T>, flag: &str) -> Result<Option<Value>> {
    v.map(|v| {
        v.try_into()
            .map(Value::Integer)
            .map_err(|_| anyhow!("value for `{flag}` is out of range: {v}"))
    })
    .transpose()
}

/// Serialize any value into the document tree's representation.
fn value_of<T: Serialize>(v: &T) -> Result<Value> {
    Value::try_from(v).context("serialize configuration value")
}

/// Parse a `--set` right-hand side.
///
/// It is read by the same grammar the config file uses, so `12`, `true`, `1.5`
/// and `[1, 2]` mean there what they would mean in the file. Anything that is
/// not valid TOML is taken as a string, which is what makes the common cases
/// (`--set upstream.base=https://h`, `--set defaults.summary_period=30s`,
/// `--set chains.c.backfill_floor=tip`) need no shell quoting.
fn parse_scalar(raw: &str) -> Value {
    toml::from_str::<Table>(&format!("v = {raw}"))
        .ok()
        .and_then(|mut t| t.remove("v"))
        .unwrap_or_else(|| Value::String(raw.to_owned()))
}

/// Apply one override to the document, then re-validate the whole document so a
/// bad key or value is reported against `origin` — the flag, environment
/// variable, or `--set` argument that introduced it — rather than surfacing
/// anonymously once every layer has been merged.
fn set_checked(tree: &mut Table, key: &str, value: Value, origin: &str) -> Result<()> {
    let path: Vec<&str> = key.split('.').collect();
    if path.iter().any(|p| p.is_empty()) {
        bail!("{origin}: `{key}` has an empty path segment");
    }
    set_path(tree, &path, key, value).with_context(|| origin.to_owned())?;
    deserialize_tree(tree)
        .map(|_| ())
        .with_context(|| origin.to_owned())
}

/// Insert `value` at the dotted `path`, creating intermediate tables.
fn set_path(tbl: &mut Table, path: &[&str], key: &str, value: Value) -> Result<()> {
    match path {
        [] => bail!("empty configuration key"),
        [last] => {
            tbl.insert((*last).to_owned(), value);
            Ok(())
        }
        [head, rest @ ..] => {
            let entry = tbl
                .entry(*head)
                .or_insert_with(|| Value::Table(Table::new()));
            let sub = entry
                .as_table_mut()
                .ok_or_else(|| anyhow!("cannot set `{key}`: `{head}` is not a table"))?;
            set_path(sub, rest, key, value)
        }
    }
}

/// Deserialize the document into the typed schema. This is where
/// `deny_unknown_fields` rejects a typo, wherever it came from.
fn deserialize_tree(tree: &Table) -> Result<File> {
    Value::Table(tree.clone())
        .try_into()
        .context("invalid configuration")
}

/// Turn the merged document into the resolved configuration.
fn build(
    cli: &Cli,
    env: &Env,
    file: File,
    enabled: &[Chain],
    mut notices: Vec<Notice>,
) -> Result<Resolved> {
    let network = file.network.unwrap_or(Network::Mainnet);
    let pre_tokened = urls_already_carry_a_token(&file);
    let upstream = resolve_upstream(env, file.upstream, network, pre_tokened, &mut notices)?;
    let server = resolve_server(file.server)?;

    let defaults = file.defaults.unwrap_or_default();
    if defaults.enabled.is_some() {
        bail!(
            "`defaults.enabled` would mean \"serve no chains\"; set `enabled` in a `[chains.<x>]` table, or select with `--chains`",
        );
    }
    let base_dir = defaults
        .data_dir
        .clone()
        .unwrap_or_else(|| network.default_data_dir());

    let mut chains = BTreeMap::new();
    for &chain in enabled {
        let per = file
            .chains
            .as_ref()
            .and_then(|m| m.get(&chain))
            .cloned()
            .unwrap_or_default();
        let cfg = resolve_chain(chain, &upstream, &defaults, &per, &base_dir, &mut notices)?;
        chains.insert(chain, cfg);
    }
    // Two chains sharing a store directory would each reject the other's
    // `meta/chain` stamp on open; catching it here names the key instead.
    for (chain, cfg) in &chains {
        if let Some((other, _)) = chains
            .iter()
            .find(|(c, o)| *c != chain && o.data_dir == cfg.data_dir)
        {
            bail!(
                "chains `{}` and `{}` resolve to the same data dir {}; set `chains.{}.data_dir`",
                chain.as_str(),
                other.as_str(),
                cfg.data_dir.display(),
                other.as_str(),
            );
        }
    }

    let print = if cli.print_config_example {
        Some(PrintMode::Example)
    } else if cli.print_config {
        Some(PrintMode::Config)
    } else {
        None
    };

    Ok(Resolved {
        network,
        log_level: file.log_level.unwrap_or(LogLevel::Info),
        stop_time: cli.stop_time,
        upstream,
        server,
        chains,
        print,
        notices,
    })
}

/// Whether a URL written in the file already carries the token parameter.
///
/// A bypass token is a bypass token wherever it is spelled. When one rides in an
/// explicit `rpc_url` rather than in `token_file`, neve cannot see a `Token` — so
/// without this it would apply the untokened public-endpoint cap and throttle an
/// upstream that has no limit to respect.
fn urls_already_carry_a_token(file: &File) -> bool {
    let param = file
        .upstream
        .as_ref()
        .and_then(|u| u.token_param.as_deref())
        .unwrap_or("token");
    let carries = |url: &Option<String>| {
        url.as_deref()
            .is_some_and(|u| u.contains(&format!("?{param}=")) || u.contains(&format!("&{param}=")))
    };
    let has = |c: &ChainFile| carries(&c.rpc_url) || carries(&c.ws_url);
    file.defaults.as_ref().is_some_and(has)
        || file.chains.as_ref().is_some_and(|m| m.values().any(has))
        || file.upstream.as_ref().is_some_and(|u| carries(&u.base))
}

fn resolve_upstream(
    env: &Env,
    file: Option<UpstreamFile>,
    network: Network,
    pre_tokened: bool,
    notices: &mut Vec<Notice>,
) -> Result<Upstream> {
    let file = file.unwrap_or_default();
    let kind = file.kind.unwrap_or_default();
    let base = file
        .base
        .unwrap_or_else(|| network.default_base_url())
        .trim_end_matches('/')
        .to_owned();
    if base.is_empty() {
        bail!("`upstream.base` is empty");
    }

    let token_file = file.token_file;
    let env_token = env.get("NEVE_UPSTREAM_TOKEN");
    if token_file.is_some() && env_token.is_some() {
        notices.push(Notice::Info(
            "both `upstream.token_file` and NEVE_UPSTREAM_TOKEN are set; using the file".to_owned(),
        ));
    }
    let token = match (&token_file, env_token) {
        (Some(path), _) => Some(read_token_file(path)?),
        (None, Some(value)) => {
            let value = value.trim();
            if value.is_empty() {
                bail!("NEVE_UPSTREAM_TOKEN is set but empty");
            }
            Some(Token::new(value))
        }
        (None, None) => None,
    };

    // The host cap corresponds to the endpoint's actual limit, which is per-IP
    // for the whole host; the reason it landed where it did is worth a line in
    // the log, because "why is my fill slow" and "why am I being 429'd" are the
    // two questions it answers.
    let (max_rps, why) = match (file.max_rps, kind, token.is_some() || pre_tokened) {
        (Some(rps), _, _) => {
            if !rps.is_finite() || rps < 0.0 {
                bail!("`upstream.max_rps` must be a non-negative number, got {rps}");
            }
            (
                (rps > 0.0).then_some(rps),
                "configured with `upstream.max_rps`",
            )
        }
        (None, UpstreamKind::Neve, _) => {
            (None, "upstream is another neve, which has no rate limit")
        }
        (None, UpstreamKind::Avalanchego, true) => (
            None,
            "an upstream token is in play, and bypassing the limit is what a token is for",
        ),
        (None, UpstreamKind::Avalanchego, false) => (
            Some(PUBLIC_MAX_RPS),
            "public-endpoint default for an untokened upstream",
        ),
    };
    notices.push(Notice::Info(match max_rps {
        Some(rps) => {
            format!("upstream host request cap: {rps} req/s, shared by every chain ({why})")
        }
        None => format!("upstream host request cap: none ({why})"),
    }));

    Ok(Upstream {
        kind,
        base,
        token,
        token_param: file.token_param.unwrap_or_else(|| "token".to_owned()),
        token_file,
        max_rps,
        // `recip` rather than `1.0 / rps` so the interval is a method call on a
        // value already checked to be positive and finite.
        host_pacer: max_rps.map(|rps| Arc::new(Pacer::new(Duration::from_secs_f64(rps.recip())))),
    })
}

fn read_token_file(path: &Path) -> Result<Token> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read `upstream.token_file` {}", path.display()))?;
    let token = text.trim();
    if token.is_empty() {
        bail!("`upstream.token_file` {} is empty", path.display());
    }
    Ok(Token::new(token))
}

fn resolve_server(file: Option<ServerFile>) -> Result<Server> {
    let file = file.unwrap_or_default();
    let max_connections = file.max_connections.unwrap_or(1024);
    if max_connections == 0 {
        bail!("`server.max_connections` must be at least 1");
    }
    let max_blocks_per_request = file.max_blocks_per_request.unwrap_or(10_000);
    if max_blocks_per_request == 0 {
        bail!("`server.max_blocks_per_request` must be at least 1");
    }
    let idle = file
        .idle_timeout
        .map_or(Duration::from_secs(60), HumanDuration::get);
    Ok(Server {
        addr: file
            .addr
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8545))),
        max_connections,
        // Zero disables the reaper; keep that a `None` rather than a magic zero.
        idle_timeout: (idle > Duration::ZERO).then_some(idle),
        max_blocks_per_request,
    })
}

fn resolve_chain(
    chain: Chain,
    upstream: &Upstream,
    defaults: &ChainFile,
    per: &ChainFile,
    base_dir: &Path,
    notices: &mut Vec<Notice>,
) -> Result<ChainCfg> {
    let builtin = Builtin::for_chain(chain, upstream.kind);
    let mut merged = ChainFile::default();
    merged.overlay(defaults);
    merged.overlay(per);

    let key = |name: &str| format!("chains.{}.{name}", chain.as_str());

    let (derived_rpc, derived_ws) = derive_endpoints(upstream.kind, &upstream.base, chain)?;
    let rpc_url = merged.rpc_url.clone().unwrap_or(derived_rpc);
    let ws_url = merged.ws_url.clone().unwrap_or(derived_ws);
    let host_pacer = host_pacer_for(chain, upstream, &rpc_url, notices);
    let (rpc_url, ws_url) = match &upstream.token {
        Some(token) => (
            with_token(&rpc_url, &upstream.token_param, token),
            with_token(&ws_url, &upstream.token_param, token),
        ),
        None => (rpc_url, ws_url),
    };

    // `data_dir` is the one key whose `[defaults]` meaning differs from its
    // per-chain meaning: there it is the base every chain hangs off (two chains
    // cannot share one store), so it is resolved from `per` alone and the base
    // supplies the rest.
    let data_dir = per
        .data_dir
        .clone()
        .unwrap_or_else(|| chain.data_dir(base_dir));

    let concurrency = merged.concurrency.unwrap_or(builtin.concurrency);
    if concurrency == 0 {
        bail!("`{}` must be at least 1", key("concurrency"));
    }
    let join_buffer_cap = merged.join_buffer_cap.unwrap_or(builtin.join_buffer_cap);
    if join_buffer_cap == 0 {
        bail!("`{}` must be at least 1", key("join_buffer_cap"));
    }
    let summary_period = merged
        .summary_period
        .map_or(builtin.summary_period, HumanDuration::get);
    if summary_period.is_zero() {
        bail!("`{}` must be greater than zero", key("summary_period"));
    }
    let poll_interval = merged
        .poll_interval
        .map_or(builtin.poll_interval, HumanDuration::get);
    if poll_interval.is_zero() {
        bail!("`{}` must be greater than zero", key("poll_interval"));
    }

    Ok(ChainCfg {
        chain,
        rpc_url,
        ws_url,
        data_dir,
        backfill_floor: merged
            .backfill_floor
            .unwrap_or(builtin.backfill_floor)
            .height(),
        request_interval: merged
            .request_interval
            .map_or(builtin.request_interval, HumanDuration::get),
        concurrency,
        poll_interval,
        max_wait: merged.max_wait.map_or(builtin.max_wait, HumanDuration::get),
        ws_idle_timeout: merged
            .ws_idle_timeout
            .map_or(builtin.ws_idle_timeout, HumanDuration::get),
        prefetch_delay_cap: merged
            .prefetch_delay_cap
            .map_or(builtin.prefetch_delay_cap, HumanDuration::get),
        // Only the C-chain has an event-log feed, so the setting narrows here
        // rather than at the point of use — otherwise `--print-config` would
        // report `ingest_logs = true` for a chain that ingests no logs. The
        // P-chain's second record half (reward UTXOs) arrives in a later phase.
        ingest_logs: merged.ingest_logs.unwrap_or(builtin.ingest_logs) && chain == Chain::C,
        join_buffer_cap,
        summary_period,
        subscribe_blocks: matches!(upstream.kind, UpstreamKind::Neve),
        host_pacer,
    })
}

/// The host pacer, but only for a chain that is actually on the upstream host.
///
/// The cap exists because the endpoint's rate limit is per-IP for the whole
/// host, which is a fact about *that host*. A chain pointed somewhere else by an
/// explicit `rpc_url` — the usual shape of a fast first fill, where the P-chain
/// reads from a private node while the C-chain stays on the public endpoint — is
/// not spending that budget, and charging it against the cap would throttle it
/// to the public rate for no reason.
fn host_pacer_for(
    chain: Chain,
    upstream: &Upstream,
    rpc_url: &str,
    notices: &mut Vec<Notice>,
) -> Option<Arc<Pacer>> {
    let pacer = upstream.host_pacer.as_ref()?;
    let (host, mine) = (authority(&upstream.base), authority(rpc_url));
    if mine
        .zip(host)
        .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b))
    {
        return Some(Arc::clone(pacer));
    }
    notices.push(Notice::Info(format!(
        "chains.{}.rpc_url points at {}, not the upstream host {}; the host request cap does not apply to it",
        chain.as_str(),
        mine.unwrap_or("an unparseable URL"),
        host.unwrap_or("(none)"),
    )));
    None
}

/// The `host[:port]` of a URL. Hand-rolled because this is the only place neve
/// needs to take a URL apart, and the shapes it must handle are the ones it
/// writes itself.
fn authority(url: &str) -> Option<&str> {
    let rest = url
        .split_once("://")
        .map_or(url, |(_, rest)| rest)
        .trim_start_matches('/');
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    (!authority.is_empty()).then_some(authority)
}

/// `(rpc_url, ws_url)` for `chain`, before any explicit override or token.
///
/// An avalanchego host tells chains apart by URL path, so each chain gets its
/// own path under `base`. A neve upstream serves every chain — JSON-RPC, the
/// WebSocket, and `/health` — on one socket, telling them apart by method
/// namespace, so `base` *is* the endpoint for all of them.
fn derive_endpoints(kind: UpstreamKind, base: &str, chain: Chain) -> Result<(String, String)> {
    match kind {
        UpstreamKind::Avalanchego => {
            let ws = match chain.ws_path() {
                Some(path) => format!("{}{path}", derive_ws_url(base)?),
                None => String::new(),
            };
            Ok((format!("{base}{}", chain.rpc_path()), ws))
        }
        UpstreamKind::Neve => Ok((base.to_owned(), derive_ws_url(base)?)),
    }
}

/// Derive a WebSocket URL from an HTTP(S) base, preserving host/port/path.
/// `ws://` / `wss://` inputs pass through unchanged.
pub(crate) fn derive_ws_url(base: &str) -> Result<String> {
    if let Some(rest) = base.strip_prefix("https://") {
        Ok(format!("wss://{rest}"))
    } else if let Some(rest) = base.strip_prefix("http://") {
        Ok(format!("ws://{rest}"))
    } else if base.starts_with("ws://") || base.starts_with("wss://") {
        Ok(base.to_owned())
    } else {
        bail!("upstream base must be an http(s):// (or ws(s)://) URL, got: {base}")
    }
}

/// Append `?<param>=<token>` to an upstream URL.
///
/// Three cases, all of which occur: no query at all, a URL that already carries
/// one (so the separator is `&`), and a URL that already carries *this*
/// parameter — where appending a second copy would at best be ignored and at
/// worst be rejected, so the URL is left exactly as the operator wrote it. An
/// empty URL (the P-chain's absent WebSocket) stays empty.
fn with_token(url: &str, param: &str, token: &Token) -> String {
    if url.is_empty() {
        return String::new();
    }
    let value = token.expose();
    match url.split_once('?') {
        None => format!("{url}?{param}={value}"),
        // A bare trailing `?` is a query slot with nothing in it, so the token
        // goes straight after it rather than behind a stray `&`.
        Some((_, "")) => format!("{url}{param}={value}"),
        Some((_, query)) => {
            let present = query
                .split('&')
                .any(|pair| pair.split('=').next() == Some(param));
            if present {
                url.to_owned()
            } else {
                format!("{url}&{param}={value}")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// --print-config output
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct RedactedUpstream {
    kind: UpstreamKind,
    base: String,
    token_param: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_file: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
}

#[derive(Debug, Serialize)]
struct RedactedServer {
    addr: String,
    max_connections: u32,
    idle_timeout: String,
    max_blocks_per_request: u64,
}

#[derive(Debug, Serialize)]
struct RedactedChain {
    rpc_url: String,
    ws_url: String,
    data_dir: String,
    backfill_floor: Value,
    request_interval: String,
    concurrency: usize,
    poll_interval: String,
    max_wait: String,
    ws_idle_timeout: String,
    prefetch_delay_cap: String,
    ingest_logs: bool,
    join_buffer_cap: usize,
    summary_period: String,
}

impl RedactedChain {
    fn of(cfg: &ChainCfg) -> Self {
        Self {
            // The token rides in the query string, so the query goes.
            rpc_url: redact_url(&cfg.rpc_url).into_owned(),
            ws_url: redact_url(&cfg.ws_url).into_owned(),
            data_dir: cfg.data_dir.display().to_string(),
            backfill_floor: cfg.backfill_floor.map_or_else(
                || Value::String("tip".to_owned()),
                |h| Value::Integer(i64::try_from(h).unwrap_or(i64::MAX)),
            ),
            request_interval: fmt_duration(cfg.request_interval),
            concurrency: cfg.concurrency,
            poll_interval: fmt_duration(cfg.poll_interval),
            max_wait: fmt_duration(cfg.max_wait),
            ws_idle_timeout: fmt_duration(cfg.ws_idle_timeout),
            prefetch_delay_cap: fmt_duration(cfg.prefetch_delay_cap),
            ingest_logs: cfg.ingest_logs,
            join_buffer_cap: cfg.join_buffer_cap,
            summary_period: fmt_duration(cfg.summary_period),
        }
    }
}

/// The annotated reference file written by `--print-config-example`.
///
/// It is `deploy/config.toml.example`, compiled in, so the binary always carries
/// its own current reference: an operator on a box whose `/etc` is months stale
/// can still ask the binary they are actually running what it supports.
/// `Cargo.toml` narrows the package with neither `include` nor `exclude`, so
/// `deploy/` ships in the published crate and this `include_str!` resolves for
/// anyone building from crates.io.
///
/// Every key is present and **live** at its built-in default rather than
/// commented out, which is what lets `example_resolves_to_the_builtin_defaults`
/// assert that resolving the file reproduces resolving nothing at all. That test
/// plus `example_documents_every_key_the_schema_accepts` is the drift alarm: a
/// field added to the schema, or a default changed, fails the suite until this
/// file catches up.
pub const EXAMPLE_CONFIG: &str = include_str!("../deploy/config.toml.example");

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Parse a CLI from arguments, as a user would type them.
    fn cli(args: &[&str]) -> Cli {
        Cli::parse_from(std::iter::once("neve").chain(args.iter().copied()))
    }

    /// Resolve with a synthetic environment and no implicit `/etc` config, so a
    /// test's outcome depends on nothing outside the test.
    fn resolve_env(args: &[&str], vars: &[(&str, &str)]) -> Result<Resolved> {
        let env = Env {
            vars: vars
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            default_config: None,
        };
        cli(args).resolve_with(&env)
    }

    fn resolve(args: &[&str]) -> Resolved {
        resolve_env(args, &[]).unwrap()
    }

    /// Resolve with `text` as the config file.
    fn resolve_file(text: &str, args: &[&str]) -> Result<Resolved> {
        resolve_file_env(text, args, &[])
    }

    fn resolve_file_env(text: &str, args: &[&str], vars: &[(&str, &str)]) -> Result<Resolved> {
        let dir = crate::test_support::unique_temp_dir("config");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, text).unwrap();
        let path = path.display().to_string();
        let mut full = vec!["--config", path.as_str()];
        full.extend_from_slice(args);
        resolve_env(&full, vars)
    }

    fn c(r: &Resolved) -> &ChainCfg {
        r.chains.get(&Chain::C).expect("C-chain enabled")
    }

    fn p(r: &Resolved) -> &ChainCfg {
        r.chains.get(&Chain::P).expect("P-chain enabled")
    }

    // -- defaults -----------------------------------------------------------

    /// With nothing configured at all, both chains run. This is the deliberate
    /// behavior change from the flag-only era, where the default was C only.
    #[test]
    fn no_configuration_enables_both_chains() {
        let r = resolve(&[]);
        assert_eq!(
            r.chains.keys().copied().collect::<Vec<_>>(),
            [Chain::C, Chain::P]
        );
        assert_eq!(r.network, Network::Mainnet);
        assert_eq!(r.log_level, LogLevel::Info);
        assert_eq!(r.server.addr.to_string(), "127.0.0.1:8545");
        assert_eq!(r.server.max_connections, 1024);
        assert_eq!(r.server.idle_timeout, Some(Duration::from_secs(60)));
        assert_eq!(r.server.max_blocks_per_request, 10_000);
    }

    /// The built-in per-chain layer, including the one deliberate change: the
    /// P-chain anchors at genesis, the C-chain at the tip.
    #[test]
    fn builtin_per_chain_defaults() {
        let r = resolve(&[]);
        assert_eq!(c(&r).request_interval, Duration::from_millis(40));
        assert_eq!(p(&r).request_interval, Duration::from_millis(200));
        assert_eq!(c(&r).backfill_floor, None);
        assert_eq!(p(&r).backfill_floor, Some(0));
        for cfg in r.chains.values() {
            assert_eq!(cfg.max_wait, Duration::from_secs(65 * 60));
            assert_eq!(cfg.ws_idle_timeout, Duration::from_secs(120));
            assert_eq!(cfg.summary_period, Duration::from_secs(300));
            assert_eq!(cfg.prefetch_delay_cap, Duration::ZERO);
            assert!(!cfg.ingest_logs);
            assert_eq!(cfg.join_buffer_cap, 8192);
            assert_eq!(cfg.poll_interval, Duration::from_secs(1));
            assert_eq!(cfg.concurrency, 8);
            assert!(!cfg.subscribe_blocks);
        }
    }

    // -- precedence ---------------------------------------------------------

    /// Every layer in turn overrides the one below it, on the same key.
    #[test]
    fn precedence_runs_builtin_defaults_chain_env_flag_set() {
        // built-in
        assert_eq!(c(&resolve(&[])).summary_period, Duration::from_secs(300));

        // [defaults] beats built-in, for every chain
        let r = resolve_file("[defaults]\nsummary_period = \"1m\"\n", &[]).unwrap();
        assert_eq!(c(&r).summary_period, Duration::from_secs(60));
        assert_eq!(p(&r).summary_period, Duration::from_secs(60));

        // [chains.<x>] beats [defaults], for that chain only
        let r = resolve_file(
            "[defaults]\nsummary_period = \"1m\"\n[chains.c]\n[chains.p]\nsummary_period = \"2m\"\n",
            &[],
        )
        .unwrap();
        assert_eq!(c(&r).summary_period, Duration::from_secs(60));
        assert_eq!(p(&r).summary_period, Duration::from_secs(120));

        // environment beats the file
        let file = "[chains.c]\nrpc_url = \"http://from-file\"\n[chains.p]\n";
        let r = resolve_file_env(file, &[], &[("NEVE_RPC_URL", "http://from-env")]).unwrap();
        assert_eq!(c(&r).rpc_url, "http://from-env");

        // a flag beats the environment
        let r = resolve_file_env(
            file,
            &["--rpc-url", "http://from-flag"],
            &[("NEVE_RPC_URL", "http://from-env")],
        )
        .unwrap();
        assert_eq!(c(&r).rpc_url, "http://from-flag");

        // --set beats everything
        let r = resolve_file_env(
            file,
            &[
                "--rpc-url",
                "http://from-flag",
                "--set",
                "chains.c.rpc_url=http://from-set",
            ],
            &[("NEVE_RPC_URL", "http://from-env")],
        )
        .unwrap();
        assert_eq!(c(&r).rpc_url, "http://from-set");
    }

    /// A deprecated flag or environment variable still works, and says so once.
    #[test]
    fn deprecated_inputs_warn_once() {
        let r = resolve_env(
            &["--p-request-interval", "0"],
            &[("NEVE_P_RPC_URL", "http://p.local")],
        )
        .unwrap();
        assert_eq!(p(&r).request_interval, Duration::ZERO);
        assert_eq!(p(&r).rpc_url, "http://p.local");
        let warnings: Vec<&String> = r
            .notices
            .iter()
            .filter_map(|n| match n {
                Notice::Warn(m) => Some(m),
                Notice::Info(_) => None,
            })
            .collect();
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings.iter().any(|m| m.contains("NEVE_P_RPC_URL")));
        assert!(
            warnings
                .iter()
                .any(|m| m.contains("--p-request-interval") && m.contains("request_interval"))
        );
    }

    // -- --set --------------------------------------------------------------

    #[test]
    fn set_reaches_a_nested_key_and_creates_missing_tables() {
        let r = resolve(&["--set", "chains.p.concurrency=32"]);
        assert_eq!(p(&r).concurrency, 32);
        let r = resolve(&["--set", "server.max_connections=4096"]);
        assert_eq!(r.server.max_connections, 4096);
    }

    /// A typo'd `--set` key is a startup error that names the key, rather than a
    /// setting that silently does nothing.
    #[test]
    fn set_with_an_unknown_key_is_an_error_naming_it() {
        let err = resolve_env(&["--set", "chains.p.concurency=32"], &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("chains.p.concurency=32"), "{err}");

        let err = resolve_env(&["--set", "upstreem.base=http://h"], &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("upstreem.base=http://h"), "{err}");

        // Deep enough to prove the error survives the nesting.
        let err = format!(
            "{:#}",
            resolve_env(&["--set", "chains.x.rpc_url=http://h"], &[]).unwrap_err()
        );
        assert!(err.contains("unknown variant"), "{err}");
    }

    #[test]
    fn set_values_parse_as_toml_then_fall_back_to_strings() {
        assert_eq!(parse_scalar("12"), Value::Integer(12));
        assert_eq!(parse_scalar("true"), Value::Boolean(true));
        assert_eq!(parse_scalar("\"12\""), Value::String("12".to_owned()));
        // Not valid TOML on its own — exactly the cases that must not need
        // shell quoting.
        assert_eq!(
            parse_scalar("http://h:8545"),
            Value::String("http://h:8545".to_owned())
        );
        assert_eq!(parse_scalar("40ms"), Value::String("40ms".to_owned()));
        assert_eq!(parse_scalar("tip"), Value::String("tip".to_owned()));
    }

    #[test]
    fn set_without_an_equals_sign_is_rejected() {
        let err = resolve_env(&["--set", "network"], &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("KEY=VALUE"), "{err}");
    }

    // -- scalar shapes ------------------------------------------------------

    #[test]
    fn durations_accept_a_string_or_a_bare_integer() {
        let r = resolve_file("[defaults]\nsummary_period = 90\n", &[]).unwrap();
        assert_eq!(c(&r).summary_period, Duration::from_secs(90));
        let r = resolve_file("[defaults]\nsummary_period = \"90s\"\n", &[]).unwrap();
        assert_eq!(c(&r).summary_period, Duration::from_secs(90));
        let r = resolve_file("[defaults]\nsummary_period = \"1m 30s\"\n", &[]).unwrap();
        assert_eq!(c(&r).summary_period, Duration::from_secs(90));

        // Units compose, with or without the spaces. This is the form the
        // example file and the key docs advertise, so it is pinned here: a
        // documented spelling that does not parse is worse than none.
        let r = resolve_file("[defaults]\nsummary_period = \"1h2m50ms\"\n", &[]).unwrap();
        assert_eq!(
            c(&r).summary_period,
            Duration::from_secs(3720) + Duration::from_millis(50),
        );

        // The toml error carries the offending line, so the operator is told
        // which key they mistyped and where.
        let err = format!(
            "{:#}",
            resolve_file("[defaults]\nsummary_period = \"90 fortnights\"\n", &[]).unwrap_err()
        );
        assert!(err.contains("summary_period"), "{err}");
    }

    #[test]
    fn backfill_floor_accepts_an_integer_or_tip() {
        let r = resolve_file("[chains.c]\nbackfill_floor = 12345\n", &[]).unwrap();
        assert_eq!(c(&r).backfill_floor, Some(12345));
        let r = resolve_file("[chains.p]\nbackfill_floor = \"tip\"\n", &[]).unwrap();
        assert_eq!(p(&r).backfill_floor, None);

        let err = format!(
            "{:#}",
            resolve_file("[chains.c]\nbackfill_floor = \"genesis\"\n", &[]).unwrap_err()
        );
        assert!(err.contains("tip"), "{err}");
    }

    #[test]
    fn durations_round_trip_through_the_printed_form() {
        for d in [
            Duration::ZERO,
            Duration::from_millis(40),
            Duration::from_millis(1500),
            Duration::from_secs(90),
            Duration::from_secs(300),
            Duration::from_secs(3900),
            Duration::from_secs(7200),
        ] {
            let text = fmt_duration(d);
            assert_eq!(parse_human_duration(&text).unwrap(), d, "{text}");
        }
        assert_eq!(fmt_duration(Duration::from_secs(3900)), "65m");
        assert_eq!(fmt_duration(Duration::from_secs(7200)), "2h");
    }

    #[test]
    fn idle_timeout_zero_disables_the_reaper() {
        let r = resolve_file("[server]\nidle_timeout = 0\n", &[]).unwrap();
        assert_eq!(r.server.idle_timeout, None);
    }

    // -- URLs ---------------------------------------------------------------

    /// Each chain derives its own endpoints from one `base`, and the P-chain
    /// never gets a WebSocket against avalanchego (it has no upstream push
    /// mechanism to subscribe to).
    #[test]
    fn per_chain_endpoints() {
        let r = resolve(&["--network", "testnet"]);
        assert_eq!(c(&r).rpc_url, "https://api.avax-test.network/ext/bc/C/rpc");
        assert_eq!(c(&r).ws_url, "wss://api.avax-test.network/ext/bc/C/ws");
        assert_eq!(p(&r).rpc_url, "https://api.avax-test.network/ext/bc/P");
        assert!(
            p(&r).ws_url.is_empty(),
            "the P-chain has no upstream WebSocket"
        );

        // One `base` re-points every chain at a private host.
        let r = resolve_file("[upstream]\nbase = \"http://node.local:9650/\"\n", &[]).unwrap();
        assert_eq!(c(&r).rpc_url, "http://node.local:9650/ext/bc/C/rpc");
        assert_eq!(c(&r).ws_url, "ws://node.local:9650/ext/bc/C/ws");
        assert_eq!(p(&r).rpc_url, "http://node.local:9650/ext/bc/P");

        // An explicit per-chain URL always wins over derivation.
        let r = resolve(&[
            "--rpc-url",
            "http://c.local",
            "--p-rpc-url",
            "http://p.local",
        ]);
        assert_eq!(c(&r).rpc_url, "http://c.local");
        assert_eq!(p(&r).rpc_url, "http://p.local");
    }

    /// `--mirror-from` points every chain at the one upstream neve endpoint,
    /// overriding the per-chain derivation — chains are told apart there by
    /// method namespace, not by URL — and is exactly `kind = "neve"` + `base`.
    #[test]
    fn mirror_from_overrides_every_chain() {
        let r = resolve(&["--mirror-from", "http://10.0.0.5:8545/"]);
        for cfg in r.chains.values() {
            assert_eq!(cfg.rpc_url, "http://10.0.0.5:8545");
            assert_eq!(cfg.ws_url, "ws://10.0.0.5:8545");
            // A neve upstream is unthrottled, serves `newBlocks`, and reports
            // its own retained range, so the floor comes from its /health.
            assert_eq!(cfg.request_interval, Duration::ZERO);
            assert!(cfg.subscribe_blocks);
            assert_eq!(cfg.backfill_floor, None);
        }
        assert_eq!(r.upstream.kind, UpstreamKind::Neve);
        assert_eq!(r.upstream.base, "http://10.0.0.5:8545");
        assert!(r.upstream.host_pacer.is_none());

        // ... and the file spelling resolves identically.
        let from_file = resolve_file(
            "[upstream]\nkind = \"neve\"\nbase = \"http://10.0.0.5:8545/\"\n",
            &[],
        )
        .unwrap();
        assert_eq!(from_file.upstream.base, r.upstream.base);
        assert_eq!(c(&from_file).rpc_url, c(&r).rpc_url);
        assert_eq!(c(&from_file).ws_url, c(&r).ws_url);
    }

    #[test]
    fn derive_ws_url_maps_schemes() {
        assert_eq!(derive_ws_url("https://h:1").unwrap(), "wss://h:1");
        assert_eq!(derive_ws_url("http://h:1").unwrap(), "ws://h:1");
        assert_eq!(derive_ws_url("wss://h:1").unwrap(), "wss://h:1");
        assert!(derive_ws_url("h:1").is_err());
    }

    // -- token --------------------------------------------------------------

    #[test]
    fn token_is_appended_to_every_url_shape() {
        let token = Token::new("secret");
        // No query at all.
        assert_eq!(
            with_token("https://h/ext/bc/P", "token", &token),
            "https://h/ext/bc/P?token=secret"
        );
        // A query is already there.
        assert_eq!(
            with_token("https://h/p?a=1", "token", &token),
            "https://h/p?a=1&token=secret"
        );
        // The parameter is already there: leave the URL exactly as written.
        assert_eq!(
            with_token("https://h/p?token=other", "token", &token),
            "https://h/p?token=other"
        );
        assert_eq!(
            with_token("https://h/p?a=1&token=other&b=2", "token", &token),
            "https://h/p?a=1&token=other&b=2"
        );
        // A bare `?` is a query slot, not a query.
        assert_eq!(
            with_token("https://h/p?", "token", &token),
            "https://h/p?token=secret"
        );
        // The P-chain's absent WebSocket stays absent.
        assert_eq!(with_token("", "token", &token), "");
    }

    #[test]
    fn token_from_a_file_lands_on_every_chain_url() {
        let dir = crate::test_support::unique_temp_dir("config-token");
        std::fs::create_dir_all(&dir).unwrap();
        let token_path = dir.join("token");
        std::fs::write(&token_path, "s3cr3t\n").unwrap();
        let r = resolve_file(
            &format!(
                "[upstream]\ntoken_file = {:?}\n",
                token_path.display().to_string()
            ),
            &[],
        )
        .unwrap();
        assert!(c(&r).rpc_url.ends_with("?token=s3cr3t"));
        assert!(c(&r).ws_url.ends_with("?token=s3cr3t"));
        assert!(p(&r).rpc_url.ends_with("?token=s3cr3t"));
        assert!(p(&r).ws_url.is_empty());
    }

    #[test]
    fn token_comes_from_the_environment_when_no_file_is_configured() {
        let r = resolve_env(&[], &[("NEVE_UPSTREAM_TOKEN", " s3cr3t\n")]).unwrap();
        assert_eq!(r.upstream.token.as_ref().map(Token::expose), Some("s3cr3t"));
    }

    /// The whole point of the newtype: no formatting path reaches the value.
    /// Not just the `Token` field — by resolution time the token has been
    /// appended to every URL, and `upstream.base` may have carried one all
    /// along, so the check is against the whole `Resolved`.
    #[test]
    fn the_token_never_appears_in_debug_or_printed_output() {
        let r = resolve_env(&[], &[("NEVE_UPSTREAM_TOKEN", "s3cr3t")]).unwrap();
        let debug = format!("{r:?}");
        assert!(!debug.contains("s3cr3t"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert_eq!(format!("{}", Token::new("s3cr3t")), "<redacted>");

        // A token written into `base` by hand is redacted the same way.
        let hand_written =
            resolve_file("[upstream]\nbase = \"https://h?token=s3cr3t\"\n", &[]).unwrap();
        let debug = format!("{hand_written:?}");
        assert!(!debug.contains("s3cr3t"), "{debug}");
        assert!(
            !hand_written.to_redacted_toml().unwrap().contains("s3cr3t"),
            "token in printed config",
        );

        // The resolved URLs *do* carry it, which is exactly why the printed
        // form redacts query strings.
        assert!(c(&r).rpc_url.contains("s3cr3t"));
        let printed = r.to_redacted_toml().unwrap();
        assert!(!printed.contains("s3cr3t"), "{printed}");
        assert!(
            printed.contains("<redacted, from NEVE_UPSTREAM_TOKEN>"),
            "{printed}"
        );
    }

    // -- host pacer ---------------------------------------------------------

    #[test]
    fn max_rps_defaults_to_the_public_cap_unless_a_token_or_a_neve_upstream() {
        let r = resolve(&[]);
        assert_eq!(r.upstream.max_rps, Some(25.0));
        assert!(r.upstream.host_pacer.is_some());

        let r = resolve_env(&[], &[("NEVE_UPSTREAM_TOKEN", "s3cr3t")]).unwrap();
        assert_eq!(r.upstream.max_rps, None);
        assert!(r.upstream.host_pacer.is_none());

        let r = resolve(&["--mirror-from", "http://h:8545"]);
        assert_eq!(r.upstream.max_rps, None);

        // Explicit wins in either direction.
        let r = resolve(&["--set", "upstream.max_rps=50"]);
        assert_eq!(r.upstream.max_rps, Some(50.0));
        let r = resolve(&["--set", "upstream.max_rps=0"]);
        assert_eq!(r.upstream.max_rps, None);
        assert!(r.upstream.host_pacer.is_none());
        // An integer in the file is as good as a float.
        let r = resolve_file("[upstream]\nmax_rps = 7\n", &[]).unwrap();
        assert_eq!(r.upstream.max_rps, Some(7.0));
    }

    // -- chain selection ----------------------------------------------------

    #[test]
    fn chains_flag_selects_a_subset_and_enables_chains_the_file_omits() {
        let r = resolve(&["--chains", "p"]);
        assert_eq!(r.chains.keys().copied().collect::<Vec<_>>(), [Chain::P]);

        // The file declares only the C-chain; the flag adds P on pure defaults
        // and drops C.
        let r = resolve_file(
            "[chains.c]\nrequest_interval = \"1s\"\n",
            &["--chains", "p"],
        )
        .unwrap();
        assert_eq!(r.chains.keys().copied().collect::<Vec<_>>(), [Chain::P]);
        assert_eq!(p(&r).request_interval, Duration::from_millis(200));

        // Order and duplicates don't change the instance layout.
        let r = resolve(&["--chains", "p,c,p"]);
        assert_eq!(
            r.chains.keys().copied().collect::<Vec<_>>(),
            [Chain::C, Chain::P]
        );
    }

    #[test]
    fn a_chains_table_in_the_file_is_the_enabled_set() {
        let r = resolve_file("[chains.p]\n", &[]).unwrap();
        assert_eq!(r.chains.keys().copied().collect::<Vec<_>>(), [Chain::P]);

        // A [defaults] table alone does not restrict anything.
        let r = resolve_file("[defaults]\nsummary_period = \"1m\"\n", &[]).unwrap();
        assert_eq!(r.chains.len(), 2);
    }

    /// A deprecated C-chain flag materializes `[chains.c]` in the document, and
    /// must not thereby become a chain selector.
    #[test]
    fn a_c_chain_flag_does_not_disable_the_p_chain() {
        let r = resolve(&["--rpc-url", "http://c.local"]);
        assert_eq!(r.chains.len(), 2);
    }

    /// A flag naming no chain applies to every chain this run serves, and beats
    /// the file everywhere — including a chain that names the same key in its
    /// own table. Landing such a flag in `[defaults]` instead would put it
    /// *below* `[chains.<x>]` in the precedence order, so the file would
    /// silently win and the flag would be a no-op.
    #[test]
    fn a_chainless_flag_beats_a_per_chain_key() {
        let file = "[chains.c]\nsummary_period = \"1m\"\nmax_wait = \"10m\"\n[chains.p]\n";
        let r = resolve_file(file, &["--summary-period", "30s", "--max-wait", "70m"]).unwrap();
        for cfg in r.chains.values() {
            assert_eq!(cfg.summary_period, Duration::from_secs(30));
            assert_eq!(cfg.max_wait, Duration::from_secs(70 * 60));
        }

        // Same for the one *visible* flag of this shape, which is the case an
        // operator is most likely to hit.
        let r = resolve_file("[chains.c]\ningest_logs = false\n", &["--ingest-logs"]).unwrap();
        assert!(c(&r).ingest_logs);
    }

    /// `ingest_logs` narrows to the chains that have logs at resolve time, so
    /// `--print-config` reports what the run will actually do.
    #[test]
    fn ingest_logs_is_a_c_chain_setting_however_it_is_set() {
        let r = resolve_file("[defaults]\ningest_logs = true\n", &[]).unwrap();
        assert!(c(&r).ingest_logs);
        assert!(
            !p(&r).ingest_logs,
            "the P-chain has no event logs to ingest"
        );
        assert!(
            r.to_redacted_toml()
                .unwrap()
                .contains("ingest_logs = false")
        );
    }

    // -- enabling and disabling chains --------------------------------------

    /// `enabled = false` turns a chain off while keeping its settings, and
    /// `--chains` overrides that for one run.
    #[test]
    fn enabled_false_disables_a_chain_and_chains_flag_overrides_it() {
        let file = "[chains.c]\n[chains.p]\nenabled = false\nconcurrency = 32\n";
        let r = resolve_file(file, &[]).unwrap();
        assert_eq!(r.chains.keys().copied().collect::<Vec<_>>(), [Chain::C]);

        // The selector wins, and the disabled block's tuning comes back with it.
        let r = resolve_file(file, &["--chains", "c,p"]).unwrap();
        assert_eq!(r.chains.len(), 2);
        assert_eq!(p(&r).concurrency, 32);

        // `--set` reaches the same key, in both directions.
        let r = resolve_file(file, &["--set", "chains.p.enabled=true"]).unwrap();
        assert_eq!(r.chains.len(), 2);
        let r = resolve(&["--set", "chains.c.enabled=false"]);
        assert_eq!(r.chains.keys().copied().collect::<Vec<_>>(), [Chain::P]);
    }

    #[test]
    fn disabling_every_chain_is_an_error() {
        let err = format!(
            "{:#}",
            resolve_file("[chains.c]\nenabled = false\n", &[]).unwrap_err()
        );
        assert!(err.contains("every chain is disabled"), "{err}");
    }

    /// `enabled` in `[defaults]` could only mean "serve nothing", so it is
    /// rejected rather than silently ignored by the per-chain merge.
    #[test]
    fn enabled_is_rejected_in_defaults() {
        let err = format!(
            "{:#}",
            resolve_file("[defaults]\nenabled = false\n", &[]).unwrap_err()
        );
        assert!(err.contains("defaults.enabled"), "{err}");
    }

    /// A `--set` for a chain this run does not serve would validate and then do
    /// nothing, because the chain set is fixed before the overrides merge.
    #[test]
    fn a_set_for_an_idle_chain_is_refused() {
        let err = format!(
            "{:#}",
            resolve_file("[chains.c]\n", &["--set", "chains.p.concurrency=4"]).unwrap_err()
        );
        assert!(err.contains("does not serve"), "{err}");
        assert!(err.contains("--chains p"), "{err}");

        // ...but naming the chain alongside it is exactly the fix suggested.
        let r = resolve_file(
            "[chains.c]\n",
            &["--chains", "c,p", "--set", "chains.p.concurrency=4"],
        )
        .unwrap();
        assert_eq!(p(&r).concurrency, 4);
    }

    // -- the host request cap -----------------------------------------------

    /// The cap follows the host it describes. A chain pointed at a private node
    /// is not spending the public endpoint's per-IP budget, so charging it
    /// against the cap would throttle a private fill to the public rate.
    /// A token spelled into a URL is still a bypass token. Missing that would
    /// apply the untokened public cap to an upstream with no limit to respect —
    /// silently, and at a quarter of the configured rate.
    #[test]
    fn a_token_written_into_a_url_also_lifts_the_cap() {
        let r = resolve_file(
            "[chains.c]\nrpc_url = \"https://api.avax.network/ext/bc/C/rpc?token=abc\"\n",
            &["--chains", "c"],
        )
        .unwrap();
        assert_eq!(r.upstream.max_rps, None);

        // The parameter name follows `token_param`, so a renamed one still counts.
        let file =
            "[upstream]\ntoken_param = \"key\"\n[chains.c]\nrpc_url = \"http://n/rpc?key=abc\"\n";
        assert_eq!(
            resolve_file(file, &["--chains", "c"])
                .unwrap()
                .upstream
                .max_rps,
            None,
        );

        // A URL with some other query is not a token, and stays capped.
        let file = "[chains.c]\nrpc_url = \"https://api.avax.network/ext/bc/C/rpc?trace=1\"\n";
        assert_eq!(
            resolve_file(file, &["--chains", "c"])
                .unwrap()
                .upstream
                .max_rps,
            Some(PUBLIC_MAX_RPS),
        );
    }

    #[test]
    fn the_host_cap_skips_a_chain_pointed_somewhere_else() {
        let r = resolve_file(
            "[chains.c]\n[chains.p]\nrpc_url = \"http://node.local:9650/ext/bc/P\"\n",
            &[],
        )
        .unwrap();
        assert!(c(&r).host_pacer.is_some(), "the C-chain is on the host");
        assert!(p(&r).host_pacer.is_none(), "the P-chain is not");
        assert!(
            r.notices.iter().any(|n| matches!(
                n,
                Notice::Info(m) if m.contains("node.local:9650") && m.contains("does not apply")
            )),
            "the operator is told which chain the cap stopped covering",
        );

        // Same host, explicit URL: still capped, and still the *same* pacer.
        let r = resolve_file(
            "[chains.c]\n[chains.p]\nrpc_url = \"https://api.avax.network/ext/bc/P\"\n",
            &[],
        )
        .unwrap();
        let (a, b) = (
            c(&r).host_pacer.as_ref().unwrap(),
            p(&r).host_pacer.as_ref().unwrap(),
        );
        assert!(
            Arc::ptr_eq(a, b),
            "one budget shared, not one budget each — the limit is per host",
        );
    }

    #[test]
    fn an_unknown_chain_key_is_rejected() {
        let err = format!("{:#}", resolve_file("[chains.x]\n", &[]).unwrap_err());
        assert!(
            err.contains("chains.x") || err.contains("unknown variant"),
            "{err}"
        );
    }

    // -- data dirs ----------------------------------------------------------

    /// The C-chain store stays at the base itself; the P-chain nests under it,
    /// and a per-chain `data_dir` names that chain's directory exactly.
    #[test]
    fn per_chain_data_dirs() {
        let r = resolve(&["--data-dir", "/srv/neve"]);
        assert_eq!(c(&r).data_dir, PathBuf::from("/srv/neve"));
        assert_eq!(p(&r).data_dir, PathBuf::from("/srv/neve/p"));

        let r = resolve(&["--data-dir", "/srv/neve", "--p-data-dir", "/mnt/big/p"]);
        assert_eq!(c(&r).data_dir, PathBuf::from("/srv/neve"));
        assert_eq!(p(&r).data_dir, PathBuf::from("/mnt/big/p"));

        // The default base follows the network.
        let r = resolve(&["--network", "testnet"]);
        assert_eq!(c(&r).data_dir, PathBuf::from("./blockstore-data-testnet"));
        assert_eq!(p(&r).data_dir, PathBuf::from("./blockstore-data-testnet/p"));
    }

    #[test]
    fn two_chains_may_not_share_a_data_dir() {
        let err = resolve_file(
            "[chains.c]\ndata_dir = \"/srv/x\"\n[chains.p]\ndata_dir = \"/srv/x\"\n",
            &[],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("same data dir"), "{err}");
    }

    #[test]
    fn per_chain_backfill_floors() {
        let r = resolve(&["--backfill-floor", "10", "--p-backfill-floor", "20"]);
        assert_eq!(c(&r).backfill_floor, Some(10));
        assert_eq!(p(&r).backfill_floor, Some(20));
        // Without a flag each chain keeps its own default.
        let r = resolve(&["--backfill-floor", "10"]);
        assert_eq!(c(&r).backfill_floor, Some(10));
        assert_eq!(p(&r).backfill_floor, Some(0));
    }

    // -- file handling ------------------------------------------------------

    #[test]
    fn an_unknown_file_key_is_rejected_with_its_position() {
        let err = format!(
            "{:#}",
            resolve_file("[server]\nadr = \"127.0.0.1:1\"\n", &[]).unwrap_err()
        );
        assert!(err.contains("adr"), "{err}");
    }

    #[test]
    fn an_explicitly_requested_config_file_must_exist() {
        let err = resolve_env(&["--config", "/nonexistent/neve.toml"], &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("/nonexistent/neve.toml"), "{err}");
    }

    #[test]
    fn neve_config_names_the_file_like_the_flag() {
        let dir = crate::test_support::unique_temp_dir("config-env");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "[chains.p]\nconcurrency = 3\n").unwrap();
        let r = resolve_env(&[], &[("NEVE_CONFIG", &path.display().to_string())]).unwrap();
        assert_eq!(r.chains.keys().copied().collect::<Vec<_>>(), [Chain::P]);
        assert_eq!(p(&r).concurrency, 3);
    }

    // -- printing -----------------------------------------------------------

    #[test]
    fn print_modes_are_reported() {
        assert_eq!(resolve(&[]).print, None);
        assert_eq!(resolve(&["--print-config"]).print, Some(PrintMode::Config));
        assert_eq!(
            resolve(&["--print-config-example"]).print,
            Some(PrintMode::Example)
        );
    }

    /// The printed form parses back as a config file (bar the redactions it
    /// documents), and reproduces the same settings.
    #[test]
    fn printed_config_is_a_config_file() {
        let r = resolve(&["--network", "testnet", "--set", "chains.p.concurrency=32"]);
        let printed = r.to_redacted_toml().unwrap();
        let round = resolve_file(&printed, &[]).unwrap();
        assert_eq!(round.network, Network::Testnet);
        assert_eq!(p(&round).concurrency, 32);
        assert_eq!(c(&round).request_interval, c(&r).request_interval);
        assert_eq!(p(&round).backfill_floor, p(&r).backfill_floor);
    }

    // -- the example file, and its drift alarm ------------------------------
    //
    // `deploy/config.toml.example` is compiled in (`EXAMPLE_CONFIG`) and is the
    // only human-readable statement of what neve's defaults *are*. Three tests
    // keep it from rotting into a lie, each catching a different way it can:
    //
    //   * a key removed or renamed in the schema  -> `example_parses_...`
    //   * a key *added* to the schema             -> `example_documents_...`
    //   * a default value changed                 -> `example_resolves_...`
    //
    // The first and third are the two the design called for; the second exists
    // because equality alone cannot see an added key. A field added to
    // `ChainFile` and forgotten here resolves identically on both sides of the
    // comparison — both take the built-in — so only a key-set check notices.

    /// Every key in the example is one the schema accepts, at a value it
    /// accepts. `deny_unknown_fields` makes this catch a key the schema no
    /// longer has, and the custom scalar parsers make it catch a value whose
    /// spelling has changed.
    #[test]
    fn example_parses_under_the_deny_unknown_fields_schema() {
        let file: File = toml::from_str(EXAMPLE_CONFIG)
            .unwrap_or_else(|e| panic!("deploy/config.toml.example does not parse:\n{e}"));
        // Both chains, so the example matches the no-file default enabled set.
        let chains = file.chains.expect("the example declares [chains.*] tables");
        assert_eq!(
            chains.keys().copied().collect::<Vec<_>>(),
            [Chain::C, Chain::P]
        );
    }

    /// Which field names a table of the schema accepts, taken from the schema
    /// itself rather than from a list someone has to remember to update:
    /// `deny_unknown_fields` makes serde enumerate them in the error it raises
    /// for a key it does not know, and that error is generated from the struct.
    /// Add a field to `ChainFile` and it appears here on the next build.
    fn schema_keys(table_header: &str) -> Vec<String> {
        let doc = format!("{table_header}zzz_not_a_real_key = 1\n");
        let err = toml::from_str::<File>(&doc)
            .expect_err("an unknown key must be rejected")
            .to_string();
        let (_, list) = err
            .split_once("expected one of ")
            .unwrap_or_else(|| panic!("serde no longer enumerates fields:\n{err}"));
        list.trim()
            .split(',')
            .map(|f| f.trim().trim_matches('`').to_owned())
            .collect()
    }

    /// Every key the schema accepts is written down in the example.
    ///
    /// This is the assertion that catches a *new* key: an undocumented one
    /// resolves to its built-in on both sides of
    /// `example_resolves_to_the_builtin_defaults`, so that test stays green
    /// while the reference file silently stops being a reference.
    #[test]
    fn example_documents_every_key_the_schema_accepts() {
        /// Keys the example documents as a commented-out line rather than a
        /// live value, for two reasons.
        ///
        /// `token_file` has no default at all: a live line would name a path
        /// that does not exist on the box resolving it, and would flip
        /// `upstream.max_rps` off besides. The rest are *derived* — their
        /// default comes from `network` or `upstream.base` — so writing them
        /// live would pin a copy of this file to mainnet, and `--network
        /// testnet` against that copy would change the label while leaving
        /// every URL and store path pointed at mainnet.
        ///
        /// The `contains` check below still requires the commented line, so a
        /// derived key cannot go undocumented either.
        const NO_LIVE_DEFAULT: &[&str] = &[
            "token_file",
            "base",
            "max_rps",
            "rpc_url",
            "ws_url",
            "data_dir",
        ];

        let example: Table = toml::from_str(EXAMPLE_CONFIG).unwrap();
        let keys_of = |table: Option<&Value>| -> Vec<String> {
            table
                .and_then(Value::as_table)
                .map(|t| t.keys().cloned().collect())
                .unwrap_or_default()
        };

        let mut checked: u32 = 0;
        let mut check = |scope: &str, want: Vec<String>, have: &[String]| {
            for key in want {
                if NO_LIVE_DEFAULT.contains(&key.as_str()) {
                    // "#key = ", not "# key = ": the example spells a disabled
                    // setting without the space, so prose and settings are
                    // distinguishable on sight.
                    assert!(
                        EXAMPLE_CONFIG.contains(&format!("\n#{key} = ")),
                        "`{scope}{key}` is derived or has no default, so the \
                         example must document it as a disabled `#{key} = …` \
                         line rather than pin it to a live value",
                    );
                    continue;
                }
                assert!(
                    have.contains(&key),
                    "`{scope}{key}` is missing from deploy/config.toml.example — \
                     add it, at its built-in default, with a comment saying what \
                     it does",
                );
                checked = checked.saturating_add(1);
            }
        };

        let top: Vec<String> = example.keys().cloned().collect();
        check("", schema_keys(""), &top);
        check(
            "upstream.",
            schema_keys("[upstream]\n"),
            &keys_of(example.get("upstream")),
        );
        check(
            "server.",
            schema_keys("[server]\n"),
            &keys_of(example.get("server")),
        );

        // A per-chain key may be written in [defaults] or in the chain's own
        // table; either documents it. The two are checked separately rather
        // than unioned so a chain-specific default (`request_interval`,
        // `backfill_floor`) cannot be documented only under the chain whose
        // value happens to be prettier.
        let chains = keys_of(example.get("chains"));
        assert_eq!(chains, ["c", "p"], "the example must show every chain");
        let per_chain = schema_keys("[defaults]\n");
        let defaults = keys_of(example.get("defaults"));
        for chain in &chains {
            let mut have = defaults.clone();
            have.extend(keys_of(
                example
                    .get("chains")
                    .and_then(Value::as_table)
                    .and_then(|t| t.get(chain)),
            ));
            check(&format!("chains.{chain}."), per_chain.clone(), &have);
        }
        assert!(
            checked > 30,
            "only {checked} keys checked; extraction broke"
        );
    }

    /// Compare two rendered configurations line by line, naming the key that
    /// drifted. `assert_eq!` on the two whole documents would be one line of
    /// code, but it fails with two escaped kilobyte-long strings printed side
    /// by side and leaves the reader to diff them by eye — which is how a
    /// failing drift test gets "fixed" by deleting it.
    fn assert_same_config(example: &str, builtin: &str) {
        let (a, b): (Vec<&str>, Vec<&str>) = (example.lines().collect(), builtin.lines().collect());
        let mut section = "(top level)";
        for i in 0..a.len().max(b.len()) {
            let l = a.get(i).copied().unwrap_or("<end of file>");
            let r = b.get(i).copied().unwrap_or("<end of file>");
            if l.starts_with('[') && l == r {
                section = l;
            }
            assert_eq!(
                l, r,
                "deploy/config.toml.example no longer states neve's defaults, \
                 in {section}: the example resolves to `{l}`, neve with no \
                 config file at all resolves to `{r}`. Fix whichever is wrong \
                 — if the default moved on purpose, the example moves with it.",
            );
        }
    }

    /// Resolving the example produces exactly what resolving nothing produces:
    /// the file documents the *true* defaults, not the ones they used to be.
    ///
    /// The comparison is structural, field by field, via `to_redacted_toml` —
    /// which serializes every resolved knob of `upstream`, `server` and each
    /// chain — plus explicit asserts for the handful of fields that render
    /// awkwardly there. It deliberately does not compare `notices` (the
    /// host-cap line names a different *reason* when `max_rps` is written out
    /// than when it is inferred, though the number is the same) or
    /// `host_pacer`, which is an `Arc` with interior state; `max_rps`, the
    /// value the pacer is built from, is compared instead.
    ///
    /// Nothing host-dependent varies between the two sides: both resolve with
    /// the same empty synthetic environment, no `/etc/neve/config.toml`, and no
    /// flags, so `network` (and with it the default base URL and data dir) is
    /// mainnet on both.
    #[test]
    fn example_resolves_to_the_builtin_defaults() {
        let from_example = resolve_file(EXAMPLE_CONFIG, &[]).unwrap();
        let bare = resolve(&[]);

        assert_same_config(
            &from_example.to_redacted_toml().unwrap(),
            &bare.to_redacted_toml().unwrap(),
        );

        // The same claim again for what `to_redacted_toml` does not carry.
        assert_eq!(from_example.network, bare.network);
        assert_eq!(from_example.log_level, bare.log_level);
        assert_eq!(from_example.upstream.kind, bare.upstream.kind);
        assert_eq!(from_example.upstream.max_rps, bare.upstream.max_rps);
        assert_eq!(
            from_example.upstream.host_pacer.is_some(),
            bare.upstream.host_pacer.is_some()
        );
        assert_eq!(from_example.server.idle_timeout, bare.server.idle_timeout);
        assert_eq!(
            from_example.chains.keys().collect::<Vec<_>>(),
            bare.chains.keys().collect::<Vec<_>>(),
        );
        for (chain, want) in &bare.chains {
            let got = from_example.chains.get(chain).expect("same enabled set");
            assert_eq!(got.subscribe_blocks, want.subscribe_blocks);
            assert_eq!(got.data_dir, want.data_dir);
            assert_eq!(got.ws_url, want.ws_url, "chain {}", chain.as_str());
        }
    }

    /// Every live key in the example is preceded by a comment, so a key added
    /// to it cannot arrive undocumented. Blank lines between the comment and
    /// the key are fine; a key sitting directly under a `[table]` header is
    /// not.
    #[test]
    fn every_example_key_is_documented_by_a_comment_above_it() {
        let mut previous: Option<&str> = None;
        let mut keys: u32 = 0;
        for line in EXAMPLE_CONFIG.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let is_key =
                !trimmed.starts_with('#') && !trimmed.starts_with('[') && trimmed.contains(" = ");
            if is_key {
                keys = keys.saturating_add(1);
                let above = previous.unwrap_or("");
                assert!(
                    above.trim_start().starts_with('#'),
                    "`{trimmed}` in deploy/config.toml.example has no comment \
                     above it; every key there is documentation first",
                );
            }
            previous = Some(line);
        }
        assert!(keys > 20, "only {keys} keys found; the walk is broken");
    }

    // -- validation ---------------------------------------------------------

    #[test]
    fn zero_valued_knobs_that_would_break_a_run_are_rejected() {
        for (arg, key) in [
            ("chains.p.concurrency=0", "concurrency"),
            ("defaults.join_buffer_cap=0", "join_buffer_cap"),
            ("defaults.summary_period=0", "summary_period"),
            ("server.max_connections=0", "max_connections"),
            ("server.max_blocks_per_request=0", "max_blocks_per_request"),
        ] {
            let err = resolve_env(&["--set", arg], &[]).unwrap_err().to_string();
            assert!(err.contains(key), "{arg}: {err}");
        }
    }
}
