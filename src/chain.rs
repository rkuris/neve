//! Which Avalanche chain an instance mirrors, and everything that follows from
//! that choice: upstream endpoints, on-disk location, metric label, and the
//! per-chain ingest knobs.
//!
//! One neve process runs one *instance* per selected chain (`--chains`). An
//! instance owns its own store, its own upstream connection, and its own
//! metric label; the serving socket, connection accounting, and HTTP endpoints
//! are shared. Stores stay single-chain — the `meta/chain` stamp
//! (`crate::storage`) rejects opening a C-chain store as P-chain or vice versa.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::subscribe::LiveTx;
use crate::upstream::Pacer;

/// Which Avalanche network to target. Shared across chains: a mainnet instance
/// mirrors mainnet's C-chain *and* mainnet's P-chain, never a mix.
///
/// The serde spelling is the wire/config spelling (`network = "mainnet"`), and
/// deliberately matches `as_str` — the same word names the network on the
/// command line, in the config file, and in the store's `meta` stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[clap(rename_all = "lower")]
#[serde(rename_all = "lowercase")]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }

    /// Public-endpoint host for this network. Chains differ only by path, so the
    /// host is factored out here.
    const fn host(self) -> &'static str {
        match self {
            Self::Mainnet => "api.avax.network",
            Self::Testnet => "api.avax-test.network",
        }
    }

    /// Default `--data-dir` base, per network, so swapping networks can't
    /// cross-pollinate stores.
    pub fn default_data_dir(self) -> PathBuf {
        PathBuf::from(format!("./blockstore-data-{}", self.as_str()))
    }

    /// Default `upstream.base` for this network: the public endpoint's origin,
    /// with no path. Every chain's endpoint hangs off it (`Chain::rpc_path`),
    /// which is what lets one `base` key point a whole multi-chain instance at a
    /// different host.
    pub fn default_base_url(self) -> String {
        format!("https://{}", self.host())
    }
}

/// Which chain one instance mirrors. The wire spelling (`as_str`) doubles as
/// the `chain` metric label and the on-disk `meta/chain` stamp, so it must stay
/// stable across releases.
///
/// The serde spelling is that same word, which is what makes `[chains.c]` /
/// `[chains.p]` work as config-file table keys: a typo'd key is rejected by the
/// enum ("unknown variant `x`") rather than silently ignored.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ValueEnum, Serialize, Deserialize,
)]
#[clap(rename_all = "lower")]
#[serde(rename_all = "lowercase")]
pub enum Chain {
    /// The EVM C-chain: `eth_*` JSON-RPC, `newHeads` WebSocket push.
    C,
    /// The platform P-chain: `platform.*` JSON-RPC, height polling (no push
    /// mechanism exists upstream — see `docs/p-chain-indexing-plan.md`).
    P,
}

impl Chain {
    /// Every chain neve can mirror, in `meta/chain` order. The default chain set
    /// when a config file names none, and the fan-out target for any setting
    /// that applies to all of them.
    pub const ALL: &'static [Self] = &[Self::C, Self::P];

    /// Stable short name: the `chain` metric label, the `meta/chain` stamp, and
    /// the on-disk subdirectory for non-default chains.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::C => "c",
            Self::P => "p",
        }
    }

    /// This chain's JSON-RPC path on an avalanchego host, appended to
    /// `upstream.base`. Chains differ only by path, which is why one `base` key
    /// re-points every chain at once.
    pub const fn rpc_path(self) -> &'static str {
        match self {
            Self::C => "/ext/bc/C/rpc",
            Self::P => "/ext/bc/P",
        }
    }

    /// This chain's WebSocket path on an avalanchego host, or `None` for a chain
    /// with no upstream push mechanism. The P-chain has none at all — no
    /// `eth_subscribe` analog, and the old X-chain pubsub was removed in
    /// avalanchego v1.11.13 — so a P instance polls `platform.getHeight`
    /// instead.
    pub const fn ws_path(self) -> Option<&'static str> {
        match self {
            Self::C => Some("/ext/bc/C/ws"),
            Self::P => None,
        }
    }

    /// This chain's store location under the `--data-dir` base.
    ///
    /// The C-chain sits at the base itself and every other chain gets a
    /// subdirectory. That asymmetry is load-bearing: C-chain stores in the field
    /// live at the base, and moving them would mean a resync.
    pub fn data_dir(self, base: &Path) -> PathBuf {
        match self {
            Self::C => base.to_path_buf(),
            Self::P => base.join(self.as_str()),
        }
    }

    /// Whether this chain's live path publishes the **complete record** to
    /// subscribers, and can therefore serve `newRecords` live.
    ///
    /// The P-chain writes the whole record before it announces, so it can. The
    /// C-chain deliberately announces a tip block *before* its logs are joined —
    /// so `eth_getBlockByNumber` doesn't wait on the logs round-trip — which
    /// means no complete record exists at that moment. `oldRecords` is
    /// unaffected on either chain: a stored record is complete by definition.
    pub const fn publishes_live_records(self) -> bool {
        match self {
            Self::C => false,
            Self::P => true,
        }
    }

    /// On-disk record-format version for this chain's store. Each chain
    /// numbers its own layout independently — the `meta/chain` stamp is
    /// verified alongside the version, so a C-chain `1` and a P-chain `1` can
    /// never be confused for one another.
    #[allow(
        clippy::match_same_arms,
        reason = "each chain's version is its own counter; they start equal but move independently"
    )]
    pub const fn format_version(self) -> u32 {
        match self {
            // `[block, logs]` — the combined record from the logs milestone.
            // A version-1 store may also hold bare block objects below the
            // height at which it was upgraded; `crate::record` reads both.
            Self::C => 1,
            // `[blockJSON, blockHexBytes, rewards]` — see Decision 1 in
            // `docs/p-chain-indexing-plan.md`.
            Self::P => 1,
        }
    }
}

/// Normalize a `--chains` selection into the instance layout: deduplicated and
/// ordered, so `c,p` and `p,c` build the same set of instances. Rejects an empty
/// selection — a neve with no chains would serve nothing.
pub fn normalize_chains(chains: &[Chain]) -> Result<Vec<Chain>> {
    let mut out: Vec<Chain> = Vec::new();
    for &chain in chains {
        if !out.contains(&chain) {
            out.push(chain);
        }
    }
    if out.is_empty() {
        bail!("--chains selected no chains; there would be nothing to serve");
    }
    out.sort_unstable();
    Ok(out)
}

/// Per-chain runtime knobs, available deep in that chain's ingest/backfill
/// paths. One of these per running instance, built from that chain's
/// `crate::config::ChainCfg`.
///
/// Two fields are deliberately *not* per-chain, because the thing they describe
/// isn't: `fatal`, so any chain's unrecoverable condition brings the whole
/// process down rather than leaving a half-serving mirror, and `host_pacer`,
/// because the upstream's rate limit applies to the host rather than to each
/// chain path separately.
#[derive(Clone)]
pub struct IngestCfg {
    /// Which chain this config drives — the metric label and the dialect
    /// selector for the ingest/serving paths.
    pub chain: Chain,
    pub max_wait: Duration,
    /// Reconnect the WebSocket if no `newHeads` arrive within this window.
    /// C-chain only (the P-chain has no upstream socket).
    pub ws_idle_timeout: Duration,
    /// Upstream WebSocket endpoint, or empty for a chain that polls instead.
    pub ws_url: String,
    pub rpc_url: String,
    /// How long the P-chain tip poller waits between `platform.getHeight`
    /// calls. Unused on the C-chain, which is push-driven.
    pub poll_interval: Duration,
    /// Publishes each freshly-persisted block to subscribers (the fan-out
    /// source for every live subscription kind). Only the live path feeds this;
    /// backfill does not (those aren't "new"). Clone is cheap — it's a
    /// `broadcast::Sender` handle.
    pub blocks: LiveTx,
    /// Subscribe to `newBlocks` (whole block, no follow-up fetch) instead of
    /// `newHeads` (header, then fetch). `true` in `--mirror-from` mode, where
    /// the upstream is a neve that serves the extension; `false` against the
    /// public endpoint, which only offers `newHeads`.
    pub subscribe_blocks: bool,
    /// Minimum delay between backfill block fetches. `40ms` (~25 req/s) by
    /// default to stay under Cloudflare on the public endpoint; `0` against a
    /// neve upstream, which has no such limit. From
    /// `chains.<x>.request_interval`.
    pub backfill_inter_fetch: Duration,
    /// Enforces `backfill_inter_fetch` **globally**, across however many
    /// requests are in flight. Shared by every fetch on this chain, which is
    /// what lets `fetch_concurrency` be raised without raising the request rate.
    /// Bounds *this chain*; `host_pacer` bounds the sum. Wait on both through
    /// [`IngestCfg::pace`] rather than reaching for either directly.
    pub pacer: Arc<Pacer>,
    /// The whole process's request budget, or `None` when uncapped.
    ///
    /// The public endpoint's rate limit is enforced **per IP for the entire
    /// host**, not per chain path: a hard P-chain backfill will throttle a
    /// C-chain instance at the same address (measured — see `CLAUDE.md` and
    /// `docs/p-chain-indexing-plan.md`). A per-chain pacer therefore cannot
    /// express that limit, because two chains each politely holding to 25 req/s
    /// jointly issue 50. This one `Pacer` is shared by every chain in the
    /// process, so the cap bounds their sum. From `upstream.max_rps`, which
    /// defaults to 25 req/s against an untokened public endpoint and to no cap
    /// when a token is configured (bypassing the limit is what a token is for)
    /// or when the upstream is another neve.
    pub host_pacer: Option<Arc<Pacer>>,
    /// How many heights a fill keeps in flight at once, on either chain — the
    /// P-chain's poll loop and the C-chain's backfill run. Bounded by the pacer,
    /// so this only ever buys back round-trip latency — it cannot exceed the
    /// configured request rate.
    pub fetch_concurrency: usize,
    /// Lowest height backfill should fill down to. `Some(floor)` in mirror
    /// mode (the upstream's earliest retained height) lets backfill begin
    /// from `floor` without waiting for a `newHead` to anchor the store.
    /// `None` keeps the original "anchor at first newHead, fill forward only"
    /// behavior.
    pub backfill_floor: Option<u64>,
    /// Upper bound on the adaptive live-`newHeads` pre-fetch delay (see
    /// `crate::eth::ingest::AimdDelay`). `0` (the default) disables the
    /// pre-delay; a non-zero cap lets the controller park a delay against a
    /// fast private upstream to trim `empty` fetches. From
    /// `chains.<x>.prefetch_delay_cap`. C-chain only.
    pub prefetch_delay_cap: Duration,
    /// Notified when something fatal happens (e.g. upstream throttle exceeds
    /// `max_wait`). main's select! awaits this and exits with an error.
    /// Shared by every chain instance.
    pub fatal: Arc<Notify>,
    /// Notified once the mirror's `oldBlocks` bootstrap has finished streaming
    /// the historical range (or given up). The backfill loop waits on this in
    /// mirror mode so it doesn't race the bootstrap's ascending frontier with
    /// redundant HTTPS fetches. Unused (never awaited) outside mirror mode.
    pub bootstrap_done: Arc<Notify>,
    /// Minimum wall-clock between `backfill` progress lines. Set to the
    /// `summary` period (`chains.<x>.summary_period`) so the two operator-visible
    /// keep the same cadence: at tens of heights a second a height-based
    /// throttle emits ten lines per summary, each restating the previous one.
    pub progress_period: Duration,
    /// Fetch and store event logs alongside blocks on the backfill path. From
    /// `--ingest-logs`; off by default. C-chain only.
    pub ingest_logs: bool,
    /// Which upstream method supplies those logs. Probed at startup rather than
    /// configured, because it is a property of the endpoint rather than a
    /// preference — and the endpoint changes under a running deployment when an
    /// operator repoints `rpc_url` from their own node to the public one after
    /// a fill. Ignored when `ingest_logs` is off.
    pub logs_source: LogsSource,
}

/// Where a block's logs come from. Probed by
/// [`crate::eth::ingest::probe_logs_source`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogsSource {
    /// `eth_getBlockReceipts`, with the logs lifted out of the receipts. One
    /// call per block, so it overlaps with the block fetch and needs no
    /// range window — but the public Avalanche endpoint does not serve it
    /// (`-32601`), so this is only reachable against your own node.
    Receipts,
    /// `eth_getLogs` over a block range. Works everywhere. On the backfill path
    /// it forces a serial per-run window fetch whose response grows with log
    /// density (~121 MB per 2048 blocks at 2026 mainnet rates).
    GetLogs,
}

impl IngestCfg {
    /// Wait for this request's turn under **both** budgets: the host-wide cap
    /// shared with every other chain in the process, and this chain's own
    /// inter-request interval. Every upstream request on a paced path goes
    /// through here.
    ///
    /// Awaiting them in sequence composes correctly — each request must claim a
    /// slot from each pacer, so the effective rate is whichever of the two is
    /// slower — and the host budget goes first so chains take turns on it in
    /// arrival order rather than after each has served its own interval.
    pub async fn pace(&self) {
        if let Some(host) = &self.host_pacer {
            host.wait().await;
        }
        self.pacer.wait().await;
    }
}

impl std::fmt::Debug for IngestCfg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestCfg")
            .field("chain", &self.chain)
            // Redacted: an upstream URL may carry a rate-limit bypass token in its
            // query, and a Debug dump is exactly the place that gets pasted around.
            .field("rpc_url", &crate::upstream::redact_url(&self.rpc_url))
            .field("ws_url", &crate::upstream::redact_url(&self.ws_url))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn normalize_dedups_and_orders() {
        assert_eq!(normalize_chains(&[Chain::C]).unwrap(), vec![Chain::C]);
        // Either order of the pair yields the same instance layout.
        assert_eq!(
            normalize_chains(&[Chain::P, Chain::C]).unwrap(),
            normalize_chains(&[Chain::C, Chain::P]).unwrap(),
        );
        // Duplicates collapse.
        assert_eq!(
            normalize_chains(&[Chain::P, Chain::P]).unwrap(),
            vec![Chain::P],
        );
    }

    #[test]
    fn normalize_rejects_an_empty_selection() {
        assert!(normalize_chains(&[]).is_err());
    }

    /// A config for a chain that pays `chain_interval` per request and shares
    /// `host` with every other chain in the process. Only the pacing fields
    /// matter here; nothing else is reachable from `pace`.
    fn paced_cfg(chain: Chain, chain_interval: Duration, host: Option<Arc<Pacer>>) -> IngestCfg {
        let (blocks, _) = tokio::sync::broadcast::channel(1);
        IngestCfg {
            chain,
            max_wait: Duration::ZERO,
            ws_idle_timeout: Duration::ZERO,
            ws_url: String::new(),
            rpc_url: String::new(),
            poll_interval: Duration::ZERO,
            blocks,
            subscribe_blocks: false,
            backfill_inter_fetch: chain_interval,
            pacer: Arc::new(Pacer::new(chain_interval)),
            host_pacer: host,
            fetch_concurrency: 1,
            backfill_floor: None,
            prefetch_delay_cap: Duration::ZERO,
            fatal: Arc::new(Notify::new()),
            bootstrap_done: Arc::new(Notify::new()),
            progress_period: Duration::from_secs(60),
            ingest_logs: false,
            logs_source: LogsSource::GetLogs,
        }
    }

    /// The bug the host pacer fixes: the upstream's limit is per-IP for the
    /// whole host, so two chains each holding to their own budget jointly
    /// exceed it. Sharing one pacer bounds their *sum* — four requests split
    /// across two chains are spaced as if one chain had issued all four.
    #[tokio::test(start_paused = true)]
    async fn the_host_pacer_bounds_every_chain_together() {
        let host = Arc::new(Pacer::new(Duration::from_millis(100)));
        // Both chains are individually unthrottled, so any spacing observed is
        // the host budget's doing and nothing else's.
        let c = paced_cfg(Chain::C, Duration::ZERO, Some(Arc::clone(&host)));
        let p = paced_cfg(Chain::P, Duration::ZERO, Some(Arc::clone(&host)));

        let start = tokio::time::Instant::now();
        c.pace().await;
        p.pace().await;
        c.pace().await;
        p.pace().await;
        let elapsed = start.elapsed();
        assert!(
            (Duration::from_millis(300)..Duration::from_millis(310)).contains(&elapsed),
            "four requests across two chains at 100ms apart should take ~300ms, took {elapsed:?}",
        );
    }

    /// Without a host cap the chains are independent, which is both the
    /// pre-existing behavior and what a tokened or neve upstream configures.
    #[tokio::test(start_paused = true)]
    async fn chains_are_independent_when_the_host_is_uncapped() {
        let c = paced_cfg(Chain::C, Duration::from_millis(100), None);
        let p = paced_cfg(Chain::P, Duration::from_millis(100), None);
        let start = tokio::time::Instant::now();
        c.pace().await;
        p.pace().await;
        // Each chain's first request claims its own pacer's opening slot, so
        // neither waits out the other's interval. (Not exactly zero: the paused
        // clock advances in millisecond steps even for an already-elapsed
        // deadline.)
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "neither chain should wait on the other, waited {elapsed:?}",
        );
    }

    /// The slower of the two budgets is the one that binds: a chain politer
    /// than the host cap is not sped up by it.
    #[tokio::test(start_paused = true)]
    async fn the_per_chain_interval_still_applies_under_a_looser_host_cap() {
        let host = Arc::new(Pacer::new(Duration::from_millis(10)));
        let p = paced_cfg(Chain::P, Duration::from_millis(200), Some(host));
        let start = tokio::time::Instant::now();
        for _ in 0..3 {
            p.pace().await;
        }
        let elapsed = start.elapsed();
        assert!(
            (Duration::from_millis(400)..Duration::from_millis(410)).contains(&elapsed),
            "three requests at the chain's 200ms should take ~400ms, took {elapsed:?}",
        );
    }

    /// The C-chain store stays at the data-dir base, where C-chain stores in the
    /// field live; other chains nest under it.
    #[test]
    fn data_dir_keeps_c_chain_at_the_base() {
        let base = Path::new("/var/lib/neve");
        assert_eq!(Chain::C.data_dir(base), PathBuf::from("/var/lib/neve"));
        assert_eq!(Chain::P.data_dir(base), PathBuf::from("/var/lib/neve/p"));
    }

    /// Every chain's endpoint is exactly `upstream.base` plus its own path,
    /// which is the property one `base` key relies on to re-point a whole
    /// multi-chain instance at a custom host.
    #[test]
    fn endpoints_are_the_base_plus_a_per_chain_path() {
        assert_eq!(
            Network::Mainnet.default_base_url(),
            "https://api.avax.network",
        );
        assert_eq!(
            format!(
                "{}{}",
                Network::Mainnet.default_base_url(),
                Chain::C.rpc_path()
            ),
            "https://api.avax.network/ext/bc/C/rpc",
        );
        assert_eq!(
            format!(
                "{}{}",
                Network::Testnet.default_base_url(),
                Chain::P.rpc_path()
            ),
            "https://api.avax-test.network/ext/bc/P",
        );
        // The P-chain has no upstream push mechanism to subscribe to.
        assert_eq!(Chain::C.ws_path(), Some("/ext/bc/C/ws"));
        assert_eq!(Chain::P.ws_path(), None);
    }
}
