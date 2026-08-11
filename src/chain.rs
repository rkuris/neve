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
use tokio::sync::Notify;

use crate::subscribe::LiveTx;
use crate::upstream::Pacer;

/// Which Avalanche network to target. Shared across chains: a mainnet instance
/// mirrors mainnet's C-chain *and* mainnet's P-chain, never a mix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lower")]
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
}

/// Which chain one instance mirrors. The wire spelling (`as_str`) doubles as
/// the `chain` metric label and the on-disk `meta/chain` stamp, so it must stay
/// stable across releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ValueEnum)]
#[clap(rename_all = "lower")]
pub enum Chain {
    /// The EVM C-chain: `eth_*` JSON-RPC, `newHeads` WebSocket push.
    C,
    /// The platform P-chain: `platform.*` JSON-RPC, height polling (no push
    /// mechanism exists upstream — see `docs/p-chain-indexing-plan.md`).
    P,
}

impl Chain {
    /// Stable short name: the `chain` metric label, the `meta/chain` stamp, and
    /// the on-disk subdirectory for non-default chains.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::C => "c",
            Self::P => "p",
        }
    }

    /// Default HTTPS JSON-RPC endpoint on the public Avalanche endpoint.
    pub fn default_rpc_url(self, network: Network) -> String {
        let host = network.host();
        match self {
            Self::C => format!("https://{host}/ext/bc/C/rpc"),
            Self::P => format!("https://{host}/ext/bc/P"),
        }
    }

    /// Default WebSocket endpoint, or `None` for a chain with no upstream push
    /// mechanism. The P-chain has none at all — no `eth_subscribe` analog, and
    /// the old X-chain pubsub was removed in avalanchego v1.11.13 — so a P
    /// instance polls `platform.getHeight` instead.
    pub fn default_ws_url(self, network: Network) -> Option<String> {
        let host = network.host();
        match self {
            Self::C => Some(format!("wss://{host}/ext/bc/C/ws")),
            Self::P => None,
        }
    }

    /// This chain's store location under the `--data-dir` base.
    ///
    /// The C-chain sits at the base itself, exactly where every store written
    /// before multi-chain support lives, so existing data dirs keep working with
    /// no migration and no resync. Every other chain gets a subdirectory.
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
/// paths. One of these per running instance; the `fatal` notifier is the one
/// field deliberately shared, so any chain's unrecoverable condition brings the
/// whole process down rather than leaving a half-serving mirror.
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
    /// default to stay under Cloudflare on the public endpoint; `0` in
    /// `--mirror-from` mode, where the upstream is another neve with no such
    /// limit.
    pub backfill_inter_fetch: Duration,
    /// Enforces `backfill_inter_fetch` **globally**, across however many
    /// requests are in flight. Shared by every fetch on this chain, which is
    /// what lets `fetch_concurrency` be raised without raising the request rate.
    pub pacer: Arc<Pacer>,
    /// How many heights the P-chain fill keeps in flight at once. Bounded by
    /// the pacer, so this only ever buys back round-trip latency — it cannot
    /// exceed the configured request rate.
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
    /// `--prefetch-delay-cap`. C-chain only.
    pub prefetch_delay_cap: Duration,
    /// Notified when something fatal happens (e.g. upstream throttle exceeds
    /// `--max-wait`). main's select! awaits this and exits with an error.
    /// Shared by every chain instance.
    pub fatal: Arc<Notify>,
    /// Notified once the mirror's `oldBlocks` bootstrap has finished streaming
    /// the historical range (or given up). The backfill loop waits on this in
    /// mirror mode so it doesn't race the bootstrap's ascending frontier with
    /// redundant HTTPS fetches. Unused (never awaited) outside mirror mode.
    pub bootstrap_done: Arc<Notify>,
    /// Fetch and store event logs alongside blocks on the backfill path (one
    /// `eth_getLogs` per ~2048-block window). From `--ingest-logs`; off by
    /// default. C-chain only.
    pub ingest_logs: bool,
}

impl std::fmt::Debug for IngestCfg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestCfg")
            .field("chain", &self.chain)
            .field("rpc_url", &self.rpc_url)
            .field("ws_url", &self.ws_url)
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

    /// The C-chain store stays at the `--data-dir` base so pre-multi-chain data
    /// dirs open unchanged; other chains nest under it.
    #[test]
    fn data_dir_keeps_c_chain_at_the_base() {
        let base = Path::new("/var/lib/neve");
        assert_eq!(Chain::C.data_dir(base), PathBuf::from("/var/lib/neve"));
        assert_eq!(Chain::P.data_dir(base), PathBuf::from("/var/lib/neve/p"));
    }

    #[test]
    fn default_endpoints_are_per_chain_paths_on_one_host() {
        assert_eq!(
            Chain::C.default_rpc_url(Network::Mainnet),
            "https://api.avax.network/ext/bc/C/rpc",
        );
        assert_eq!(
            Chain::P.default_rpc_url(Network::Testnet),
            "https://api.avax-test.network/ext/bc/P",
        );
        // The P-chain has no upstream push mechanism to subscribe to.
        assert!(Chain::C.default_ws_url(Network::Mainnet).is_some());
        assert!(Chain::P.default_ws_url(Network::Mainnet).is_none());
    }
}
