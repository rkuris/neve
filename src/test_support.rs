//! Test-only helpers shared across modules: throwaway data dirs and
//! ready-to-use chain instances.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use tokio::sync::broadcast;

use crate::chain::Chain;
use crate::rpc::ChainServe;
use crate::storage::Storage;

/// The C-chain mainnet network identity (`eth_chainId` in decimal), the default
/// stamp for test stores.
pub const C_IDENTITY: &str = "43114";

/// A unique data dir under the system temp dir, tagged with `prefix` so a
/// leftover directory names the test that made it. Pid + nanos + a
/// process-wide counter, because parallel tests can share a coarse clock tick
/// and must never collide on the same fjall keyspace. Not auto-cleaned.
pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "neve-{}-{}-{}-{}",
        prefix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    ))
}

/// The network identity to stamp a test store for `chain` with.
pub const fn identity_for(chain: Chain) -> &'static str {
    match chain {
        Chain::C => C_IDENTITY,
        Chain::P => "test-genesis-id",
    }
}

/// A `ChainServe` over a fresh empty store for `chain`, rooted under `base`
/// exactly where a real run would put it.
pub fn chain_serve(chain: Chain, base: &Path) -> ChainServe {
    let data_dir = chain.data_dir(base);
    let storage = Storage::open(&data_dir, chain, identity_for(chain), None)
        .expect("open test store for chain");
    let (blocks, _) = broadcast::channel::<Value>(16);
    ChainServe {
        chain,
        storage,
        data_dir,
        identity: identity_for(chain).to_owned(),
        behind_tip: Arc::new(AtomicU64::new(0)),
        blocks,
        join: None,
    }
}
