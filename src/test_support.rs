//! Test-only helpers shared across modules: throwaway data dirs and
//! ready-to-use chain instances.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;
use tokio::sync::broadcast;

use crate::chain::Chain;
use crate::record;
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

/// Write a minimal block at `height`, building the record layout `storage`'s
/// chain expects, and index it under a height-derived hash. The block JSON is
/// shaped like that chain's real one (`number`/`transactions` on the C-chain,
/// `height`/`txs` on the P-chain) so a reader can't accidentally depend on the
/// wrong dialect.
pub async fn put_block(storage: &Storage, height: u64) {
    let chain = storage.chain();
    let block = match chain {
        Chain::C => json!({
            "number": format!("0x{height:x}"),
            "hash": format!("0x{height:064x}"),
            "transactions": [],
        }),
        Chain::P => json!({
            "height": height,
            "id": format!("block-{height}"),
            "time": 1_780_000_000u64.wrapping_add(height),
            "txs": [],
        }),
    };
    let block_bytes = serde_json::to_vec(&block).expect("serialize test block");
    // The trailing (derived-data) elements, empty as they are for a chain whose
    // secondary feeds aren't ingesting. The P-chain's element 1 is the block's
    // canonical bytes, which have no empty form, so use a stub hex string.
    let derived: Vec<&[u8]> = match chain {
        Chain::C => vec![record::EMPTY_ARRAY],
        Chain::P => vec![br#""0x00""#, record::EMPTY_ARRAY],
    };
    let mut elements: Vec<&[u8]> = vec![&block_bytes];
    elements.extend(derived);

    let mut hash = [0u8; 32];
    hash[24..].copy_from_slice(&height.to_be_bytes());
    storage
        .put(height, hash, &[], &elements)
        .await
        .expect("write test block");
}

/// A `ChainServe` over a fresh empty store for `chain`, rooted under `base`
/// exactly where a real run would put it.
pub fn chain_serve(chain: Chain, base: &Path) -> ChainServe {
    let data_dir = chain.data_dir(base);
    let storage = Storage::open(&data_dir, chain, identity_for(chain), None)
        .expect("open test store for chain");
    let (blocks, _) = broadcast::channel(16);
    ChainServe {
        chain,
        storage,
        data_dir,
        identity: identity_for(chain).to_owned(),
        behind_tip: Arc::new(AtomicU64::new(0)),
        blocks,
        join: None,
        ingests_logs: false,
    }
}
