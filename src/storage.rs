use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use blockstore::{Store, StoreOptions};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use tokio::sync::Mutex;
use tracing::debug;

/// Shared storage handle. Cheap to clone (Arcs inside).
#[derive(Clone, Debug)]
pub struct Storage {
    inner: Arc<Inner>,
}

struct Inner {
    bs_dir: std::path::PathBuf,
    store: Mutex<Option<Store>>,
    keyspace: Keyspace,
    hash_to_height: PartitionHandle,
    /// `tx_hash (32) → height (u64 LE) ++ index (u32 LE)` (12 bytes).
    /// Populated on ingest; powers `eth_getTransactionByHash`.
    tx_to_block: PartitionHandle,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner").field("bs_dir", &self.bs_dir).finish_non_exhaustive()
    }
}

impl Storage {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let bs_dir = data_dir.join("blocks");
        let idx_dir = data_dir.join("index");
        std::fs::create_dir_all(&bs_dir)?;

        let keyspace = Config::new(&idx_dir).open()?;
        let hash_to_height =
            keyspace.open_partition("hash_to_height", PartitionCreateOptions::default())?;
        debug!(
            approx_len = hash_to_height.approximate_len(),
            "opened partition hash_to_height",
        );
        let tx_to_block =
            keyspace.open_partition("tx_to_block", PartitionCreateOptions::default())?;
        debug!(
            approx_len = tx_to_block.approximate_len(),
            "opened partition tx_to_block",
        );

        let store = if bs_dir.join("blockdb.idx").exists() {
            let s = Store::open(&bs_dir, &bs_dir, StoreOptions::default())
                .context("opening blockstore")?;
            debug!(
                min_height = s.min_block_height(),
                max_contiguous = s.max_contiguous_height(),
                high_water = s.height_highwater(),
                "opened blockstore",
            );
            Some(s)
        } else {
            debug!("blockstore not yet created (no blocks ingested)");
            None
        };

        Ok(Self {
            inner: Arc::new(Inner {
                bs_dir,
                store: Mutex::new(store),
                keyspace,
                hash_to_height,
                tx_to_block,
            }),
        })
    }

    /// Highest stored height (0 if nothing yet). Uses blockstore's
    /// `height_highwater` so gaps in the stored range (from WS reconnect or
    /// restart) don't pin the reported tip to the floor of the first gap.
    pub async fn high_water(&self) -> u64 {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.store.blocking_lock();
            guard.as_ref().map_or(0, Store::height_highwater)
        })
        .await
        .unwrap_or(0)
    }

    /// Highest height H such that every block in `[min_block_height, H]` is
    /// present. Drives the backfill worker — `H + 1` is the next hole.
    pub async fn max_contiguous_height(&self) -> u64 {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.store.blocking_lock();
            guard.as_ref().map_or(0, Store::max_contiguous_height)
        })
        .await
        .unwrap_or(0)
    }

    /// Read a block's stored bytes by height. Out-of-range heights (below the
    /// blockstore's `min_height` or above our high-water mark) return `None`
    /// rather than an error — this is the "we don't have it" signal that
    /// drives the 421 response in the HTTP layer.
    pub async fn get_by_height(&self, height: u64) -> Result<Option<Vec<u8>>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            let guard = inner.store.blocking_lock();
            let Some(store) = guard.as_ref() else {
                debug!(height, "read miss: store not opened yet");
                return Ok(None);
            };
            if height < store.min_block_height() || height > store.height_highwater() {
                debug!(
                    height,
                    min = store.min_block_height(),
                    high_water = store.height_highwater(),
                    "read miss: out of range",
                );
                return Ok(None);
            }
            if let Some(arc) = store.read_block(height)? {
                debug!(height, bytes = arc.as_ref().len(), "read block by height");
                Ok(Some(arc.as_ref().to_vec()))
            } else {
                debug!(height, "read miss: hole in stored range");
                Ok(None)
            }
        })
        .await?
    }

    /// Read a block's stored bytes by 32-byte hash.
    pub async fn get_by_hash(&self, hash: [u8; 32]) -> Result<Option<Vec<u8>>> {
        let Some(slice) = self.inner.hash_to_height.get(hash)? else {
            debug!(hash = %hex::encode(hash), "hash_to_height miss");
            return Ok(None);
        };
        let bytes: [u8; 8] = slice
            .as_ref()
            .try_into()
            .map_err(|_| anyhow!("bad height entry in index"))?;
        let height = u64::from_le_bytes(bytes);
        debug!(hash = %hex::encode(hash), height, "hash_to_height hit");
        self.get_by_height(height).await
    }

    /// Look up where a transaction lives: `(height, tx_index)` if we've
    /// indexed it during ingest, `None` otherwise.
    pub fn get_tx_location(&self, tx_hash: [u8; 32]) -> Result<Option<(u64, u32)>> {
        let Some(slice) = self.inner.tx_to_block.get(tx_hash)? else {
            debug!(tx_hash = %hex::encode(tx_hash), "tx_to_block miss");
            return Ok(None);
        };
        let bytes: [u8; 12] = slice
            .as_ref()
            .try_into()
            .map_err(|_| anyhow!("bad tx_to_block entry"))?;
        let height = u64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes"));
        let idx = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
        debug!(tx_hash = %hex::encode(tx_hash), height, idx, "tx_to_block hit");
        Ok(Some((height, idx)))
    }

    /// Insert a block at the given height and update both indexes
    /// (`hash → height` and `tx_hash → (height, idx)`). Lazily opens the
    /// blockstore on the very first call so its `minimum_height` can be
    /// anchored at `height`.
    pub async fn put(
        &self,
        height: u64,
        hash: [u8; 32],
        tx_hashes: &[[u8; 32]],
        block_bytes: Vec<u8>,
    ) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let bs_dir = inner.bs_dir.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.store.blocking_lock();
            if guard.is_none() {
                let opts = StoreOptions {
                    truncate: true,
                    minimum_height: height,
                    ..StoreOptions::default()
                };
                let s =
                    Store::open(&bs_dir, &bs_dir, opts).context("opening blockstore")?;
                *guard = Some(s);
            }
            guard
                .as_ref()
                .expect("store initialized above")
                .write_block(height, &block_bytes)?;
            Ok(())
        })
        .await??;

        debug!(
            height,
            hash = %hex::encode(hash),
            "indexed hash_to_height",
        );
        self.inner.hash_to_height.insert(hash, height.to_le_bytes())?;
        for (idx, tx_hash) in tx_hashes.iter().enumerate() {
            let mut value = [0u8; 12];
            value[0..8].copy_from_slice(&height.to_le_bytes());
            value[8..12].copy_from_slice(&(idx as u32).to_le_bytes());
            debug!(
                height,
                idx,
                tx_hash = %hex::encode(tx_hash),
                "indexed tx_to_block",
            );
            self.inner.tx_to_block.insert(tx_hash, value)?;
        }
        self.inner.keyspace.persist(PersistMode::Buffer)?;
        Ok(())
    }
}
