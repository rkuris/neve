use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use blockstore::{Store, StoreOptions};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use tokio::sync::Mutex;

/// Shared storage handle. Cheap to clone (Arcs inside).
#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

struct Inner {
    bs_dir: std::path::PathBuf,
    store: Mutex<Option<Store>>,
    keyspace: Keyspace,
    hash_to_height: PartitionHandle,
    /// Highest height we have stored, or 0 if none yet.
    high_water: AtomicU64,
}

impl Storage {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let bs_dir = data_dir.join("blocks");
        let idx_dir = data_dir.join("index");
        std::fs::create_dir_all(&bs_dir)?;

        let keyspace = Config::new(&idx_dir).open()?;
        let hash_to_height =
            keyspace.open_partition("hash_to_height", PartitionCreateOptions::default())?;

        let (store, high_water) = if bs_dir.join("blockdb.idx").exists() {
            let s = Store::open(&bs_dir, &bs_dir, StoreOptions::default())
                .context("opening blockstore")?;
            let hw = s.max_contiguous_height();
            (Some(s), hw)
        } else {
            (None, 0)
        };

        Ok(Self {
            inner: Arc::new(Inner {
                bs_dir,
                store: Mutex::new(store),
                keyspace,
                hash_to_height,
                high_water: AtomicU64::new(high_water),
            }),
        })
    }

    /// Highest stored height (0 if nothing yet).
    pub fn high_water(&self) -> u64 {
        self.inner.high_water.load(Ordering::Acquire)
    }

    /// Read a block's stored bytes by height.
    pub async fn get_by_height(&self, height: u64) -> Result<Option<Vec<u8>>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            let guard = inner.store.blocking_lock();
            let Some(store) = guard.as_ref() else {
                return Ok(None);
            };
            match store.read_block(height)? {
                Some(arc) => Ok(Some(arc.as_ref().to_vec())),
                None => Ok(None),
            }
        })
        .await?
    }

    /// Read a block's stored bytes by 32-byte hash.
    pub async fn get_by_hash(&self, hash: [u8; 32]) -> Result<Option<Vec<u8>>> {
        let Some(slice) = self.inner.hash_to_height.get(hash)? else {
            return Ok(None);
        };
        let bytes: [u8; 8] = slice
            .as_ref()
            .try_into()
            .map_err(|_| anyhow!("bad height entry in index"))?;
        let height = u64::from_le_bytes(bytes);
        self.get_by_height(height).await
    }

    /// Insert a block at the given height and update the hash→height index.
    /// Lazily opens the blockstore on the very first call so its
    /// `minimum_height` can be anchored at `height`.
    pub async fn put(&self, height: u64, hash: [u8; 32], block_bytes: Vec<u8>) -> Result<()> {
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

        self.inner.hash_to_height.insert(hash, height.to_le_bytes())?;
        self.inner.keyspace.persist(PersistMode::Buffer)?;
        // High-water is monotone in our ingest path, but use fetch_max for safety.
        self.inner.high_water.fetch_max(height, Ordering::AcqRel);
        Ok(())
    }
}
