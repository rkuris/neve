use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
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
    /// `height (u64 LE) → JSON array of receipts for that block`.
    /// Populated on ingest from `eth_getBlockReceipts`; powers
    /// `eth_getTransactionReceipt`.
    receipts_by_height: PartitionHandle,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner").field("bs_dir", &self.bs_dir).finish_non_exhaustive()
    }
}

impl Storage {
    /// Open (or create) the storage at `data_dir`. The upstream-reported
    /// chain ID (queried via `eth_chainId` at startup, then passed in
    /// decimal here) is stamped into a `meta` fjall partition on first open
    /// and verified on every subsequent open; a mismatch returns an error
    /// rather than silently mixing data. Anchoring on chain ID rather than
    /// a user-supplied label means `--rpc-url` overrides are caught too.
    pub fn open(data_dir: &Path, chain_id: u64) -> Result<Self> {
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
        let receipts_by_height =
            keyspace.open_partition("receipts_by_height", PartitionCreateOptions::default())?;
        debug!(
            approx_len = receipts_by_height.approximate_len(),
            "opened partition receipts_by_height",
        );
        // Chain-ID stamp lives in its own `meta` partition. We only need it
        // at open time, so the partition handle is scoped to this block
        // (not held in `Inner`).
        {
            let meta = keyspace.open_partition("meta", PartitionCreateOptions::default())?;
            let chain_id_str = chain_id.to_string();
            if let Some(slice) = meta.get("chain_id")? {
                let stored = std::str::from_utf8(slice.as_ref())
                    .context("meta/chain_id is not valid UTF-8")?;
                if stored != chain_id_str {
                    bail!(
                        "data dir {} is stamped for chain_id {}, refusing to open with chain_id {}",
                        data_dir.display(),
                        stored,
                        chain_id_str,
                    );
                }
                debug!(chain_id = stored, "chain_id stamp verified");
            } else {
                meta.insert("chain_id", chain_id_str.as_str())?;
                keyspace.persist(PersistMode::Buffer)?;
                debug!(chain_id = %chain_id_str, "chain_id stamp written");
            }
        }

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
                receipts_by_height,
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

    /// Lowest stored block height (0 if the store hasn't been opened yet —
    /// nothing has been ingested). Anchored on first ingest to whatever
    /// height newHeads first delivered.
    pub async fn min_height(&self) -> u64 {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.store.blocking_lock();
            guard.as_ref().map_or(0, Store::min_block_height)
        })
        .await
        .unwrap_or(0)
    }

    /// On-disk directory holding the blockstore files (block bytes + `.idx`).
    pub fn blockdb_dir(&self) -> &Path {
        &self.inner.bs_dir
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
                debug!(height, "block not present: store not opened yet");
                return Ok(None);
            };
            if height < store.min_block_height() || height > store.height_highwater() {
                debug!(
                    height,
                    min = store.min_block_height(),
                    high_water = store.height_highwater(),
                    "block not present: out of range",
                );
                return Ok(None);
            }
            if let Some(arc) = store.read_block(height)? {
                debug!(height, bytes = arc.as_ref().len(), "read block by height");
                Ok(Some(arc.as_ref().to_vec()))
            } else {
                debug!(height, "block not present: gap in stored range");
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

    /// Fetch the raw JSON `eth_getBlockReceipts` array for a block height.
    /// `None` if we haven't ingested receipts for that height.
    pub fn get_receipts_at_height(&self, height: u64) -> Result<Option<Vec<u8>>> {
        let key = height.to_le_bytes();
        let Some(slice) = self.inner.receipts_by_height.get(key)? else {
            debug!(height, "receipts_by_height miss");
            return Ok(None);
        };
        debug!(height, bytes = slice.as_ref().len(), "receipts_by_height hit");
        Ok(Some(slice.as_ref().to_vec()))
    }

    /// Insert a block at the given height and update both indexes
    /// (`hash → height` and `tx_hash → (height, idx)`), plus the receipts
    /// partition. Lazily opens the blockstore on the very first call so its
    /// `minimum_height` can be anchored at `height`.
    ///
    /// # Write ordering and partial-failure behavior
    ///
    /// The four writes happen in two stages:
    ///
    /// 1. Blockstore `write_block` (height → bytes), then
    /// 2. A single atomic fjall `Batch` covering all index writes
    ///    (`hash_to_height` + each `tx_to_block` entry + optionally
    ///    `receipts_by_height`).
    ///
    /// The fjall batch is all-or-nothing — within the batch there is no
    /// "some tx indexes written, some not" state. The remaining failure
    /// window is a crash between stage 1 and stage 2: the blockstore has
    /// the block but no fjall index points at it. Symptom: lookups by
    /// hash / tx / receipt for that one block return 421; `eth_getBlockBy
    /// Number(<height>)` and `eth_blockNumber` still succeed. The
    /// blockstore's `max_contiguous_height` will have advanced past this
    /// height, so the backfill worker won't refill the indexes
    /// automatically — accept this as a known mild-corruption mode for
    /// the prototype, fixable later by writing the fjall batch first or
    /// by moving block bytes into fjall.
    pub async fn put(
        &self,
        height: u64,
        hash: [u8; 32],
        tx_hashes: &[[u8; 32]],
        block_bytes: Vec<u8>,
        receipts_bytes: Option<Vec<u8>>,
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

        let mut batch = self.inner.keyspace.batch();
        debug!(
            height,
            hash = %hex::encode(hash),
            "indexed hash_to_height",
        );
        batch.insert(&self.inner.hash_to_height, hash, height.to_le_bytes());
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
            batch.insert(&self.inner.tx_to_block, *tx_hash, value);
        }
        if let Some(rb) = receipts_bytes {
            debug!(height, bytes = rb.len(), "indexed receipts_by_height");
            batch.insert(&self.inner.receipts_by_height, height.to_le_bytes(), rb);
        }
        batch.commit()?;
        // PersistMode::Buffer is the batch's default durability when
        // manual_journal_persist is off (the keyspace was opened without
        // it), so an explicit persist call is no longer needed here.
        Ok(())
    }

    /// Flush durably to disk. Steady-state writes use `PersistMode::Buffer`
    /// (no fsync), so the journal tail lives in the OS page cache — fine for a
    /// graceful process exit, lost on power failure. Call this on shutdown to
    /// `fsync` the journal. The blockstore separately checkpoints in its own
    /// `Drop` when the runtime tears the tasks down.
    pub async fn persist(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<()> {
            inner.keyspace.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await?
    }
}
