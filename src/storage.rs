use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use blockstore::{Store, StoreOptions};
// fjall 3 renamed its concepts: the old `Keyspace` (the whole store) is now a
// `Database`, and the old `PartitionHandle` (a column family) is now a
// `Keyspace`. So `Inner::db` is the store and the `Keyspace` fields are what
// used to be partitions.
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use tokio::sync::RwLock;
use tracing::debug;

use crate::record;

/// On-disk record-format version, stamped in the `meta` keyspace next to the
/// chain-ID stamp and verified on every open. Bump it whenever the stored
/// record layout changes so an incompatible store is rejected up front with a
/// clear "wipe and resync" error instead of being silently mis-parsed per
/// request. Version 1 introduced the combined `[block, logs]` record; a store
/// written before format versioning (no format-version key) holds the old
/// bare-block layout and is refused.
const FORMAT_VERSION: u32 = 1;

/// `meta` keyspace key holding the [`FORMAT_VERSION`] stamp.
const FORMAT_VERSION_KEY: &str = "format_version";

/// Verify the chain-ID and on-disk format-version stamps in the `meta`
/// keyspace, writing them on a genuinely fresh store. Rejects, with a clear
/// "wipe and resync" error, three incompatibilities a silent open would turn
/// into per-request corruption: a chain-ID mismatch, an unknown format version,
/// and a pre-format-version store (the old bare-block layout).
///
/// "Genuinely fresh" is keyed off `has_block_data` (does `blocks/blockdb.idx`
/// exist?), not off the chain-ID stamp: a store can hold bare-block data with an
/// empty `meta` keyspace, so any store with block data but no format-version
/// stamp is refused. Fresh stamps are fsynced so the format stamp is durable
/// before the first block lands.
fn verify_and_stamp_meta(
    db: &Database,
    data_dir: &Path,
    chain_id: u64,
    has_block_data: bool,
) -> Result<()> {
    let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
    let chain_id_str = chain_id.to_string();
    let stored_chain = meta.get("chain_id")?;
    let stored_fmt = meta.get(FORMAT_VERSION_KEY)?;

    // Chain ID: must match a previously stamped value.
    if let Some(slice) = &stored_chain {
        let stored =
            std::str::from_utf8(slice.as_ref()).context("meta/chain_id is not valid UTF-8")?;
        if stored != chain_id_str {
            bail!(
                "data dir {} is stamped for chain_id {}, refusing to open with chain_id {}",
                data_dir.display(),
                stored,
                chain_id_str,
            );
        }
        debug!(chain_id = stored, "chain_id stamp verified");
    }

    // Format version: reject an incompatible on-disk layout up front rather
    // than mis-parsing every read.
    match &stored_fmt {
        Some(slice) => {
            let stored = std::str::from_utf8(slice.as_ref())
                .context("meta/format_version is not valid UTF-8")?;
            let stored_ver: u32 = stored
                .parse()
                .with_context(|| format!("meta/format_version {stored:?} is not a u32"))?;
            if stored_ver != FORMAT_VERSION {
                bail!(
                    "data dir {} was written with on-disk format version {} but this build \
                     requires version {}; the record layout changed and there is no migration \
                     — delete the data dir and let neve resync",
                    data_dir.display(),
                    stored_ver,
                    FORMAT_VERSION,
                );
            }
            debug!(format_version = stored_ver, "format-version stamp verified");
        }
        // No format-version stamp, but the store is not empty (block data on
        // disk, or a chain-ID already stamped): it predates format versioning
        // and holds the bare-block layout this build cannot read.
        None if has_block_data || stored_chain.is_some() => {
            bail!(
                "data dir {} holds data in an unversioned (pre-logs) on-disk format — no \
                 format-version stamp — which this build cannot read; there is no migration: \
                 delete the data dir and let neve resync",
                data_dir.display(),
            );
        }
        None => {}
    }

    // Stamp whatever is missing (a genuinely-fresh store here), fsynced so the
    // format stamp lands before the first block and no open sees a half-stamp.
    if stored_chain.is_none() {
        meta.insert("chain_id", chain_id_str.as_str())?;
        debug!(chain_id = %chain_id_str, "chain_id stamp written");
    }
    if stored_fmt.is_none() {
        meta.insert(FORMAT_VERSION_KEY, FORMAT_VERSION.to_string().as_str())?;
        debug!(
            format_version = FORMAT_VERSION,
            "format-version stamp written"
        );
    }
    if stored_chain.is_none() || stored_fmt.is_none() {
        db.persist(PersistMode::SyncAll)?;
    }
    Ok(())
}

/// Shared storage handle. Cheap to clone (Arcs inside).
#[derive(Clone, Debug)]
pub struct Storage {
    inner: Arc<Inner>,
}

struct Inner {
    bs_dir: std::path::PathBuf,
    /// `RwLock` (not `Mutex`) so block reads run concurrently. The blockstore
    /// reads via positional `read_at` (no shared file cursor) and its only
    /// interior mutability is atomics + a `parking_lot::Mutex`, so it is `Sync`
    /// and many readers can share `&Store` safely. Only the rare lazy-open and
    /// writes (`put`) take the exclusive write lock; the hot read path takes a
    /// shared read lock and no longer serializes on a single mutex.
    store: RwLock<Option<Store>>,
    db: Database,
    hash_to_height: Keyspace,
    /// `tx_hash (32) → height (u64 LE) ++ index (u32 LE)` (12 bytes).
    /// Populated on ingest; powers `eth_getTransactionByHash`.
    tx_to_block: Keyspace,
    /// When the blockstore is created fresh, anchor its `minimum_height`
    /// here instead of at the first block written. Set in `--mirror-from`
    /// mode to the upstream's earliest retained height so backfill can
    /// reproduce the whole upstream range rather than only forward from the
    /// tip. `None` keeps the original "anchor at first ingest" behavior.
    /// Ignored when the store already exists (its floor is already baked in).
    anchor_floor: Option<u64>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("bs_dir", &self.bs_dir)
            .finish_non_exhaustive()
    }
}

impl Storage {
    /// Open (or create) the storage at `data_dir`. The upstream-reported
    /// chain ID (queried via `eth_chainId` at startup, then passed in
    /// decimal here) is stamped into a `meta` fjall keyspace on first open
    /// and verified on every subsequent open; a mismatch returns an error
    /// rather than silently mixing data. Anchoring on chain ID rather than
    /// a user-supplied label means `--rpc-url` overrides are caught too.
    pub fn open(data_dir: &Path, chain_id: u64, anchor_floor: Option<u64>) -> Result<Self> {
        let bs_dir = data_dir.join("blocks");
        let idx_dir = data_dir.join("index");
        std::fs::create_dir_all(&bs_dir)?;

        let db = Database::builder(&idx_dir).open()?;
        let hash_to_height = db.keyspace("hash_to_height", KeyspaceCreateOptions::default)?;
        debug!(
            approx_len = hash_to_height.approximate_len(),
            "opened keyspace hash_to_height",
        );
        let tx_to_block = db.keyspace("tx_to_block", KeyspaceCreateOptions::default)?;
        debug!(
            approx_len = tx_to_block.approximate_len(),
            "opened keyspace tx_to_block",
        );
        // Decided once: gates both the format-version check and the lazy store
        // open below.
        let has_block_data = bs_dir.join("blockdb.idx").exists();
        verify_and_stamp_meta(&db, data_dir, chain_id, has_block_data)?;

        let store = if has_block_data {
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
                store: RwLock::new(store),
                db,
                hash_to_height,
                tx_to_block,
                anchor_floor,
            }),
        })
    }

    /// Highest stored height (0 if nothing yet). Uses blockstore's
    /// `height_highwater` so gaps in the stored range (from WS reconnect or
    /// restart) don't pin the reported tip to the floor of the first gap.
    pub async fn high_water(&self) -> u64 {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.store.blocking_read();
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
            let guard = inner.store.blocking_read();
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
            let guard = inner.store.blocking_read();
            guard.as_ref().map_or(0, Store::max_contiguous_height)
        })
        .await
        .unwrap_or(0)
    }

    /// Read a block's stored bytes by height. Out-of-range heights (below the
    /// blockstore's `min_height` or above our high-water mark) return `None`
    /// rather than an error — this is the "we don't have it" signal that
    /// drives the 421 response in the HTTP layer.
    ///
    /// The returned [`record::BlockBytes`] owns the decompressed combined
    /// `[block, logs]` record and derefs to just the block half — the single
    /// choke point every block-bytes read flows through (by-height, by-hash,
    /// oldBlocks, bulk export), so they all see bare block JSON without knowing
    /// the record shape.
    pub async fn get_by_height(&self, height: u64) -> Result<Option<record::BlockBytes>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<Option<record::BlockBytes>> {
            let guard = inner.store.blocking_read();
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
                let bytes = record::BlockBytes::new(arc)
                    .with_context(|| format!("decoding stored record at height {height}"))?;
                debug!(height, bytes = bytes.as_ref().len(), "read block by height");
                Ok(Some(bytes))
            } else {
                debug!(height, "block not present: gap in stored range");
                Ok(None)
            }
        })
        .await?
    }

    /// Read a block's stored bytes by 32-byte hash.
    pub async fn get_by_hash(&self, hash: [u8; 32]) -> Result<Option<record::BlockBytes>> {
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
    /// anchored — at the configured `anchor_floor` (mirror mode) when set and
    /// `<= height`, otherwise at `height` itself.
    ///
    /// # Write ordering and partial-failure behavior
    ///
    /// The writes happen in two stages:
    ///
    /// 1. Blockstore `write_block` of the combined `[block, logs]` record
    ///    (`logs` is the caller's logs half — `record::EMPTY_LOGS` until log
    ///    ingestion fills it), then
    /// 2. A single atomic fjall `Batch` covering all index writes
    ///    (`hash_to_height` + each `tx_to_block` entry).
    ///
    /// The fjall batch is all-or-nothing — within the batch there is no
    /// "some tx indexes written, some not" state. The remaining failure
    /// window is a crash between stage 1 and stage 2: the blockstore has
    /// the block but no fjall index points at it. Symptom: lookups by
    /// hash / tx for that one block return 421; `eth_getBlockBy
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
        block_bytes: &[u8],
        logs: &[u8],
    ) -> Result<()> {
        // Build the combined [block, logs] record up front so the blocking task
        // only does the write (the owned `combined` is all it needs). `logs` is
        // the caller's already-serialized logs array (record::EMPTY_LOGS until
        // log ingestion supplies it).
        let combined = record::encode(block_bytes, logs);
        let inner = Arc::clone(&self.inner);
        let bs_dir = inner.bs_dir.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.store.blocking_write();
            if guard.is_none() {
                // Anchor at the configured floor when set (mirror mode), so the
                // store can hold the whole upstream range; otherwise anchor at
                // this first block. Clamp to `height` so we never set a floor
                // above the block we're about to write (blockstore requires
                // minimum_height <= every stored height).
                let minimum_height = inner
                    .anchor_floor
                    .filter(|&f| f <= height)
                    .unwrap_or(height);
                let opts = StoreOptions {
                    truncate: true,
                    minimum_height,
                    ..StoreOptions::default()
                };
                let s = Store::open(&bs_dir, &bs_dir, opts).context("opening blockstore")?;
                *guard = Some(s);
            }
            guard
                .as_ref()
                .expect("store initialized above")
                .write_block(height, &combined)?;
            Ok(())
        })
        .await??;

        let mut batch = self.inner.db.batch();
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
        batch.commit()?;
        // The batch's default durability is PersistMode::Buffer (no per-write
        // fsync); the journal tail lives in the page cache until `persist` is
        // called on shutdown. No explicit persist needed here.
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
            inner.db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    const CHAIN_ID: u64 = 43_114;

    /// Per-test data dir under the system temp dir. Mirrors the helper in
    /// `rpc.rs`/`bulk.rs`: pid + nanos + an atomic counter so parallel tests
    /// never collide on the same fjall keyspace. Not auto-cleaned.
    fn unique_temp_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "neve-storage-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ))
    }

    /// Hand-build a fjall index with the given `meta` stamps and nothing else,
    /// to simulate a store written by a different (or older) neve build.
    fn stamp_meta(dir: &std::path::Path, stamps: &[(&str, &str)]) {
        std::fs::create_dir_all(dir).unwrap();
        let db = Database::builder(dir.join("index")).open().unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        for (k, v) in stamps {
            meta.insert(*k, *v).unwrap();
        }
        db.persist(PersistMode::Buffer).unwrap();
    }

    /// Write one bare (pre-logs) block straight through the blockstore so
    /// `blocks/blockdb.idx` exists, without touching the `meta` keyspace — the
    /// on-disk shape an old neve produced before format versioning (and before
    /// chain-ID stamping).
    fn write_bare_block(dir: &std::path::Path, height: u64, bytes: &[u8]) {
        let bs_dir = dir.join("blocks");
        std::fs::create_dir_all(&bs_dir).unwrap();
        let opts = StoreOptions {
            truncate: true,
            minimum_height: height,
            ..StoreOptions::default()
        };
        let store = Store::open(&bs_dir, &bs_dir, opts).unwrap();
        store.write_block(height, bytes).unwrap();
        // Drop checkpoints the index, so blockdb.idx is on disk afterwards.
        drop(store);
    }

    /// A block put into the store comes back through `get_by_height` as the bare
    /// block JSON (element `[0]`), even though it is stored as a combined
    /// `[block, logs]` record.
    #[tokio::test]
    async fn put_get_roundtrip_unwraps_block_half() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, CHAIN_ID, None).unwrap();
        let block = br#"{"number":"0xa","hash":"0xbb","transactions":[]}"#.to_vec();
        storage
            .put(10, [0xbb; 32], &[], &block, record::EMPTY_LOGS)
            .await
            .unwrap();

        let got = storage.get_by_height(10).await.unwrap().unwrap();
        assert_eq!(got.as_ref(), block.as_slice());
    }

    /// The value actually on disk is the combined record (so logs can be added
    /// later with no migration): reach past `get_by_height` to the raw stored
    /// bytes and confirm they equal `[block, []]`.
    #[tokio::test]
    async fn stored_value_is_combined_record() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, CHAIN_ID, None).unwrap();
        let block = br#"{"number":"0x1"}"#.to_vec();
        storage
            .put(1, [1; 32], &[], &block, record::EMPTY_LOGS)
            .await
            .unwrap();

        let raw = {
            let guard = storage.inner.store.read().await;
            guard.as_ref().unwrap().read_block(1).unwrap().unwrap()
        };
        assert_eq!(raw.as_ref(), record::encode(&block, record::EMPTY_LOGS));
    }

    /// Reopening a freshly-created, correctly-stamped store succeeds and the
    /// data is still readable.
    #[tokio::test]
    async fn reopen_same_version_ok() {
        let dir = unique_temp_dir();
        {
            let storage = Storage::open(&dir, CHAIN_ID, None).unwrap();
            storage
                .put(5, [5; 32], &[], br#"{"number":"0x5"}"#, record::EMPTY_LOGS)
                .await
                .unwrap();
            storage.persist().await.unwrap();
        }
        let reopened = Storage::open(&dir, CHAIN_ID, None).unwrap();
        assert!(reopened.get_by_height(5).await.unwrap().is_some());
    }

    /// A store with a chain-ID stamp but no format-version stamp (the pre-logs
    /// on-disk layout) is refused, not silently mis-parsed.
    #[tokio::test]
    async fn rejects_pre_logs_store_without_format_stamp() {
        let dir = unique_temp_dir();
        stamp_meta(&dir, &[("chain_id", "43114")]);
        let err = Storage::open(&dir, CHAIN_ID, None).unwrap_err().to_string();
        assert!(err.contains("format"), "unexpected error: {err}");
    }

    /// A store stamped with a different (incompatible) format version is
    /// refused, naming the offending version.
    #[tokio::test]
    async fn rejects_incompatible_format_version() {
        let dir = unique_temp_dir();
        stamp_meta(&dir, &[("chain_id", "43114"), (FORMAT_VERSION_KEY, "999")]);
        let err = Storage::open(&dir, CHAIN_ID, None).unwrap_err().to_string();
        assert!(err.contains("999"), "unexpected error: {err}");
    }

    /// Regression: a store with real bare-block data on disk but an empty `meta`
    /// keyspace (no chain-ID, no format-version — the layout an old neve made
    /// before chain-ID stamping) must be refused. Gating on chain-ID presence
    /// instead of block-data presence would mis-classify this as fresh, stamp
    /// it, and then fail to decode every read.
    #[tokio::test]
    async fn rejects_blockstore_data_without_any_meta_stamp() {
        let dir = unique_temp_dir();
        write_bare_block(&dir, 100, br#"{"number":"0x64"}"#);
        let err = Storage::open(&dir, CHAIN_ID, None).unwrap_err().to_string();
        assert!(err.contains("format"), "unexpected error: {err}");
    }
}
