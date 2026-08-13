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
use tracing::{debug, info};

use crate::chain::Chain;
use crate::record;

/// `meta` keyspace key holding the [`Chain::format_version`] stamp.
const FORMAT_VERSION_KEY: &str = "format_version";

/// `meta` keyspace key holding the network-identity stamp — the opaque string
/// the caller derived from the upstream (the decimal EVM chain ID on the
/// C-chain, the genesis block ID on the P-chain). Named `chain_id` for the
/// C-chain value it has always held, so pre-multi-chain stores verify unchanged.
const IDENTITY_KEY: &str = "chain_id";

/// `meta` keyspace key holding the [`Chain`] stamp, so a store is provably
/// one chain's and mixing is refused. Absent on stores written before
/// multi-chain support — those are C-chain by construction (see
/// [`verify_and_stamp_meta`]).
const CHAIN_KEY: &str = "chain";

/// Verify the `meta` keyspace's three stamps — chain, network identity, and
/// on-disk format version — writing them on a genuinely fresh store. Rejects,
/// with a clear "wipe and resync" error, every incompatibility a silent open
/// would turn into per-request corruption: a different chain, a different
/// network, an unknown format version, and block data carrying no network stamp
/// at all (which cannot be checked against anything).
///
/// A store missing only the *format-version* stamp is the pre-combined-record
/// layout and is **adopted**, not rejected — see below.
///
/// A store with block data but no format-version stamp predates record
/// versioning and holds bare block objects; it is *adopted* (and stamped)
/// rather than refused, because `crate::record` reads that layout. Fresh stamps
/// are fsynced so they are durable before the first block lands.
///
/// A store with data but no `chain` stamp predates multi-chain support and is
/// therefore a C-chain store; it is adopted (and stamped) when opened as
/// C-chain, and refused when opened as any other chain. That is what keeps
/// existing data dirs working with no migration.
fn verify_and_stamp_meta(
    db: &Database,
    data_dir: &Path,
    chain: Chain,
    identity: &str,
    has_block_data: bool,
) -> Result<()> {
    let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
    let stored_identity = meta.get(IDENTITY_KEY)?;
    let stored_fmt = meta.get(FORMAT_VERSION_KEY)?;
    let stored_chain = meta.get(CHAIN_KEY)?;
    // Any stamp or block data means the store is not fresh — used below to tell
    // "adopt this as a legacy C-chain store" from "stamp a brand-new one".
    let populated = has_block_data || stored_identity.is_some() || stored_fmt.is_some();

    // Chain: a store holds exactly one chain's blocks. An unstamped populated
    // store is a pre-multi-chain (C-chain) store.
    match &stored_chain {
        Some(slice) => {
            let stored =
                std::str::from_utf8(slice.as_ref()).context("meta/chain is not valid UTF-8")?;
            if stored != chain.as_str() {
                bail!(
                    "data dir {} is stamped for the {} chain, refusing to open it as the {} \
                     chain; each chain needs its own data dir",
                    data_dir.display(),
                    stored,
                    chain.as_str(),
                );
            }
            debug!(chain = stored, "chain stamp verified");
        }
        None if populated && chain != Chain::C => {
            bail!(
                "data dir {} holds data written before multi-chain support, which is C-chain \
                 data, so it cannot be opened as the {} chain; point --{}-data-dir somewhere else",
                data_dir.display(),
                chain.as_str(),
                chain.as_str(),
            );
        }
        None => {}
    }

    // Network identity: must match a previously stamped value.
    if let Some(slice) = &stored_identity {
        let stored = std::str::from_utf8(slice.as_ref())
            .with_context(|| format!("meta/{IDENTITY_KEY} is not valid UTF-8"))?;
        if stored != identity {
            bail!(
                "data dir {} is stamped for {} network {}, refusing to open with network {}",
                data_dir.display(),
                chain.as_str(),
                stored,
                identity,
            );
        }
        debug!(identity = stored, "network-identity stamp verified");
    }

    // Format version: reject an incompatible on-disk layout up front rather
    // than mis-parsing every read.
    let want_fmt = chain.format_version();
    match &stored_fmt {
        Some(slice) => {
            let stored = std::str::from_utf8(slice.as_ref())
                .context("meta/format_version is not valid UTF-8")?;
            let stored_ver: u32 = stored
                .parse()
                .with_context(|| format!("meta/format_version {stored:?} is not a u32"))?;
            if stored_ver != want_fmt {
                bail!(
                    "data dir {} was written with on-disk format version {} but this build \
                     requires version {} for the {} chain; the record layout changed and there \
                     is no migration — delete the data dir and let neve resync",
                    data_dir.display(),
                    stored_ver,
                    want_fmt,
                    chain.as_str(),
                );
            }
            debug!(format_version = stored_ver, "format-version stamp verified");
        }
        // No format-version stamp, but the store is not empty (block data on
        // disk, or an identity already stamped): it predates format versioning
        // and holds bare block objects rather than element arrays.
        //
        // That layout is readable — `crate::record` serves a bare block as
        // element 0 and reports its derived elements as absent — so the store is
        // adopted and stamped rather than refused. Heights written before this
        // point keep answering block reads and 421 anything derived (logs);
        // heights written after are full records. Both coexist, so no resync.
        // Adoptable: the identity stamp was verified above, so we know which
        // network this data belongs to and only the layout is old.
        None if stored_identity.is_some() => {
            info!(
                path = %data_dir.display(),
                "adopting a store written before record versioning: existing heights hold bare \
                 blocks, so derived reads (e.g. eth_getLogs) defer upstream for them",
            );
        }
        // Block data but *no* identity stamp: nothing here says which network
        // these blocks came from, and adopting would stamp them with whatever
        // this process happens to be pointed at — silently and permanently
        // binding, say, mainnet data to a testnet identity. Refuse instead;
        // there is no way to tell from the store itself.
        None if has_block_data => {
            bail!(
                "data dir {} holds block data with no network stamp, so there is no way to \
                 verify which network it belongs to; it predates network stamping — delete \
                 the data dir and let neve resync",
                data_dir.display(),
            );
        }
        None => {}
    }

    // Stamp whatever is missing — a genuinely-fresh store, or a legacy C-chain
    // store gaining its `chain` stamp — fsynced so the stamps land before the
    // first block and no open sees a half-stamp.
    if stored_chain.is_none() {
        meta.insert(CHAIN_KEY, chain.as_str())?;
        debug!(chain = chain.as_str(), "chain stamp written");
    }
    if stored_identity.is_none() {
        meta.insert(IDENTITY_KEY, identity)?;
        debug!(identity, "network-identity stamp written");
    }
    if stored_fmt.is_none() {
        meta.insert(FORMAT_VERSION_KEY, want_fmt.to_string().as_str())?;
        debug!(format_version = want_fmt, "format-version stamp written");
    }
    if stored_chain.is_none() || stored_identity.is_none() || stored_fmt.is_none() {
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
    /// Which chain's blocks this store holds. Verified against the `meta/chain`
    /// stamp on open, and carried here so read/write paths can pick the
    /// chain's record layout without threading it through every call.
    chain: Chain,
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
            .field("chain", &self.chain)
            .field("bs_dir", &self.bs_dir)
            .finish_non_exhaustive()
    }
}

impl Storage {
    /// Open (or create) `chain`'s storage at `data_dir`. `identity` is the
    /// opaque network fingerprint the caller queried from the upstream at
    /// startup — the decimal EVM chain ID from `eth_chainId` on the C-chain, the
    /// genesis block ID on the P-chain. It is stamped into a `meta` fjall
    /// keyspace on first open alongside the chain and format version, and all
    /// three are verified on every subsequent open; a mismatch returns an error
    /// rather than silently mixing data. Anchoring on an upstream-derived
    /// fingerprint rather than a user-supplied label means `rpc_url`
    /// overrides are caught too.
    pub fn open(
        data_dir: &Path,
        chain: Chain,
        identity: &str,
        anchor_floor: Option<u64>,
    ) -> Result<Self> {
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
        verify_and_stamp_meta(&db, data_dir, chain, identity, has_block_data)?;

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
                chain,
                bs_dir,
                store: RwLock::new(store),
                db,
                hash_to_height,
                tx_to_block,
                anchor_floor,
            }),
        })
    }

    /// Which chain's blocks this store holds.
    pub fn chain(&self) -> Chain {
        self.inner.chain
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

    /// Has anything ever been written? Distinguishes a genuinely empty store
    /// from one holding only height 0, which the height accessors report
    /// identically (both give 0). Ingest needs the distinction to decide between
    /// anchoring a fresh store and resuming one.
    pub async fn is_empty(&self) -> bool {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || inner.store.blocking_read().is_none())
            .await
            .unwrap_or(true)
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
    /// The returned [`record::Element`] owns the decompressed record and derefs
    /// to just the canonical block JSON (element 0) — the single choke point
    /// every block-bytes read flows through (by-height, by-hash, oldBlocks, bulk
    /// export), so they all see bare block JSON without knowing which chain's
    /// record shape they are holding.
    pub async fn get_by_height(&self, height: u64) -> Result<Option<record::Element>> {
        self.get_element(height, record::BLOCK).await
    }

    /// Read one element of the record stored at `height`. Element
    /// [`record::BLOCK`] is the block JSON on every chain; the trailing indexes
    /// are the chain's derived data (see [`crate::record`]).
    pub async fn get_element(&self, height: u64, idx: usize) -> Result<Option<record::Element>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<Option<record::Element>> {
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
                // `None` means the record has no such element — a height stored
                // before the combined record has no derived data. That is a miss
                // (→ 421), not an empty result.
                let Some(bytes) = record::Element::at(arc, idx)
                    .with_context(|| format!("decoding stored record at height {height}"))?
                else {
                    debug!(
                        height,
                        idx, "record element absent (pre-combined-record height)"
                    );
                    return Ok(None);
                };
                debug!(
                    height,
                    idx,
                    bytes = bytes.as_ref().len(),
                    "read record element by height",
                );
                Ok(Some(bytes))
            } else {
                debug!(height, "block not present: gap in stored range");
                Ok(None)
            }
        })
        .await?
    }

    /// Resolve a 32-byte block hash to its height via the `hash_to_height` index,
    /// `None` if we haven't indexed it.
    pub fn height_of_hash(&self, hash: [u8; 32]) -> Result<Option<u64>> {
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
        Ok(Some(height))
    }

    /// Read a block's stored bytes by 32-byte hash.
    pub async fn get_by_hash(&self, hash: [u8; 32]) -> Result<Option<record::Element>> {
        match self.height_of_hash(hash)? {
            Some(height) => self.get_by_height(height).await,
            None => Ok(None),
        }
    }

    /// The whole stored record at `height`, undecomposed. This is what a mirror
    /// needs: every element, including the chain's derived data, which a
    /// block-only read would drop.
    ///
    /// `None` for a height written before the combined record: there is no
    /// record there, only a bare block, and a mirror must not be handed one in
    /// place of the other.
    pub async fn get_record(&self, height: u64) -> Result<Option<Arc<[u8]>>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<Option<Arc<[u8]>>> {
            let guard = inner.store.blocking_read();
            let Some(store) = guard.as_ref() else {
                return Ok(None);
            };
            if height < store.min_block_height() || height > store.height_highwater() {
                return Ok(None);
            }
            let Some(arc) = store.read_block(height)? else {
                return Ok(None);
            };
            // A height stored before the combined record has no record to give —
            // only a bare block. Report it as absent rather than handing a
            // subscriber an object where it expects a full element array.
            if !record::is_combined_record(arc.as_ref()) {
                debug!(
                    height,
                    "no combined record at this height (pre-upgrade block)"
                );
                return Ok(None);
            }
            Ok(Some(arc))
        })
        .await?
    }

    /// Read element `idx` of every record in the inclusive height range, in
    /// order — e.g. the per-block JSON log arrays that back `eth_getLogs`.
    /// `None` if any height in the range is missing (an incomplete range the
    /// caller must not serve as a partial result). One blocking task for the
    /// whole range so a scan takes a single read lock.
    pub async fn read_element_range(
        &self,
        from: u64,
        to: u64,
        idx: usize,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<Option<Vec<Vec<u8>>>> {
            let guard = inner.store.blocking_read();
            let Some(store) = guard.as_ref() else {
                return Ok(None);
            };
            let mut out = Vec::new();
            for height in from..=to {
                let Some(arc) = store.read_block(height)? else {
                    return Ok(None);
                };
                // A height with no such element makes the whole range
                // unanswerable — the caller must not serve a partial result.
                let Some(part) = record::element(arc.as_ref(), idx)
                    .with_context(|| format!("decoding stored record at height {height}"))?
                else {
                    debug!(
                        height,
                        idx, "record element absent; range cannot be answered completely",
                    );
                    return Ok(None);
                };
                out.push(part.to_vec());
            }
            Ok(Some(out))
        })
        .await?
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
    /// 1. Blockstore `write_block` of the encoded record (element 0 is the
    ///    block JSON; the trailing elements are the chain's derived data —
    ///    `record::EMPTY_ARRAY` for a feed that isn't ingesting yet), then
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
        elements: &[&[u8]],
    ) -> Result<()> {
        // Wrong element count means a caller built a record for a different
        // chain's layout; refuse rather than write something unreadable.
        let want = record::arity(self.inner.chain);
        if elements.len() != want {
            bail!(
                "the {} chain's record has {want} elements, got {}",
                self.inner.chain.as_str(),
                elements.len(),
            );
        }
        // Encode up front so the blocking task only does the write (the owned
        // `combined` is all it needs).
        let combined = record::encode(elements);
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

    /// The C-chain mainnet identity stamp: `eth_chainId` in decimal.
    const IDENTITY: &str = "43114";

    /// Open a C-chain store at `dir` with the standard test identity.
    fn open_c(dir: &std::path::Path) -> Result<Storage> {
        Storage::open(dir, Chain::C, IDENTITY, None)
    }

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
        let storage = open_c(&dir).unwrap();
        let block = br#"{"number":"0xa","hash":"0xbb","transactions":[]}"#.to_vec();
        storage
            .put(10, [0xbb; 32], &[], &[&block, record::EMPTY_ARRAY])
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
        let storage = open_c(&dir).unwrap();
        let block = br#"{"number":"0x1"}"#.to_vec();
        storage
            .put(1, [1; 32], &[], &[&block, record::EMPTY_ARRAY])
            .await
            .unwrap();

        let raw = {
            let guard = storage.inner.store.read().await;
            guard.as_ref().unwrap().read_block(1).unwrap().unwrap()
        };
        assert_eq!(raw.as_ref(), record::encode(&[&block, record::EMPTY_ARRAY]),);
    }

    /// Reopening a freshly-created, correctly-stamped store succeeds and the
    /// data is still readable.
    #[tokio::test]
    async fn reopen_same_version_ok() {
        let dir = unique_temp_dir();
        {
            let storage = open_c(&dir).unwrap();
            storage
                .put(
                    5,
                    [5; 32],
                    &[],
                    &[br#"{"number":"0x5"}"#, record::EMPTY_ARRAY],
                )
                .await
                .unwrap();
            storage.persist().await.unwrap();
        }
        let reopened = open_c(&dir).unwrap();
        assert!(reopened.get_by_height(5).await.unwrap().is_some());
    }

    /// Block data with **no network stamp at all** cannot be verified against
    /// anything: adopting it would stamp whatever network this process happens
    /// to point at, silently binding (say) mainnet blocks to a testnet identity
    /// forever. Refuse instead — the store itself carries no way to tell.
    #[tokio::test]
    async fn refuses_block_data_with_no_network_stamp() {
        let dir = unique_temp_dir();
        write_bare_block(&dir, 100, br#"{"number":"0x64"}"#);
        let err = open_c(&dir).unwrap_err().to_string();
        assert!(err.contains("no network stamp"), "unexpected error: {err}");
    }

    /// A store with a chain-ID stamp but no format-version stamp is the
    /// pre-logs layout. It is adopted and stamped rather than refused, so an
    /// upgrade keeps its history instead of resyncing.
    #[tokio::test]
    async fn adopts_pre_logs_store_without_format_stamp() {
        let dir = unique_temp_dir();
        stamp_meta(&dir, &[("chain_id", "43114")]);

        let storage = open_c(&dir).unwrap();
        assert_eq!(storage.chain(), Chain::C);
        drop(storage);

        // The stamps are now on disk, so the next open verifies rather than infers.
        let db = Database::builder(dir.join("index")).open().unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        assert_eq!(
            meta.get(FORMAT_VERSION_KEY).unwrap().unwrap().as_ref(),
            b"1"
        );
        assert_eq!(meta.get(CHAIN_KEY).unwrap().unwrap().as_ref(), b"c");
    }

    /// A store stamped with a different (incompatible) format version is
    /// refused, naming the offending version.
    #[tokio::test]
    async fn rejects_incompatible_format_version() {
        let dir = unique_temp_dir();
        stamp_meta(&dir, &[("chain_id", "43114"), (FORMAT_VERSION_KEY, "999")]);
        let err = open_c(&dir).unwrap_err().to_string();
        assert!(err.contains("999"), "unexpected error: {err}");
    }

    /// The production-upgrade path, end to end: a store holding real **bare
    /// block** data with no `meta` stamps at all — what every neve wrote before
    /// the combined record — must open, serve its blocks unchanged, and report
    /// derived data as absent rather than empty.
    ///
    /// Refusing this is what took a 22 GB production store offline; serving
    /// `[]` for its logs would be worse still, since a client would read "this
    /// height has no logs" when the truth is "we never ingested them".
    #[tokio::test]
    async fn adopts_and_reads_a_bare_block_store() {
        let dir = unique_temp_dir();
        let bare = br#"{"number":"0x64","hash":"0xabc","transactions":[]}"#;
        // Production's shape: the network is stamped (so it can be verified),
        // only the record format predates the combined layout.
        stamp_meta(&dir, &[("chain_id", IDENTITY)]);
        write_bare_block(&dir, 100, bare);

        let storage = open_c(&dir).unwrap();

        // The block reads back byte-identical to what the old build wrote.
        let got = storage.get_by_height(100).await.unwrap().unwrap();
        assert_eq!(got.as_ref(), bare.as_slice());

        // Its logs are ABSENT, not empty — so eth_getLogs defers upstream (421)
        // instead of claiming the height had no logs.
        assert!(
            storage
                .read_element_range(100, 100, record::C_LOGS)
                .await
                .unwrap()
                .is_none(),
            "a pre-combined-record height must not report empty logs",
        );

        // New writes land as full records alongside the old bare blocks, and
        // both read correctly from the same store.
        storage
            .put(
                101,
                [101; 32],
                &[],
                &[br#"{"number":"0x65"}"#, br#"[{"address":"0xa"}]"#],
            )
            .await
            .unwrap();
        assert_eq!(
            storage.get_by_height(101).await.unwrap().unwrap().as_ref(),
            br#"{"number":"0x65"}"#.as_slice(),
        );
        let logs = storage
            .read_element_range(101, 101, record::C_LOGS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0], br#"[{"address":"0xa"}]"#.to_vec());

        // A range spanning both layouts is still unanswerable, because the old
        // half has no logs to report.
        assert!(
            storage
                .read_element_range(100, 101, record::C_LOGS)
                .await
                .unwrap()
                .is_none(),
        );

        // And a mirror asking for the whole record gets nothing for the legacy
        // height — it must not receive a bare block dressed up as a record —
        // while the height written after the upgrade streams normally.
        assert!(storage.get_record(100).await.unwrap().is_none());
        assert!(storage.get_record(101).await.unwrap().is_some());
    }

    /// A store written before multi-chain support has no `chain` stamp. It holds
    /// C-chain data by construction, so opening it as C-chain must succeed with
    /// no migration — this is what keeps existing data dirs alive — and the open
    /// must add the missing stamp.
    #[tokio::test]
    async fn adopts_and_stamps_a_pre_multichain_c_store() {
        let dir = unique_temp_dir();
        stamp_meta(&dir, &[("chain_id", "43114"), (FORMAT_VERSION_KEY, "1")]);

        let storage = open_c(&dir).unwrap();
        assert_eq!(storage.chain(), Chain::C);
        storage
            .put(
                9,
                [9; 32],
                &[],
                &[br#"{"number":"0x9"}"#, record::EMPTY_ARRAY],
            )
            .await
            .unwrap();
        drop(storage);

        // The stamp is now on disk, so the next open verifies rather than infers.
        let db = Database::builder(dir.join("index")).open().unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let stamped = meta.get(CHAIN_KEY).unwrap().unwrap();
        assert_eq!(stamped.as_ref(), b"c");
    }

    /// The same unstamped store must NOT be adopted by a different chain: its
    /// data is C-chain data, and silently opening it as P-chain would mis-parse
    /// every record.
    #[tokio::test]
    async fn refuses_pre_multichain_store_as_another_chain() {
        let dir = unique_temp_dir();
        stamp_meta(&dir, &[("chain_id", "43114"), (FORMAT_VERSION_KEY, "1")]);
        let err = Storage::open(&dir, Chain::P, "genesis-id", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("multi-chain"), "unexpected error: {err}");
    }

    /// An explicitly stamped store refuses the other chain, naming both sides.
    #[tokio::test]
    async fn rejects_cross_chain_open() {
        let dir = unique_temp_dir();
        // Create and stamp a real P-chain store, then try to open it as C.
        Storage::open(&dir, Chain::P, "genesis-id", None).unwrap();
        let err = open_c(&dir).unwrap_err().to_string();
        assert!(err.contains("stamped for the p chain"), "unexpected: {err}");
    }

    /// Two chains under one `--data-dir` base land in separate directories and
    /// neither open disturbs the other.
    #[tokio::test]
    async fn sibling_chain_stores_coexist_under_one_base() {
        let base = unique_temp_dir();
        let c = Storage::open(&Chain::C.data_dir(&base), Chain::C, IDENTITY, None).unwrap();
        let p = Storage::open(&Chain::P.data_dir(&base), Chain::P, "genesis-id", None).unwrap();
        assert_eq!(c.chain(), Chain::C);
        assert_eq!(p.chain(), Chain::P);

        c.put(
            1,
            [1; 32],
            &[],
            &[br#"{"number":"0x1"}"#, record::EMPTY_ARRAY],
        )
        .await
        .unwrap();
        // The P store is untouched by the C write, and reopening C still works.
        assert!(p.get_by_height(1).await.unwrap().is_none());
        assert!(c.get_by_height(1).await.unwrap().is_some());
    }

    /// A network mismatch on the same chain is refused — the guard that catches
    /// pointing a mainnet data dir at a testnet endpoint.
    #[tokio::test]
    async fn rejects_identity_mismatch() {
        let dir = unique_temp_dir();
        open_c(&dir).unwrap();
        let err = Storage::open(&dir, Chain::C, "43113", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("43113"), "unexpected error: {err}");
    }
}
