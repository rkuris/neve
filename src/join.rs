//! In-memory block↔logs join buffer.
//!
//! Blocks and logs arrive on independent streams; a height is durably written
//! (as the combined `[block, logs]` record) only once both halves are present.
//! Incomplete halves are held here, keyed by height: the first half to arrive
//! waits for the second, then the pair is joined and written, so the store only
//! ever holds complete records.
//!
//! A cap on the number of pending heights bounds memory. On overflow the whole
//! buffer is flushed (all pending halves dropped) and the triggering height
//! deferred — all left for backfill to re-derive. Flushing, rather than just
//! refusing the new height, avoids a wedge: once a stalled live source resumes
//! at the tip it never re-sends the stranded heights, so their lone halves could
//! never complete and would pin the buffer full forever. See
//! `docs/neve-logs-ingestion-plan.md`.
//!
//! Wired into the C-chain live ingest path (`crate::eth::ingest`):
//! `newHeads`→`on_block` and a per-block `eth_getLogs`→`on_logs`, with block
//! reads consulting `buffered_block` for an in-flight tip. Backfill joins
//! window-locally and does not use this buffer.
//!
//! Nothing here is C-chain-specific beyond the metric label: the two halves are
//! opaque byte strings, so the same buffer joins a P-chain block to its
//! fetched-at-ingest reward UTXOs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use anyhow::Result;
use metrics::{counter, gauge, histogram};

use crate::chain::Chain;
use crate::metrics::{
    JOIN_BUFFER_CAP_HIT_TOTAL, JOIN_BUFFER_CAPACITY, JOIN_BUFFER_INCOMPLETE,
    JOIN_BUFFER_INCOMPLETE_BYTES, JOIN_BUFFER_OLDEST_PENDING, JOIN_COMPLETED_TOTAL, JOIN_LATENCY,
};
use crate::storage::Storage;

/// Which half a metric label refers to (`half` / `first` = "block" | "log").
#[derive(Clone, Copy)]
enum Half {
    Block,
    Log,
}

impl Half {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Log => "log",
        }
    }
}

/// Result of feeding one half into the buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinOutcome {
    /// Both halves are now present; the combined record was written.
    Completed,
    /// First half stored; waiting for the other.
    Buffered,
    /// Cap reached: the buffer was flushed and this height dropped, all left for
    /// backfill to re-derive.
    Deferred,
}

/// A buffered half is owned but never mutated after insertion, so `Box<[u8]>`
/// (no spare capacity) rather than `Vec<u8>`.
struct PendingBlock {
    hash: [u8; 32],
    tx_hashes: Vec<[u8; 32]>,
    bytes: Box<[u8]>,
    since: Instant,
}

struct PendingLogs {
    bytes: Box<[u8]>,
    since: Instant,
}

enum Pending {
    Block(PendingBlock),
    Logs(PendingLogs),
}

/// The decision made under the lock, acted on (write / metrics) after release.
/// Its byte fields are transient (written then dropped), so plain `Vec<u8>`.
enum WriteAct {
    Write {
        hash: [u8; 32],
        tx_hashes: Vec<[u8; 32]>,
        block_bytes: Vec<u8>,
        logs: Vec<u8>,
        first: Half,
        since: Instant,
    },
    Buffered,
    Deferred,
}

struct Inner {
    storage: Storage,
    /// Which chain's halves this buffer joins — the `chain` metric label. Taken
    /// from the store so it can never disagree with what gets written.
    chain: Chain,
    /// Max pending heights before the buffer is flushed on the next new height.
    max_entries: usize,
    pending: Mutex<HashMap<u64, Pending>>,
}

/// Shared, cheaply-clonable join buffer.
#[derive(Clone)]
pub(crate) struct JoinBuffer {
    inner: Arc<Inner>,
}

impl JoinBuffer {
    pub fn new(storage: Storage, max_entries: usize) -> Self {
        let chain = storage.chain();
        Self {
            inner: Arc::new(Inner {
                storage,
                chain,
                max_entries,
                pending: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The `chain` label every series this buffer records carries.
    fn chain(&self) -> &'static str {
        self.inner.chain.as_str()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Pending>> {
        // Recover the guard on poison rather than unwrap-panicking: the buffer is
        // a cache, and a poisoned mutex just means a prior write task panicked.
        self.inner
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// A block arrived for `height`. Completes (and writes) if its logs are
    /// already buffered; otherwise buffers the block, flushing on overflow.
    pub async fn on_block(
        &self,
        height: u64,
        hash: [u8; 32],
        tx_hashes: Vec<[u8; 32]>,
        block_bytes: Vec<u8>,
    ) -> Result<JoinOutcome> {
        let act = {
            let mut pending = self.lock();
            match pending.remove(&height) {
                Some(Pending::Logs(logs)) => WriteAct::Write {
                    hash,
                    tx_hashes,
                    block_bytes,
                    logs: Vec::from(logs.bytes),
                    first: Half::Log,
                    since: logs.since,
                },
                Some(Pending::Block(prev)) => {
                    // Duplicate block (retry/reorg): replace, keep the original
                    // wait start so latency still measures the true first arrival.
                    pending.insert(
                        height,
                        Pending::Block(PendingBlock {
                            hash,
                            tx_hashes,
                            bytes: block_bytes.into_boxed_slice(),
                            since: prev.since,
                        }),
                    );
                    WriteAct::Buffered
                }
                None if pending.len() >= self.inner.max_entries => {
                    pending.clear();
                    WriteAct::Deferred
                }
                None => {
                    pending.insert(
                        height,
                        Pending::Block(PendingBlock {
                            hash,
                            tx_hashes,
                            bytes: block_bytes.into_boxed_slice(),
                            since: Instant::now(),
                        }),
                    );
                    WriteAct::Buffered
                }
            }
        };
        self.apply(height, act).await
    }

    /// Logs arrived for `height`. Completes (and writes) if the block is already
    /// buffered; otherwise buffers the logs, flushing on overflow.
    pub async fn on_logs(&self, height: u64, logs_bytes: Vec<u8>) -> Result<JoinOutcome> {
        let act = {
            let mut pending = self.lock();
            match pending.remove(&height) {
                Some(Pending::Block(block)) => WriteAct::Write {
                    hash: block.hash,
                    tx_hashes: block.tx_hashes,
                    block_bytes: Vec::from(block.bytes),
                    logs: logs_bytes,
                    first: Half::Block,
                    since: block.since,
                },
                Some(Pending::Logs(prev)) => {
                    pending.insert(
                        height,
                        Pending::Logs(PendingLogs {
                            bytes: logs_bytes.into_boxed_slice(),
                            since: prev.since,
                        }),
                    );
                    WriteAct::Buffered
                }
                None if pending.len() >= self.inner.max_entries => {
                    pending.clear();
                    WriteAct::Deferred
                }
                None => {
                    pending.insert(
                        height,
                        Pending::Logs(PendingLogs {
                            bytes: logs_bytes.into_boxed_slice(),
                            since: Instant::now(),
                        }),
                    );
                    WriteAct::Buffered
                }
            }
        };
        self.apply(height, act).await
    }

    /// Carry out the post-lock decision: record metrics and, on completion, the
    /// durable write.
    async fn apply(&self, height: u64, act: WriteAct) -> Result<JoinOutcome> {
        match act {
            WriteAct::Write {
                hash,
                tx_hashes,
                block_bytes,
                logs,
                first,
                since,
            } => {
                counter!(JOIN_COMPLETED_TOTAL, "chain" => self.chain(), "first" => first.as_str())
                    .increment(1);
                histogram!(JOIN_LATENCY, "chain" => self.chain())
                    .record(since.elapsed().as_secs_f64());
                self.inner
                    .storage
                    .put(height, hash, &tx_hashes, &block_bytes, &logs)
                    .await?;
                Ok(JoinOutcome::Completed)
            }
            WriteAct::Buffered => Ok(JoinOutcome::Buffered),
            WriteAct::Deferred => {
                counter!(JOIN_BUFFER_CAP_HIT_TOTAL, "chain" => self.chain()).increment(1);
                Ok(JoinOutcome::Deferred)
            }
        }
    }

    /// Block bytes for an in-flight height (block buffered, not yet written), so
    /// reads can be served before the durable write. (Wired into the read path
    /// when the live ingest path routes through the buffer.)
    pub fn buffered_block(&self, height: u64) -> Option<Vec<u8>> {
        match self.lock().get(&height) {
            Some(Pending::Block(b)) => Some(b.bytes.to_vec()),
            _ => None,
        }
    }

    /// Refresh the buffer gauges (call periodically): per-half pending count and
    /// bytes, oldest-pending dwell time, and the capacity. Computed by iterating
    /// the (small) buffer, so ingest carries no running counters. A rising
    /// oldest-age with a small count means one side stalled.
    #[expect(clippy::cast_precision_loss)]
    pub fn sample(&self) {
        let pending = self.lock();
        let now = Instant::now();
        let age = |t: Instant| now.saturating_duration_since(t).as_secs_f64();

        let block_count = pending
            .values()
            .filter(|p| matches!(p, Pending::Block(_)))
            .count();
        let log_count = pending
            .values()
            .filter(|p| matches!(p, Pending::Logs(_)))
            .count();
        let block_bytes: usize = pending
            .values()
            .filter_map(|p| match p {
                Pending::Block(b) => Some(b.bytes.len()),
                Pending::Logs(_) => None,
            })
            .sum();
        let log_bytes: usize = pending
            .values()
            .filter_map(|p| match p {
                Pending::Logs(l) => Some(l.bytes.len()),
                Pending::Block(_) => None,
            })
            .sum();
        let block_oldest = pending
            .values()
            .filter_map(|p| match p {
                Pending::Block(b) => Some(b.since),
                Pending::Logs(_) => None,
            })
            .min();
        let log_oldest = pending
            .values()
            .filter_map(|p| match p {
                Pending::Logs(l) => Some(l.since),
                Pending::Block(_) => None,
            })
            .min();

        let chain = self.chain();
        gauge!(JOIN_BUFFER_INCOMPLETE, "chain" => chain, "half" => Half::Block.as_str())
            .set(block_count as f64);
        gauge!(JOIN_BUFFER_INCOMPLETE, "chain" => chain, "half" => Half::Log.as_str())
            .set(log_count as f64);
        gauge!(JOIN_BUFFER_INCOMPLETE_BYTES, "chain" => chain, "half" => Half::Block.as_str())
            .set(block_bytes as f64);
        gauge!(JOIN_BUFFER_INCOMPLETE_BYTES, "chain" => chain, "half" => Half::Log.as_str())
            .set(log_bytes as f64);
        gauge!(JOIN_BUFFER_OLDEST_PENDING, "chain" => chain, "half" => Half::Block.as_str())
            .set(block_oldest.map_or(0.0, age));
        gauge!(JOIN_BUFFER_OLDEST_PENDING, "chain" => chain, "half" => Half::Log.as_str())
            .set(log_oldest.map_or(0.0, age));
        gauge!(JOIN_BUFFER_CAPACITY, "chain" => chain).set(self.inner.max_entries as f64);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for JoinBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinBuffer")
            .field("pending", &self.lock().len())
            .field("max_entries", &self.inner.max_entries)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    use metrics_exporter_prometheus::PrometheusBuilder;

    const IDENTITY: &str = "43114";
    const BLOCK: &[u8] = br#"{"number":"0x5"}"#;
    const BLOCK_B: &[u8] = br#"{"number":"0x5","v":2}"#;
    const LOGS: &[u8] = br#"[{"address":"0xabc"}]"#;

    fn unique_temp_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "neve-join-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ))
    }

    fn buffer(max_entries: usize) -> (Storage, JoinBuffer) {
        let storage = Storage::open(&unique_temp_dir(), Chain::C, IDENTITY, None).unwrap();
        let buf = JoinBuffer::new(storage.clone(), max_entries);
        (storage, buf)
    }

    #[tokio::test]
    async fn block_then_logs_completes() {
        let (storage, buf) = buffer(16);

        assert_eq!(
            buf.on_block(5, [5; 32], vec![], BLOCK.to_vec())
                .await
                .unwrap(),
            JoinOutcome::Buffered
        );
        assert_eq!(buf.len(), 1);
        assert!(storage.get_by_height(5).await.unwrap().is_none());
        assert_eq!(buf.buffered_block(5).as_deref(), Some(BLOCK));

        assert_eq!(
            buf.on_logs(5, LOGS.to_vec()).await.unwrap(),
            JoinOutcome::Completed
        );
        assert!(buf.is_empty());
        let got = storage.get_by_height(5).await.unwrap().unwrap();
        assert_eq!(got.as_ref(), BLOCK);
    }

    #[tokio::test]
    async fn logs_then_block_completes() {
        let (storage, buf) = buffer(16);

        assert_eq!(
            buf.on_logs(7, LOGS.to_vec()).await.unwrap(),
            JoinOutcome::Buffered
        );
        // A buffered log half is not a servable block.
        assert_eq!(buf.buffered_block(7), None);

        assert_eq!(
            buf.on_block(7, [7; 32], vec![], BLOCK.to_vec())
                .await
                .unwrap(),
            JoinOutcome::Completed
        );
        assert!(buf.is_empty());
        assert!(storage.get_by_height(7).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn cap_flushes_buffer_and_defers() {
        let (_storage, buf) = buffer(1);

        assert_eq!(
            buf.on_block(1, [1; 32], vec![], BLOCK.to_vec())
                .await
                .unwrap(),
            JoinOutcome::Buffered
        );
        // At cap: the next new height flushes the whole buffer and is deferred.
        assert_eq!(
            buf.on_block(2, [2; 32], vec![], BLOCK.to_vec())
                .await
                .unwrap(),
            JoinOutcome::Deferred
        );
        assert!(buf.is_empty());

        // After the flush there is room again.
        assert_eq!(
            buf.on_block(2, [2; 32], vec![], BLOCK.to_vec())
                .await
                .unwrap(),
            JoinOutcome::Buffered
        );
    }

    #[tokio::test]
    async fn duplicate_block_replaces_and_keeps_one_entry() {
        let (storage, buf) = buffer(16);

        buf.on_block(3, [3; 32], vec![], BLOCK.to_vec())
            .await
            .unwrap();
        assert_eq!(
            buf.on_block(3, [0x33; 32], vec![], BLOCK_B.to_vec())
                .await
                .unwrap(),
            JoinOutcome::Buffered
        );
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.buffered_block(3).as_deref(), Some(BLOCK_B));

        buf.on_logs(3, LOGS.to_vec()).await.unwrap();
        let got = storage.get_by_height(3).await.unwrap().unwrap();
        assert_eq!(got.as_ref(), BLOCK_B);
    }

    #[tokio::test]
    async fn sample_reports_per_half_levels() {
        let (_storage, buf) = buffer(16);
        // One buffered block half (h1) and one buffered log half (h2).
        buf.on_block(1, [1; 32], vec![], BLOCK.to_vec())
            .await
            .unwrap();
        buf.on_logs(2, LOGS.to_vec()).await.unwrap();

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || buf.sample());
        let out = handle.render();

        assert!(
            out.contains(r#"neve_join_buffer_incomplete{chain="c",half="block"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"neve_join_buffer_incomplete{chain="c",half="log"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"neve_join_buffer_capacity{chain="c"} 16"#),
            "{out}"
        );
    }
}
