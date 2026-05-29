use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::core::async_trait;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{PendingSubscriptionSink, ServerBuilder, ServerConfig, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::metrics::SubMetricsGuard;
use crate::storage::Storage;

/// JSON-RPC error code we use for "block not found" — matches geth's `-32000`
/// style (server error range), with a descriptive message.
const BLOCK_NOT_FOUND: i32 = -32000;

fn err(msg: impl Into<String>) -> ErrorObjectOwned {
    ErrorObjectOwned::owned::<()>(BLOCK_NOT_FOUND, msg.into(), None)
}

#[rpc(server, namespace = "eth")]
pub trait EthApi {
    #[method(name = "chainId")]
    async fn chain_id(&self) -> Result<String, ErrorObjectOwned>;

    #[method(name = "blockNumber")]
    async fn block_number(&self) -> Result<String, ErrorObjectOwned>;

    #[method(name = "getBlockByNumber")]
    async fn get_block_by_number(
        &self,
        block: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned>;

    #[method(name = "getBlockByHash")]
    async fn get_block_by_hash(
        &self,
        hash: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned>;

    #[method(name = "getBlockTransactionCountByNumber")]
    async fn get_block_transaction_count_by_number(
        &self,
        block: String,
    ) -> Result<Option<String>, ErrorObjectOwned>;

    #[method(name = "getBlockTransactionCountByHash")]
    async fn get_block_transaction_count_by_hash(
        &self,
        hash: String,
    ) -> Result<Option<String>, ErrorObjectOwned>;

    #[method(name = "getTransactionByBlockNumberAndIndex")]
    async fn get_transaction_by_block_number_and_index(
        &self,
        block: String,
        index: String,
    ) -> Result<Option<Value>, ErrorObjectOwned>;

    #[method(name = "getTransactionByBlockHashAndIndex")]
    async fn get_transaction_by_block_hash_and_index(
        &self,
        hash: String,
        index: String,
    ) -> Result<Option<Value>, ErrorObjectOwned>;

    #[method(name = "getTransactionByHash")]
    async fn get_transaction_by_hash(
        &self,
        hash: String,
    ) -> Result<Option<Value>, ErrorObjectOwned>;

    #[method(name = "getTransactionReceipt")]
    async fn get_transaction_receipt(
        &self,
        hash: String,
    ) -> Result<Option<Value>, ErrorObjectOwned>;

    /// `eth_subscribe(kind, from?, to?)` — server-push of blocks.
    ///
    /// Live kinds ignore `from`/`to` and stream the tip as it advances:
    /// `"newHeads"` pushes the block header (geth-compatible); `"newBlocks"`
    /// is a neve extension that pushes the **whole** block (transactions
    /// included) so a downstream mirror can persist it without a follow-up
    /// `eth_getBlockByNumber` round-trip.
    ///
    /// `"oldBlocks"` is a neve extension that replays a historical range from
    /// storage: `from` (hex, required) is the inclusive start; `to` (hex,
    /// optional) the inclusive end. With `to` omitted the stream follows the
    /// contiguous tip as it advances and completes once caught up — the
    /// mirror's bootstrap-done signal. A request we cannot serve gaplessly
    /// (`from` below our earliest block, or `to` past the contiguous tip) is
    /// rejected up front.
    ///
    /// Generates `eth_subscribe` / `eth_unsubscribe`, with notifications under
    /// method `eth_subscription` (distinguished by subscription id). WebSocket
    /// transport only.
    #[subscription(name = "subscribe" => "subscription", unsubscribe = "unsubscribe", item = Value)]
    async fn subscribe(
        &self,
        kind: String,
        from: Option<String>,
        to: Option<String>,
    ) -> SubscriptionResult;
}

/// How a JSON-RPC caller named the block: a tag/number string (the
/// `eth_*ByNumber` family) or a 32-byte hash (`eth_*ByHash` family).
enum BlockSelector {
    Number(String),
    Hash(String),
    Height(u64),
}

/// Which `eth_subscribe` kind a subscriber asked for. `newHeads` is the
/// geth-compatible header stream; `newBlocks` is a neve extension that pushes
/// whole blocks (transactions included) so a downstream mirror can persist them
/// without a follow-up fetch. `oldBlocks` is a neve extension that replays a
/// historical range straight from storage. The wire spellings live here —
/// parsed by `from_wire`, rendered by `as_str` (also the metrics `kind` label).
#[derive(Debug)]
pub(crate) enum SubKind {
    NewHeads,
    NewBlocks,
    OldBlocks,
}

impl SubKind {
    /// Parse an `eth_subscribe(kind)` wire token; `None` for unsupported kinds.
    fn from_wire(s: &str) -> Option<Self> {
        match s {
            "newHeads" => Some(Self::NewHeads),
            "newBlocks" => Some(Self::NewBlocks),
            "oldBlocks" => Some(Self::OldBlocks),
            _ => None,
        }
    }

    /// Whether this kind delivers headers (transactions stripped) rather than
    /// whole blocks. Only the live `newHeads` stream strips; `newBlocks` and
    /// the historical `oldBlocks` replay forward whole blocks.
    const fn strips_transactions(&self) -> bool {
        matches!(self, Self::NewHeads)
    }

    /// The wire / metrics-label spelling.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::NewHeads => "newHeads",
            Self::NewBlocks => "newBlocks",
            Self::OldBlocks => "oldBlocks",
        }
    }
}

pub struct EthApiImpl {
    storage: Storage,
    chain_id: u64,
    /// Live-tip fan-out carrying the **full** block. `persist_block` publishes
    /// each stored block here; one receiver is handed to every subscriber.
    /// `newHeads` subscribers strip transactions from their own copy;
    /// `newBlocks` subscribers forward it whole.
    blocks: broadcast::Sender<Value>,
}

impl EthApiImpl {
    pub const fn new(storage: Storage, chain_id: u64, blocks: broadcast::Sender<Value>) -> Self {
        Self {
            storage,
            chain_id,
            blocks,
        }
    }

    /// Resolve a selector to stored block bytes, decode the JSON once, then
    /// hand the parsed `Value` to `project`. Outer `None` = block not in our
    /// store (drives the 200→421 middleware); inner `None` from `project` =
    /// projection-level miss (e.g. tx index out of range), same 421 behavior.
    async fn lookup_block<F, R>(
        &self,
        sel: BlockSelector,
        project: F,
    ) -> Result<Option<R>, ErrorObjectOwned>
    where
        F: FnOnce(Value) -> Result<Option<R>, ErrorObjectOwned>,
    {
        let bytes = match sel {
            BlockSelector::Number(tag) => {
                let h = self.resolve_block_tag(&tag).await?;
                self.storage.get_by_height(h).await
            }
            BlockSelector::Hash(hash) => {
                let arr = parse_hash(&hash)?;
                self.storage.get_by_hash(arr).await
            }
            BlockSelector::Height(h) => self.storage.get_by_height(h).await,
        }
        .map_err(|e| err(format!("storage error: {e}")))?;

        let Some(bytes) = bytes else { return Ok(None) };
        let v: Value =
            serde_json::from_slice(&bytes).map_err(|e| err(format!("stored block decode: {e}")))?;
        project(v)
    }

    async fn resolve_block_tag(&self, tag: &str) -> Result<u64, ErrorObjectOwned> {
        match tag {
            "latest" | "finalized" | "safe" => {
                let hw = self.storage.high_water().await;
                if hw == 0 {
                    Err(err("no blocks stored yet"))
                } else {
                    Ok(hw)
                }
            }
            "earliest" | "pending" => Err(err(format!("unsupported block tag: {tag}"))),
            hex => {
                let stripped = hex.strip_prefix("0x").unwrap_or(hex);
                u64::from_str_radix(stripped, 16)
                    .map_err(|_| err(format!("invalid block number: {hex}")))
            }
        }
    }
}

#[async_trait]
impl EthApiServer for EthApiImpl {
    async fn chain_id(&self) -> Result<String, ErrorObjectOwned> {
        Ok(format!("0x{:x}", self.chain_id))
    }

    async fn block_number(&self) -> Result<String, ErrorObjectOwned> {
        Ok(format!("0x{:x}", self.storage.high_water().await))
    }

    async fn get_block_by_number(
        &self,
        block: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        self.lookup_block(BlockSelector::Number(block), |v| {
            Ok(Some(shape_block(v, full_tx)))
        })
        .await
    }

    async fn get_block_by_hash(
        &self,
        hash: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        self.lookup_block(BlockSelector::Hash(hash), |v| {
            Ok(Some(shape_block(v, full_tx)))
        })
        .await
    }

    async fn get_block_transaction_count_by_number(
        &self,
        block: String,
    ) -> Result<Option<String>, ErrorObjectOwned> {
        self.lookup_block(BlockSelector::Number(block), |v| Ok(Some(tx_count_hex(&v))))
            .await
    }

    async fn get_block_transaction_count_by_hash(
        &self,
        hash: String,
    ) -> Result<Option<String>, ErrorObjectOwned> {
        self.lookup_block(BlockSelector::Hash(hash), |v| Ok(Some(tx_count_hex(&v))))
            .await
    }

    async fn get_transaction_by_block_number_and_index(
        &self,
        block: String,
        index: String,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        let idx = parse_quantity(&index)? as usize;
        self.lookup_block(BlockSelector::Number(block), |v| {
            Ok(nth_transaction(v, idx))
        })
        .await
    }

    async fn get_transaction_by_block_hash_and_index(
        &self,
        hash: String,
        index: String,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        let idx = parse_quantity(&index)? as usize;
        self.lookup_block(BlockSelector::Hash(hash), |v| Ok(nth_transaction(v, idx)))
            .await
    }

    async fn get_transaction_by_hash(
        &self,
        hash: String,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        let arr = parse_hash(&hash)?;
        let Some((height, tx_idx)) = self
            .storage
            .get_tx_location(arr)
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        self.lookup_block(BlockSelector::Height(height), |v| {
            Ok(nth_transaction(v, tx_idx as usize))
        })
        .await
    }

    async fn get_transaction_receipt(
        &self,
        hash: String,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        let arr = parse_hash(&hash)?;
        let Some((height, tx_idx)) = self
            .storage
            .get_tx_location(arr)
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        let Some(bytes) = self
            .storage
            .get_receipts_at_height(height)
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        let mut arr_v: Value = serde_json::from_slice(&bytes)
            .map_err(|e| err(format!("stored receipts decode: {e}")))?;
        let Some(items) = arr_v.as_array_mut() else {
            return Ok(None);
        };
        let idx = tx_idx as usize;
        if idx >= items.len() {
            return Ok(None);
        }
        Ok(Some(items.swap_remove(idx)))
    }

    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        kind: String,
        from: Option<String>,
        to: Option<String>,
    ) -> SubscriptionResult {
        // Reject kinds our store can't back (logs, newPendingTransactions,
        // syncing) with a clear error rather than opening a silently-dead
        // subscription.
        let Some(sub_kind) = SubKind::from_wire(&kind) else {
            pending
                .reject(err(format!("unsupported subscription kind: {kind}")))
                .await;
            return Ok(());
        };
        match sub_kind {
            // Historical range replay, served straight from storage.
            SubKind::OldBlocks => self.serve_old_blocks(pending, from, to).await,
            // Live tip fan-out from the broadcast channel (from/to ignored).
            live => self.serve_live(pending, live).await,
        }
    }
}

impl EthApiImpl {
    /// Live-tip subscription: forward each freshly-ingested block off the
    /// broadcast channel until the client goes away. `newHeads` strips
    /// transactions; `newBlocks` forwards whole blocks.
    async fn serve_live(
        &self,
        pending: PendingSubscriptionSink,
        kind: SubKind,
    ) -> SubscriptionResult {
        let strip_txs = kind.strips_transactions();
        let label = kind.as_str();
        // subscribe() BEFORE accept() so we don't miss a block produced in the
        // gap between the two awaits.
        let mut rx = self.blocks.subscribe();
        let sink = pending.accept().await?;
        let metrics = SubMetricsGuard::new(kind);
        loop {
            tokio::select! {
                // Client disconnected / called eth_unsubscribe.
                () = sink.closed() => break,
                recv = rx.recv() => match recv {
                    Ok(mut block) => {
                        // The broadcast carries the full block; for newHeads we
                        // strip transactions from our own (already-cloned) copy
                        // to match geth's header shape.
                        if strip_txs && let Some(obj) = block.as_object_mut() {
                            obj.remove("transactions");
                        }
                        let msg = serde_json::value::to_raw_value(&block)?;
                        let sent_bytes = msg.get().len() as u64;
                        if let Err(e) = sink.send(msg).await {
                            debug!(kind = label, error = %e, "subscriber send failed; closing subscription");
                            break;
                        }
                        metrics.sent_bytes(sent_bytes);
                    }
                    // Slow consumer fell behind the ring buffer. Drop the gap
                    // and resume from the live tip — this is not a gapless feed
                    // anyway (that's what backfill is for).
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        metrics.lagged(n);
                        warn!(kind = label, skipped = n, "subscriber lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
        Ok(())
    }

    /// Historical range replay for `oldBlocks`. Streams `[start..=end]` straight
    /// from storage with natural backpressure (`sink.send().await` awaits a full
    /// buffer). `end == None` follows the contiguous tip as it advances
    /// (re-read each pass) and completes once the cursor catches it — the
    /// mirror's bootstrap-done signal. We refuse at subscribe time anything we
    /// cannot serve gaplessly (`start` below our earliest block, or an explicit
    /// `end` past the contiguous tip), so the loop never hits a hole:
    /// `min_height` is stable and `max_contiguous` only grows, so a range that
    /// validates here stays valid for the whole stream.
    async fn serve_old_blocks(
        &self,
        pending: PendingSubscriptionSink,
        from: Option<String>,
        to: Option<String>,
    ) -> SubscriptionResult {
        let Some(from) = from else {
            pending
                .reject(err("oldBlocks requires a 'from' block number"))
                .await;
            return Ok(());
        };
        let start = match parse_quantity(&from) {
            Ok(h) => h,
            Err(e) => {
                pending.reject(e).await;
                return Ok(());
            }
        };
        let end = match to {
            Some(t) => match parse_quantity(&t) {
                Ok(h) => Some(h),
                Err(e) => {
                    pending.reject(e).await;
                    return Ok(());
                }
            },
            None => None,
        };

        // Refuse requests we can't satisfy gaplessly.
        let min = self.storage.min_height().await;
        let contig = self.storage.max_contiguous_height().await;
        if start < min {
            pending
                .reject(err(format!(
                    "start {start} before earliest stored block {min}"
                )))
                .await;
            return Ok(());
        }
        if let Some(e) = end {
            if e < start {
                pending
                    .reject(err(format!("end {e} before start {start}")))
                    .await;
                return Ok(());
            }
            if e > contig {
                pending
                    .reject(err(format!("end {e} beyond contiguous tip {contig}")))
                    .await;
                return Ok(());
            }
        }

        let sink = pending.accept().await?;
        let metrics = SubMetricsGuard::new(SubKind::OldBlocks);
        let mut h = start;
        loop {
            // Open-ended streams follow the contiguous tip as it advances; a
            // fixed `end` was already validated against it at subscribe time.
            let target = match end {
                Some(e) => e,
                None => self.storage.max_contiguous_height().await,
            };
            if h > target {
                break; // caught up to the tip → range exhausted, close the sink
            }
            let bytes = match self.storage.get_by_height(h).await {
                Ok(Some(b)) => b,
                // Gapless by construction; never spin on a surprise hole.
                Ok(None) => break,
                Err(e) => {
                    debug!(height = h, error = %e, "oldBlocks storage read failed; closing");
                    break;
                }
            };
            // Stored bytes are already-serialized JSON; hand them over without a
            // parse+reserialize round-trip (from_string still validates).
            let msg = match String::from_utf8(bytes)
                .map_err(|e| e.to_string())
                .and_then(|s| {
                    serde_json::value::RawValue::from_string(s).map_err(|e| e.to_string())
                }) {
                Ok(m) => m,
                Err(e) => {
                    debug!(height = h, error = %e, "stored block decode failed; closing");
                    break;
                }
            };
            let sent_bytes = msg.get().len() as u64;
            if let Err(e) = sink.send(msg).await {
                debug!(height = h, error = %e, "oldBlocks send failed; closing subscription");
                break;
            }
            metrics.sent_bytes(sent_bytes);
            h = h.saturating_add(1);
        }
        Ok(())
    }
}

fn parse_hash(hash: &str) -> Result<[u8; 32], ErrorObjectOwned> {
    let stripped = hash.strip_prefix("0x").unwrap_or(hash);
    let raw = hex::decode(stripped).map_err(|e| err(format!("bad hash: {e}")))?;
    raw.as_slice()
        .try_into()
        .map_err(|_| err("hash must be 32 bytes"))
}

fn parse_quantity(q: &str) -> Result<u64, ErrorObjectOwned> {
    let stripped = q.strip_prefix("0x").unwrap_or(q);
    u64::from_str_radix(stripped, 16).map_err(|_| err(format!("invalid quantity: {q}")))
}

fn tx_count_hex(v: &Value) -> String {
    let n = v
        .get("transactions")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    format!("0x{n:x}")
}

fn nth_transaction(mut v: Value, idx: usize) -> Option<Value> {
    let txs = v.get_mut("transactions").and_then(Value::as_array_mut)?;
    (idx < txs.len()).then(|| txs.swap_remove(idx))
}

/// If `full_tx=false`, collapse the `transactions` array to bare hashes;
/// otherwise return the block as-is.
fn shape_block(mut v: Value, full_tx: bool) -> Value {
    if !full_tx && let Some(txs) = v.get_mut("transactions").and_then(Value::as_array_mut) {
        for tx in txs.iter_mut() {
            if let Some(hash) = tx.get("hash").cloned() {
                *tx = hash;
            }
        }
    }
    v
}

#[expect(
    clippy::too_many_arguments,
    reason = "independent wiring collaborators, not a cohesive group"
)]
pub async fn serve(
    addr: SocketAddr,
    storage: Storage,
    data_dir: PathBuf,
    chain_id: u64,
    max_connections: u32,
    behind_tip: std::sync::Arc<std::sync::atomic::AtomicU64>,
    blocks: broadcast::Sender<Value>,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
) -> Result<ServerHandle> {
    let health_state =
        crate::health::HealthState::new(storage.clone(), data_dir, chain_id, behind_tip);
    // `/health` and `/metrics` short-circuit before the 200→421 rewrite, which
    // only concerns JSON-RPC responses.
    let http_mw = tower::ServiceBuilder::new()
        .layer(crate::health::HealthLayer::new(health_state))
        .layer(crate::metrics::MetricsLayer::new(metrics_handle))
        .layer(crate::middleware::NotFound421Layer);
    let server = ServerBuilder::default()
        .set_config(
            ServerConfig::builder()
                .max_connections(max_connections)
                .build(),
        )
        .set_http_middleware(http_mw)
        .build(addr)
        .await?;
    let actual = server.local_addr()?;
    let handle = server.start(EthApiImpl::new(storage, chain_id, blocks).into_rpc());
    info!(%actual, "JSON-RPC server listening");
    Ok(handle)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use jsonrpsee::core::params::ArrayParams;
    use serde_json::json;

    /// `rpc_params!` is gated behind jsonrpsee's client features, which we don't
    /// pull in; build array params by hand instead. Single positional arg.
    fn kind(k: &str) -> ArrayParams {
        let mut p = ArrayParams::new();
        p.insert(k).unwrap();
        p
    }

    /// `eth_subscribe("oldBlocks", from, to?)` params.
    fn old_blocks(from: &str, to: Option<&str>) -> ArrayParams {
        let mut p = ArrayParams::new();
        p.insert("oldBlocks").unwrap();
        p.insert(from).unwrap();
        if let Some(t) = to {
            p.insert(t).unwrap();
        }
        p
    }

    /// Unique temp dir so parallel tests don't collide on the fjall keyspace.
    /// A process-wide counter guards against same-nanosecond collisions between
    /// tests running concurrently (the system clock resolution can be coarse).
    fn unique_temp_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "neve-rpc-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ))
    }

    /// Write a minimal full block (empty transactions array) at `height`.
    async fn put_test_block(storage: &Storage, height: u64) {
        let block = json!({
            "number": format!("0x{height:x}"),
            "hash": format!("0x{height:064x}"),
            "transactions": [],
        });
        let bytes = serde_json::to_vec(&block).unwrap();
        let mut hash = [0u8; 32];
        hash[24..].copy_from_slice(&height.to_be_bytes());
        storage.put(height, hash, &[], bytes, None).await.unwrap();
    }

    fn sample_block() -> Value {
        json!({
            "hash": "0xaa",
            "number": "0x1",
            "transactions": [
                {"hash": "0x11", "from": "0xaaa"},
                {"hash": "0x22", "from": "0xbbb"},
                {"hash": "0x33", "from": "0xccc"},
            ],
        })
    }

    #[test]
    fn tx_count_hex_counts_array_len() {
        assert_eq!(tx_count_hex(&sample_block()), "0x3");
        // Empty array.
        assert_eq!(tx_count_hex(&json!({"transactions": []})), "0x0");
        // Missing transactions field → 0, not an error.
        assert_eq!(tx_count_hex(&json!({})), "0x0");
        // Boundary: 16 → 0x10 (verifies hex formatting, not decimal).
        let txs: Vec<Value> = (0..16).map(|_| json!({"hash": "0x0"})).collect();
        assert_eq!(tx_count_hex(&json!({"transactions": txs})), "0x10");
    }

    #[test]
    fn nth_transaction_in_range_returns_tx() {
        let tx = nth_transaction(sample_block(), 1).unwrap();
        assert_eq!(tx["hash"], "0x22");
    }

    #[test]
    fn nth_transaction_out_of_range_returns_none() {
        assert!(nth_transaction(sample_block(), 3).is_none());
    }

    #[test]
    fn nth_transaction_missing_field_returns_none() {
        assert!(nth_transaction(json!({}), 0).is_none());
    }

    #[test]
    fn shape_block_full_tx_keeps_objects() {
        let shaped = shape_block(sample_block(), true);
        let txs = shaped["transactions"].as_array().unwrap();
        assert!(txs[0].is_object());
        assert_eq!(txs[0]["hash"], "0x11");
    }

    #[test]
    fn shape_block_no_full_tx_collapses_to_hashes() {
        let shaped = shape_block(sample_block(), false);
        let txs = shaped["transactions"].as_array().unwrap();
        assert_eq!(txs.len(), 3);
        assert!(txs[0].is_string());
        assert_eq!(txs[0], "0x11");
        assert_eq!(txs[1], "0x22");
        assert_eq!(txs[2], "0x33");
    }

    #[test]
    fn shape_block_preserves_other_fields() {
        // Collapsing transactions must not perturb sibling keys.
        let shaped = shape_block(sample_block(), false);
        assert_eq!(shaped["hash"], "0xaa");
        assert_eq!(shaped["number"], "0x1");
    }

    #[test]
    fn parse_quantity_accepts_hex_with_and_without_prefix() {
        assert_eq!(parse_quantity("0x10").unwrap(), 16);
        assert_eq!(parse_quantity("10").unwrap(), 16);
        assert_eq!(parse_quantity("0x0").unwrap(), 0);
        assert!(parse_quantity("0xZZ").is_err());
    }

    #[test]
    fn parse_hash_round_trip() {
        let h = "0x".to_owned() + &"ab".repeat(32);
        let bytes = parse_hash(&h).unwrap();
        assert_eq!(bytes, [0xab; 32]);
        // Wrong length.
        assert!(parse_hash("0xab").is_err());
        // Bad hex.
        assert!(parse_hash("0xZZ").is_err());
    }

    /// Drive the `eth_subscribe("newHeads")` path in-process (no network): a
    /// non-newHeads kind is rejected, and heads published to the broadcast
    /// channel are delivered to the subscriber in order. This is the
    /// server-side half of chaining one neve to another.
    #[tokio::test]
    async fn subscription_rejects_others_strips_heads_keeps_blocks() {
        // An empty store is sufficient — the live subscription path only touches
        // `blocks`, never storage.
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43114, None).unwrap();
        let (block_tx, _) = broadcast::channel::<Value>(16);
        let module = EthApiImpl::new(storage, 43114, block_tx.clone()).into_rpc();

        // Unsupported kinds are rejected, not silently accepted into a
        // never-firing subscription.
        assert!(
            module
                .subscribe_unbounded("eth_subscribe", kind("logs"))
                .await
                .is_err()
        );

        // Both kinds accepted. The impl calls blocks.subscribe() before
        // accept(), so a send after subscribe_unbounded returns is guaranteed
        // to be observed by both subscribers.
        let mut heads = module
            .subscribe_unbounded("eth_subscribe", kind("newHeads"))
            .await
            .unwrap();
        let mut full = module
            .subscribe_unbounded("eth_subscribe", kind("newBlocks"))
            .await
            .unwrap();

        // The broadcast carries the full block (transactions present).
        block_tx
            .send(json!({
                "number": "0x1",
                "hash": "0xaa",
                "transactions": [{"hash": "0x11"}, {"hash": "0x22"}],
            }))
            .unwrap();

        // newHeads strips transactions; the header fields survive.
        let (h, _) = heads.next::<Value>().await.unwrap().unwrap();
        assert_eq!(h["number"], "0x1");
        assert_eq!(h["hash"], "0xaa");
        assert!(h.get("transactions").is_none(), "newHeads must strip txs");

        // newBlocks forwards the whole block, transactions intact.
        let (b, _) = full.next::<Value>().await.unwrap().unwrap();
        assert_eq!(b["number"], "0x1");
        assert_eq!(b["transactions"].as_array().unwrap().len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// `oldBlocks` replays a finite stored range as whole blocks, in order, then
    /// completes (closes the sink) once the range is exhausted. This is the
    /// server-side half of a mirror's bootstrap and of future fan-out slices.
    #[tokio::test]
    async fn old_blocks_streams_finite_range_then_completes() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43114, None).unwrap();
        for h in 10..=12u64 {
            put_test_block(&storage, h).await;
        }
        let (block_tx, _) = broadcast::channel::<Value>(16);
        let module = EthApiImpl::new(storage, 43114, block_tx).into_rpc();

        let mut sub = module
            .subscribe_unbounded("eth_subscribe", old_blocks("0xa", Some("0xc")))
            .await
            .unwrap();
        for h in 10..=12u64 {
            let (b, _) = sub.next::<Value>().await.unwrap().unwrap();
            assert_eq!(b["number"], format!("0x{h:x}"));
            // Whole block forwarded (transactions array present), like newBlocks.
            assert!(b["transactions"].is_array());
        }
        // Range exhausted → server closes the subscription.
        assert!(
            sub.next::<Value>().await.is_none(),
            "stream should end at the range end"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// With `to` omitted, `oldBlocks` streams up to the contiguous tip and then
    /// completes — the mirror's bootstrap-done signal. (No concurrent producer
    /// here, so it terminates deterministically at the current tip.)
    #[tokio::test]
    async fn old_blocks_open_ended_streams_to_contiguous_tip() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43114, None).unwrap();
        for h in 10..=12u64 {
            put_test_block(&storage, h).await;
        }
        let (block_tx, _) = broadcast::channel::<Value>(16);
        let module = EthApiImpl::new(storage, 43114, block_tx).into_rpc();

        let mut sub = module
            .subscribe_unbounded("eth_subscribe", old_blocks("0xa", None))
            .await
            .unwrap();
        for h in 10..=12u64 {
            let (b, _) = sub.next::<Value>().await.unwrap().unwrap();
            assert_eq!(b["number"], format!("0x{h:x}"));
        }
        assert!(
            sub.next::<Value>().await.is_none(),
            "should close on catching the contiguous tip"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Requests we can't serve gaplessly are refused at subscribe time, not
    /// opened into a doomed stream. Store holds [10..=12], so `min_height`=10 and
    /// `max_contiguous`=12.
    #[tokio::test]
    async fn old_blocks_rejects_unsatisfiable_ranges() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43114, None).unwrap();
        for h in 10..=12u64 {
            put_test_block(&storage, h).await;
        }
        let (block_tx, _) = broadcast::channel::<Value>(16);
        let module = EthApiImpl::new(storage, 43114, block_tx).into_rpc();

        // start below earliest stored block (min_height = 10)
        assert!(
            module
                .subscribe_unbounded("eth_subscribe", old_blocks("0x9", Some("0xc")))
                .await
                .is_err(),
            "start below min_height must be rejected"
        );
        // end beyond the contiguous tip (max_contiguous = 12)
        assert!(
            module
                .subscribe_unbounded("eth_subscribe", old_blocks("0xa", Some("0xd")))
                .await
                .is_err(),
            "end beyond contiguous tip must be rejected"
        );
        // end before start
        assert!(
            module
                .subscribe_unbounded("eth_subscribe", old_blocks("0xc", Some("0xa")))
                .await
                .is_err(),
            "end before start must be rejected"
        );
        // missing required `from`
        assert!(
            module
                .subscribe_unbounded("eth_subscribe", kind("oldBlocks"))
                .await
                .is_err(),
            "oldBlocks without a 'from' must be rejected"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
