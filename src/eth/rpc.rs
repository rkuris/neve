//! The C-chain serving dialect: geth-compatible `eth_*` JSON-RPC over the
//! stored `[block, logs]` records, plus the `newHeads` / `newBlocks` /
//! `oldBlocks` subscriptions.
//!
//! Every read that our store can't authoritatively answer returns `Ok(None)`,
//! which serializes to `result: null` and is rewritten to HTTP 421 by
//! `crate::middleware` so the fronting pool retries against a full node. A
//! *partial* answer is never returned.

use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::core::async_trait;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::PendingSubscriptionSink;
use jsonrpsee::types::ErrorObjectOwned;
use serde::Deserialize;
use serde_json::Value;

use crate::chain::Chain;
use crate::join::JoinBuffer;
use crate::rpc::{ChainServe, err, parse_hash, parse_quantity};
use crate::storage::Storage;
use crate::subscribe::{self, LiveTx, SubKind};

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

    /// `eth_getLogs(filter)` — logs matching `filter` across a block range,
    /// served from stored records. `None` (→ 421 → upstream fallback) when the
    /// requested range isn't fully present, so a partial result is never
    /// returned; an over-large range is a hard error (clients chunk).
    #[method(name = "getLogs")]
    async fn get_logs(&self, filter: LogFilter) -> Result<Option<Value>, ErrorObjectOwned>;

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

/// Subscription kinds the C-chain dialect names. Whether each can actually be
/// delivered is a separate question the shared layer answers — `newRecords` is
/// in this vocabulary but the C-chain live path can't back it today (see
/// [`Chain::publishes_live_records`]), while `oldRecords` works fine because a
/// stored record is complete by definition.
const ETH_SUB_KINDS: &[SubKind] = &[
    SubKind::NewHeads,
    SubKind::NewBlocks,
    SubKind::NewRecords,
    SubKind::OldBlocks,
    SubKind::OldRecords,
];

/// How a JSON-RPC caller named the block: a tag/number string (the
/// `eth_*ByNumber` family) or a 32-byte hash (`eth_*ByHash` family).
enum BlockSelector {
    Number(String),
    Hash(String),
    Height(u64),
}

/// Largest block span a single `eth_getLogs` may scan, matching the upstream
/// `eth_getLogs` cap (~2048 — see `avalanche-public-endpoint-quirks`) so clients'
/// existing chunking works unchanged. A larger range is a hard error.
const MAX_GETLOGS_RANGE: u64 = 2048;

/// Blocks read per `read_element_range` batch while serving `eth_getLogs`, so a wide
/// scan bounds peak input memory and store read-lock hold instead of
/// materializing the whole (up to [`MAX_GETLOGS_RANGE`]) range at once.
const GETLOGS_READ_CHUNK: u64 = 256;

/// The `eth_getLogs` filter object. All fields optional; `from_block`/`to_block`
/// resolve to "latest" when absent. `block_hash` selects a single block instead
/// of a range.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct LogFilter {
    from_block: Option<String>,
    to_block: Option<String>,
    block_hash: Option<String>,
    address: Option<OneOrMany>,
    topics: Option<Vec<Option<OneOrMany>>>,
}

/// A filter slot (address, or one topic position) that accepts a single value or
/// any-of an array. Comparison is ASCII-case-insensitive so a checksummed filter
/// address still matches the lowercase address in a stored log.
#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    fn matches(&self, candidate: &str) -> bool {
        match self {
            Self::One(s) => s.eq_ignore_ascii_case(candidate),
            Self::Many(v) => v.iter().any(|s| s.eq_ignore_ascii_case(candidate)),
        }
    }
}

/// Does one stored log match the filter's address + topic constraints? An absent
/// (or `null`) constraint matches anything; a log with fewer topics than a
/// specified position fails that position (eth `getLogs` semantics).
fn log_matches(
    log: &Value,
    address: Option<&OneOrMany>,
    topics: Option<&[Option<OneOrMany>]>,
) -> bool {
    if let Some(address) = address {
        let log_addr = log.get("address").and_then(Value::as_str).unwrap_or("");
        if !address.matches(log_addr) {
            return false;
        }
    }
    if let Some(topics) = topics {
        let empty = Vec::new();
        let log_topics = log
            .get("topics")
            .and_then(Value::as_array)
            .unwrap_or(&empty);
        for (i, slot) in topics.iter().enumerate() {
            let Some(slot) = slot else { continue };
            let Some(log_topic) = log_topics.get(i).and_then(Value::as_str) else {
                return false;
            };
            if !slot.matches(log_topic) {
                return false;
            }
        }
    }
    true
}

pub struct EthApiImpl {
    storage: Storage,
    /// The EVM chain ID `eth_chainId` reports, parsed back out of the store's
    /// network-identity stamp.
    chain_id: u64,
    /// Live-tip fan-out. The ingest path publishes each stored block here; one
    /// receiver is handed to every subscriber, which projects it for its kind.
    blocks: LiveTx,
    /// In-flight join buffer when log ingestion is on. Block reads consult it so
    /// a just-arrived tip block (buffered while its logs are fetched, not yet in
    /// the store) is still serveable from memory. `None` when logs are off.
    join: Option<JoinBuffer>,
}

impl EthApiImpl {
    /// Build the `eth_*` service over a C-chain instance. The chain ID comes
    /// from the instance's network-identity stamp (decimal `eth_chainId`); an
    /// unparseable stamp can only mean a non-C instance was routed here, so it
    /// degrades to 0 rather than panicking in the serving path.
    pub fn new(c: &ChainServe) -> Self {
        debug_assert_eq!(c.chain, Chain::C, "eth dialect requires a C-chain instance");
        Self {
            storage: c.storage.clone(),
            chain_id: c.identity.parse().unwrap_or(0),
            blocks: c.blocks.clone(),
            join: c.join.clone(),
        }
    }

    /// Read a block by height as a parsed `Value`, consulting the in-flight join
    /// buffer when the store doesn't have it yet (a tip block mid-join). The
    /// store path stays zero-copy (`record::Element` parsed in place); only the
    /// rarer buffer fallback copies.
    async fn read_block_value(&self, height: u64) -> Result<Option<Value>, ErrorObjectOwned> {
        if let Some(bytes) = self
            .storage
            .get_by_height(height)
            .await
            .map_err(|e| err(format!("storage error: {e}")))?
        {
            let v = serde_json::from_slice(&bytes)
                .map_err(|e| err(format!("stored block decode: {e}")))?;
            return Ok(Some(v));
        }
        if let Some(raw) = self.join.as_ref().and_then(|b| b.buffered_block(height)) {
            let v = serde_json::from_slice(&raw)
                .map_err(|e| err(format!("buffered block decode: {e}")))?;
            return Ok(Some(v));
        }
        Ok(None)
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
        // Height-based selectors consult the join buffer for an in-flight tip
        // block; by-hash can't (a buffered block isn't in the hash index until
        // its durable write), so it stays store-only.
        let v: Option<Value> = match sel {
            BlockSelector::Number(tag) => {
                let h = self.resolve_block_tag(&tag).await?;
                self.read_block_value(h).await?
            }
            BlockSelector::Height(h) => self.read_block_value(h).await?,
            BlockSelector::Hash(hash) => {
                let arr = parse_hash(&hash)?;
                match self
                    .storage
                    .get_by_hash(arr)
                    .await
                    .map_err(|e| err(format!("storage error: {e}")))?
                {
                    Some(bytes) => Some(
                        serde_json::from_slice(&bytes)
                            .map_err(|e| err(format!("stored block decode: {e}")))?,
                    ),
                    None => None,
                }
            }
        };

        let Some(v) = v else { return Ok(None) };
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

    async fn get_logs(&self, filter: LogFilter) -> Result<Option<Value>, ErrorObjectOwned> {
        // Resolve the height range. `blockHash` selects a single block; a hit is
        // a present block, so it's always serveable.
        let (from, to) = if let Some(block_hash) = &filter.block_hash {
            let arr = parse_hash(block_hash)?;
            let Some(h) = self
                .storage
                .height_of_hash(arr)
                .map_err(|e| err(format!("storage error: {e}")))?
            else {
                return Ok(None);
            };
            (h, h)
        } else {
            let from = self
                .resolve_block_tag(filter.from_block.as_deref().unwrap_or("latest"))
                .await?;
            let to = self
                .resolve_block_tag(filter.to_block.as_deref().unwrap_or("latest"))
                .await?;
            if from > to {
                return Err(err(format!("fromBlock ({from}) is after toBlock ({to})")));
            }
            let span = to.saturating_sub(from).saturating_add(1);
            if span > MAX_GETLOGS_RANGE {
                return Err(err(format!(
                    "getLogs range too large: {span} blocks (max {MAX_GETLOGS_RANGE})"
                )));
            }
            // Completeness: only answer if the whole range is present and
            // contiguous (so logs are complete); otherwise punt to upstream.
            let min = self.storage.min_height().await;
            let contiguous = self.storage.max_contiguous_height().await;
            if from < min || to > contiguous {
                return Ok(None);
            }
            (from, to)
        };

        // Read the range in chunks so a wide scan never materializes every
        // block's logs at once (and holds the store read-lock only per chunk);
        // only matching logs accumulate in `out`.
        let mut out: Vec<Value> = Vec::new();
        let mut chunk_start = from;
        while chunk_start <= to {
            let chunk_end = chunk_start
                .saturating_add(GETLOGS_READ_CHUNK)
                .saturating_sub(1)
                .min(to);
            let Some(per_height) = self
                .storage
                .read_element_range(chunk_start, chunk_end, crate::record::C_LOGS)
                .await
                .map_err(|e| err(format!("storage error: {e}")))?
            else {
                return Ok(None);
            };
            for logs_bytes in per_height {
                let logs: Value = serde_json::from_slice(&logs_bytes)
                    .map_err(|e| err(format!("stored logs decode: {e}")))?;
                if let Some(arr) = logs.as_array() {
                    for log in arr {
                        if log_matches(log, filter.address.as_ref(), filter.topics.as_deref()) {
                            out.push(log.clone());
                        }
                    }
                }
            }
            chunk_start = chunk_end.saturating_add(1);
        }
        crate::metrics::getlogs_served("range");
        Ok(Some(Value::Array(out)))
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
        // Ranges are hex quantities in the eth dialect.
        let (from, to) = match (
            from.as_deref().map(parse_quantity).transpose(),
            to.as_deref().map(parse_quantity).transpose(),
        ) {
            (Ok(f), Ok(t)) => (f, t),
            (Err(e), _) | (_, Err(e)) => {
                pending.reject(e).await;
                return Ok(());
            }
        };
        let req = subscribe::SubRequest {
            kind: sub_kind,
            from,
            to,
        };
        subscribe::serve(
            Chain::C,
            &self.storage,
            &self.blocks,
            pending,
            req,
            ETH_SUB_KINDS,
        )
        .await
    }
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::test_support::{C_IDENTITY, chain_serve, put_block, unique_temp_dir};
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

    /// A C-chain `eth_*` service over a fresh empty store, plus the store and
    /// the live fan-out sender so tests can drive both sides.
    fn eth_service(dir: &std::path::Path) -> (Storage, LiveTx, EthApiImpl) {
        let c = chain_serve(Chain::C, dir);
        (c.storage.clone(), c.blocks.clone(), EthApiImpl::new(&c))
    }

    /// The chain ID served by `eth_chainId` comes from the instance's
    /// network-identity stamp, so it can't drift from what the store is
    /// stamped for.
    #[tokio::test]
    async fn chain_id_comes_from_the_instance_identity() {
        let dir = unique_temp_dir("eth-chainid");
        let (_storage, _tx, eth) = eth_service(&dir);
        assert_eq!(eth.chain_id().await.unwrap(), "0xa86a");
        assert_eq!(C_IDENTITY.parse::<u64>().unwrap(), 0xa86a);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn read_block_value_falls_back_to_in_flight_buffer() {
        let dir = unique_temp_dir("eth-buffer");
        let mut c = chain_serve(Chain::C, &dir);

        // A block buffered mid-join (logs not yet fetched), NOT in the store.
        let buf = JoinBuffer::new(c.storage.clone(), 16);
        let block = json!({ "number": "0x64", "transactions": [] });
        let bytes = serde_json::to_vec(&block).unwrap();
        buf.on_block(0x64, [0x64; 32], vec![], bytes).await.unwrap();
        assert!(c.storage.get_by_height(0x64).await.unwrap().is_none());

        // Without the buffer wired, the same height is a miss (drives the 421 path).
        let eth_no_buf = EthApiImpl::new(&c);
        assert!(eth_no_buf.read_block_value(0x64).await.unwrap().is_none());

        // With it, the in-flight tip block resolves from memory.
        c.join = Some(buf);
        let eth = EthApiImpl::new(&c);
        let v = eth.read_block_value(0x64).await.unwrap().unwrap();
        assert_eq!(v["number"], "0x64");
        std::fs::remove_dir_all(&dir).ok();
    }

    fn log_filter(v: Value) -> LogFilter {
        serde_json::from_value(v).unwrap()
    }

    async fn put_block_with_logs(storage: &Storage, h: u64, logs: Value) {
        let block = json!({
            "number": format!("0x{h:x}"),
            "hash": format!("0x{h:064x}"),
            "transactions": [],
        });
        let block_bytes = serde_json::to_vec(&block).unwrap();
        let logs_bytes = serde_json::to_vec(&logs).unwrap();
        let mut hash = [0u8; 32];
        hash[24..].copy_from_slice(&h.to_be_bytes());
        storage
            .put(h, hash, &[], &[&block_bytes, &logs_bytes])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_logs_filters_range_address_and_topics() {
        let dir = unique_temp_dir("eth-getlogs");
        let (storage, _tx, eth) = eth_service(&dir);
        // Heights 1..=3, two logs each: one at 0xAAA (topics 0x01,0x02), one at
        // 0xBBB (topic 0x09).
        for h in 1..=3u64 {
            let logs = json!([
                {"address": "0xAAA", "topics": ["0x01", "0x02"]},
                {"address": "0xBBB", "topics": ["0x09"]},
            ]);
            put_block_with_logs(&storage, h, logs).await;
        }

        // Whole range, no filter → all 6 logs.
        let all = eth
            .get_logs(log_filter(json!({"fromBlock": "0x1", "toBlock": "0x3"})))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all.as_array().unwrap().len(), 6);

        // Address filter, checksummed → matches the lowercase stored address.
        let by_addr = eth
            .get_logs(log_filter(
                json!({"fromBlock": "0x1", "toBlock": "0x3", "address": "0xaaa"}),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_addr.as_array().unwrap().len(), 3);

        // topics[0] = 0x09 → only the 0xBBB logs.
        let by_topic = eth
            .get_logs(log_filter(
                json!({"fromBlock": "0x1", "toBlock": "0x3", "topics": ["0x09"]}),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_topic.as_array().unwrap().len(), 3);

        // blockHash selects a single block's logs.
        let by_hash = eth
            .get_logs(log_filter(json!({"blockHash": format!("0x{:064x}", 2u64)})))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_hash.as_array().unwrap().len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn get_logs_punts_out_of_range_and_rejects_oversized() {
        let dir = unique_temp_dir("eth-getlogs-range");
        let (storage, _tx, eth) = eth_service(&dir);
        for h in 1..=3u64 {
            put_block_with_logs(&storage, h, json!([])).await;
        }

        // `to` past the contiguous tip → incomplete → punt (None → 421 → upstream).
        assert!(
            eth.get_logs(log_filter(json!({"fromBlock": "0x1", "toBlock": "0x9"})))
                .await
                .unwrap()
                .is_none()
        );
        // Over the per-request block cap → hard error (clients chunk).
        assert!(
            eth.get_logs(log_filter(json!({"fromBlock": "0x1", "toBlock": "0x901"})))
                .await
                .is_err()
        );
        std::fs::remove_dir_all(&dir).ok();
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

    /// Drive the `eth_subscribe("newHeads")` path in-process (no network): a
    /// non-newHeads kind is rejected, and heads published to the broadcast
    /// channel are delivered to the subscriber in order. This is the
    /// server-side half of chaining one neve to another.
    #[tokio::test]
    async fn subscription_rejects_others_strips_heads_keeps_blocks() {
        // An empty store is sufficient — the live subscription path only touches
        // `blocks`, never storage.
        let dir = unique_temp_dir("eth-sub");
        let (_storage, block_tx, eth) = eth_service(&dir);
        let module = eth.into_rpc();

        // Unsupported kinds are rejected, not silently accepted into a
        // never-firing subscription.
        assert!(
            module
                .subscribe_unbounded("eth_subscribe", kind("logs"))
                .await
                .is_err()
        );
        // `newRecords` is in the dialect's vocabulary but the C-chain live path
        // can't back it — the rejection must say so rather than open a stream
        // that never fires.
        assert!(
            module
                .subscribe_unbounded("eth_subscribe", kind("newRecords"))
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

        // The fan-out carries the full block (transactions present).
        block_tx
            .send(std::sync::Arc::new(crate::subscribe::LiveUpdate {
                block: json!({
                    "number": "0x1",
                    "hash": "0xaa",
                    "transactions": [{"hash": "0x11"}, {"hash": "0x22"}],
                }),
                record: None,
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
        let dir = unique_temp_dir("eth-oldblocks");
        let (storage, _tx, eth) = eth_service(&dir);
        for h in 10..=12u64 {
            put_block(&storage, h).await;
        }
        let module = eth.into_rpc();

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
        let dir = unique_temp_dir("eth-oldblocks-open");
        let (storage, _tx, eth) = eth_service(&dir);
        for h in 10..=12u64 {
            put_block(&storage, h).await;
        }
        let module = eth.into_rpc();

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
        let dir = unique_temp_dir("eth-oldblocks-reject");
        let (storage, _tx, eth) = eth_service(&dir);
        for h in 10..=12u64 {
            put_block(&storage, h).await;
        }
        let module = eth.into_rpc();

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
