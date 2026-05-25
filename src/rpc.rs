use std::net::SocketAddr;

use anyhow::Result;
use jsonrpsee::core::async_trait;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{ServerBuilder, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use serde_json::Value;
use tracing::info;

use crate::storage::Storage;

/// JSON-RPC error code we use for "block not found" — matches geth's `-32000`
/// style (server error range), with a descriptive message.
const BLOCK_NOT_FOUND: i32 = -32000;

fn err(msg: impl Into<String>) -> ErrorObjectOwned {
    ErrorObjectOwned::owned::<()>(BLOCK_NOT_FOUND, msg.into(), None)
}

#[rpc(server, namespace = "eth")]
pub trait EthApi {
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
}

/// How a JSON-RPC caller named the block: a tag/number string (the
/// `eth_*ByNumber` family) or a 32-byte hash (`eth_*ByHash` family).
enum BlockSelector {
    Number(String),
    Hash(String),
    Height(u64),
}

pub struct EthApiImpl {
    storage: Storage,
}

impl EthApiImpl {
    pub const fn new(storage: Storage) -> Self {
        Self { storage }
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
        let v: Value = serde_json::from_slice(&bytes)
            .map_err(|e| err(format!("stored block decode: {e}")))?;
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
    async fn block_number(&self) -> Result<String, ErrorObjectOwned> {
        Ok(format!("0x{:x}", self.storage.high_water().await))
    }

    async fn get_block_by_number(
        &self,
        block: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        self.lookup_block(BlockSelector::Number(block), |v| Ok(Some(shape_block(v, full_tx))))
            .await
    }

    async fn get_block_by_hash(
        &self,
        hash: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        self.lookup_block(BlockSelector::Hash(hash), |v| Ok(Some(shape_block(v, full_tx))))
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
        self.lookup_block(BlockSelector::Number(block), |v| Ok(nth_transaction(v, idx)))
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
    if !full_tx
        && let Some(txs) = v.get_mut("transactions").and_then(Value::as_array_mut)
    {
        for tx in txs.iter_mut() {
            if let Some(hash) = tx.get("hash").cloned() {
                *tx = hash;
            }
        }
    }
    v
}

pub async fn serve(addr: SocketAddr, storage: Storage) -> Result<ServerHandle> {
    let http_mw = tower::ServiceBuilder::new().layer(crate::middleware::NotFound421Layer);
    let server = ServerBuilder::default()
        .set_http_middleware(http_mw)
        .build(addr)
        .await?;
    let actual = server.local_addr()?;
    let handle = server.start(EthApiImpl::new(storage).into_rpc());
    info!(%actual, "JSON-RPC server listening");
    Ok(handle)
}
