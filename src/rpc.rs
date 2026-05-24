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
}

pub struct EthApiImpl {
    storage: Storage,
}

impl EthApiImpl {
    pub const fn new(storage: Storage) -> Self {
        Self { storage }
    }

    fn resolve_block_tag(&self, tag: &str) -> Result<u64, ErrorObjectOwned> {
        match tag {
            "latest" | "finalized" | "safe" => {
                let hw = self.storage.high_water();
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
        Ok(format!("0x{:x}", self.storage.high_water()))
    }

    async fn get_block_by_number(
        &self,
        block: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        let height = self.resolve_block_tag(&block)?;
        let Some(bytes) = self
            .storage
            .get_by_height(height)
            .await
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_and_shape(&bytes, full_tx)?))
    }

    async fn get_block_by_hash(
        &self,
        hash: String,
        full_tx: bool,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        let stripped = hash.strip_prefix("0x").unwrap_or(&hash);
        let raw = hex::decode(stripped).map_err(|e| err(format!("bad hash: {e}")))?;
        let arr: [u8; 32] = raw
            .as_slice()
            .try_into()
            .map_err(|_| err("hash must be 32 bytes"))?;
        let Some(bytes) = self
            .storage
            .get_by_hash(arr)
            .await
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_and_shape(&bytes, full_tx)?))
    }
}

/// Parse the stored block JSON and (if `full_tx=false`) collapse the
/// transactions array to a list of hashes.
fn decode_and_shape(bytes: &[u8], full_tx: bool) -> Result<Value, ErrorObjectOwned> {
    let mut v: Value =
        serde_json::from_slice(bytes).map_err(|e| err(format!("stored block decode: {e}")))?;
    if !full_tx
        && let Some(txs) = v.get_mut("transactions").and_then(Value::as_array_mut) {
            for tx in txs.iter_mut() {
                if let Some(hash) = tx.get("hash").cloned() {
                    *tx = hash;
                }
            }
        }
    Ok(v)
}

pub async fn serve(addr: SocketAddr, storage: Storage) -> Result<ServerHandle> {
    let server = ServerBuilder::default().build(addr).await?;
    let actual = server.local_addr()?;
    let handle = server.start(EthApiImpl::new(storage).into_rpc());
    info!(%actual, "JSON-RPC server listening");
    Ok(handle)
}
