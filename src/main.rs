mod rpc;
mod storage;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::storage::Storage;

const WS_URL: &str = "wss://api.avax.network/ext/bc/C/ws";
const RPC_URL: &str = "https://api.avax.network/ext/bc/C/rpc";

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("install rustls crypto provider"))?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let data_dir = PathBuf::from(
        std::env::var("BLOCKSTORE_DIR").unwrap_or_else(|_| "./blockstore-data".to_owned()),
    );
    std::fs::create_dir_all(&data_dir)?;
    let storage = Storage::open(&data_dir)?;
    info!(path = %data_dir.display(), high_water = storage.high_water(), "storage opened");

    let rpc_addr: std::net::SocketAddr = std::env::var("RPC_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8545".to_owned())
        .parse()?;
    let _rpc_handle = rpc::serve(rpc_addr, storage.clone()).await?;

    ingest(storage).await
}

async fn ingest(storage: Storage) -> Result<()> {
    let http = reqwest::Client::builder().build()?;
    let mut attempt: u32 = 0;
    loop {
        match run_session(&storage, &http).await {
            Ok(()) => {
                info!("websocket session ended cleanly, reconnecting");
                attempt = 0;
            }
            Err(e) => {
                warn!(error = %e, attempt, "websocket session failed");
                attempt = attempt.saturating_add(1);
            }
        }
        // Exponential backoff: 500ms, 1s, 2s, 4s, 8s; cap at 30s.
        let backoff_ms = 500u64.saturating_mul(1u64 << attempt.min(6)).min(30_000);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }
}

#[allow(clippy::too_many_lines)]
async fn run_session(storage: &Storage, http: &reqwest::Client) -> Result<()> {
    info!(url = WS_URL, "connecting websocket");
    let (ws, _) = connect_async(WS_URL).await.context("connecting websocket")?;
    let (mut tx, mut rx) = ws.split();

    tx.send(Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"],
        })
        .to_string(),
    ))
    .await?;

    while let Some(msg) = rx.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "websocket error");
                break;
            }
        };

        let text = match msg {
            Message::Text(t) => t,
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            Message::Ping(p) => {
                tx.send(Message::Pong(p)).await.ok();
                continue;
            }
            Message::Close(_) => {
                info!("server closed connection");
                break;
            }
            _ => continue,
        };

        let v: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "bad json");
                continue;
            }
        };

        if let Some(result) = v.get("result")
            && v.get("id").is_some()
            && v.get("method").is_none()
        {
            info!(sub = %result, "subscribed");
            continue;
        }

        if v.get("method").and_then(Value::as_str) != Some("eth_subscription") {
            continue;
        }

        let Some(head) = v.get("params").and_then(|p| p.get("result")) else {
            continue;
        };
        let number_hex = head
            .get("number")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("newHead missing number"))?.to_owned();
        let height = u64::from_str_radix(number_hex.trim_start_matches("0x"), 16)?;
        let head_hash = head
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("newHead missing hash"))?.to_owned();
        info!(height, hash = %head_hash, "new head");

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [number_hex, true],
        });
        let block = {
            let mut got: Option<Value> = None;
            for attempt in 0..5u32 {
                let resp: Value = match http.post(RPC_URL).json(&body).send().await {
                    Ok(r) => match r.json().await {
                        Ok(j) => j,
                        Err(e) => {
                            warn!(error = %e, height, "decode rpc response");
                            break;
                        }
                    },
                    Err(e) => {
                        warn!(error = %e, height, "rpc request failed");
                        break;
                    }
                };
                if let Some(result) = resp.get("result")
                    && !result.is_null()
                {
                    got = Some(result.clone());
                    break;
                }
                let backoff = 250u64.saturating_mul(1u64 << attempt.min(10));
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
            if let Some(b) = got { b } else {
                warn!(height, "block still unavailable after retries");
                continue;
            }
        };

        let body_hash = block.get("hash").and_then(Value::as_str).unwrap_or("");
        if body_hash != head_hash {
            warn!(height, head = %head_hash, body = %body_hash, "hash mismatch (fork?)");
            continue;
        }

        let hash_bytes = match decode_hash(&head_hash) {
            Ok(h) => h,
            Err(e) => {
                warn!(error = %e, "bad hash on newHead");
                continue;
            }
        };
        let bytes = serde_json::to_vec(&block)?;
        let len = bytes.len();
        storage.put(height, hash_bytes, bytes).await?;
        info!(height, bytes = len, "stored block");
    }

    Ok(())
}

fn decode_hash(s: &str) -> Result<[u8; 32]> {
    let raw = hex::decode(s.trim_start_matches("0x"))?;
    raw.as_slice()
        .try_into()
        .map_err(|_| anyhow!("hash must be 32 bytes"))
}
