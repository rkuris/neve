//! Upstream-HTTP behavior shared by every chain's fetch path: the browser
//! user-agent the public endpoint's WAF wants, and `Retry-After`-aware throttle
//! handling.
//!
//! Both chains talk to the same public Avalanche endpoint behind the same
//! Cloudflare rules, so this is genuinely common ground rather than one chain's
//! code borrowed by another.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as TungError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{error, info, warn};

use crate::chain::IngestCfg;
use crate::metrics;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) type WsTx = SplitSink<WsStream, Message>;
pub(crate) type WsRx = SplitStream<WsStream>;

/// Open the upstream WebSocket (browser UA for the WAF bypass) and split it
/// into a read/write pair. A 429 / 503 on the handshake is surfaced as a
/// transient error after honoring `Retry-After`, so the caller's reconnect
/// path retries with backoff. No subscription is sent here — the caller picks
/// the method and kind, which is the only chain-specific part.
pub(crate) async fn connect_ws(cfg: &IngestCfg) -> Result<(WsTx, WsRx)> {
    info!(chain = cfg.chain.as_str(), url = %cfg.ws_url, "connecting websocket");
    let mut req = cfg.ws_url.as_str().into_client_request()?;
    req.headers_mut().insert(
        "User-Agent",
        BROWSER_UA
            .parse()
            .context("BROWSER_UA is not a valid header value")?,
    );
    let ws = match connect_async(req).await {
        Ok((ws, _)) => ws,
        Err(TungError::Http(resp))
            if resp.status() == http::StatusCode::TOO_MANY_REQUESTS
                || resp.status() == http::StatusCode::SERVICE_UNAVAILABLE =>
        {
            let retry_after = retry_after_from_headers(resp.headers()).unwrap_or(5);
            handle_throttle(
                cfg,
                "websocket connect",
                retry_after,
                resp.status().as_u16(),
            )
            .await;
            // handle_throttle returns only if we slept; loop the caller by surfacing
            // a transient error so the reconnect path takes over with its backoff.
            return Err(anyhow!("ws throttled (slept {retry_after}s, retrying)"));
        }
        Err(e) => return Err(anyhow::Error::from(e).context("connecting websocket")),
    };
    Ok(ws.split())
}

/// Send one JSON-RPC request over the socket.
pub(crate) async fn send_request(tx: &mut WsTx, body: &Value) -> Result<()> {
    tx.send(Message::Text(body.to_string().into())).await?;
    Ok(())
}

/// Pull the next JSON frame off the WebSocket, handling pings, close frames,
/// and unparseable payloads internally. `None` when the stream ends or breaks.
/// Classifying the frame is the caller's job — that part is dialect-specific.
pub(crate) async fn next_frame(tx: &mut WsTx, rx: &mut WsRx) -> Option<Value> {
    while let Some(msg) = rx.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "websocket error");
                return None;
            }
        };
        let text = match msg {
            // tungstenite 0.26+ holds text as Utf8Bytes and binary/ping as Bytes;
            // normalize to an owned String so the JSON parse below is unchanged.
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            Message::Ping(p) => {
                tx.send(Message::Pong(p)).await.ok();
                continue;
            }
            Message::Close(_) => {
                info!("server closed connection");
                return None;
            }
            _ => continue,
        };
        if let Ok(v) = serde_json::from_str::<Value>(&text) {
            return Some(v);
        }
        warn!("bad json");
    }
    None
}

/// Sent on the WS handshake and every HTTPS RPC request. The Cloudflare
/// `Human Rate Limit Bypass` WAF rule requires a non-empty UA that doesn't
/// match any known-automation substring; a real-browser UA from a non-
/// datacenter ASN is the cheapest way into that bypass. TLS JA3 fingerprint
/// still comes from rustls and is *not* impersonated here.
pub(crate) const BROWSER_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/// Handle a 429 / 503 response with a `Retry-After` value. If the wait is
/// within `cfg.max_wait`, just sleep and return (caller will retry). If it's
/// longer than `cfg.max_wait`, log an ERROR, signal the fatal channel, and
/// park forever — main's select! will pick up the notify and exit with an
/// error. Parking avoids racing the caller into more requests.
pub(crate) async fn handle_throttle(cfg: &IngestCfg, what: &str, retry_after: u64, status: u16) {
    metrics::upstream_retry_after(cfg.chain, retry_after);
    let wait = Duration::from_secs(retry_after);
    if wait > cfg.max_wait {
        error!(
            chain = cfg.chain.as_str(),
            what,
            status,
            retry_after,
            max_wait_secs = cfg.max_wait.as_secs(),
            "upstream throttled longer than --max-wait; shutting down",
        );
        cfg.fatal.notify_one();
        std::future::pending::<()>().await;
        return;
    }
    warn!(
        chain = cfg.chain.as_str(),
        what, status, retry_after, "throttled by upstream, sleeping",
    );
    tokio::time::sleep(wait).await;
}

/// Parse a `Retry-After` header. Supports the integer-seconds form; the
/// HTTP-date form is rarer and not worth a chrono dependency to handle.
pub(crate) fn retry_after_secs(resp: &reqwest::Response) -> Option<u64> {
    retry_after_from_headers(resp.headers())
}

pub(crate) fn retry_after_from_headers(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
}
