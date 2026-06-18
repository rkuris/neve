//! `GET /blocks?from=<n>&to=<n>` — streaming NDJSON bulk export of a stored
//! block range.
//!
//! `oldBlocks` (the WS subscription) is the right tool for a *mirror bootstrap*
//! that streams history and then follows the live tip on the same connection.
//! For a one-shot dump, jsonrpsee keeps the WS open after the last block (it
//! multiplexes subscriptions and exposes no connection-close hook), so the
//! client can't tell when it's done. HTTP fits that shape natively: this layer
//! streams one block per line and sets `Connection: close`, so the response ends
//! the connection — `curl ... > blocks.ndjson` streams and exits at EOF.
//!
//! Implemented as a tower layer that short-circuits `GET /blocks` ahead of the
//! JSON-RPC dispatch (like `/health` and `/metrics`), and sits OUTSIDE the
//! 200→421 rewrite so the streaming body is never buffered. Reads are demand-
//! driven (one `get_by_height` per polled frame), so a huge range streams with
//! natural backpressure rather than materializing in memory.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{FutureExt, stream};
use http::{Method, StatusCode, header};
use http_body::Frame;
use http_body_util::StreamBody;
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use tower::{Layer, Service};
use tracing::{debug, warn};

use crate::storage::Storage;

#[derive(Clone, Debug)]
pub struct BulkBlocksLayer {
    storage: Storage,
    /// Largest range (inclusive block count) a single request may ask for.
    max_blocks: u64,
}

impl BulkBlocksLayer {
    pub const fn new(storage: Storage, max_blocks: u64) -> Self {
        Self {
            storage,
            max_blocks,
        }
    }
}

impl<S> Layer<S> for BulkBlocksLayer {
    type Service = BulkBlocksService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        BulkBlocksService {
            inner,
            storage: self.storage.clone(),
            max_blocks: self.max_blocks,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BulkBlocksService<S> {
    inner: S,
    storage: Storage,
    max_blocks: u64,
}

impl<S> Service<HttpRequest<HttpBody>> for BulkBlocksService<S>
where
    S: Service<HttpRequest<HttpBody>, Response = HttpResponse<HttpBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = HttpResponse<HttpBody>;
    type Error = S::Error;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<HttpBody>) -> Self::Future {
        if req.method() == Method::GET && req.uri().path() == "/blocks" {
            let storage = self.storage.clone();
            let max_blocks = self.max_blocks;
            let query = req.uri().query().unwrap_or_default().to_owned();
            return async move { Ok(serve_range(&storage, max_blocks, &query).await) }.boxed();
        }
        self.inner.call(req).boxed()
    }
}

/// Validate the requested range and, if good, return a streaming NDJSON response;
/// otherwise a plaintext error. All responses set `Connection: close` so the
/// socket ends with the transfer.
async fn serve_range(storage: &Storage, max_blocks: u64, query: &str) -> HttpResponse<HttpBody> {
    let (from, to_opt) = match parse_range(query) {
        Ok(r) => r,
        Err(msg) => return error_response(StatusCode::BAD_REQUEST, &msg),
    };

    // Only serve a gapless, fully-present range — same guarantee as `oldBlocks`.
    let min = storage.min_height().await;
    let contiguous = storage.max_contiguous_height().await;
    if from < min || from > contiguous {
        return error_response(
            StatusCode::RANGE_NOT_SATISFIABLE,
            &format!("'from' {from} outside available [{min}..={contiguous}]"),
        );
    }

    // `to` is optional: default to a full `max_blocks` window from `from`,
    // clamped to the contiguous tip — so `?from=X` streams the next chunk.
    let to = match to_opt {
        Some(t) if t < from => {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("'to' ({t}) is before 'from' ({from})"),
            );
        }
        Some(t) if t > contiguous => {
            return error_response(
                StatusCode::RANGE_NOT_SATISFIABLE,
                &format!("'to' {t} past the contiguous tip {contiguous}"),
            );
        }
        Some(t) => t,
        None => from
            .saturating_add(max_blocks.saturating_sub(1))
            .min(contiguous),
    };

    // `to >= from` holds here, so the saturating ops are exact; they keep the
    // arithmetic-overflow lint happy without a real overflow path.
    let count = to.saturating_sub(from).saturating_add(1);
    if count > max_blocks {
        return error_response(
            StatusCode::BAD_REQUEST,
            &format!("requested {count} blocks; max {max_blocks} per request"),
        );
    }

    // Demand-driven: each polled frame reads the next block from storage.
    let body_stream = stream::unfold(
        (storage.clone(), from),
        move |(storage, height)| async move {
            if height > to {
                return None;
            }
            match storage.get_by_height(height).await {
                Ok(Some(mut bytes)) => {
                    bytes.push(b'\n'); // NDJSON: one block per line
                    let next = height.saturating_add(1);
                    Some((
                        Ok::<_, Infallible>(Frame::data(Bytes::from(bytes))),
                        (storage, next),
                    ))
                }
                // The range was validated as gapless+present, so neither of these
                // should fire; truncate (and log) rather than spin or panic.
                Ok(None) => {
                    debug!(
                        height,
                        "bulk: unexpected gap in validated range; truncating"
                    );
                    None
                }
                Err(e) => {
                    warn!(height, error = %e, "bulk: storage read failed; truncating");
                    None
                }
            }
        },
    );

    HttpResponse::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(header::CONNECTION, "close")
        .body(HttpBody::new(StreamBody::new(body_stream)))
        .expect("static response shape is valid")
}

/// Parse `from` (required) and `to` (optional) out of the query string, each
/// decimal or `0x`-prefixed hex.
fn parse_range(query: &str) -> Result<(u64, Option<u64>), String> {
    let mut from = None;
    let mut to = None;
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        match key {
            "from" => from = Some(parse_height(value)?),
            "to" => to = Some(parse_height(value)?),
            _ => {}
        }
    }
    let from = from.ok_or("missing 'from' query parameter")?;
    Ok((from, to))
}

/// A block height as decimal or `0x`-prefixed hex.
fn parse_height(s: &str) -> Result<u64, String> {
    match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).map_err(|_| format!("invalid hex height: {s}")),
        None => s.parse::<u64>().map_err(|_| format!("invalid height: {s}")),
    }
}

fn error_response(status: StatusCode, msg: &str) -> HttpResponse<HttpBody> {
    HttpResponse::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CONNECTION, "close")
        .body(HttpBody::from(format!("{msg}\n")))
        .expect("static response shape is valid")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};

    /// Inner service that should never be called for `GET /blocks`.
    #[derive(Clone)]
    struct Unreachable;
    impl Service<HttpRequest<HttpBody>> for Unreachable {
        type Response = HttpResponse<HttpBody>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Infallible>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: HttpRequest<HttpBody>) -> Self::Future {
            std::future::ready(Ok(HttpResponse::new(HttpBody::from("inner\n"))))
        }
    }

    fn unique_temp_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "neve-bulk-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ))
    }

    async fn put_block(storage: &Storage, height: u64) {
        let block = json!({ "number": format!("0x{height:x}"), "transactions": [] });
        let mut hash = [0u8; 32];
        hash[24..].copy_from_slice(&height.to_be_bytes());
        storage
            .put(height, hash, &[], serde_json::to_vec(&block).unwrap())
            .await
            .unwrap();
    }

    async fn get(svc: &mut BulkBlocksService<Unreachable>, uri: &str) -> (u16, Vec<u8>) {
        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.unwrap();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(uri)
            .body(HttpBody::empty())
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        let status = resp.status().as_u16();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, bytes)
    }

    fn svc(storage: Storage, max_blocks: u64) -> BulkBlocksService<Unreachable> {
        BulkBlocksService {
            inner: Unreachable,
            storage,
            max_blocks,
        }
    }

    #[test]
    fn parse_height_decimal_and_hex() {
        assert_eq!(parse_height("10").unwrap(), 10);
        assert_eq!(parse_height("0x52aba41").unwrap(), 86_686_273);
        assert_eq!(parse_height("0X10").unwrap(), 16);
        assert!(parse_height("0xzz").is_err());
        assert!(parse_height("nope").is_err());
    }

    #[test]
    fn parse_range_requires_from_to_optional() {
        assert!(parse_range("to=10").is_err()); // `from` required
        assert_eq!(parse_range("from=10").unwrap(), (10, None));
        assert_eq!(parse_range("from=10&to=0x14").unwrap(), (10, Some(20)));
    }

    #[tokio::test]
    async fn streams_range_as_ndjson() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43_114, None).unwrap();
        for h in 10..=12 {
            put_block(&storage, h).await;
        }
        let mut s = svc(storage, 100);

        let (status, body) = get(&mut s, "/blocks?from=10&to=12").await;
        assert_eq!(status, 200);
        let lines: Vec<&[u8]> = body
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines.len(), 3, "one NDJSON line per block");
        for (i, line) in lines.iter().enumerate() {
            let v: Value = serde_json::from_slice(line).unwrap();
            assert_eq!(v["number"], format!("0x{:x}", 10 + i));
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rejects_oversized_range() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43_114, None).unwrap();
        for h in 10..=12 {
            put_block(&storage, h).await;
        }
        let mut s = svc(storage, 2); // cap below the 3-block request

        let (status, body) = get(&mut s, "/blocks?from=10&to=12").await;
        assert_eq!(status, 400);
        assert!(String::from_utf8_lossy(&body).contains("max 2 per request"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rejects_range_outside_store() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43_114, None).unwrap();
        for h in 10..=12 {
            put_block(&storage, h).await;
        }
        let mut s = svc(storage, 100);

        // below the floor and past the tip → 416 Range Not Satisfiable.
        let (status, _) = get(&mut s, "/blocks?from=9&to=12").await;
        assert_eq!(status, 416);
        let (status, _) = get(&mut s, "/blocks?from=10&to=13").await;
        assert_eq!(status, 416);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn to_defaults_to_a_capped_window() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43_114, None).unwrap();
        for h in 10..=14 {
            put_block(&storage, h).await;
        }

        // `?from=10` with a cap of 3 → to defaults to 10+3-1 = 12 (3 blocks).
        let mut s = svc(storage.clone(), 3);
        let (status, body) = get(&mut s, "/blocks?from=10").await;
        assert_eq!(status, 200);
        assert_eq!(
            body.split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .count(),
            3
        );

        // A cap bigger than what's stored clamps to the contiguous tip (5 blocks).
        let mut s = svc(storage, 100);
        let (status, body) = get(&mut s, "/blocks?from=10").await;
        assert_eq!(status, 200);
        assert_eq!(
            body.split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .count(),
            5
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn bad_request_on_missing_from_or_swapped_params() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43_114, None).unwrap();
        for h in 10..=12 {
            put_block(&storage, h).await;
        }
        let mut s = svc(storage, 100);

        assert_eq!(get(&mut s, "/blocks").await.0, 400); // no params at all
        assert_eq!(get(&mut s, "/blocks?to=12").await.0, 400); // `from` still required
        assert_eq!(get(&mut s, "/blocks?from=11&to=10").await.0, 400); // explicit to < from
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn non_blocks_path_passes_through() {
        let dir = unique_temp_dir();
        let storage = Storage::open(&dir, 43_114, None).unwrap();
        let mut s = svc(storage, 100);

        let (_, body) = get(&mut s, "/something-else").await;
        assert_eq!(
            body, b"inner\n",
            "non-/blocks requests reach the inner service"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
