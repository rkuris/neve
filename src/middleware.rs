//! HTTP middleware that rewrites the response status code from 200 to 421
//! (Misdirected Request) when the JSON-RPC envelope reports `result: null`.
//!
//! This implements the api-worker contract sketched in
//! `avalanchego/StreamingChangeProofs.md`: when the mirror can't authoritatively
//! answer a request (block/hash not in our local tail), respond with 421 so
//! the front-end retries against a full-node pool.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use http::StatusCode;
use http_body_util::BodyExt;
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use serde_json::Value;
use tower::{Layer, Service};
use tracing::debug;

#[derive(Clone, Debug)]
pub struct NotFound421Layer;

impl<S> Layer<S> for NotFound421Layer {
    type Service = NotFound421<S>;
    fn layer(&self, inner: S) -> Self::Service {
        NotFound421 { inner }
    }
}

#[derive(Clone, Debug)]
pub struct NotFound421<S> {
    inner: S,
}

impl<S> Service<HttpRequest<HttpBody>> for NotFound421<S>
where
    S: Service<HttpRequest<HttpBody>, Response = HttpResponse<HttpBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = HttpResponse<HttpBody>;
    type Error = S::Error;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<HttpBody>) -> Self::Future {
        let fut = self.inner.call(req);
        async move {
            let res = fut.await?;
            let (mut parts, body) = res.into_parts();
            // Buffer the body — JSON-RPC envelopes are tiny.
            let bytes_vec: Vec<u8> = match body.collect().await {
                Ok(b) => b.to_bytes().to_vec(),
                Err(_) => {
                    // If we can't buffer, just send an empty 200 body. This is
                    // a degenerate path that shouldn't fire in practice.
                    return Ok(HttpResponse::from_parts(parts, HttpBody::empty()));
                }
            };

            if is_result_null(&bytes_vec) {
                parts.status = StatusCode::MISDIRECTED_REQUEST;
            }

            Ok(HttpResponse::from_parts(parts, HttpBody::from(bytes_vec)))
        }
        .boxed()
    }
}

/// Return true iff the body is a JSON-RPC success envelope whose `result`
/// field is JSON `null`. Batch requests (top-level array) are conservatively
/// left untouched.
fn is_result_null(bytes: &[u8]) -> bool {
    let Ok(v) = serde_json::from_slice::<Value>(bytes) else {
        // Non-JSON bodies are expected backpressure, not anomalies: jsonrpsee
        // answers over-limit load with a plaintext 4xx/5xx. Logging one WARN
        // per occurrence floods syslog under load, so this is debug-level.
        debug!("non-JSON RPC response — leaving status untouched");
        return false;
    };
    matches!(v.get("result"), Some(Value::Null))
}
