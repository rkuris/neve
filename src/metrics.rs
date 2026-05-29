//! Prometheus metrics: the `GET /metrics` endpoint plus typed recording
//! helpers used across the ingest, backfill, upstream-fetch, and subscription
//! paths.
//!
//! Names follow Prometheus conventions with a `neve_` prefix; histograms are
//! classic (explicit buckets) rather than native.

#![expect(
    clippy::cast_precision_loss,
    reason = "prometheus uses f64 and we're well within f64 bounds for all u64's we're sending"
)]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, anyhow};
use futures_util::FutureExt;
use http::{Method, StatusCode, header};
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tower::{Layer, Service};

use crate::rpc::SubKind;

// ---- metric names ---------------------------------------------------------

const BUILD_INFO: &str = "neve_build_info";
const PROCESS_START_TIME: &str = "neve_process_start_time_seconds";

const INGEST_HEAD_HEIGHT: &str = "neve_ingest_head_height";
const INGEST_CONTIGUOUS_HEIGHT: &str = "neve_ingest_contiguous_height";
const INGEST_BEHIND_BLOCKS: &str = "neve_ingest_behind_blocks";
const INGEST_BLOCKS_TOTAL: &str = "neve_ingest_blocks_total";

const UPSTREAM_REQUESTS_TOTAL: &str = "neve_upstream_requests_total";
const UPSTREAM_RETRY_AFTER_SECONDS: &str = "neve_upstream_retry_after_seconds";
const UPSTREAM_CONNECTED_SINCE: &str = "neve_upstream_connected_since_seconds";
const UPSTREAM_WS_RECONNECTS_TOTAL: &str = "neve_upstream_ws_reconnects_total";
const UPSTREAM_WS_IDLE_TIMEOUTS_TOTAL: &str = "neve_upstream_ws_idle_timeouts_total";

const SUB_OPEN: &str = "neve_sub_open";
const SUB_LAGGED_TOTAL: &str = "neve_sub_lagged_total";
const SUB_SENT_BYTES_TOTAL: &str = "neve_sub_sent_bytes_total";

// ---- bounded label values -------------------------------------------------

/// Which path persisted a block — the `source` label on `neve_ingest_blocks_total`.
#[derive(Debug)]
pub enum BlockSource {
    /// The live WebSocket ingester (`newHeads`/`newBlocks`).
    Live,
    /// The backfill worker closing a gap.
    Backfill,
}

impl BlockSource {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Backfill => "backfill",
        }
    }
}

/// Outcome of an upstream HTTPS request — the `outcome` label on
/// `neve_upstream_requests_total`. A small fixed enum so the series count can't
/// blow up regardless of what the upstream returns.
#[derive(Debug)]
pub enum UpstreamOutcome {
    /// A usable response (HTTP 2xx with a non-null JSON-RPC `result`).
    Ok,
    /// HTTP 2xx but a null/absent `result` — the block isn't available upstream
    /// yet. Tracks how often live fetches outrun the RPC backend's propagation.
    Empty,
    /// HTTP 429 Too Many Requests.
    TooManyRequests,
    /// HTTP 503 Service Unavailable.
    ServiceUnavailable,
    /// Transport error, non-2xx HTTP status, or response decode failure.
    Error,
}

impl UpstreamOutcome {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Empty => "empty",
            Self::TooManyRequests => "429",
            Self::ServiceUnavailable => "503",
            Self::Error => "error",
        }
    }
}

/// Classify an HTTP status into a request outcome. Only the throttle codes map
/// to distinct variants; any other non-success status is `Error`. `Ok`/`Empty`
/// can't be derived from status alone (they depend on the response body), so
/// the success path classifies those itself.
impl From<StatusCode> for UpstreamOutcome {
    fn from(status: StatusCode) -> Self {
        if status == StatusCode::TOO_MANY_REQUESTS {
            Self::TooManyRequests
        } else if status == StatusCode::SERVICE_UNAVAILABLE {
            Self::ServiceUnavailable
        } else {
            Self::Error
        }
    }
}

// ---- recording helpers ----------------------------------------------------

/// Publish the static process-metadata gauges once at startup: `build_info`
/// (constant 1, version + git commit carried in labels, joined onto other
/// series in `PromQL`) and the process start time as unix epoch seconds (let
/// Prometheus derive uptime via `time() - neve_process_start_time_seconds`
/// rather than counting it ourselves). The commit comes from `build.rs`
/// (`"unknown"` outside a git checkout).
pub fn process_metadata() {
    gauge!(
        BUILD_INFO,
        "version" => env!("CARGO_PKG_VERSION"),
        "commit" => env!("NEVE_GIT_COMMIT"),
    )
    .set(1.0);

    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    gauge!(PROCESS_START_TIME).set(start);
}

/// Publish the freshness gauges from one backfill-loop snapshot: the stored
/// tip, the contiguous frontier, and how far behind the upstream tip we are.
pub fn ingest_heights(head: u64, contiguous: u64, behind: u64) {
    gauge!(INGEST_HEAD_HEIGHT).set(head as f64);
    gauge!(INGEST_CONTIGUOUS_HEIGHT).set(contiguous as f64);
    gauge!(INGEST_BEHIND_BLOCKS).set(behind as f64);
}

/// Count one persisted block, tagged by which path stored it.
pub fn block_persisted(source: BlockSource) {
    counter!(INGEST_BLOCKS_TOTAL, "source" => source.as_str()).increment(1);
}

/// Count one upstream HTTPS request by outcome. Accepts anything convertible
/// into an `UpstreamOutcome` (e.g. an HTTP `StatusCode`) so call sites needn't
/// spell out the conversion.
pub fn upstream_request(outcome: impl Into<UpstreamOutcome>) {
    counter!(UPSTREAM_REQUESTS_TOTAL, "outcome" => outcome.into().as_str()).increment(1);
}

/// Record a `Retry-After` value (seconds) the upstream asked us to wait.
pub fn upstream_retry_after(secs: u64) {
    histogram!(UPSTREAM_RETRY_AFTER_SECONDS).record(secs as f64);
}

/// Mark the upstream live subscription as (re)established now: set the
/// connected-since gauge to the current unix epoch seconds. Prometheus derives
/// the current session's age as `time() - neve_upstream_connected_since_seconds`;
/// each reset (paired with a `ws_reconnect` bump) marks a fresh session, so a
/// recently-reset gauge plus a climbing reconnect counter reveals flapping.
pub fn upstream_connected() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    gauge!(UPSTREAM_CONNECTED_SINCE).set(now);
}

/// Count one WebSocket reconnect (a session ended or failed and we looped).
pub fn ws_reconnect() {
    counter!(UPSTREAM_WS_RECONNECTS_TOTAL).increment(1);
}

/// Count one idle-watchdog timeout (no `newHeads` within the configured window).
pub fn ws_idle_timeout() {
    counter!(UPSTREAM_WS_IDLE_TIMEOUTS_TOTAL).increment(1);
}

/// RAII guard for one active subscription. Bumps `neve_sub_open` on creation
/// and drops it on `Drop`, so the gauge is balanced no matter how the
/// subscription loop exits (client disconnect, send error, or a `?` early
/// return). Carries the kind so call sites don't repeat the label.
#[derive(Debug)]
pub struct SubMetricsGuard {
    kind: SubKind,
}

impl SubMetricsGuard {
    /// Increment the active-subscription gauge (decremented on drop).
    pub fn new(kind: SubKind) -> Self {
        gauge!(SUB_OPEN, "kind" => kind.as_str()).increment(1.0);
        Self { kind }
    }

    /// A slow subscriber fell behind the broadcast ring; `n` blocks were skipped.
    pub fn lagged(&self, n: u64) {
        counter!(SUB_LAGGED_TOTAL, "kind" => self.kind.as_str()).increment(n);
    }

    /// Serialized wire bytes pushed to the subscriber after a successful send.
    pub fn sent_bytes(&self, bytes: u64) {
        counter!(SUB_SENT_BYTES_TOTAL, "kind" => self.kind.as_str()).increment(bytes);
    }
}

impl Drop for SubMetricsGuard {
    fn drop(&mut self) {
        gauge!(SUB_OPEN, "kind" => self.kind.as_str()).decrement(1.0);
    }
}

// ---- recorder setup -------------------------------------------------------

/// Bucket bounds (seconds) for `neve_upstream_retry_after_seconds`. `Retry-After`
/// values run seconds-to-minutes, so the buckets are coarse and span up to the
/// 10-minute neighborhood of the default `--max-wait`.
const RETRY_AFTER_BUCKETS: &[f64] = &[0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0];

/// Build the Prometheus recorder, install it as the global `metrics` recorder,
/// describe every series (help text + units), and return a handle for rendering
/// the `/metrics` payload. Histograms get explicit buckets here (classic, not
/// native).
pub fn install() -> Result<PrometheusHandle> {
    let recorder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(UPSTREAM_RETRY_AFTER_SECONDS.to_owned()),
            RETRY_AFTER_BUCKETS,
        )
        .context("configuring retry-after histogram buckets")?
        .build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|e| anyhow!("install global metrics recorder: {e}"))?;
    describe_metrics();
    process_metadata();
    Ok(handle)
}

/// Help text + units for each series. Called once after the recorder is global.
fn describe_metrics() {
    describe_gauge!(
        BUILD_INFO,
        "Build metadata as a constant 1; version and short git commit carried in labels."
    );
    describe_gauge!(
        PROCESS_START_TIME,
        metrics::Unit::Seconds,
        "Process start time (unix epoch seconds). Uptime = time() - this."
    );
    describe_gauge!(
        INGEST_HEAD_HEIGHT,
        "Highest stored block height (the blockstore high-water mark)."
    );
    describe_gauge!(
        INGEST_CONTIGUOUS_HEIGHT,
        "Highest gap-free stored block height."
    );
    describe_gauge!(
        INGEST_BEHIND_BLOCKS,
        "Blocks between the contiguous frontier and the upstream tip (0 = caught up). Primary freshness alerting signal."
    );
    describe_counter!(
        INGEST_BLOCKS_TOTAL,
        "Blocks persisted. Label source={live|backfill}."
    );
    describe_counter!(
        UPSTREAM_REQUESTS_TOTAL,
        "Upstream HTTPS requests. Label outcome={ok|empty|429|503|error}."
    );
    describe_histogram!(
        UPSTREAM_RETRY_AFTER_SECONDS,
        metrics::Unit::Seconds,
        "Retry-After delays requested by the upstream on 429/503."
    );
    describe_gauge!(
        UPSTREAM_CONNECTED_SINCE,
        metrics::Unit::Seconds,
        "Unix epoch seconds of the last successful upstream live subscribe. Session age = time() - this."
    );
    describe_counter!(
        UPSTREAM_WS_RECONNECTS_TOTAL,
        "WebSocket reconnects to the upstream."
    );
    describe_counter!(
        UPSTREAM_WS_IDLE_TIMEOUTS_TOTAL,
        "Idle-watchdog timeouts that forced a WebSocket reconnect."
    );
    describe_gauge!(
        SUB_OPEN,
        "Active eth_subscribe subscriptions. Label kind={newHeads|newBlocks|oldBlocks}."
    );
    describe_counter!(
        SUB_LAGGED_TOTAL,
        "Blocks dropped for subscribers that fell behind the broadcast ring. Label kind={newHeads|newBlocks} (live kinds only)."
    );
    describe_counter!(
        SUB_SENT_BYTES_TOTAL,
        "Serialized bytes pushed to subscribers. Label kind={newHeads|newBlocks|oldBlocks}."
    );
}

// ---- `GET /metrics` tower layer -------------------------------------------

/// Tower layer that serves `GET /metrics` from a [`PrometheusHandle`], passing
/// every other request through. Sibling to `health::HealthLayer`.
#[derive(Clone, Debug)]
pub struct MetricsLayer {
    handle: PrometheusHandle,
}

impl MetricsLayer {
    pub const fn new(handle: PrometheusHandle) -> Self {
        Self { handle }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            handle: self.handle.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetricsService<S> {
    inner: S,
    handle: PrometheusHandle,
}

// Short-circuits `GET /metrics` with the rendered Prometheus payload (text
// exposition content-type); every other request falls through to the inner
// service unchanged.
impl<S> Service<HttpRequest<HttpBody>> for MetricsService<S>
where
    S: Service<HttpRequest<HttpBody>, Response = HttpResponse<HttpBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = HttpResponse<HttpBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<HttpBody>) -> Self::Future {
        if req.method() == Method::GET && req.uri().path() == "/metrics" {
            let body = self.handle.render();
            let resp = HttpResponse::builder()
                .status(StatusCode::OK)
                .header(
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )
                .body(HttpBody::from(body))
                .expect("static metrics response is valid");
            return std::future::ready(Ok(resp)).boxed();
        }
        self.inner.call(req).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exercise the typed helpers against a thread-local recorder (no global
    /// install, so this is parallel-safe), then assert the rendered exposition
    /// carries the expected names, labels, and bucket lines — locking in the
    /// metric contract dashboards and the spec depend on.
    ///
    /// Not exhaustive by construction: Rust can't enumerate the helpers, so a
    /// new metric isn't covered until it's recorded and asserted here by hand.
    /// Update this test when adding a helper.
    #[test]
    fn helpers_render_expected_series() {
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full(UPSTREAM_RETRY_AFTER_SECONDS.to_owned()),
                RETRY_AFTER_BUCKETS,
            )
            .expect("bucket config")
            .build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            process_metadata();
            ingest_heights(100, 90, 10);
            block_persisted(BlockSource::Live);
            block_persisted(BlockSource::Backfill);
            upstream_request(UpstreamOutcome::Ok);
            upstream_request(UpstreamOutcome::Empty);
            upstream_request(UpstreamOutcome::TooManyRequests);
            upstream_retry_after(7);
            upstream_connected();
            ws_reconnect();
            ws_idle_timeout();
            let guard = SubMetricsGuard::new(SubKind::NewHeads);
            guard.sent_bytes(512);
            guard.lagged(3);
            // guard dropped here: neve_sub_open returns to 0.
        });

        let out = handle.render();

        // Process metadata: build_info is a constant 1 with version/commit
        // labels; start time is a positive epoch-seconds gauge.
        assert!(
            out.contains(&format!(
                r#"neve_build_info{{version="{}",commit="{}"}} 1"#,
                env!("CARGO_PKG_VERSION"),
                env!("NEVE_GIT_COMMIT"),
            )),
            "{out}"
        );
        assert!(out.contains("neve_process_start_time_seconds "), "{out}");
        // Gauges.
        assert!(out.contains("neve_ingest_head_height 100"), "{out}");
        assert!(out.contains("neve_ingest_behind_blocks 10"), "{out}");
        // Counters with bounded labels.
        assert!(
            out.contains(r#"neve_ingest_blocks_total{source="live"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"neve_ingest_blocks_total{source="backfill"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"neve_upstream_requests_total{outcome="empty"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"neve_upstream_requests_total{outcome="429"} 1"#),
            "{out}"
        );
        assert!(
            out.contains("neve_upstream_connected_since_seconds "),
            "{out}"
        );
        assert!(out.contains("neve_upstream_ws_reconnects_total 1"), "{out}");
        assert!(
            out.contains("neve_upstream_ws_idle_timeouts_total 1"),
            "{out}"
        );
        // Histogram with explicit buckets.
        assert!(
            out.contains("neve_upstream_retry_after_seconds_bucket"),
            "{out}"
        );
        assert!(
            out.contains("neve_upstream_retry_after_seconds_count 1"),
            "{out}"
        );
        // Subscription series (open balanced back to 0 after the guard dropped).
        assert!(out.contains(r#"neve_sub_open{kind="newHeads"} 0"#), "{out}");
        assert!(
            out.contains(r#"neve_sub_sent_bytes_total{kind="newHeads"} 512"#),
            "{out}"
        );
        assert!(
            out.contains(r#"neve_sub_lagged_total{kind="newHeads"} 3"#),
            "{out}"
        );
    }
}
