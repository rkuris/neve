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
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, anyhow};
use futures_util::FutureExt;
use http::{Method, StatusCode, header};
use jsonrpsee::core::middleware::{Batch, Notification, RpcServiceT};
use jsonrpsee::core::server::MethodResponse;
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use jsonrpsee::types::Request;
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
const INGEST_LAST_BLOCK_TIMESTAMP: &str = "neve_ingest_last_block_timestamp_seconds";

const RPC_REQUESTS_TOTAL: &str = "neve_rpc_requests_total";
const RPC_REQUEST_DURATION_SECONDS: &str = "neve_rpc_request_duration_seconds";
const RPC_OPEN_CONNECTIONS: &str = "neve_rpc_open_connections";
const RPC_MISDIRECTED_TOTAL: &str = "neve_rpc_misdirected_total";

const UPSTREAM_REQUESTS_TOTAL: &str = "neve_upstream_requests_total";
const UPSTREAM_FIRST_ATTEMPT_TOTAL: &str = "neve_upstream_first_attempt_total";
const UPSTREAM_PREFETCH_DELAY_SECONDS: &str = "neve_upstream_prefetch_delay_seconds";
const UPSTREAM_REQUEST_DURATION_SECONDS: &str = "neve_upstream_request_duration_seconds";
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

/// Create the standard process-stats collector and register its help text once.
/// Exposes the conventional `process_*` series (no `neve_` prefix): CPU seconds,
/// resident/virtual memory, open/max fds, thread count, and the process start
/// time. Call [`metrics_process::Collector::collect`] on the returned handle
/// periodically (from the metrics-upkeep loop) to refresh them. Reads `/proc` on
/// Linux (the deploy target); macOS and other platforms report a subset.
///
/// Note the collector also emits `process_start_time_seconds`, a benign duplicate
/// of our unconditional `neve_process_start_time_seconds` (different name, no
/// collision); we keep ours since it's set at install without depending on the
/// periodic collect.
pub fn process_collector() -> metrics_process::Collector {
    let collector = metrics_process::Collector::default();
    collector.describe();
    collector
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

/// Record the block-header timestamp (unix epoch seconds) of the latest live
/// block. Alert on staleness directly via
/// `time() - neve_ingest_last_block_timestamp_seconds > N` — this catches a
/// frozen upstream tip, which `neve_ingest_behind_blocks` (0 whenever we match
/// the tip, stuck or not) cannot. Set from the live path only: backfill writes
/// older blocks whose timestamps would drag the gauge backward.
pub fn last_block_timestamp(secs: u64) {
    gauge!(INGEST_LAST_BLOCK_TIMESTAMP).set(secs as f64);
}

/// Record one served JSON-RPC method call: bump the per-method/status counter
/// and observe its wall-clock latency. `method` is pre-clamped to the registered
/// set (or `"other"`) by the middleware, so label cardinality stays bounded.
pub fn rpc_call(method: &'static str, status: &'static str, secs: f64) {
    counter!(RPC_REQUESTS_TOTAL, "method" => method, "status" => status).increment(1);
    histogram!(RPC_REQUEST_DURATION_SECONDS, "method" => method).record(secs);
}

/// Count one response the `NotFound421` layer rewrote 200→421: a client asked
/// for a block/hash this mirror's tail doesn't hold. A high rate means clients
/// are routinely missing our window and falling back to the full-node pool.
pub fn rpc_misdirected() {
    counter!(RPC_MISDIRECTED_TOTAL).increment(1);
}

/// Record one upstream HTTPS request attempt: its outcome and its wall-clock
/// latency (`secs`). `secs` is per-attempt — the request round-trip including
/// body download/decode, *excluding* any retry backoff sleep that follows.
/// Accepts anything convertible into an `UpstreamOutcome` (e.g. an HTTP
/// `StatusCode`) so call sites needn't spell out the conversion. Latency spans
/// modes that differ by orders of magnitude (sub-ms mirror to 100ms+ public),
/// so the histogram uses a wide geometric ladder ([`UPSTREAM_DURATION_BUCKETS`]).
pub fn upstream_request(outcome: impl Into<UpstreamOutcome>, secs: f64) {
    counter!(UPSTREAM_REQUESTS_TOTAL, "outcome" => outcome.into().as_str()).increment(1);
    histogram!(UPSTREAM_REQUEST_DURATION_SECONDS).record(secs);
}

/// Record live-path AIMD state once per head, on its *first* clean (2xx)
/// upstream outcome. Two signals the retry-counting `UPSTREAM_REQUESTS_TOTAL`
/// can't give:
/// - `first_try_ok`: the first-attempt outcome — the actual signal the
///   `AimdDelay` controller learns from. `UPSTREAM_REQUESTS_TOTAL{outcome=...}`
///   counts every retry too, so it overstates the miss rate the controller
///   drives toward (~`DEC/(INC+DEC)`); this counter measures it directly.
/// - `prefetch_delay`: the delay the controller has parked at, so we can see
///   whether it converged low (lag is short) or saturated at its cap.
///
/// Live path only — backfill never calls this (old blocks always exist).
pub fn upstream_first_attempt(first_try_ok: bool, prefetch_delay: f64) {
    let outcome = if first_try_ok { "ok" } else { "empty" };
    counter!(UPSTREAM_FIRST_ATTEMPT_TOTAL, "outcome" => outcome).increment(1);
    gauge!(UPSTREAM_PREFETCH_DELAY_SECONDS).set(prefetch_delay);
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

/// Bucket bounds (seconds) for `neve_upstream_request_duration_seconds`. A
/// geometric ladder (~1.4x, the 1-1.5-2-3-5-7 decade pattern) from a 10ms floor
/// to a 2s near-timeout ceiling. Geometric rather than linear because upstream
/// latency tracks deployment distance: a same-region backend, a cross-country
/// (west↔east, ~60-80ms RTT before TLS/routers) gateway, and a congested path
/// each shift the whole distribution along the ladder — constant *relative*
/// resolution keeps the same handful of buckets across the bulk wherever it
/// lands, instead of a linear ladder that assumes a fixed distance. Floored at
/// 10ms (sub-10ms is below realistic network latency and lumps into the first
/// bucket); topped at 2s with `+Inf` capturing the timeout tail. The series is
/// unlabeled (low cardinality), so the bucket count costs little.
const UPSTREAM_DURATION_BUCKETS: &[f64] = &[
    0.01, 0.015, 0.02, 0.03, 0.05, 0.07, 0.1, 0.15, 0.2, 0.3, 0.5, 0.7, 1.0, 1.5, 2.0,
];

/// Bucket bounds (seconds) for `neve_rpc_request_duration_seconds`. A geometric
/// (~2.5x, 1-2.5-5) ladder from a 25µs floor to a 0.5s ceiling rather than a
/// linear one: the same histogram has to span methods orders of magnitude apart
/// — `eth_chainId` is an in-memory constant (tens of µs) while a heavy
/// `eth_getLogs` or a cold-cache read reaches into the ms — so constant
/// *relative* resolution gives every method a few useful buckets wherever its
/// distribution sits. Span is the measured envelope, not a guess: served-request
/// service time benchmarks at p50 ~0.21ms / p99 ~0.8ms unloaded (c1), and even
/// deep saturation tops out around p99 ~25ms (8-core) / ~77ms (t4g.small) — so
/// the ceiling sits at 0.5s (~6x the worst p99, with `+Inf` catching pathological
/// stalls), and the 25µs floor resolves the fastest in-memory methods rather than
/// jamming them into one bottom bucket. One shared layout (the exporter buckets
/// by metric name, not by the `method` label) keeps the series
/// cross-method-aggregable; a constant-time method crossing into the ms buckets
/// is a saturation signal worth catching.
const RPC_DURATION_BUCKETS: &[f64] = &[
    0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
    0.5,
];

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
        .set_buckets_for_metric(
            Matcher::Full(RPC_REQUEST_DURATION_SECONDS.to_owned()),
            RPC_DURATION_BUCKETS,
        )
        .context("configuring rpc-duration histogram buckets")?
        .set_buckets_for_metric(
            Matcher::Full(UPSTREAM_REQUEST_DURATION_SECONDS.to_owned()),
            UPSTREAM_DURATION_BUCKETS,
        )
        .context("configuring upstream-duration histogram buckets")?
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
    describe_gauge!(
        INGEST_LAST_BLOCK_TIMESTAMP,
        metrics::Unit::Seconds,
        "Block-header timestamp of the latest live block (unix epoch seconds). Staleness = time() - this."
    );
    describe_counter!(
        RPC_REQUESTS_TOTAL,
        "Served JSON-RPC method calls. Labels method (registered eth_* set, else \"other\") and status={ok|error}."
    );
    describe_histogram!(
        RPC_REQUEST_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Served JSON-RPC method-call latency. Label method (clamped to the registered set)."
    );
    describe_gauge!(
        RPC_OPEN_CONNECTIONS,
        "Open JSON-RPC transport connections currently being served."
    );
    describe_counter!(
        RPC_MISDIRECTED_TOTAL,
        "Responses rewritten 200->421: a requested block/hash is outside this mirror's stored tail."
    );
    describe_counter!(
        UPSTREAM_REQUESTS_TOTAL,
        "Upstream HTTPS requests. Label outcome={ok|empty|429|503|error}."
    );
    describe_counter!(
        UPSTREAM_FIRST_ATTEMPT_TOTAL,
        "Live newHeads fetches by first-attempt outcome={ok|empty} (one per head; excludes retries, unlike neve_upstream_requests_total). The AIMD controller drives the empty share toward DEC/(INC+DEC)."
    );
    describe_gauge!(
        UPSTREAM_PREFETCH_DELAY_SECONDS,
        metrics::Unit::Seconds,
        "Current AIMD pre-fetch delay parked before the first live newHeads fetch."
    );
    describe_histogram!(
        UPSTREAM_REQUEST_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Per-attempt upstream HTTPS request latency (round-trip incl. body decode, excl. retry backoff)."
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

// ---- JSON-RPC method-level middleware -------------------------------------

/// RAII counter for one open JSON-RPC connection. Held behind an `Arc` in the
/// per-connection [`RpcMetricsService`] so the per-call clones share it and the
/// gauge decrements exactly once, when the last clone drops at connection close.
#[derive(Debug)]
struct ConnGuard;

impl ConnGuard {
    fn new() -> Self {
        gauge!(RPC_OPEN_CONNECTIONS).increment(1.0);
        Self
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        gauge!(RPC_OPEN_CONNECTIONS).decrement(1.0);
    }
}

/// `RpcServiceT` middleware that records per-method call counts, latency, and
/// open-connection count. Built per connection (via `RpcServiceBuilder::layer_fn`
/// in `rpc::serve`), so `ConnGuard` tracks connections and `methods` carries the
/// registered method names used to clamp the `method` label.
#[derive(Clone)]
pub struct RpcMetricsService<S> {
    inner: S,
    /// Registered method names (`&'static str` from `RpcModule::method_names`),
    /// shared across connections. Used to clamp the `method` label so an unknown
    /// method can't blow up label cardinality.
    methods: Arc<[&'static str]>,
    /// Decrements `neve_rpc_open_connections` when the connection closes.
    _conn: Arc<ConnGuard>,
}

impl<S> RpcMetricsService<S> {
    /// Wrap `inner` for one connection, bumping the open-connection gauge.
    pub fn new(inner: S, methods: Arc<[&'static str]>) -> Self {
        Self {
            inner,
            methods,
            _conn: Arc::new(ConnGuard::new()),
        }
    }

    /// Clamp a wire method name to the registered set, or `"other"`, keeping the
    /// `method` label bounded. Linear scan over the dozen registered names.
    fn label(&self, method: &str) -> &'static str {
        self.methods
            .iter()
            .copied()
            .find(|&m| m == method)
            .unwrap_or("other")
    }
}

impl<S> RpcServiceT for RpcMetricsService<S>
where
    S: RpcServiceT<MethodResponse = MethodResponse> + Send + Sync + Clone + 'static,
{
    type MethodResponse = S::MethodResponse;
    type NotificationResponse = S::NotificationResponse;
    type BatchResponse = S::BatchResponse;

    fn call<'a>(&self, req: Request<'a>) -> impl Future<Output = Self::MethodResponse> + Send + 'a {
        let method = self.label(req.method.as_ref());
        let inner = self.inner.clone();
        async move {
            let start = Instant::now();
            let rp = inner.call(req).await;
            let status = if rp.is_success() { "ok" } else { "error" };
            rpc_call(method, status, start.elapsed().as_secs_f64());
            rp
        }
    }

    fn batch<'a>(&self, batch: Batch<'a>) -> impl Future<Output = Self::BatchResponse> + Send + 'a {
        // Batches are forwarded untimed: jsonrpsee re-invokes `call` for each
        // inner request, so the per-method counters/latency still capture them.
        self.inner.batch(batch)
    }

    fn notification<'a>(
        &self,
        n: Notification<'a>,
    ) -> impl Future<Output = Self::NotificationResponse> + Send + 'a {
        self.inner.notification(n)
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
    #[expect(
        clippy::too_many_lines,
        reason = "contract test deliberately exercises every helper and asserts every rendered series in one place; splitting would scatter the contract"
    )]
    fn helpers_render_expected_series() {
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full(UPSTREAM_RETRY_AFTER_SECONDS.to_owned()),
                RETRY_AFTER_BUCKETS,
            )
            .expect("bucket config")
            .set_buckets_for_metric(
                Matcher::Full(RPC_REQUEST_DURATION_SECONDS.to_owned()),
                RPC_DURATION_BUCKETS,
            )
            .expect("bucket config")
            .set_buckets_for_metric(
                Matcher::Full(UPSTREAM_REQUEST_DURATION_SECONDS.to_owned()),
                UPSTREAM_DURATION_BUCKETS,
            )
            .expect("bucket config")
            .build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            process_metadata();
            rpc_call("eth_chainId", "ok", 0.001);
            rpc_misdirected();
            ingest_heights(100, 90, 10);
            block_persisted(BlockSource::Live);
            block_persisted(BlockSource::Backfill);
            last_block_timestamp(1_780_000_000);
            upstream_request(UpstreamOutcome::Ok, 0.012);
            upstream_request(UpstreamOutcome::Empty, 0.012);
            upstream_request(UpstreamOutcome::TooManyRequests, 0.012);
            upstream_first_attempt(false, 0.04);
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
        // Served-RPC metrics: per-method/status counter, latency histogram, and
        // the 200->421 misdirected counter.
        assert!(
            out.contains(r#"neve_rpc_requests_total{method="eth_chainId",status="ok"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"neve_rpc_request_duration_seconds_bucket{method="eth_chainId""#),
            "{out}"
        );
        assert!(out.contains("neve_rpc_misdirected_total 1"), "{out}");
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
            out.contains("neve_ingest_last_block_timestamp_seconds 1780000000"),
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
            out.contains(r#"neve_upstream_first_attempt_total{outcome="empty"} 1"#),
            "{out}"
        );
        assert!(out.contains("neve_upstream_prefetch_delay_seconds 0.04"), "{out}");
        assert!(
            out.contains("neve_upstream_request_duration_seconds_bucket"),
            "{out}"
        );
        assert!(
            out.contains("neve_upstream_request_duration_seconds_count 3"),
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
