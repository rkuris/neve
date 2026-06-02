//! `GET /health` endpoint, exposed alongside the JSON-RPC server.
//!
//! Implemented as a tower layer that short-circuits any `GET /health` request
//! before it reaches the JSON-RPC dispatcher. Every other request is passed
//! through unchanged.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use http::{Method, StatusCode, header};
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use serde::Serialize;
use tower::{Layer, Service};
use tracing::warn;

use crate::storage::Storage;

/// Shared, cheap-to-clone state behind the `/health` handler.
#[derive(Clone, Debug)]
pub struct HealthState {
    inner: Arc<HealthInner>,
}

#[derive(Debug)]
struct HealthInner {
    storage: Storage,
    started_at: Instant,
    data_dir: PathBuf,
    chain_id: u64,
    /// Last-known gap between contiguous-stored height and upstream tip,
    /// published by the backfill loop. 0 means caught up.
    behind_tip: Arc<AtomicU64>,
}

impl HealthState {
    pub fn new(
        storage: Storage,
        data_dir: PathBuf,
        chain_id: u64,
        behind_tip: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Arc::new(HealthInner {
                storage,
                started_at: Instant::now(),
                data_dir,
                chain_id,
                behind_tip,
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HealthLayer {
    state: HealthState,
}

impl HealthLayer {
    pub const fn new(state: HealthState) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for HealthLayer {
    type Service = HealthService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        HealthService {
            inner,
            state: self.state.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HealthService<S> {
    inner: S,
    state: HealthState,
}

impl<S> Service<HttpRequest<HttpBody>> for HealthService<S>
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
        if req.method() == Method::GET && req.uri().path() == "/health" {
            let state = self.state.clone();
            return async move { Ok(build_health_response(&state).await) }.boxed();
        }
        self.inner.call(req).boxed()
    }
}

#[derive(Serialize, Debug)]
struct HealthReport {
    status: &'static str,
    /// Crate version (`CARGO_PKG_VERSION`), e.g. `"0.1.0"`.
    version: &'static str,
    /// Short git commit the binary was built from (`build.rs`), e.g. `"0c1ea6d"`.
    commit: &'static str,
    chain_id: u64,
    uptime_secs: u64,
    uptime: String,
    blocks: BlocksReport,
    storage: StorageReport,
    memory: MemoryReport,
}

/// Format a byte count as e.g. `"1.23 MiB"`. Wraps `human_bytes` so the
/// serialized fields stay consistent across the report.
#[allow(clippy::cast_precision_loss)]
fn human(bytes: u64) -> String {
    human_bytes::human_bytes(bytes as f64)
}

#[derive(Serialize, Debug)]
struct BlocksReport {
    /// Lowest stored height. `null` until first ingest.
    min_height: Option<u64>,
    /// Highest height H where `[min_height, H]` is gap-free.
    max_contiguous_height: Option<u64>,
    /// Highest stored height (may exceed `max_contiguous_height` if newHeads
    /// raced ahead of backfill).
    high_water: Option<u64>,
    /// Distance between `max_contiguous_height` and the upstream tip, as
    /// last observed by the backfill loop. 0 means caught up.
    behind: u64,
}

#[derive(Serialize, Debug)]
struct StorageReport {
    data_dir: String,
    blockdb_bytes: u64,
    blockdb_human: String,
    index_bytes: u64,
    index_human: String,
    total_bytes: u64,
    total_human: String,
}

#[derive(Serialize, Debug)]
struct MemoryReport {
    /// Resident set size in bytes (`null` if the platform doesn't report it).
    physical_bytes: Option<usize>,
    physical_human: Option<String>,
    /// Virtual size in bytes (`null` if the platform doesn't report it).
    virtual_bytes: Option<usize>,
    virtual_human: Option<String>,
}

async fn build_health_response(state: &HealthState) -> HttpResponse<HttpBody> {
    let inner = &state.inner;
    let min = inner.storage.min_height().await;
    let mc = inner.storage.max_contiguous_height().await;
    let hw = inner.storage.high_water().await;
    let behind = inner.behind_tip.load(Ordering::Relaxed);

    let blockdb_bytes = dir_size(inner.storage.blockdb_dir()).await;
    let index_bytes = dir_size(&inner.data_dir.join("index")).await;
    let total_bytes = blockdb_bytes.saturating_add(index_bytes);

    let mem = memory_stats::memory_stats();
    let memory = MemoryReport {
        physical_bytes: mem.map(|m| m.physical_mem),
        physical_human: mem.map(|m| human(m.physical_mem as u64)),
        virtual_bytes: mem.map(|m| m.virtual_mem),
        virtual_human: mem.map(|m| human(m.virtual_mem as u64)),
    };

    let uptime_secs = inner.started_at.elapsed().as_secs();
    let report = HealthReport {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        commit: env!("NEVE_GIT_COMMIT"),
        chain_id: inner.chain_id,
        uptime_secs,
        uptime: humantime::format_duration(Duration::from_secs(uptime_secs)).to_string(),
        blocks: BlocksReport {
            min_height: (min > 0).then_some(min),
            max_contiguous_height: (mc > 0).then_some(mc),
            high_water: (hw > 0).then_some(hw),
            behind,
        },
        storage: StorageReport {
            data_dir: inner.data_dir.display().to_string(),
            blockdb_bytes,
            blockdb_human: human(blockdb_bytes),
            index_bytes,
            index_human: human(index_bytes),
            total_bytes,
            total_human: human(total_bytes),
        },
        memory,
    };

    let body = match serde_json::to_vec_pretty(&report) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to serialize health report");
            return HttpResponse::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(HttpBody::from(format!(
                    r#"{{"status":"error","error":"{e}"}}"#
                )))
                .expect("static error response is valid");
        }
    };
    HttpResponse::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(HttpBody::from(body))
        .expect("static success response is valid")
}

/// Sum file sizes under `dir`, recursively. Returns 0 if `dir` doesn't exist
/// or can't be read. Walks synchronously on a blocking thread so we don't
/// stall the runtime on cold filesystem caches.
async fn dir_size(dir: &Path) -> u64 {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || sum_dir(&dir))
        .await
        .unwrap_or(0)
}

fn sum_dir(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let Ok(read) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in read.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            total = total.saturating_add(sum_dir(&entry.path()));
        } else if ft.is_file()
            && let Ok(meta) = entry.metadata()
        {
            total = total.saturating_add(meta.len());
        }
    }
    total
}
