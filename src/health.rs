//! `GET /health` endpoint, exposed alongside the JSON-RPC server.
//!
//! Implemented as a tower layer that short-circuits any `GET /health` request
//! before it reaches the JSON-RPC dispatcher. Every other request is passed
//! through unchanged.
//!
//! # Multi-chain shape
//!
//! One process can mirror several chains, so the report carries a `chains` map
//! keyed by chain. The `chain_id`, `blocks`, and `storage` keys are *also* kept
//! at the top level, describing the **default chain** (`default_chain` names
//! it), because a mirror's cold-start probe reads `blocks.min_height` and
//! `blocks.max_contiguous_height` from here — an older neve mirroring a newer
//! one must keep working. New consumers should read `chains.<chain>.blocks`.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use http::{Method, StatusCode, header};
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use serde::Serialize;
use tower::{Layer, Service};
use tracing::warn;

use crate::rpc::ChainServe;

/// Shared, cheap-to-clone state behind the `/health` handler.
#[derive(Clone, Debug)]
pub struct HealthState {
    inner: Arc<HealthInner>,
}

#[derive(Debug)]
struct HealthInner {
    /// One entry per running chain instance, in the `--chains` order. The first
    /// is the default chain the legacy top-level fields describe.
    chains: Vec<ChainServe>,
    started_at: Instant,
}

impl HealthState {
    pub fn new(chains: &[ChainServe]) -> Self {
        Self {
            inner: Arc::new(HealthInner {
                chains: chains.to_vec(),
                started_at: Instant::now(),
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
    uptime_secs: u64,
    uptime: String,
    /// Which chain the top-level `chain_id` / `blocks` / `storage` fields
    /// describe: `"c"` when the C-chain is running, otherwise the single
    /// selected chain.
    default_chain: &'static str,
    /// The default chain's numeric chain ID, `null` for a chain whose identity
    /// isn't a number (the P-chain's is its genesis block ID — see
    /// `chains.<chain>.network`). Kept at the top level for pre-multi-chain
    /// consumers.
    chain_id: Option<u64>,
    /// The default chain's block range. Kept at the top level because a
    /// mirror's cold-start probe reads `blocks.min_height` from here.
    blocks: BlocksReport,
    /// The default chain's on-disk sizes.
    storage: StorageReport,
    /// Every running chain instance, keyed by chain (`"c"`, `"p"`).
    chains: BTreeMap<&'static str, ChainReport>,
    memory: MemoryReport,
}

/// One chain instance's slice of the report.
#[derive(Serialize, Debug)]
struct ChainReport {
    /// Opaque network-identity stamp this store is bound to: the decimal
    /// `eth_chainId` on the C-chain, the genesis block ID on the P-chain.
    network: String,
    blocks: BlocksReport,
    storage: StorageReport,
}

/// Format a byte count as e.g. `"1.23 MiB"`. Wraps `human_bytes` so the
/// serialized fields stay consistent across the report.
#[allow(clippy::cast_precision_loss)]
fn human(bytes: u64) -> String {
    human_bytes::human_bytes(bytes as f64)
}

#[derive(Serialize, Debug, Clone, Default)]
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

#[derive(Serialize, Debug, Clone, Default)]
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

/// Collect one chain instance's block range and on-disk sizes.
async fn build_chain_report(c: &ChainServe) -> ChainReport {
    let min = c.storage.min_height().await;
    let mc = c.storage.max_contiguous_height().await;
    let hw = c.storage.high_water().await;

    let blockdb_bytes = dir_size(c.storage.blockdb_dir()).await;
    let index_bytes = dir_size(&c.data_dir.join("index")).await;
    let total_bytes = blockdb_bytes.saturating_add(index_bytes);

    ChainReport {
        network: c.identity.clone(),
        blocks: BlocksReport {
            min_height: (min > 0).then_some(min),
            max_contiguous_height: (mc > 0).then_some(mc),
            high_water: (hw > 0).then_some(hw),
            behind: c.behind_tip.load(Ordering::Relaxed),
        },
        storage: StorageReport {
            data_dir: c.data_dir.display().to_string(),
            blockdb_bytes,
            blockdb_human: human(blockdb_bytes),
            index_bytes,
            index_human: human(index_bytes),
            total_bytes,
            total_human: human(total_bytes),
        },
    }
}

async fn build_health_response(state: &HealthState) -> HttpResponse<HttpBody> {
    let inner = &state.inner;

    let mut chains: BTreeMap<&'static str, ChainReport> = BTreeMap::new();
    for c in &inner.chains {
        chains.insert(c.chain.as_str(), build_chain_report(c).await);
    }

    // The default chain backs the legacy top-level fields: the C-chain when
    // it's running (so a pre-multi-chain consumer keeps seeing C data), else the
    // only chain there is.
    let default = inner
        .chains
        .iter()
        .find(|c| c.chain == crate::chain::Chain::C)
        .or_else(|| inner.chains.first());
    let (default_chain, chain_id) = default.map_or(("none", None), |c| {
        (c.chain.as_str(), c.identity.parse::<u64>().ok())
    });
    let (default_blocks, default_storage) = default
        .and_then(|c| chains.get(c.chain.as_str()))
        .map_or_else(
            || (BlocksReport::default(), StorageReport::default()),
            |r| (r.blocks.clone(), r.storage.clone()),
        );

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
        uptime_secs,
        uptime: humantime::format_duration(Duration::from_secs(uptime_secs)).to_string(),
        default_chain,
        chain_id,
        blocks: default_blocks,
        storage: default_storage,
        chains,
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

/// Read a `blocks.<field>` number for `chain` out of an upstream neve's
/// `/health` body — the consumer side of the schema written above, kept here so
/// the two can't drift.
///
/// Prefers the per-chain `chains.<chain>.blocks.<field>`, falling back to the
/// legacy top-level `blocks.<field>` so a *newer* neve can still mirror an
/// *older* one (which has no `chains` map). The fallback only applies to the
/// chain the older neve could have been serving — the C-chain — so a P-chain
/// probe can't silently read C-chain heights.
pub(crate) fn upstream_blocks_field(
    v: &serde_json::Value,
    chain: crate::chain::Chain,
    field: &str,
) -> Option<u64> {
    let per_chain = v
        .pointer(&format!("/chains/{}/blocks/{field}", chain.as_str()))
        .and_then(serde_json::Value::as_u64);
    if per_chain.is_some() {
        return per_chain;
    }
    if chain != crate::chain::Chain::C {
        return None;
    }
    v.pointer(&format!("/blocks/{field}"))
        .and_then(serde_json::Value::as_u64)
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::chain::Chain;
    use crate::test_support::{chain_serve, unique_temp_dir};
    use serde_json::{Value, json};

    /// Render `/health` for the given instances and parse the body back.
    async fn report(chains: &[ChainServe]) -> Value {
        let resp = build_health_response(&HealthState::new(chains)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn put_block(storage: &crate::storage::Storage, h: u64) {
        let block = json!({ "number": format!("0x{h:x}"), "transactions": [] });
        let mut hash = [0u8; 32];
        hash[24..].copy_from_slice(&h.to_be_bytes());
        storage
            .put(
                h,
                hash,
                &[],
                &serde_json::to_vec(&block).unwrap(),
                crate::record::EMPTY_LOGS,
            )
            .await
            .unwrap();
    }

    /// A C+P process reports both chains under `chains`, and keeps the legacy
    /// top-level `chain_id`/`blocks`/`storage` pointed at the C-chain — the
    /// shape a pre-multi-chain mirror probe depends on.
    #[tokio::test]
    async fn multi_chain_report_keeps_legacy_top_level_on_c() {
        let base = unique_temp_dir("health-multichain");
        let c = chain_serve(Chain::C, &base);
        let p = chain_serve(Chain::P, &base);
        for h in 10..=12 {
            put_block(&c.storage, h).await;
        }
        put_block(&p.storage, 700).await;

        let v = report(&[c, p]).await;

        assert_eq!(v["default_chain"], "c");
        assert_eq!(v["chain_id"], 43114);
        // Legacy top-level view = the C-chain's range.
        assert_eq!(v["blocks"]["min_height"], 10);
        assert_eq!(v["blocks"]["max_contiguous_height"], 12);
        // Both chains present, each with its own range and identity.
        assert_eq!(v["chains"]["c"]["blocks"]["min_height"], 10);
        assert_eq!(v["chains"]["c"]["network"], "43114");
        assert_eq!(v["chains"]["p"]["blocks"]["min_height"], 700);
        assert_eq!(v["chains"]["p"]["network"], "test-genesis-id");
        // Each chain reports its own data dir, so sizes can't be conflated.
        assert_ne!(
            v["chains"]["c"]["storage"]["data_dir"],
            v["chains"]["p"]["storage"]["data_dir"],
        );
        std::fs::remove_dir_all(&base).ok();
    }

    /// A P-only process has no numeric chain ID, so `chain_id` is null and the
    /// legacy top-level fields describe the one chain that is running.
    #[tokio::test]
    async fn p_only_report_names_itself_the_default() {
        let base = unique_temp_dir("health-ponly");
        let p = chain_serve(Chain::P, &base);
        put_block(&p.storage, 700).await;

        let v = report(&[p]).await;

        assert_eq!(v["default_chain"], "p");
        assert!(
            v["chain_id"].is_null(),
            "the P-chain identity isn't a number"
        );
        assert_eq!(v["blocks"]["min_height"], 700);
        assert!(v["chains"]["c"].is_null());
        std::fs::remove_dir_all(&base).ok();
    }

    /// The reader prefers the per-chain path, so a multi-chain upstream is read
    /// per chain rather than collapsing to whatever the top level holds.
    #[test]
    fn upstream_reader_prefers_the_per_chain_path() {
        let body = json!({
            "blocks": { "min_height": 10 },
            "chains": {
                "c": { "blocks": { "min_height": 10 } },
                "p": { "blocks": { "min_height": 700 } },
            },
        });
        assert_eq!(
            upstream_blocks_field(&body, Chain::C, "min_height"),
            Some(10)
        );
        assert_eq!(
            upstream_blocks_field(&body, Chain::P, "min_height"),
            Some(700)
        );
    }

    /// An older upstream has no `chains` map. Its top-level `blocks` is C-chain
    /// data, so the C-chain falls back to it and the P-chain must NOT — reading
    /// C heights into a P store would anchor the floor at a nonsense height.
    #[test]
    fn upstream_reader_falls_back_only_for_the_c_chain() {
        let legacy = json!({ "blocks": { "min_height": 10 } });
        assert_eq!(
            upstream_blocks_field(&legacy, Chain::C, "min_height"),
            Some(10)
        );
        assert_eq!(upstream_blocks_field(&legacy, Chain::P, "min_height"), None);
    }

    #[test]
    fn upstream_reader_rejects_a_non_neve_body() {
        let v = json!({ "status": "ok" });
        assert_eq!(upstream_blocks_field(&v, Chain::C, "min_height"), None);
    }
}
