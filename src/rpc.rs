//! The shared serving surface: one socket, one accept loop, one HTTP
//! middleware stack, and one JSON-RPC method table — regardless of how many
//! chains the process is mirroring.
//!
//! Chains are distinguished by *method namespace*, not by URL path: `eth_*`
//! resolves against the C-chain store and `platform.*` against the P-chain
//! store, both merged into a single [`jsonrpsee::RpcModule`]. That keeps one
//! connection budget, one `/health`, one `/metrics`, and one `--mirror-from`
//! URL no matter the chain mix, and it means neve stays path-agnostic exactly
//! as it always has been.
//!
//! Per-chain dialects live in `crate::eth` and `crate::platform`; this module
//! owns only what they share.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{Result, bail};
use jsonrpsee::RpcModule;
use jsonrpsee::core::middleware::RpcServiceBuilder;
use jsonrpsee::server::{
    HttpBody, HttpRequest, HttpResponse, Methods, ServerBuilder, ServerConfig, ServerHandle,
    serve_with_graceful_shutdown, stop_channel,
};
use jsonrpsee::types::ErrorObjectOwned;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower::{Layer, Service};
use tracing::{info, warn};

use crate::chain::Chain;
use crate::conn::IdleTimeout;
// `into_rpc` is an extension method the `#[rpc(server)]` macro hangs off the
// generated server trait, so the trait must be in scope to call it.
use crate::eth::rpc::EthApiServer as _;
use crate::join::JoinBuffer;
use crate::storage::Storage;

/// JSON-RPC error code we use for "block not found" — matches geth's `-32000`
/// style (server error range), with a descriptive message.
const BLOCK_NOT_FOUND: i32 = -32000;

pub(crate) fn err(msg: impl Into<String>) -> ErrorObjectOwned {
    ErrorObjectOwned::owned::<()>(BLOCK_NOT_FOUND, msg.into(), None)
}

/// Which subscription kind a subscriber asked for. `newHeads` is the
/// geth-compatible header stream; `newBlocks` is a neve extension that pushes
/// whole blocks (transactions included) so a downstream mirror can persist them
/// without a follow-up fetch. `oldBlocks` is a neve extension that replays a
/// historical range straight from storage. The wire spellings live here —
/// parsed by `from_wire`, rendered by `as_str` (also the metrics `kind` label).
#[derive(Debug)]
pub(crate) enum SubKind {
    NewHeads,
    NewBlocks,
    OldBlocks,
}

impl SubKind {
    /// Parse a `subscribe(kind)` wire token; `None` for unsupported kinds.
    pub(crate) fn from_wire(s: &str) -> Option<Self> {
        match s {
            "newHeads" => Some(Self::NewHeads),
            "newBlocks" => Some(Self::NewBlocks),
            "oldBlocks" => Some(Self::OldBlocks),
            _ => None,
        }
    }

    /// Whether this kind delivers headers (transactions stripped) rather than
    /// whole blocks. Only the live `newHeads` stream strips; `newBlocks` and
    /// the historical `oldBlocks` replay forward whole blocks.
    pub(crate) const fn strips_transactions(&self) -> bool {
        matches!(self, Self::NewHeads)
    }

    /// The wire / metrics-label spelling.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::NewHeads => "newHeads",
            Self::NewBlocks => "newBlocks",
            Self::OldBlocks => "oldBlocks",
        }
    }
}

pub(crate) fn parse_hash(hash: &str) -> Result<[u8; 32], ErrorObjectOwned> {
    let stripped = hash.strip_prefix("0x").unwrap_or(hash);
    let raw = hex::decode(stripped).map_err(|e| err(format!("bad hash: {e}")))?;
    raw.as_slice()
        .try_into()
        .map_err(|_| err("hash must be 32 bytes"))
}

pub(crate) fn parse_quantity(q: &str) -> Result<u64, ErrorObjectOwned> {
    let stripped = q.strip_prefix("0x").unwrap_or(q);
    u64::from_str_radix(stripped, 16).map_err(|_| err(format!("invalid quantity: {q}")))
}

/// One chain instance's serving-side state: the store to read, the network
/// identity to report, and the live fan-out subscribers attach to. `main`
/// builds one per selected chain and hands the set to [`serve`].
#[derive(Clone, Debug)]
pub struct ChainServe {
    pub chain: Chain,
    pub storage: Storage,
    /// This chain's data directory, for the `/health` storage sizing.
    pub data_dir: std::path::PathBuf,
    /// Opaque network fingerprint stamped into the store (decimal `eth_chainId`
    /// on the C-chain, genesis block ID on the P-chain), echoed by `/health`.
    pub identity: String,
    /// Last-known gap to the upstream tip, published by this chain's backfill
    /// loop. 0 means caught up.
    pub behind_tip: Arc<AtomicU64>,
    /// Live-tip fan-out carrying the **full** block; one receiver per subscriber.
    pub blocks: broadcast::Sender<Value>,
    /// In-flight join buffer when this chain's derived-data ingestion is on, so
    /// reads can see a tip record mid-join. `None` when it's off.
    pub join: Option<JoinBuffer>,
}

/// Transport configuration for [`serve`]: where to listen and how to treat
/// connections. The cohesive "how to run the listener" knobs, grouped so the
/// `serve` argument list stays the backing state/dependencies it wires.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    /// Socket address to bind the JSON-RPC / WebSocket server to.
    pub addr: SocketAddr,
    /// Maximum concurrent connections; excess are shed at accept time.
    pub max_connections: u32,
    /// Close a connection idle (no read or write) for this long; `None` disables
    /// the reaper. See [`crate::conn::IdleTimeout`].
    pub idle_timeout: Option<Duration>,
    /// Largest range a single `GET /blocks` bulk export may return.
    pub max_blocks_per_request: u64,
}

/// Tower layer that maps an incoming `hyper::body::Incoming` request to
/// jsonrpsee's `HttpBody` before the rest of the HTTP middleware
/// (health/metrics/421), which is typed against `HttpBody`. This is the one
/// thing jsonrpsee's *private* `TowerToHyperService` does (`req.map(HttpBody::
/// new)`) that the public `serve_with_graceful_shutdown` path (via hyper-util's
/// adapter) does not — so we do it here as the outermost layer, letting the
/// whole service accept the raw `Incoming` body the server hands us.
#[derive(Clone, Debug)]
struct MapBodyLayer;

impl<S> Layer<S> for MapBodyLayer {
    type Service = MapBody<S>;
    fn layer(&self, inner: S) -> Self::Service {
        MapBody { inner }
    }
}

#[derive(Clone, Debug)]
struct MapBody<S> {
    inner: S,
}

impl<S, B> Service<HttpRequest<hyper::body::Incoming>> for MapBody<S>
where
    S: Service<HttpRequest<HttpBody>, Response = HttpResponse<B>>,
{
    type Response = HttpResponse<B>;
    type Error = S::Error;
    // Forward the inner future as-is. The body map is synchronous, so there's no
    // async work to wrap — and `S::Future` is already a boxed future, so wrapping
    // it again would add a heap allocation per request.
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<hyper::body::Incoming>) -> Self::Future {
        self.inner.call(req.map(HttpBody::new))
    }
}

/// Build the merged JSON-RPC method table: one namespace per chain instance,
/// all registered in a single module. Namespaces are disjoint (`eth_*` vs
/// `platform.*`), so a merge conflict here would mean two instances of the same
/// chain — rejected up front by `--chains` deduplication, and by this error if
/// it ever slipped through.
fn build_module(chains: &[ChainServe]) -> Result<RpcModule<()>> {
    let mut module = RpcModule::new(());
    for c in chains {
        match c.chain {
            Chain::C => module.merge(crate::eth::rpc::EthApiImpl::new(c).into_rpc())?,
            Chain::P => module.merge(crate::platform::rpc::module(c)?)?,
        }
    }
    if module.method_names().next().is_none() {
        bail!("no chains selected, so there is nothing to serve");
    }
    Ok(module)
}

pub async fn serve(
    cfg: ServeConfig,
    chains: Vec<ChainServe>,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
) -> Result<ServerHandle> {
    let ServeConfig {
        addr,
        max_connections,
        idle_timeout,
        max_blocks_per_request,
    } = cfg;
    let health_state = crate::health::HealthState::new(&chains);
    // `MapBodyLayer` (outermost) maps the server's raw `Incoming` body to the
    // `HttpBody` the rest of the stack expects. `/blocks`, `/health`, and
    // `/metrics` short-circuit before the 200→421 rewrite (which only concerns
    // JSON-RPC responses) — `/blocks` in particular MUST stay outside it so its
    // streaming body is never buffered for the null-result check.
    let http_mw = tower::ServiceBuilder::new()
        .layer(MapBodyLayer)
        .layer(crate::bulk::BulkBlocksLayer::new(
            &chains,
            max_blocks_per_request,
        ))
        .layer(crate::health::HealthLayer::new(health_state))
        .layer(crate::metrics::MetricsLayer::new(metrics_handle))
        .layer(crate::middleware::NotFound421Layer);
    let module = build_module(&chains)?;
    // Clamp the metrics `method` label to the registered set (else "other").
    let method_names: Arc<[&'static str]> = module.method_names().collect();
    // Per-connection JSON-RPC middleware: records served-call counts, latency,
    // and the open-connection gauge. Sits inside the HTTP middleware, so `/health`
    // and `/metrics` (short-circuited above) never reach it.
    let rpc_mw = RpcServiceBuilder::new().layer_fn(move |service| {
        crate::metrics::RpcMetricsService::new(service, method_names.clone())
    });
    let methods: Methods = module.into();

    // Instead of `ServerBuilder::build(addr).start(..)` (which owns its own
    // accept loop with no HTTP/1.1 idle timeout), take the per-connection
    // `TowerService` factory and drive it under our own accept loop. That lets
    // us wrap each socket in `IdleTimeout` to reap silent connections — the
    // fd-leak / slowloris fix jsonrpsee can't give us — while keeping its
    // strict parsing, every registered method, and WS subscriptions intact.
    let svc_builder = ServerBuilder::default()
        .set_config(
            ServerConfig::builder()
                .max_connections(max_connections)
                .build(),
        )
        .set_http_middleware(http_mw)
        .set_rpc_middleware(rpc_mw)
        .to_service_builder();

    // `server_handle` drives graceful shutdown: dropping it (or calling `.stop()`)
    // trips `stop_handle.shutdown()`, breaking the accept loop and gracefully
    // closing in-flight connections.
    let (stop_handle, server_handle) = stop_channel();

    let listener = TcpListener::bind(addr).await?;
    let actual = listener.local_addr()?;
    info!(%actual, ?idle_timeout, max_connections, "JSON-RPC server listening");

    tokio::spawn(async move {
        loop {
            let sock = tokio::select! {
                res = listener.accept() => match res {
                    Ok((sock, _peer)) => sock,
                    Err(e) => {
                        warn!(error = %e, "JSON-RPC accept failed");
                        continue;
                    }
                },
                () = stop_handle.clone().shutdown() => break,
            };
            // jsonrpsee's own loop sets TCP_NODELAY; match it.
            let _ = sock.set_nodelay(true);
            // `build` mints a fresh service (and connection id) per connection;
            // the shared `conn_guard` inside enforces `max_connections`.
            let service = svc_builder
                .clone()
                .build(methods.clone(), stop_handle.clone());
            // Wrap the socket so a connection idle (no read or write) past
            // `idle_timeout` is closed — the fd-leak / slowloris fix jsonrpsee
            // can't do itself. We then hand it to jsonrpsee's own connection
            // driver (`MapBodyLayer` in the stack does the `Incoming`→`HttpBody`
            // map its public path otherwise skips), which handles WS upgrades and
            // graceful shutdown. Spawn the driver future directly (not wrapped in
            // another `async` block) to avoid a rustc Send-HRTB limitation around
            // hyper-util's connection builder.
            let io = IdleTimeout::new(sock, idle_timeout);
            tokio::spawn(serve_with_graceful_shutdown(
                io,
                service,
                stop_handle.clone().shutdown(),
            ));
        }
    });

    Ok(server_handle)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_quantity_accepts_hex_with_and_without_prefix() {
        assert_eq!(parse_quantity("0x10").unwrap(), 16);
        assert_eq!(parse_quantity("10").unwrap(), 16);
        assert_eq!(parse_quantity("0x0").unwrap(), 0);
        assert!(parse_quantity("0xZZ").is_err());
    }

    #[test]
    fn parse_hash_round_trip() {
        let h = "0x".to_owned() + &"ab".repeat(32);
        let bytes = parse_hash(&h).unwrap();
        assert_eq!(bytes, [0xab; 32]);
        // Wrong length.
        assert!(parse_hash("0xab").is_err());
        // Bad hex.
        assert!(parse_hash("0xZZ").is_err());
    }

    /// A C-chain instance registers the `eth_*` namespace and nothing else, so
    /// a C-only neve can't be probed for P-chain data.
    #[test]
    fn c_chain_module_registers_only_the_eth_namespace() {
        let dir = crate::test_support::unique_temp_dir("rpc-solo");
        let chains = vec![crate::test_support::chain_serve(Chain::C, &dir)];
        let module = build_module(&chains).unwrap();
        let names: Vec<&str> = module.method_names().collect();

        assert!(names.contains(&"eth_blockNumber"), "{names:?}");
        assert!(names.contains(&"eth_getLogs"), "{names:?}");
        assert!(
            !names.iter().any(|n| n.starts_with("platform")),
            "{names:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Both namespaces coexist in one method table: a C+P process answers
    /// `eth_*` and `platform.*` on the same socket, and neither shadows the
    /// other. This is the merged-namespace dispatch contract.
    #[test]
    fn merged_module_registers_both_namespaces() {
        let base = crate::test_support::unique_temp_dir("rpc-merged");
        let chains = vec![
            crate::test_support::chain_serve(Chain::C, &base),
            crate::test_support::chain_serve(Chain::P, &base),
        ];
        let module = build_module(&chains).unwrap();
        let names: Vec<&str> = module.method_names().collect();

        assert!(names.contains(&"eth_blockNumber"), "{names:?}");
        assert!(names.contains(&"platform.getHeight"), "{names:?}");
        assert!(names.contains(&"eth_subscribe"), "{names:?}");
        std::fs::remove_dir_all(&base).ok();
    }

    /// A P-only process registers only the platform namespace.
    #[test]
    fn p_chain_module_registers_only_the_platform_namespace() {
        let dir = crate::test_support::unique_temp_dir("rpc-pchain");
        let chains = vec![crate::test_support::chain_serve(Chain::P, &dir)];
        let module = build_module(&chains).unwrap();
        let names: Vec<&str> = module.method_names().collect();

        assert!(names.contains(&"platform.getBlockByHeight"), "{names:?}");
        assert!(!names.iter().any(|n| n.starts_with("eth")), "{names:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Serving nothing is a configuration error, not a silently empty server.
    #[test]
    fn empty_chain_set_is_refused() {
        assert!(build_module(&[]).is_err());
    }
}
