//! Block subscriptions, shared by every chain's serving dialect.
//!
//! Nothing here is chain-specific: a subscription either forwards what the live
//! fan-out publishes, or replays a stored height range. The dialects differ only
//! in the method name they register it under (`eth_subscribe` vs
//! `platform.subscribe`) and in which kinds they accept.
//!
//! # Blocks vs. records
//!
//! Two payload shapes, because two audiences want different things:
//!
//! - `newHeads` / `newBlocks` / `oldBlocks` carry the **block** — element 0 of
//!   the stored record, the thing an ordinary stream consumer wants.
//! - `newRecords` / `oldRecords` carry the **whole stored record**, including
//!   the chain's derived elements. That is what a downstream *mirror* needs: a
//!   P-chain mirror fed only block JSON could never serve the hex encodings or
//!   verify a block ID, because the canonical bytes live in element 1.
//!
//! `oldRecords` works on any chain — a stored record is complete by definition.
//! `newRecords` needs the live path to have the finished record in hand when it
//! announces, which is chain-dependent (see [`Chain::publishes_live_records`]).

use std::sync::Arc;

use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::server::PendingSubscriptionSink;
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::chain::Chain;
use crate::metrics::SubMetricsGuard;
use crate::rpc::err;
use crate::storage::Storage;

/// One freshly-ingested block as published to live subscribers.
///
/// `record` is `Some` only when the ingest path had the *complete* record
/// durable at announce time. The C-chain deliberately announces a tip block
/// before its logs are joined — so the block is queryable without waiting on the
/// logs round-trip — and therefore publishes `None` there.
#[derive(Debug)]
pub(crate) struct LiveUpdate {
    /// The canonical block JSON (record element 0).
    pub block: Value,
    /// The whole stored record, when it was available.
    pub record: Option<Value>,
}

/// The live fan-out handle. `Arc` so a block is cloned once, not once per
/// subscriber.
pub(crate) type LiveTx = broadcast::Sender<Arc<LiveUpdate>>;

/// Capacity of a chain's live fan-out ring. ~minutes of tail at C-chain block
/// rate; a subscriber slower than that gets `Lagged` and resumes from the tip
/// rather than back-pressuring ingest.
pub(crate) const LIVE_CHANNEL_CAP: usize = 1024;

/// Which subscription kind a subscriber asked for. The wire spellings live here
/// — parsed by [`Self::from_wire`], rendered by [`Self::as_str`] (also the
/// metrics `kind` label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubKind {
    /// Live block headers, transactions stripped (geth-compatible). C-chain
    /// only — a P-chain block has no header/body split.
    NewHeads,
    /// Live whole blocks — a neve extension, so a consumer gets the block
    /// without a follow-up fetch.
    NewBlocks,
    /// Live whole *records* — a neve extension for downstream mirrors.
    NewRecords,
    /// Historical block replay from storage.
    OldBlocks,
    /// Historical *record* replay from storage — a mirror's bootstrap.
    OldRecords,
}

impl SubKind {
    /// Parse a `subscribe(kind)` wire token; `None` for unsupported kinds.
    pub(crate) fn from_wire(s: &str) -> Option<Self> {
        match s {
            "newHeads" => Some(Self::NewHeads),
            "newBlocks" => Some(Self::NewBlocks),
            "newRecords" => Some(Self::NewRecords),
            "oldBlocks" => Some(Self::OldBlocks),
            "oldRecords" => Some(Self::OldRecords),
            _ => None,
        }
    }

    /// The wire / metrics-label spelling.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NewHeads => "newHeads",
            Self::NewBlocks => "newBlocks",
            Self::NewRecords => "newRecords",
            Self::OldBlocks => "oldBlocks",
            Self::OldRecords => "oldRecords",
        }
    }

    /// Whether this kind streams the live tip (rather than replaying storage).
    pub(crate) const fn is_live(self) -> bool {
        matches!(self, Self::NewHeads | Self::NewBlocks | Self::NewRecords)
    }

    /// Whether this kind carries the whole stored record rather than the block.
    pub(crate) const fn wants_record(self) -> bool {
        matches!(self, Self::NewRecords | Self::OldRecords)
    }

    /// Whether this kind delivers headers (transactions stripped) rather than
    /// whole blocks.
    const fn strips_transactions(self) -> bool {
        matches!(self, Self::NewHeads)
    }
}

/// What a subscriber asked for, once the dialect has parsed it out of its own
/// param shape (positional hex on the C-chain, a named object on the P-chain).
#[derive(Debug, Clone, Copy)]
pub(crate) struct SubRequest {
    pub kind: SubKind,
    /// Inclusive start, for the replay kinds.
    pub from: Option<u64>,
    /// Inclusive end; `None` follows the contiguous tip and then completes.
    pub to: Option<u64>,
}

/// Serve one subscription: rejects kinds this chain can't back, then dispatches
/// to the live fan-out or the storage replay.
pub(crate) async fn serve(
    chain: Chain,
    storage: &Storage,
    blocks: &LiveTx,
    pending: PendingSubscriptionSink,
    req: SubRequest,
    allowed: &[SubKind],
) -> SubscriptionResult {
    let SubRequest { kind, from, to } = req;
    if !allowed.contains(&kind) {
        pending
            .reject(err(format!(
                "subscription kind {} is not available on the {} chain",
                kind.as_str(),
                chain.as_str(),
            )))
            .await;
        return Ok(());
    }
    // The dialect may name a kind its chain can't currently deliver. Say why,
    // and point at the one that does work — a mirror hitting this needs to know
    // `oldRecords` is still available to it.
    if kind == SubKind::NewRecords && !chain.publishes_live_records() {
        pending
            .reject(err(format!(
                "the {} chain announces a block before its derived data is joined, so it \
                 has no complete record to push live; use oldRecords to replay stored \
                 records, or newBlocks for the live block",
                chain.as_str(),
            )))
            .await;
        return Ok(());
    }
    if kind.is_live() {
        serve_live(chain, blocks, pending, kind).await
    } else {
        serve_range(chain, storage, pending, kind, from, to).await
    }
}

/// Live-tip subscription: forward each freshly-ingested block off the broadcast
/// channel until the client goes away, projected for the requested kind.
async fn serve_live(
    chain: Chain,
    blocks: &LiveTx,
    pending: PendingSubscriptionSink,
    kind: SubKind,
) -> SubscriptionResult {
    let label = kind.as_str();
    // subscribe() BEFORE accept() so we don't miss a block produced in the gap
    // between the two awaits.
    let mut rx = blocks.subscribe();
    let sink = pending.accept().await?;
    let metrics = SubMetricsGuard::new(chain, kind);
    loop {
        tokio::select! {
            // Client disconnected / called unsubscribe.
            () = sink.closed() => break,
            recv = rx.recv() => match recv {
                Ok(update) => {
                    let Some(payload) = project(&update, kind) else {
                        // Only reachable if a chain advertised newRecords but
                        // published none; skip rather than close the stream.
                        debug!(kind = label, "live update carried no payload for this kind");
                        continue;
                    };
                    let msg = serde_json::value::to_raw_value(&payload)?;
                    let sent_bytes = msg.get().len() as u64;
                    if let Err(e) = sink.send(msg).await {
                        debug!(kind = label, error = %e, "subscriber send failed; closing subscription");
                        break;
                    }
                    metrics.sent_bytes(sent_bytes);
                }
                // Slow consumer fell behind the ring buffer. Drop the gap and
                // resume from the live tip — this is not a gapless feed anyway
                // (that's what backfill and oldBlocks are for).
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    metrics.lagged(n);
                    warn!(kind = label, skipped = n, "subscriber lagged");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
    Ok(())
}

/// Project a live update into the payload one kind wants. `None` when the
/// update doesn't carry what the kind needs.
fn project(update: &LiveUpdate, kind: SubKind) -> Option<Value> {
    if kind.wants_record() {
        return update.record.clone();
    }
    let mut block = update.block.clone();
    if kind.strips_transactions()
        && let Some(obj) = block.as_object_mut()
    {
        obj.remove("transactions");
    }
    Some(block)
}

/// Historical range replay. Streams `[start..=end]` straight from storage with
/// natural backpressure (`sink.send().await` awaits a full buffer). `end ==
/// None` follows the contiguous tip as it advances (re-read each pass) and
/// completes once the cursor catches it — the mirror's bootstrap-done signal.
///
/// Anything we cannot serve gaplessly is refused at subscribe time (`start`
/// below our earliest block, or an explicit `end` past the contiguous tip), so
/// the loop never hits a hole: `min_height` is stable and `max_contiguous` only
/// grows, so a range that validates here stays valid for the whole stream.
async fn serve_range(
    chain: Chain,
    storage: &Storage,
    pending: PendingSubscriptionSink,
    kind: SubKind,
    from: Option<u64>,
    to: Option<u64>,
) -> SubscriptionResult {
    let Some(start) = from else {
        pending
            .reject(err(format!(
                "{} requires a 'from' block number",
                kind.as_str()
            )))
            .await;
        return Ok(());
    };

    // Refuse requests we can't satisfy gaplessly.
    let min = storage.min_height().await;
    let contig = storage.max_contiguous_height().await;
    if storage.is_empty().await {
        pending.reject(err("no blocks stored yet")).await;
        return Ok(());
    }
    if start < min {
        pending
            .reject(err(format!(
                "start {start} before earliest stored block {min}"
            )))
            .await;
        return Ok(());
    }
    if let Some(e) = to {
        if e < start {
            pending
                .reject(err(format!("end {e} before start {start}")))
                .await;
            return Ok(());
        }
        if e > contig {
            pending
                .reject(err(format!("end {e} beyond contiguous tip {contig}")))
                .await;
            return Ok(());
        }
    }

    let sink = pending.accept().await?;
    let metrics = SubMetricsGuard::new(chain, kind);
    let whole_record = kind.wants_record();
    let mut h = start;
    loop {
        // Open-ended streams follow the contiguous tip as it advances; a fixed
        // `end` was already validated against it at subscribe time.
        let target = match to {
            Some(e) => e,
            None => storage.max_contiguous_height().await,
        };
        if h > target {
            break; // caught up to the tip → range exhausted, close the sink
        }
        // Stored bytes are already-serialized JSON, so they go out without a
        // parse+reserialize round-trip.
        let stored = if whole_record {
            storage.get_record(h).await.map(|o| o.map(|a| a.to_vec()))
        } else {
            storage
                .get_by_height(h)
                .await
                .map(|o| o.map(|b| b.as_ref().to_vec()))
        };
        let bytes = match stored {
            Ok(Some(b)) => b,
            // Gapless by construction; never spin on a surprise hole.
            Ok(None) => break,
            Err(e) => {
                debug!(height = h, error = %e, "range replay storage read failed; closing");
                break;
            }
        };
        let msg = match String::from_utf8(bytes)
            .map_err(|e| e.to_string())
            .and_then(|s| serde_json::value::RawValue::from_string(s).map_err(|e| e.to_string()))
        {
            Ok(m) => m,
            Err(e) => {
                debug!(height = h, error = %e, "stored record decode failed; closing");
                break;
            }
        };
        let sent_bytes = msg.get().len() as u64;
        if let Err(e) = sink.send(msg).await {
            debug!(height = h, error = %e, "range replay send failed; closing subscription");
            break;
        }
        metrics.sent_bytes(sent_bytes);
        h = h.saturating_add(1);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use serde_json::json;

    fn update() -> LiveUpdate {
        LiveUpdate {
            block: json!({"number": "0x1", "hash": "0xaa", "transactions": [{"hash": "0x11"}]}),
            record: Some(json!([{"number": "0x1"}, []])),
        }
    }

    #[test]
    fn wire_spellings_round_trip() {
        for kind in [
            SubKind::NewHeads,
            SubKind::NewBlocks,
            SubKind::NewRecords,
            SubKind::OldBlocks,
            SubKind::OldRecords,
        ] {
            assert_eq!(SubKind::from_wire(kind.as_str()), Some(kind));
        }
        // Kinds our store can't back stay unparseable.
        assert_eq!(SubKind::from_wire("logs"), None);
        assert_eq!(SubKind::from_wire("newPendingTransactions"), None);
    }

    #[test]
    fn live_and_record_kinds_are_classified() {
        assert!(SubKind::NewBlocks.is_live());
        assert!(SubKind::NewRecords.is_live());
        assert!(!SubKind::OldRecords.is_live());

        assert!(SubKind::NewRecords.wants_record());
        assert!(SubKind::OldRecords.wants_record());
        assert!(!SubKind::NewBlocks.wants_record());
        assert!(!SubKind::OldBlocks.wants_record());
    }

    /// `newHeads` strips transactions; `newBlocks` keeps them; `newRecords`
    /// forwards the whole record array — the three payload shapes one live
    /// update has to serve.
    #[test]
    fn projection_shapes_the_payload_per_kind() {
        let u = update();

        let head = project(&u, SubKind::NewHeads).unwrap();
        assert_eq!(head["number"], "0x1");
        assert!(
            head.get("transactions").is_none(),
            "newHeads must strip txs"
        );

        let block = project(&u, SubKind::NewBlocks).unwrap();
        assert_eq!(block["transactions"].as_array().unwrap().len(), 1);

        let rec = project(&u, SubKind::NewRecords).unwrap();
        assert!(rec.is_array(), "newRecords must carry the record array");
        assert_eq!(rec.as_array().unwrap().len(), 2);
    }

    /// Stripping must not mutate the shared update — the next subscriber still
    /// gets a whole block.
    #[test]
    fn projection_does_not_mutate_the_shared_update() {
        let u = update();
        let _ = project(&u, SubKind::NewHeads).unwrap();
        let block = project(&u, SubKind::NewBlocks).unwrap();
        assert!(
            block.get("transactions").is_some(),
            "the update must survive a newHeads projection intact",
        );
    }

    /// A chain whose live path can't publish complete records yields nothing
    /// for `newRecords`, rather than a half-built payload.
    #[test]
    fn record_projection_is_none_without_a_record() {
        let u = LiveUpdate {
            block: json!({"number": "0x1"}),
            record: None,
        };
        assert!(project(&u, SubKind::NewRecords).is_none());
        assert!(project(&u, SubKind::NewBlocks).is_some());
    }
}
