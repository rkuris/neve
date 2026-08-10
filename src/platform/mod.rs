//! The P-chain (platform) pipeline: polling ingest and the `platform.*` serving
//! dialect over the stored `[blockJSON, blockBytesHex, rewards]` records.
//!
//! Sibling to `crate::eth`. The shape differs from the C-chain in three ways
//! that the plan predicted and this module absorbs:
//!
//! - **No push.** There is no `eth_subscribe` analog for P-chain blocks at all,
//!   so ingest polls `platform.getHeight`. With no push there is also no
//!   live-vs-backfill split: one loop walks the frontier to the tip.
//! - **No reorgs.** Accepted blocks are final and heights contiguous, so the
//!   C-chain's best-effort hash check has no counterpart — a missing height is
//!   only ever "not fetched yet".
//! - **Two encodings.** Clients use both `platform.getBlock`'s canonical-bytes
//!   and `json` encodings, so both are stored verbatim; the bytes make the record
//!   self-verifying (`blockID == cb58(sha256(bytes))`) and keep a codec parser
//!   out of the critical path entirely.
//!
//! See `docs/p-chain-indexing-plan.md` for the reasoning and the phase plan.

pub mod codec;
pub mod ingest;
pub mod rpc;

use serde_json::Value;

/// The transactions in a P-chain block, in order.
///
/// Three shapes exist on the wire and a from-genesis mirror holds all of them:
/// post-Banff blocks carry a **`txs` array**; Apricot-era blocks carry a
/// **single `tx` object** instead; and commit/abort blocks carry **neither**
/// (their outcome is which block was accepted, not a transaction). Reading only
/// `txs` would silently fail to index the entire pre-Banff chain, so both
/// spellings are handled here, once, for the ingest and serving paths to share.
pub(crate) fn block_txs(block: &Value) -> Vec<&Value> {
    if let Some(txs) = block.get("txs").and_then(Value::as_array) {
        return txs.iter().collect();
    }
    block.get("tx").map_or_else(Vec::new, |tx| vec![tx])
}

/// Take the `idx`-th transaction out of a block, for either shape. The
/// single-`tx` form has only index 0.
pub(crate) fn take_nth_tx(block: &mut Value, idx: usize) -> Option<Value> {
    if let Some(txs) = block.get_mut("txs").and_then(Value::as_array_mut) {
        return (idx < txs.len()).then(|| txs.swap_remove(idx));
    }
    if idx != 0 {
        return None;
    }
    block.get_mut("tx").map(Value::take)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use serde_json::json;

    /// All three real block shapes, as observed on Fuji from genesis: a
    /// post-Banff `txs` array (height 292000), an Apricot single `tx` (height 1),
    /// and a commit/abort block with neither (height 3).
    #[test]
    fn block_txs_reads_every_wire_shape() {
        let banff = json!({ "txs": [{"id": "a"}, {"id": "b"}] });
        assert_eq!(block_txs(&banff).len(), 2);
        assert_eq!(block_txs(&banff)[1]["id"], "b");

        let apricot = json!({ "tx": {"id": "solo"} });
        assert_eq!(block_txs(&apricot).len(), 1);
        assert_eq!(block_txs(&apricot)[0]["id"], "solo");

        // Commit / abort: no transactions at all, not an error.
        assert!(block_txs(&json!({ "height": 3 })).is_empty());
        assert!(block_txs(&json!({ "txs": [] })).is_empty());
    }

    #[test]
    fn take_nth_tx_indexes_every_wire_shape() {
        let mut banff = json!({ "txs": [{"id": "a"}, {"id": "b"}] });
        assert_eq!(take_nth_tx(&mut banff, 1).unwrap()["id"], "b");
        assert!(take_nth_tx(&mut json!({ "txs": [{"id": "a"}] }), 1).is_none());

        // The single-`tx` form is reachable at index 0 and nowhere else.
        let mut apricot = json!({ "tx": {"id": "solo"} });
        assert_eq!(take_nth_tx(&mut apricot, 0).unwrap()["id"], "solo");
        assert!(take_nth_tx(&mut json!({ "tx": {"id": "solo"} }), 1).is_none());

        assert!(take_nth_tx(&mut json!({ "height": 3 }), 0).is_none());
    }
}
