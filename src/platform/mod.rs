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
pub mod mirror;
pub mod rpc;

use serde_json::Value;

/// The transactions in a P-chain block, in order.
///
/// The two spellings are **not** mutually exclusive, which is the trap here.
/// Post-Banff blocks carry a `txs` array; Apricot-era blocks carry a single `tx`
/// object; commit/abort blocks carry neither (their outcome is which block was
/// accepted, not a transaction) — and a **Banff proposal block carries both**:
/// its standard transactions in `txs` and the proposal transaction, typically a
/// `RewardValidatorTx`, in `tx`. Mainnet height 25345668 is `txs: []` plus a
/// `tx`, so keying off `txs` alone drops the proposal transaction entirely and
/// every staking reward on the chain goes unindexed.
///
/// So both are read, `txs` first and the singular `tx` appended after it. That
/// ordering defines the tx index used by `tx_to_block` and [`take_nth_tx`]: when
/// `txs` is absent the singular form lands at index 0, which is what the
/// Apricot-era store already recorded.
pub(crate) fn block_txs(block: &Value) -> Vec<&Value> {
    let mut txs: Vec<&Value> = block
        .get("txs")
        .and_then(Value::as_array)
        .map_or_else(Vec::new, |arr| arr.iter().collect());
    txs.extend(block.get("tx"));
    txs
}

/// Take the `idx`-th transaction out of a block, in the same index space
/// [`block_txs`] defines: the `txs` array first, then the singular `tx`.
pub(crate) fn take_nth_tx(block: &mut Value, idx: usize) -> Option<Value> {
    let len = block
        .get("txs")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if idx < len {
        // swap_remove reorders the array, but the block value is discarded
        // after one take — callers slice a single tx out to serve `getTx`.
        return block
            .get_mut("txs")
            .and_then(Value::as_array_mut)
            .map(|arr| arr.swap_remove(idx));
    }
    if idx != len {
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

    /// A Banff **proposal** block carries both spellings, and the empty-`txs`
    /// form is the common one: mainnet height 25345668 is `txs: []` plus a
    /// `RewardValidatorTx` in `tx`. Reading only `txs` left every staking reward
    /// on the chain out of `tx_to_block`.
    #[test]
    fn block_txs_includes_the_proposal_tx_alongside_txs() {
        let empty_txs_plus_proposal = json!({ "txs": [], "tx": {"id": "reward"} });
        assert_eq!(block_txs(&empty_txs_plus_proposal).len(), 1);
        assert_eq!(block_txs(&empty_txs_plus_proposal)[0]["id"], "reward");

        // Both non-empty: `txs` first, the proposal tx appended after.
        let both = json!({ "txs": [{"id": "a"}, {"id": "b"}], "tx": {"id": "reward"} });
        let got = block_txs(&both);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0]["id"], "a");
        assert_eq!(got[1]["id"], "b");
        assert_eq!(got[2]["id"], "reward");
    }

    /// `take_nth_tx` must share `block_txs`'s index space, or `tx_to_block`
    /// records an index that `getTx` then resolves to a different transaction.
    #[test]
    fn take_nth_tx_agrees_with_block_txs_on_the_union() {
        let both = json!({ "txs": [{"id": "a"}, {"id": "b"}], "tx": {"id": "reward"} });
        for (idx, want) in ["a", "b", "reward"].iter().enumerate() {
            let listed = block_txs(&both)[idx]["id"].clone();
            let taken = take_nth_tx(&mut both.clone(), idx).unwrap()["id"].clone();
            assert_eq!(listed, *want);
            assert_eq!(taken, *want, "index {idx} disagrees");
        }
        assert!(take_nth_tx(&mut both.clone(), 3).is_none());

        // Empty `txs` plus a proposal tx: reachable at index 0, nowhere else.
        let mut proposal = json!({ "txs": [], "tx": {"id": "reward"} });
        assert_eq!(take_nth_tx(&mut proposal, 0).unwrap()["id"], "reward");
        assert!(take_nth_tx(&mut json!({ "txs": [], "tx": {"id": "r"} }), 1).is_none());
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
