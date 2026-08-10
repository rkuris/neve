//! On-disk block-record codec.
//!
//! Each blockstore value is a JSON array whose **element `[0]` is always the
//! canonical block JSON**, exactly as the upstream RPC returned it. Keeping the
//! block at a fixed index is what lets every chain-blind reader — by-height,
//! by-hash, `oldBlocks`, bulk export — hand back a block without knowing which
//! chain's record it is holding.
//!
//! Trailing elements are the chain's derived data, fetched separately from the
//! block itself and joined in before the write:
//!
//! | Chain | Record |
//! | --- | --- |
//! | C | `[blockJSON, logs]` — logs in the shape `eth_getLogs` returns |
//! | P | `[blockJSON, blockBytesHex, rewardUTXOs]` — see below |
//!
//! The P-chain stores the block twice on purpose: `platform.getBlock` has both a
//! canonical-bytes encoding and a `json` encoding and clients use both, so
//! storing each verbatim serves either without a codec parser in the read path,
//! and `sha256(bytes)` gives an integrity check the C-chain record never had.
//! See Decision 1 in `docs/p-chain-indexing-plan.md`.
//!
//! A derived element with nothing in it stores [`EMPTY_ARRAY`] — an explicit
//! empty array, never a missing entry — so "nothing was there" and "not
//! ingested yet" have the same shape on disk and adding the feed later needs no
//! migration.
//!
//! Records are built and split by raw byte manipulation rather than
//! `serde_json::Value` round-trips, so no element is ever reserialized and each
//! stays byte-for-byte identical to what the upstream returned.

use std::ops::{Deref, Range};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::value::RawValue;

use crate::chain::Chain;

/// Element index of the canonical block JSON. Fixed at 0 on every chain.
pub const BLOCK: usize = 0;

/// C-chain: that block's logs, in the shape `eth_getLogs` returns.
pub const C_LOGS: usize = 1;

/// P-chain: the block's canonical codec bytes as a `0x`-prefixed hex JSON
/// string (avalanchego's `hexnc` encoding — no trailing checksum).
pub const P_BYTES: usize = 1;

/// The stored value for a derived element with nothing in it: an explicit empty
/// JSON array.
pub const EMPTY_ARRAY: &[u8] = b"[]";

/// How many elements `chain`'s record holds. Checked on write, so a mismatched
/// element count is caught at the call site rather than becoming an unreadable
/// record.
pub const fn arity(chain: Chain) -> usize {
    match chain {
        Chain::C => 2,
        Chain::P => 3,
    }
}

/// Build a record from its already-serialized elements by raw concatenation
/// (`[` ++ e0 ++ `,` ++ e1 ++ … ++ `]`).
///
/// Each element must be exactly one serialized JSON value. Concatenating rather
/// than re-serializing keeps every element byte-identical to what the upstream
/// returned.
pub fn encode(elements: &[&[u8]]) -> Vec<u8> {
    // `wrapping_*` only to dodge the arithmetic-side-effects lint; these are
    // in-memory slice lengths plus a bracket/comma per element — they cannot wrap.
    let cap = elements
        .iter()
        .fold(2usize, |acc, e| acc.wrapping_add(e.len()).wrapping_add(1));
    let mut out = Vec::with_capacity(cap);
    out.push(b'[');
    for (i, e) in elements.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(e);
    }
    out.push(b']');
    out
}

/// Split a record into its elements as borrowed sub-slices, each byte-identical
/// to what was stored. The `Vec` holds only the (2 or 3) element bounds; no
/// element payload is copied or reparsed.
fn split(record: &[u8]) -> Result<Vec<&RawValue>> {
    let parts: Vec<&RawValue> =
        serde_json::from_slice(record).context("decoding stored record as a JSON array")?;
    if parts.len() < 2 {
        bail!(
            "stored record has {} element(s); every record layout has at least \
             a block and one derived element",
            parts.len(),
        );
    }
    Ok(parts)
}

/// Borrow element `idx` of a record — a sub-slice, byte-identical to what was
/// stored, with no allocation of the payload.
pub fn element(record: &[u8], idx: usize) -> Result<&[u8]> {
    let parts = split(record)?;
    let part = parts
        .get(idx)
        .ok_or_else(|| anyhow::anyhow!("stored record has no element [{idx}]"))?;
    Ok(part.get().as_bytes())
}

/// Byte range of element `idx` within `record`, so a caller can hold the whole
/// record alive and hand out the sub-slice with no copy (see [`Element`]).
pub fn element_span(record: &[u8], idx: usize) -> Result<Range<usize>> {
    let part = element(record, idx)?;
    // `part` is a sub-slice of `record`; its start is the address delta.
    let start = part.as_ptr().addr().wrapping_sub(record.as_ptr().addr());
    Ok(start..start.wrapping_add(part.len()))
}

/// One element of a stored record, viewed without copying: owns the
/// decompressed record (the `Arc<[u8]>` from the blockstore's `read_block`) and
/// derefs to just that element. Callers that need owned bytes `.to_vec()` the
/// deref.
#[derive(Clone)]
pub struct Element {
    record: Arc<[u8]>,
    span: Range<usize>,
}

impl Element {
    /// Locate element `idx` in a decompressed record, once. Errors if `record`
    /// isn't a JSON array with that element.
    pub fn at(record: Arc<[u8]>, idx: usize) -> Result<Self> {
        let span = element_span(&record, idx)?;
        Ok(Self { record, span })
    }
}

impl Deref for Element {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // `span` was computed from `record` in `at`, so it is always in range.
        self.record
            .get(self.span.start..self.span.end)
            .unwrap_or_default()
    }
}

impl AsRef<[u8]> for Element {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for Element {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Element")
            .field("record_len", &self.record.len())
            .field("span", &self.span)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn record_is_a_valid_json_array_of_the_right_arity() {
        let block = br#"{"number":"0x1","transactions":[]}"#;
        let logs = br#"[{"address":"0xabc"}]"#;
        let rec = encode(&[block, logs]);
        let v: serde_json::Value = serde_json::from_slice(&rec).unwrap();
        assert_eq!(v.as_array().unwrap().len(), 2);

        // A three-element (P-chain) record round-trips the same way.
        let rec = encode(&[block, br#""0xdeadbeef""#, EMPTY_ARRAY]);
        let v: serde_json::Value = serde_json::from_slice(&rec).unwrap();
        assert_eq!(v.as_array().unwrap().len(), 3);
    }

    #[test]
    fn arity_is_per_chain() {
        assert_eq!(arity(Chain::C), 2);
        assert_eq!(arity(Chain::P), 3);
    }

    #[test]
    fn block_element_is_byte_identical() {
        // Whitespace and key order in the block must survive untouched — the
        // "stored block == upstream RPC response" invariant.
        let block = br#"{ "number":"0x10",  "hash":"0xff" ,"transactions":[ "0x1" ] }"#;
        let rec = encode(&[block, EMPTY_ARRAY]);
        assert_eq!(element(&rec, BLOCK).unwrap(), block);
    }

    /// The P-chain's rewards slot. Named here rather than in the module because
    /// nothing reads it until the fetch-at-ingest rewards feed lands; the layout
    /// itself is already what ingest writes.
    const P_REWARDS: usize = 2;

    #[test]
    fn every_element_is_recovered_byte_identically() {
        let block = br#"{"id":"abc","txs":[]}"#;
        let bytes = br#""0x0000dead""#;
        let rewards = br#"[{"amount":"5"}]"#;
        let rec = encode(&[block, bytes, rewards]);
        assert_eq!(element(&rec, BLOCK).unwrap(), block);
        assert_eq!(element(&rec, P_BYTES).unwrap(), bytes);
        assert_eq!(element(&rec, P_REWARDS).unwrap(), rewards);
    }

    #[test]
    fn split_skips_tricky_trailing_elements() {
        // Brackets, commas and escaped quotes inside a later element must not
        // confuse the split: RawValue does a structural scan, not a byte search,
        // so the block element is still recovered exactly.
        let block = br#"{"number":"0x1","extra":"a,b][c"}"#;
        let logs = br#"[{"data":"0x5d2c5b","note":"]],[\"q\""}]"#;
        let rec = encode(&[block, logs]);
        assert_eq!(element(&rec, BLOCK).unwrap(), block);
    }

    #[test]
    fn rejects_bare_block_and_short_arity() {
        // A bare block object (the pre-record on-disk layout) is not an array
        // and must fail rather than be mis-read as a record.
        assert!(element(br#"{"number":"0x1"}"#, BLOCK).is_err());
        assert!(element(br"[1]", BLOCK).is_err());
    }

    #[test]
    fn rejects_an_out_of_range_element() {
        let rec = encode(&[br#"{"n":1}"#, EMPTY_ARRAY]);
        // A C-chain record has no element [2]; asking for one is an error, not
        // a silently empty read.
        assert!(element(&rec, P_REWARDS).is_err());
    }

    /// `Element` keeps the whole record alive and derefs to just its slice, so a
    /// read costs one decompression regardless of which element is wanted.
    #[test]
    fn element_view_derefs_to_its_own_slice() {
        let block = br#"{"id":"abc"}"#;
        let rewards = br#"[{"amount":"5"}]"#;
        let rec: Arc<[u8]> = encode(&[block, br#""0x00""#, rewards]).into();

        assert_eq!(
            Element::at(Arc::clone(&rec), BLOCK).unwrap().as_ref(),
            block
        );
        assert_eq!(
            Element::at(Arc::clone(&rec), P_REWARDS).unwrap().as_ref(),
            rewards,
        );
        assert!(Element::at(rec, 9).is_err());
    }
}
