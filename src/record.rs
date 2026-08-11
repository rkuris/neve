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
//!
//! # Reading stores written before the combined record
//!
//! Stores predating the logs milestone hold the **bare block object** at each
//! height, with no element array and no `format_version` stamp. Those records
//! are still readable: a JSON object is unambiguously not a JSON array, so
//! [`element`] serves the object as element 0 and reports every *derived*
//! element as **absent**.
//!
//! Absent is deliberately not the same as empty. `[]` means "we ingested this
//! height and it had no logs"; absent means "we never ingested them", which has
//! to reach the client as a 421 so they ask a full node. Conflating the two
//! would turn a missing answer into a wrong one. A store may therefore hold
//! both layouts at once — bare blocks below the upgrade, element arrays above
//! — and both read correctly.

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

/// Was this value written before the combined record existed — i.e. is it a
/// bare block object rather than an element array?
///
/// Stores written before the logs milestone hold the block JSON alone. The two
/// layouts are unambiguous — a record is a JSON *array*, a bare block a JSON
/// *object* — so the first non-whitespace byte decides, with no parsing.
fn is_bare_block(record: &[u8]) -> bool {
    record
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| *b == b'{')
}

/// Is this stored value a combined element record, rather than a
/// pre-combined-record bare block?
///
/// Only a combined record can be handed to a mirror, which needs every element —
/// so a stream that promises records must refuse a legacy value rather than send
/// the bare block and let the far end believe it received a whole record.
pub fn is_combined_record(value: &[u8]) -> bool {
    !is_bare_block(value)
}

/// Borrow element `idx` of a record — a sub-slice, byte-identical to what was
/// stored, with no allocation of the payload.
///
/// `None` means **this record has no such element**, which is different from the
/// element being empty. A pre-combined-record store answers `None` for every
/// derived element, and callers must propagate that as "can't answer" (→ 421,
/// so the client asks a full node) rather than as "nothing there". Reporting an
/// empty logs array for a height whose logs were never ingested would be a
/// wrong answer rather than a missing one.
pub fn element(record: &[u8], idx: usize) -> Result<Option<&[u8]>> {
    if is_bare_block(record) {
        if idx != BLOCK {
            return Ok(None);
        }
        // Validate it really is one JSON value, as the array path does, and
        // hand back the exact stored bytes.
        let block: &RawValue =
            serde_json::from_slice(record).context("decoding stored bare block")?;
        return Ok(Some(block.get().as_bytes()));
    }
    let parts = split(record)?;
    Ok(parts.get(idx).map(|p| p.get().as_bytes()))
}

/// Byte range of element `idx` within `record`, so a caller can hold the whole
/// record alive and hand out the sub-slice with no copy (see [`Element`]).
/// `None` when the record has no such element.
pub fn element_span(record: &[u8], idx: usize) -> Result<Option<Range<usize>>> {
    let Some(part) = element(record, idx)? else {
        return Ok(None);
    };
    // `part` is a sub-slice of `record`; its start is the address delta.
    let start = part.as_ptr().addr().wrapping_sub(record.as_ptr().addr());
    Ok(Some(start..start.wrapping_add(part.len())))
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
    /// Locate element `idx` in a decompressed record, once. `None` when the
    /// record has no such element (see [`element`]); an error only when the
    /// stored bytes aren't decodable at all.
    pub fn at(record: Arc<[u8]>, idx: usize) -> Result<Option<Self>> {
        let Some(span) = element_span(&record, idx)? else {
            return Ok(None);
        };
        Ok(Some(Self { record, span }))
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
        assert_eq!(element(&rec, BLOCK).unwrap(), Some(block.as_slice()));
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
        assert_eq!(element(&rec, BLOCK).unwrap(), Some(block.as_slice()));
        assert_eq!(element(&rec, P_BYTES).unwrap(), Some(bytes.as_slice()));
        assert_eq!(element(&rec, P_REWARDS).unwrap(), Some(rewards.as_slice()));
    }

    #[test]
    fn split_skips_tricky_trailing_elements() {
        // Brackets, commas and escaped quotes inside a later element must not
        // confuse the split: RawValue does a structural scan, not a byte search,
        // so the block element is still recovered exactly.
        let block = br#"{"number":"0x1","extra":"a,b][c"}"#;
        let logs = br#"[{"data":"0x5d2c5b","note":"]],[\"q\""}]"#;
        let rec = encode(&[block, logs]);
        assert_eq!(element(&rec, BLOCK).unwrap(), Some(block.as_slice()));
    }

    /// A malformed value is still an error — tolerating the legacy layout must
    /// not tolerate garbage.
    #[test]
    fn rejects_malformed_records() {
        assert!(element(br"[1]", BLOCK).is_err());
        assert!(element(br"not json at all", BLOCK).is_err());
        assert!(element(br#"{"unterminated": "#, BLOCK).is_err());
    }

    /// An element a record simply doesn't have reads as absent, not as an
    /// error and not as empty.
    #[test]
    fn out_of_range_element_is_absent() {
        let rec = encode(&[br#"{"n":1}"#, EMPTY_ARRAY]);
        assert_eq!(element(&rec, P_REWARDS).unwrap(), None);
    }

    /// The compatibility contract: a store written before the combined record
    /// holds bare block objects. Element 0 is the block, byte-identical; every
    /// derived element is **absent**, which callers turn into a 421 rather than
    /// an empty answer.
    #[test]
    fn bare_block_reads_as_element_zero_with_no_derived_data() {
        let bare = br#"{"number":"0x10","hash":"0xff","transactions":[{"hash":"0x1"}]}"#;
        assert!(is_bare_block(bare));
        assert_eq!(element(bare, BLOCK).unwrap(), Some(bare.as_slice()));
        assert_eq!(element(bare, C_LOGS).unwrap(), None);
        assert_eq!(element(bare, P_REWARDS).unwrap(), None);

        // Leading whitespace doesn't disguise it, and an array is never taken
        // for a bare block.
        assert!(is_bare_block(b"  \n {\"a\":1}"));
        assert!(!is_bare_block(br#"[{"a":1},[]]"#));
        assert!(!is_bare_block(b""));
    }

    /// Both layouts coexist in one store, and each reads correctly — which is
    /// what lets an upgraded store keep its history instead of resyncing.
    #[test]
    fn both_layouts_read_correctly_side_by_side() {
        let bare = br#"{"number":"0x1"}"#;
        let combined = encode(&[br#"{"number":"0x2"}"#, br#"[{"address":"0xa"}]"#]);

        assert_eq!(element(bare, BLOCK).unwrap(), Some(bare.as_slice()));
        assert_eq!(element(bare, C_LOGS).unwrap(), None);
        assert_eq!(
            element(&combined, BLOCK).unwrap(),
            Some(br#"{"number":"0x2"}"#.as_slice()),
        );
        assert_eq!(
            element(&combined, C_LOGS).unwrap(),
            Some(br#"[{"address":"0xa"}]"#.as_slice()),
        );
    }

    /// `Element` keeps the whole record alive and derefs to just its slice, so a
    /// read costs one decompression regardless of which element is wanted.
    #[test]
    fn element_view_derefs_to_its_own_slice() {
        let block = br#"{"id":"abc"}"#;
        let rewards = br#"[{"amount":"5"}]"#;
        let rec: Arc<[u8]> = encode(&[block, br#""0x00""#, rewards]).into();

        assert_eq!(
            Element::at(Arc::clone(&rec), BLOCK)
                .unwrap()
                .unwrap()
                .as_ref(),
            block,
        );
        assert_eq!(
            Element::at(Arc::clone(&rec), P_REWARDS)
                .unwrap()
                .unwrap()
                .as_ref(),
            rewards,
        );
        assert!(Element::at(rec, 9).unwrap().is_none());
    }
}
