//! On-disk block-record codec.
//!
//! Each blockstore value is a combined `[block, logs]` JSON array: element
//! `[0]` is the block exactly as `eth_getBlockByNumber` returns it with full
//! transactions (kept byte-identical, so the existing "stored block == RPC
//! response" invariant holds), and element `[1]` is that block's logs as a JSON
//! array in the shape `eth_getLogs` returns. Heights with no logs store
//! `[block, []]` — an explicit empty array, never a missing entry.
//!
//! The record is built and split by raw byte manipulation rather than
//! `serde_json::Value` round-trips, so the block half is never reserialized and
//! stays byte-for-byte identical to what the upstream returned. See
//! `docs/neve-logs-ingestion-plan.md` for the rationale.

use std::ops::{Deref, Range};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::value::RawValue;

/// The logs half stored for a height that carries no logs — and, until log
/// ingestion is wired up, for every height: an explicit empty JSON array.
pub const EMPTY_LOGS: &[u8] = b"[]";

/// Build a combined `[block, logs]` record from its two already-serialized
/// halves by raw concatenation (`[` ++ block ++ `,` ++ logs ++ `]`).
///
/// `block` must be one serialized JSON value (the block object) and `logs` one
/// serialized JSON value (the logs array). Concatenating rather than
/// re-serializing keeps the block half byte-identical to the upstream response.
pub fn encode(block: &[u8], logs: &[u8]) -> Vec<u8> {
    // `wrapping_add` only to dodge the arithmetic-side-effects lint; these are
    // in-memory slice lengths plus 3 (two brackets, comma) — they cannot wrap.
    let cap = block.len().wrapping_add(logs.len()).wrapping_add(3);
    let mut out = Vec::with_capacity(cap);
    out.push(b'[');
    out.extend_from_slice(block);
    out.push(b',');
    out.extend_from_slice(logs);
    out.push(b']');
    out
}

/// Borrow the block half (`[0]`) of a combined record — a sub-slice of `record`,
/// byte-identical to what was stored, with no allocation. The logs half is
/// validated (the record must be a two-element array) but not returned.
pub fn block_half(record: &[u8]) -> Result<&[u8]> {
    let (block, _logs): (&RawValue, &RawValue) =
        serde_json::from_slice(record).context("decoding combined [block, logs] record")?;
    Ok(block.get().as_bytes())
}

/// Borrow the logs half (`[1]`) of a combined record — a sub-slice of `record`,
/// byte-identical to what was stored (the JSON array `eth_getLogs` serves), with
/// no allocation.
pub fn logs_half(record: &[u8]) -> Result<&[u8]> {
    let (_block, logs): (&RawValue, &RawValue) =
        serde_json::from_slice(record).context("decoding combined [block, logs] record")?;
    Ok(logs.get().as_bytes())
}

/// Byte range of the block half (`[0]`) within a combined `record`, so a caller
/// can hold the whole record alive and hand out the block sub-slice with no copy
/// (see [`BlockBytes`]).
pub fn block_span(record: &[u8]) -> Result<Range<usize>> {
    let block = block_half(record)?;
    // `block` is a sub-slice of `record`; its start is the address delta.
    let start = block.as_ptr().addr().wrapping_sub(record.as_ptr().addr());
    Ok(start..start.wrapping_add(block.len()))
}

/// A stored block's bytes, viewed without copying: owns the decompressed
/// `[block, logs]` record (the `Arc<[u8]>` from the blockstore's `read_block`)
/// and derefs to just the block half (`[0]`). Callers that need owned bytes
/// `.to_vec()` the deref.
#[derive(Clone)]
pub struct BlockBytes {
    record: Arc<[u8]>,
    block: Range<usize>,
}

impl BlockBytes {
    /// Wrap a decompressed combined record, locating the block half once. Errors
    /// if `record` is not a two-element `[block, logs]` array.
    pub fn new(record: Arc<[u8]>) -> Result<Self> {
        let block = block_span(&record)?;
        Ok(Self { record, block })
    }
}

impl Deref for BlockBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // `block` was computed from `record` in `new`, so it is always in range.
        self.record
            .get(self.block.start..self.block.end)
            .unwrap_or_default()
    }
}

impl AsRef<[u8]> for BlockBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for BlockBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockBytes")
            .field("record_len", &self.record.len())
            .field("block", &self.block)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn record_is_valid_two_element_json_array() {
        let block = br#"{"number":"0x1","transactions":[]}"#;
        let logs = br#"[{"address":"0xabc"}]"#;
        let rec = encode(block, logs);
        let v: serde_json::Value = serde_json::from_slice(&rec).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn block_half_is_byte_identical() {
        // Whitespace and key order in the block must survive untouched — the
        // "stored block == upstream RPC response" invariant.
        let block = br#"{ "number":"0x10",  "hash":"0xff" ,"transactions":[ "0x1" ] }"#;
        let rec = encode(block, EMPTY_LOGS);
        assert_eq!(block_half(&rec).unwrap(), block);
    }

    #[test]
    fn block_half_skips_tricky_logs() {
        // Brackets, commas and escaped quotes inside the logs half must not
        // confuse the split: RawValue does a structural scan, not a byte search,
        // so the block half is still recovered exactly.
        let block = br#"{"number":"0x1","extra":"a,b][c"}"#;
        let logs = br#"[{"data":"0x5d2c5b","note":"]],[\"q\""}]"#;
        let rec = encode(block, logs);
        assert_eq!(block_half(&rec).unwrap(), block);
    }

    #[test]
    fn block_half_rejects_bare_block() {
        // A bare block object (the pre-logs on-disk layout) is not a two-element
        // array and must fail rather than be mis-read as a combined record.
        assert!(block_half(br#"{"number":"0x1"}"#).is_err());
    }

    #[test]
    fn block_half_rejects_wrong_arity() {
        assert!(block_half(br"[1]").is_err());
        assert!(block_half(br"[1,2,3]").is_err());
    }
}
