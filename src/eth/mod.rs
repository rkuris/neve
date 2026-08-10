//! The C-chain (EVM) pipeline: `newHeads` WebSocket ingest, gap-closing
//! backfill, and the `eth_*` serving dialect over the stored `[block, logs]`
//! records.
//!
//! Sibling to `crate::platform`, which does the same for the P-chain. What the
//! two share — storage, the join buffer, the accept loop, the 421 contract,
//! bulk export, metrics — lives in the crate root, not here.

pub mod backfill;
pub mod ingest;
pub mod rpc;
