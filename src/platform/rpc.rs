//! The P-chain serving dialect: Tier-0 `platform.*` JSON-RPC over the stored
//! `[blockJSON, blockBytesHex, rewards]` records.
//!
//! Three conventions differ from the eth dialect and are handled here rather
//! than leaking outward: methods are namespaced with a **dot**
//! (`platform.getHeight`), params arrive as a **named object** with camelCase
//! keys (`blockID`, `txID`) instead of a positional array, and unsigned numbers
//! travel as **strings**. That is why methods are registered by hand with serde
//! param structs rather than through the `#[rpc]` macro: the macro derives JSON
//! keys from Rust parameter names, which can't be camelCase without fighting the
//! language, and it has no way to accept avalanchego's number-or-string ints.
//!
//! The 421 contract is the same as everywhere else: anything this store cannot
//! authoritatively answer returns `Ok(None)`, which serializes to `result: null`
//! and is rewritten to HTTP 421 so the fronting pool retries against a real
//! node. Note this differs from avalanchego, which answers an unknown height
//! with a JSON-RPC *error* — for a mirror, "ask someone else" is the correct
//! answer, and an error would be indistinguishable from a real failure.
//!
//! What Tier 0 deliberately does not serve (`getTx` byte encodings, everything
//! needing state replay) also returns `None`, so the pool absorbs it exactly as
//! it absorbs `eth_call` today. See `docs/p-chain-indexing-plan.md`.

use anyhow::Result;
use jsonrpsee::RpcModule;
use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::server::PendingSubscriptionSink;
use jsonrpsee::types::{ErrorObjectOwned, Params};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;

use crate::chain::Chain;
use crate::platform::codec::{self, Encoding};
use crate::record;
use crate::rpc::{ChainServe, err};
use crate::storage::Storage;
use crate::subscribe::{self, LiveTx, SubKind};

/// Subscription kinds the P-chain dialect serves.
///
/// No `newHeads`: a P-chain block has no header/body split, so there is nothing
/// to strip and the geth-shaped kind would be a lie. `newRecords` **is** offered
/// here (unlike the C-chain) because the P ingest path writes the complete
/// record before it announces — see [`Chain::publishes_live_records`].
const PLATFORM_SUB_KINDS: &[SubKind] = &[
    SubKind::NewBlocks,
    SubKind::NewRecords,
    SubKind::OldBlocks,
    SubKind::OldRecords,
];

/// `{ kind, from?, to? }` — `platform.subscribe`. Heights are avalanchego
/// unsigned integers (string or number), not eth's hex quantities.
#[derive(Deserialize, Debug)]
struct SubscribeParams {
    kind: String,
    #[serde(default)]
    from: Option<Uint64>,
    #[serde(default)]
    to: Option<Uint64>,
}

/// An avalanchego unsigned integer. Serialized as a string, but its `json.Uint64`
/// also accepts a JSON number — accept both, so which one a drop-in client sends
/// can't break it.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum Uint64 {
    Num(u64),
    Str(String),
}

impl Uint64 {
    fn get(&self) -> Result<u64, ErrorObjectOwned> {
        match self {
            Self::Num(n) => Ok(*n),
            Self::Str(s) => s
                .parse()
                .map_err(|_| err(format!("expected an unsigned integer, got {s:?}"))),
        }
    }
}

/// `{ height, encoding? }` — `platform.getBlockByHeight`.
#[derive(Deserialize, Debug)]
struct ByHeight {
    height: Uint64,
    #[serde(default)]
    encoding: Option<String>,
}

/// `{ blockID, encoding? }` — `platform.getBlock`.
#[derive(Deserialize, Debug)]
struct ByBlockId {
    #[serde(rename = "blockID")]
    block_id: String,
    #[serde(default)]
    encoding: Option<String>,
}

/// `{ txID, encoding? }` — `platform.getTx` and `platform.getTxStatus`.
#[derive(Deserialize, Debug)]
struct ByTxId {
    #[serde(rename = "txID")]
    tx_id: String,
    #[serde(default)]
    encoding: Option<String>,
}

/// Deserialize a method's params object, mapping a shape error to a JSON-RPC
/// error rather than a 421 — the request itself is malformed, and upstream would
/// reject it too.
fn parse_params<'a, T: Deserialize<'a>>(params: &'a Params<'a>) -> Result<T, ErrorObjectOwned> {
    params.parse().map_err(|e| err(format!("bad params: {e}")))
}

pub struct PlatformApi {
    storage: Storage,
    /// Live-tip fan-out; one receiver per subscriber.
    blocks: LiveTx,
}

impl PlatformApi {
    /// Build the `platform.*` service over a P-chain instance.
    pub fn new(c: &ChainServe) -> Self {
        debug_assert_eq!(
            c.chain,
            Chain::P,
            "platform dialect requires a P-chain instance",
        );
        Self {
            storage: c.storage.clone(),
            blocks: c.blocks.clone(),
        }
    }

    /// `platform.subscribe(kind, from?, to?)` — the P-chain block stream.
    ///
    /// avalanchego has no push mechanism for P-chain blocks at all, so this is
    /// a neve extension with no upstream counterpart rather than a mirror of
    /// one. `newBlocks` and `oldBlocks` carry blocks; `newRecords` and
    /// `oldRecords` carry the whole stored record, which is what a downstream
    /// mirror needs (the canonical bytes live in element 1, so a block-only
    /// feed could never reproduce the hex encodings).
    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        p: SubscribeParams,
    ) -> SubscriptionResult {
        let Some(kind) = SubKind::from_wire(&p.kind) else {
            pending
                .reject(err(format!("unsupported subscription kind: {}", p.kind)))
                .await;
            return Ok(());
        };
        let (from, to) = match (
            p.from.as_ref().map(Uint64::get).transpose(),
            p.to.as_ref().map(Uint64::get).transpose(),
        ) {
            (Ok(f), Ok(t)) => (f, t),
            (Err(e), _) | (_, Err(e)) => {
                pending.reject(e).await;
                return Ok(());
            }
        };
        let req = subscribe::SubRequest { kind, from, to };
        subscribe::serve(
            Chain::P,
            &self.storage,
            &self.blocks,
            pending,
            req,
            PLATFORM_SUB_KINDS,
        )
        .await
    }

    /// Our contiguous tip, or `None` when nothing is stored yet. Checked via
    /// emptiness rather than `> 0`, because 0 is a legitimate height (genesis).
    async fn tip(&self) -> Option<u64> {
        if self.storage.is_empty().await {
            return None;
        }
        Some(self.storage.max_contiguous_height().await)
    }

    /// `platform.getHeight` — our contiguous tip, the height below which every
    /// block is present. The analog of `eth_blockNumber`, except it reports the
    /// *contiguous* frontier rather than the high-water mark: P-chain heights
    /// arrive in order so the two coincide, and the frontier can never advertise
    /// a height whose predecessors are missing.
    async fn get_height(&self) -> Result<Option<Value>, ErrorObjectOwned> {
        Ok(self.tip().await.map(|h| json!({ "height": h.to_string() })))
    }

    /// `platform.getTimestamp` — the chain time of the block at our tip. This is
    /// the one field genuinely reformatted rather than served verbatim: `time` is
    /// unix seconds inside the block JSON, but RFC 3339 in this response.
    async fn get_timestamp(&self) -> Result<Option<Value>, ErrorObjectOwned> {
        let Some(tip) = self.tip().await else {
            return Ok(None);
        };
        let Some(block) = self.stored_block(tip).await? else {
            return Ok(None);
        };
        let Some(secs) = block.get("time").and_then(Value::as_u64) else {
            debug!(height = tip, "stored P-chain block has no usable 'time'");
            return Ok(None);
        };
        let at = std::time::UNIX_EPOCH
            .checked_add(std::time::Duration::from_secs(secs))
            .ok_or_else(|| err("block timestamp out of range"))?;
        Ok(Some(json!({
            "timestamp": humantime::format_rfc3339_seconds(at).to_string(),
        })))
    }

    /// The stored block JSON at `height`, parsed.
    async fn stored_block(&self, height: u64) -> Result<Option<Value>, ErrorObjectOwned> {
        let Some(stored) = self
            .storage
            .get_by_height(height)
            .await
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        serde_json::from_slice(&stored)
            .map(Some)
            .map_err(|e| err(format!("stored P-chain block decode: {e}")))
    }

    /// Render one stored height as `{ "block": …, "encoding": … }`.
    ///
    /// Neither branch reserializes: the JSON encoding hands back the stored JSON
    /// element as-is, and the byte encodings re-render from the stored canonical
    /// bytes. That is the whole point of storing both.
    async fn render_block(
        &self,
        height: u64,
        encoding: Encoding,
    ) -> Result<Option<Value>, ErrorObjectOwned> {
        let idx = if encoding == Encoding::Json {
            record::BLOCK
        } else {
            record::P_BYTES
        };
        let Some(stored) = self
            .storage
            .get_element(height, idx)
            .await
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        let stored: Value = serde_json::from_slice(&stored)
            .map_err(|e| err(format!("stored P-chain record decode: {e}")))?;

        let block = if encoding == Encoding::Json {
            stored
        } else {
            let hex = stored
                .as_str()
                .ok_or_else(|| err("stored block bytes are not a hex string"))?;
            let bytes =
                codec::hexnc_decode(hex).map_err(|e| err(format!("stored block bytes: {e}")))?;
            match encoding.render_bytes(&bytes) {
                Some(s) => Value::String(s),
                // Unreachable: only Json renders none, and it took the other arm.
                None => return Ok(None),
            }
        };
        Ok(Some(
            json!({ "block": block, "encoding": encoding.as_str() }),
        ))
    }

    /// `platform.getBlockByHeight` — the blockstore's primary key.
    async fn get_block_by_height(&self, p: ByHeight) -> Result<Option<Value>, ErrorObjectOwned> {
        let height = p.height.get()?;
        let encoding = parse_encoding(p.encoding.as_deref())?;
        self.render_block(height, encoding).await
    }

    /// `platform.getBlock` — the same block, addressed by its CB58 ID through
    /// the `hash_to_height` index. A malformed ID is a hard error (the caller
    /// sent nonsense); a well-formed ID we haven't indexed is a miss, so it
    /// becomes a 421 and the pool answers.
    async fn get_block(&self, p: ByBlockId) -> Result<Option<Value>, ErrorObjectOwned> {
        let encoding = parse_encoding(p.encoding.as_deref())?;
        let raw = codec::cb58_decode(&p.block_id)
            .map_err(|e| err(format!("bad blockID {}: {e}", p.block_id)))?;
        let Some(height) = self
            .storage
            .height_of_hash(raw)
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        self.render_block(height, encoding).await
    }

    /// `platform.getTx` — one transaction, sliced out of its block's stored
    /// JSON. (Verified against the live endpoint: upstream's `getTx` JSON is
    /// identical to the tx as embedded in `getBlock` JSON.)
    ///
    /// Only `encoding: "json"` is servable: a tx's canonical bytes are not
    /// separately stored, and cutting them out of the block's bytes would need
    /// exactly the codec parser this design exists to avoid — so the byte
    /// encodings return `None` → 421 rather than a reserialized guess.
    async fn get_tx(&self, p: ByTxId) -> Result<Option<Value>, ErrorObjectOwned> {
        let encoding = parse_encoding(p.encoding.as_deref())?;
        if encoding != Encoding::Json {
            debug!(
                tx = %p.tx_id,
                encoding = encoding.as_str(),
                "getTx byte encodings need a codec parser; deferring to upstream",
            );
            return Ok(None);
        }
        let raw =
            codec::cb58_decode(&p.tx_id).map_err(|e| err(format!("bad txID {}: {e}", p.tx_id)))?;
        let Some((height, idx)) = self
            .storage
            .get_tx_location(raw)
            .map_err(|e| err(format!("storage error: {e}")))?
        else {
            return Ok(None);
        };
        let Some(mut block) = self.stored_block(height).await? else {
            return Ok(None);
        };
        let Some(tx) = crate::platform::take_nth_tx(&mut block, idx as usize) else {
            // Indexed but not present: the record and the index disagree. Punt
            // rather than answer wrongly.
            debug!(height, idx, "tx index points past the stored block's txs");
            return Ok(None);
        };
        Ok(Some(
            json!({ "tx": tx, "encoding": Encoding::Json.as_str() }),
        ))
    }

    /// `platform.getTxStatus` — `Committed` for anything we hold. A mirror can
    /// never report `Processing` or `Dropped`: those describe a node's local
    /// mempool, which no indexer can see. A miss is therefore a 421 rather than
    /// a guess.
    fn get_tx_status(&self, p: ByTxId) -> Result<Option<Value>, ErrorObjectOwned> {
        let raw =
            codec::cb58_decode(&p.tx_id).map_err(|e| err(format!("bad txID {}: {e}", p.tx_id)))?;
        let found = self
            .storage
            .get_tx_location(raw)
            .map_err(|e| err(format!("storage error: {e}")))?
            .is_some();
        Ok(found.then(|| json!({ "status": "Committed" })))
    }
}

/// Register the Tier-0 `platform.*` methods. Names are spelled in full (dot
/// separator and all) so what's registered is exactly what goes on the wire.
pub fn module(c: &ChainServe) -> Result<RpcModule<PlatformApi>> {
    let mut m = RpcModule::new(PlatformApi::new(c));
    m.register_async_method("platform.getHeight", |_params, api, _ext| async move {
        api.get_height().await
    })?;
    m.register_async_method("platform.getTimestamp", |_params, api, _ext| async move {
        api.get_timestamp().await
    })?;
    m.register_async_method(
        "platform.getBlockByHeight",
        |params, api, _ext| async move { api.get_block_by_height(parse_params(&params)?).await },
    )?;
    m.register_async_method("platform.getBlock", |params, api, _ext| async move {
        api.get_block(parse_params(&params)?).await
    })?;
    m.register_async_method("platform.getTx", |params, api, _ext| async move {
        api.get_tx(parse_params(&params)?).await
    })?;
    m.register_async_method("platform.getTxStatus", |params, api, _ext| async move {
        api.get_tx_status(parse_params(&params)?)
    })?;
    // Notifications arrive under `platform.subscription`, matching how
    // `eth_subscribe` names its own (`eth_subscription`).
    m.register_subscription(
        "platform.subscribe",
        "platform.subscription",
        "platform.unsubscribe",
        |params, pending, api, _ext| async move {
            let parsed = match parse_params::<SubscribeParams>(&params) {
                Ok(p) => p,
                Err(e) => {
                    pending.reject(e).await;
                    return Ok(());
                }
            };
            api.subscribe(pending, parsed).await
        },
    )?;
    Ok(m)
}

/// Parse the `encoding` param, mapping a bad spelling to a JSON-RPC error rather
/// than a 421 — the caller sent something wrong, and upstream would reject it too.
fn parse_encoding(s: Option<&str>) -> Result<Encoding, ErrorObjectOwned> {
    Encoding::parse(s).map_err(|e| err(e.to_string()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::test_support::{chain_serve, unique_temp_dir};

    /// A P-chain service over a fresh store, plus the store to seed.
    fn service(dir: &std::path::Path) -> (Storage, PlatformApi) {
        let c = chain_serve(Chain::P, dir);
        (c.storage.clone(), PlatformApi::new(&c))
    }

    /// Write a realistic P-chain record at `height`: canonical bytes whose
    /// sha256 really is the block ID, the matching JSON, and the given txs.
    /// Returns the block ID.
    async fn put_pblock(storage: &Storage, height: u64, tx_ids: &[[u8; 32]]) -> String {
        // Any bytes serve as "canonical" here; what matters is that the ID we
        // index under is derived from them, exactly as ingest does it.
        let bytes = format!("block-bytes-at-height-{height}").into_bytes();
        let block_id = codec::block_id_of(&bytes);
        let hexnc = Encoding::Hexnc.render_bytes(&bytes).unwrap();
        let txs: Vec<Value> = tx_ids
            .iter()
            .map(|id| json!({ "id": codec::cb58_encode(id), "unsignedTx": {"memo": "0x"} }))
            .collect();
        let block_json = json!({
            "id": block_id,
            "height": height,
            "time": 1_786_114_324u64.wrapping_add(height),
            "parentID": codec::cb58_encode(&[1u8; 32]),
            "txs": txs,
        });
        let json_bytes = serde_json::to_vec(&block_json).unwrap();
        let hexnc_bytes = serde_json::to_vec(&Value::String(hexnc)).unwrap();
        storage
            .put(
                height,
                codec::cb58_decode(&block_id).unwrap(),
                tx_ids,
                &[&json_bytes, &hexnc_bytes, record::EMPTY_ARRAY],
            )
            .await
            .unwrap();
        block_id
    }

    fn by_height(height: u64, encoding: Option<&str>) -> ByHeight {
        ByHeight {
            height: Uint64::Num(height),
            encoding: encoding.map(str::to_owned),
        }
    }

    fn by_tx(id: &[u8; 32], encoding: Option<&str>) -> ByTxId {
        ByTxId {
            tx_id: codec::cb58_encode(id),
            encoding: encoding.map(str::to_owned),
        }
    }

    /// An empty store answers every read with `None` — the 421 that sends the
    /// caller to a real node — rather than inventing a height or a timestamp.
    #[tokio::test]
    async fn empty_store_defers_everything() {
        let dir = unique_temp_dir("platform-empty");
        let (_storage, api) = service(&dir);

        assert!(api.get_height().await.unwrap().is_none());
        assert!(api.get_timestamp().await.unwrap().is_none());
        assert!(
            api.get_block_by_height(by_height(5, None))
                .await
                .unwrap()
                .is_none()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Numbers go out as strings, the way avalanchego serializes them.
    #[tokio::test]
    async fn get_height_reports_the_contiguous_tip_as_a_string() {
        let dir = unique_temp_dir("platform-height");
        let (storage, api) = service(&dir);
        put_pblock(&storage, 700, &[]).await;
        put_pblock(&storage, 701, &[]).await;

        let v = api.get_height().await.unwrap().unwrap();
        assert_eq!(v["height"], "701", "height must be a string, not a number");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Height 0 is a real height, so a store holding only genesis must report
    /// `"0"` rather than looking empty.
    #[tokio::test]
    async fn genesis_only_store_reports_height_zero() {
        let dir = unique_temp_dir("platform-genesis");
        let (storage, api) = service(&dir);
        put_pblock(&storage, 0, &[]).await;

        let v = api.get_height().await.unwrap().unwrap();
        assert_eq!(v["height"], "0");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Every encoding is served from what was stored: `json` hands back the
    /// stored JSON untouched, and the byte encodings re-render the stored
    /// canonical bytes, with `hex`/`hexc` adding the checksum `hexnc` omits.
    #[tokio::test]
    async fn every_encoding_is_served_from_storage() {
        let dir = unique_temp_dir("platform-encodings");
        let (storage, api) = service(&dir);
        let block_id = put_pblock(&storage, 700, &[]).await;

        let j = api
            .get_block_by_height(by_height(700, Some("json")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(j["encoding"], "json");
        assert_eq!(j["block"]["height"], 700);
        assert_eq!(j["block"]["id"], block_id);

        let nc = api
            .get_block_by_height(by_height(700, Some("hexnc")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(nc["encoding"], "hexnc");
        let nc_hex = nc["block"].as_str().unwrap().to_owned();

        // The default encoding is `hex`, and it extends `hexnc` by the checksum.
        let default = api
            .get_block_by_height(by_height(700, None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(default["encoding"], "hex");
        let hex = default["block"].as_str().unwrap();
        assert!(hex.starts_with(&nc_hex), "{hex} must extend {nc_hex}");
        assert_eq!(hex.len(), nc_hex.len().saturating_add(8));

        // `hexc` renders like `hex` but echoes its own spelling.
        let hexc = api
            .get_block_by_height(by_height(700, Some("hexc")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hexc["encoding"], "hexc");
        assert_eq!(hexc["block"], default["block"]);

        // The stored bytes really do hash to the block ID we indexed under —
        // the self-verifying property, checked end to end through the store.
        let bytes = codec::hexnc_decode(&nc_hex).unwrap();
        assert_eq!(codec::block_id_of(&bytes), block_id);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A height we don't hold is a 421 (`None`), never an error — that's what
    /// routes the caller to the pool. This differs from upstream on purpose.
    /// A bad encoding, by contrast, IS an error: the request itself is wrong.
    #[tokio::test]
    async fn unheld_height_defers_but_a_bad_encoding_errors() {
        let dir = unique_temp_dir("platform-miss");
        let (storage, api) = service(&dir);
        put_pblock(&storage, 700, &[]).await;

        assert!(
            api.get_block_by_height(by_height(9_999, None))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            api.get_block_by_height(by_height(700, Some("cb58")))
                .await
                .is_err()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `getBlock` reaches the same record through the CB58 block ID.
    #[tokio::test]
    async fn get_block_resolves_a_cb58_id() {
        let dir = unique_temp_dir("platform-getblock");
        let (storage, api) = service(&dir);
        let block_id = put_pblock(&storage, 700, &[]).await;

        let by_id = api
            .get_block(ByBlockId {
                block_id: block_id.clone(),
                encoding: Some("json".to_owned()),
            })
            .await
            .unwrap()
            .unwrap();
        let by_h = api
            .get_block_by_height(by_height(700, Some("json")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_id, by_h);

        // An ID we haven't indexed defers; a malformed one is a hard error.
        let unknown = ByBlockId {
            block_id: codec::cb58_encode(&[0xee; 32]),
            encoding: None,
        };
        assert!(api.get_block(unknown).await.unwrap().is_none());
        let bad = ByBlockId {
            block_id: "not-cb58!!".to_owned(),
            encoding: None,
        };
        assert!(api.get_block(bad).await.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `getTx` slices the transaction out of its block's stored JSON, and
    /// `getTxStatus` reports `Committed` for it.
    #[tokio::test]
    async fn get_tx_slices_from_the_stored_block() {
        let dir = unique_temp_dir("platform-gettx");
        let (storage, api) = service(&dir);
        let tx_a = [0xa1; 32];
        let tx_b = [0xb2; 32];
        put_pblock(&storage, 700, &[tx_a, tx_b]).await;

        let v = api
            .get_tx(by_tx(&tx_b, Some("json")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v["encoding"], "json");
        assert_eq!(v["tx"]["id"], codec::cb58_encode(&tx_b));

        let status = api.get_tx_status(by_tx(&tx_a, None)).unwrap().unwrap();
        assert_eq!(status["status"], "Committed");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A tx we don't hold defers rather than claiming to know its status — a
    /// mirror cannot see a mempool, so `Processing`/`Dropped` are unanswerable.
    #[tokio::test]
    async fn unknown_tx_defers_rather_than_guessing_status() {
        let dir = unique_temp_dir("platform-gettx-miss");
        let (storage, api) = service(&dir);
        put_pblock(&storage, 700, &[[0xa1; 32]]).await;

        let unknown = [0xcc; 32];
        assert!(api.get_tx_status(by_tx(&unknown, None)).unwrap().is_none());
        assert!(
            api.get_tx(by_tx(&unknown, Some("json")))
                .await
                .unwrap()
                .is_none()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A tx's canonical bytes aren't separately stored, so the byte encodings of
    /// `getTx` defer to upstream instead of reserializing a guess.
    #[tokio::test]
    async fn get_tx_byte_encodings_defer() {
        let dir = unique_temp_dir("platform-gettx-hex");
        let (storage, api) = service(&dir);
        let tx = [0xa1; 32];
        put_pblock(&storage, 700, &[tx]).await;

        for enc in [None, Some("hex"), Some("hexc"), Some("hexnc")] {
            assert!(
                api.get_tx(by_tx(&tx, enc)).await.unwrap().is_none(),
                "encoding {enc:?} must defer",
            );
        }
        // json still works, so the deferral is about the encoding, not the tx.
        assert!(
            api.get_tx(by_tx(&tx, Some("json")))
                .await
                .unwrap()
                .is_some()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `getTimestamp` reformats the tip block's unix `time` into the RFC 3339
    /// shape avalanchego returns.
    #[tokio::test]
    async fn get_timestamp_renders_rfc3339_from_the_tip() {
        let dir = unique_temp_dir("platform-timestamp");
        let (storage, api) = service(&dir);
        // put_pblock sets time = 1_786_114_324 + height.
        put_pblock(&storage, 0, &[]).await;

        let v = api.get_timestamp().await.unwrap().unwrap();
        let ts = v["timestamp"].as_str().unwrap();
        assert!(ts.ends_with('Z'), "{ts} must be RFC 3339 UTC");
        // 1_786_114_324 is the real chain time of Fuji P-chain block 292000.
        assert_eq!(ts, "2026-08-07T14:52:04Z");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The registered wire names use avalanchego's dot separator, not
    /// jsonrpsee's default underscore.
    #[test]
    fn registers_dot_separated_method_names() {
        let dir = unique_temp_dir("platform-names");
        let c = chain_serve(Chain::P, &dir);
        let module = module(&c).unwrap();
        let names: Vec<&str> = module.method_names().collect();

        for want in [
            "platform.getHeight",
            "platform.getTimestamp",
            "platform.getBlock",
            "platform.getBlockByHeight",
            "platform.getTx",
            "platform.getTxStatus",
        ] {
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        assert!(
            !names.iter().any(|n| n.starts_with("platform_")),
            "{names:?}",
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// End-to-end through the JSON-RPC layer: params really are read **by name**
    /// from an object, in any key order, with camelCase spellings and
    /// avalanchego's string-or-number integers. This is the dialect contract that
    /// positional parsing would silently violate.
    #[tokio::test]
    async fn params_bind_by_name_in_the_upstream_dialect() {
        let dir = unique_temp_dir("platform-params");
        let c = chain_serve(Chain::P, &dir);
        let block_id = put_pblock(&c.storage, 700, &[[0xa1; 32]]).await;
        let module = module(&c).unwrap();

        let call = async |params: String| -> Value {
            let req = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"platform.getBlockByHeight","params":{params}}}"#
            );
            let (resp, _) = module.raw_json_request(&req, 1).await.unwrap();
            serde_json::from_str(resp.get()).unwrap()
        };

        // A number, a string, and reversed key order all bind identically.
        for params in [
            r#"{"height":700,"encoding":"json"}"#,
            r#"{"height":"700","encoding":"json"}"#,
            r#"{"encoding":"json","height":700}"#,
        ] {
            let v = call(params.to_owned()).await;
            assert_eq!(v["result"]["block"]["height"], 700, "{params} -> {v}");
        }

        // The camelCase `blockID` key is what binds — `block_id` must not.
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"platform.getBlock","params":{{"blockID":"{block_id}","encoding":"json"}}}}"#
        );
        let (resp, _) = module.raw_json_request(&req, 1).await.unwrap();
        let v: Value = serde_json::from_str(resp.get()).unwrap();
        assert_eq!(v["result"]["block"]["id"], block_id.as_str(), "{resp}");

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"platform.getBlock","params":{{"block_id":"{block_id}"}}}}"#
        );
        let (resp, _) = module.raw_json_request(&req, 1).await.unwrap();
        let v: Value = serde_json::from_str(resp.get()).unwrap();
        assert!(
            v.get("error").is_some(),
            "snake_case key must not bind: {resp}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
