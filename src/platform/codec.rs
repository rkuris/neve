//! The P-chain's ID and byte-encoding conventions.
//!
//! Three small schemes, all built on a single trailing SHA-256 checksum:
//!
//! - **CB58** — how avalanchego spells every 32-byte ID on the wire (block IDs,
//!   tx IDs): `base58(payload ++ sha256(payload)[28..32])`. Not Bitcoin's
//!   `Base58Check`, which double-hashes.
//! - **`hex` / `hexc`** — the default byte encoding of `platform.getBlock` and
//!   friends: `0x` ++ hex(`bytes` ++ `sha256(bytes)[28..32]`).
//! - **`hexnc`** — the same without the checksum. This is what neve *stores*,
//!   because it is the raw canonical block and the checksum is derivable.
//!
//! The block ID follows from the bytes — `blockID == cb58(sha256(blockBytes))` —
//! so a stored record is self-verifying, an integrity check the C-chain record
//! never had. Verified against `api.avax-test.network` at height 292000.

use anyhow::{Result, bail};
use sha2::{Digest, Sha256};

/// Bytes of trailing checksum in CB58 and in the `hex`/`hexc` encodings.
const CHECKSUM_LEN: usize = 4;

/// SHA-256 of `bytes`.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// The 4-byte checksum avalanchego appends: the *last* four bytes of
/// `sha256(payload)`.
fn checksum(payload: &[u8]) -> [u8; CHECKSUM_LEN] {
    let digest = sha256(payload);
    // `digest` is a fixed 32 bytes and CHECKSUM_LEN is 4, so the split point is
    // always in range; `split_at` keeps that provable without an index.
    let (_, tail) = digest.split_at(digest.len().saturating_sub(CHECKSUM_LEN));
    let mut out = [0u8; CHECKSUM_LEN];
    out.copy_from_slice(tail);
    out
}

/// Encode a 32-byte ID as CB58.
pub fn cb58_encode(id: &[u8; 32]) -> String {
    let mut buf = Vec::with_capacity(id.len().saturating_add(CHECKSUM_LEN));
    buf.extend_from_slice(id);
    buf.extend_from_slice(&checksum(id));
    bs58::encode(buf).into_string()
}

/// Decode a CB58 string into the 32-byte ID it carries, verifying the checksum.
/// Rejects anything that isn't exactly a 32-byte payload, so a malformed ID
/// can't become a wrong-but-plausible index key.
pub fn cb58_decode(s: &str) -> Result<[u8; 32]> {
    let raw = bs58::decode(s)
        .into_vec()
        .map_err(|e| anyhow::anyhow!("not valid base58: {e}"))?;
    let want_len = 32usize.saturating_add(CHECKSUM_LEN);
    if raw.len() != want_len {
        bail!(
            "CB58 ID decodes to {} bytes, expected {want_len} (32-byte ID + \
             {CHECKSUM_LEN}-byte checksum)",
            raw.len(),
        );
    }
    let (payload, got) = raw.split_at(32);
    if got != checksum(payload) {
        bail!("CB58 checksum mismatch");
    }
    let mut id = [0u8; 32];
    id.copy_from_slice(payload);
    Ok(id)
}

/// The block ID that `bytes` must have: `cb58(sha256(bytes))`. Used at ingest to
/// check the two halves of the stored record against each other.
pub fn block_id_of(bytes: &[u8]) -> String {
    cb58_encode(&sha256(bytes))
}

/// Parse avalanchego's `hexnc` form (`0x`-prefixed plain hex, no checksum) back
/// into bytes — the inverse of what we store.
pub fn hexnc_decode(s: &str) -> Result<Vec<u8>> {
    let body = s
        .strip_prefix("0x")
        .ok_or_else(|| anyhow::anyhow!("hex encoding must start with 0x"))?;
    Ok(hex::decode(body)?)
}

/// Render `bytes` in avalanchego's checksummed `hex` / `hexc` encoding.
pub fn hex_with_checksum(bytes: &[u8]) -> String {
    let mut buf = Vec::with_capacity(bytes.len().saturating_add(CHECKSUM_LEN));
    buf.extend_from_slice(bytes);
    buf.extend_from_slice(&checksum(bytes));
    format!("0x{}", hex::encode(buf))
}

/// Which encoding a `platform.*` caller asked for. avalanchego accepts these
/// four spellings and defaults to `hex`; anything else is an error rather than a
/// silent fallback, so a typo doesn't quietly return the wrong shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    /// `0x` hex **with** the trailing checksum. avalanchego's default.
    #[default]
    Hex,
    /// Alias for [`Self::Hex`], preserved so the response echoes what was asked.
    Hexc,
    /// `0x` hex with no checksum — what neve stores.
    Hexnc,
    /// The decoded JSON object.
    Json,
}

impl Encoding {
    /// Parse the `encoding` param; `None` means the caller omitted it.
    pub fn parse(s: Option<&str>) -> Result<Self> {
        match s {
            None | Some("hex") => Ok(Self::Hex),
            Some("hexc") => Ok(Self::Hexc),
            Some("hexnc") => Ok(Self::Hexnc),
            Some("json") => Ok(Self::Json),
            Some(other) => bail!("unknown encoding: {other}"),
        }
    }

    /// The spelling to echo back in the response's `encoding` field.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hex => "hex",
            Self::Hexc => "hexc",
            Self::Hexnc => "hexnc",
            Self::Json => "json",
        }
    }

    /// Render stored canonical `bytes` in this encoding. `None` for
    /// [`Self::Json`], which is served from the stored JSON element instead —
    /// neve never reserializes one representation into the other.
    pub fn render_bytes(self, bytes: &[u8]) -> Option<String> {
        match self {
            Self::Hex | Self::Hexc => Some(hex_with_checksum(bytes)),
            Self::Hexnc => Some(format!("0x{}", hex::encode(bytes))),
            Self::Json => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// Fuji P-chain height 292000, captured live on 2026-08-10. The block ID is
    /// what `platform.getBlockByHeight(292000, "json")` reported, and the bytes
    /// are its `hexnc` encoding — so this pins the whole
    /// bytes → sha256 → CB58 chain against the reference implementation.
    const FUJI_292000_ID: &str = "cDkr8hB3eX6RM8XJMu4nR9rvwUbKSHqbNx2a9U13JkCYyctbx";

    /// The genesis block, 46 bytes, from the same probe — the smallest real
    /// block there is.
    const FUJI_GENESIS_HEXNC: &str = "0x0000000000022e6b699298a664793bff42dae9c1af8d9c54645d8b376fd331e0b67475578e0a0000000000000000";

    #[test]
    fn cb58_round_trips() {
        let id = [0xab; 32];
        let s = cb58_encode(&id);
        assert_eq!(cb58_decode(&s).unwrap(), id);
    }

    #[test]
    fn cb58_decodes_a_real_block_id() {
        let id = cb58_decode(FUJI_292000_ID).unwrap();
        // Re-encoding must reproduce the exact wire string.
        assert_eq!(cb58_encode(&id), FUJI_292000_ID);
    }

    /// A corrupted ID must be refused, not silently indexed under the wrong key.
    #[test]
    fn cb58_rejects_a_bad_checksum() {
        let id = [0x11; 32];
        let good = cb58_encode(&id);
        // Flip a character in the payload region; base58 stays valid, checksum won't.
        let mut chars: Vec<char> = good.chars().collect();
        chars[1] = if chars[1] == 'a' { 'b' } else { 'a' };
        let bad: String = chars.into_iter().collect();
        assert!(cb58_decode(&bad).is_err(), "{bad} should fail the checksum");
    }

    #[test]
    fn cb58_rejects_wrong_length_and_non_base58() {
        // Valid base58, but not 36 bytes.
        assert!(cb58_decode("abc").is_err());
        // `0` and `l` are not in the base58 alphabet.
        assert!(cb58_decode("0l0l").is_err());
        assert!(cb58_decode("").is_err());
    }

    /// The genesis block's stored bytes derive its block ID, which is the
    /// integrity check ingest runs on every height.
    #[test]
    fn block_id_derives_from_the_bytes() {
        let bytes = hexnc_decode(FUJI_GENESIS_HEXNC).unwrap();
        assert_eq!(
            bytes.len(),
            46,
            "the Fuji P-chain genesis block is 46 bytes"
        );
        // Self-consistency: the derived ID decodes back to sha256 of the bytes.
        let id = block_id_of(&bytes);
        assert_eq!(cb58_decode(&id).unwrap(), sha256(&bytes));
    }

    /// `hex` is `hexnc` plus the four checksum bytes — the relation verified
    /// live against the endpoint.
    #[test]
    fn hex_appends_the_checksum_to_hexnc() {
        let bytes = hexnc_decode(FUJI_GENESIS_HEXNC).unwrap();
        let plain = Encoding::Hexnc.render_bytes(&bytes).unwrap();
        let summed = Encoding::Hex.render_bytes(&bytes).unwrap();
        assert_eq!(plain, FUJI_GENESIS_HEXNC);
        assert!(summed.starts_with(&plain), "{summed} must extend {plain}");
        // Exactly 4 bytes (8 hex chars) longer.
        assert_eq!(summed.len(), plain.len().saturating_add(8));
        // And those bytes are the tail of the digest.
        let expect = hex::encode(checksum(&bytes));
        assert!(summed.ends_with(&expect), "{summed} must end with {expect}");
        // `hexc` is a pure alias of `hex`, same rendering.
        assert_eq!(Encoding::Hexc.render_bytes(&bytes).unwrap(), summed);
    }

    #[test]
    fn hexnc_round_trips_a_real_block() {
        let bytes = hexnc_decode(FUJI_GENESIS_HEXNC).unwrap();
        assert_eq!(
            Encoding::Hexnc.render_bytes(&bytes).unwrap(),
            FUJI_GENESIS_HEXNC,
        );
        // A missing 0x prefix is an error, not a lenient parse.
        assert!(hexnc_decode("00ff").is_err());
        assert!(hexnc_decode("0xzz").is_err());
        // Odd-length hex is half a byte and must not decode.
        assert!(hexnc_decode("0xabc").is_err());
    }

    #[test]
    fn encoding_parses_every_accepted_spelling_and_defaults_to_hex() {
        assert_eq!(Encoding::parse(None).unwrap(), Encoding::Hex);
        assert_eq!(Encoding::parse(Some("hex")).unwrap(), Encoding::Hex);
        assert_eq!(Encoding::parse(Some("hexc")).unwrap(), Encoding::Hexc);
        assert_eq!(Encoding::parse(Some("hexnc")).unwrap(), Encoding::Hexnc);
        assert_eq!(Encoding::parse(Some("json")).unwrap(), Encoding::Json);
        // Unknown spellings are refused rather than silently defaulted.
        assert!(Encoding::parse(Some("cb58")).is_err());
        assert!(Encoding::parse(Some("")).is_err());
    }

    /// Each spelling echoes itself back, so a client that asked for `hexc` sees
    /// `hexc` even though it renders identically to `hex`.
    #[test]
    fn encoding_echoes_its_own_spelling() {
        for e in [
            Encoding::Hex,
            Encoding::Hexc,
            Encoding::Hexnc,
            Encoding::Json,
        ] {
            assert_eq!(Encoding::parse(Some(e.as_str())).unwrap(), e);
        }
    }

    /// The JSON encoding is served from the stored JSON element, never rendered
    /// from bytes — that's what keeps both representations verbatim.
    #[test]
    fn json_encoding_renders_no_bytes() {
        assert!(Encoding::Json.render_bytes(b"anything").is_none());
    }
}
