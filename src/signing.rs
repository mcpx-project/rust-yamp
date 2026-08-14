//! Signing / attesting proxy (corpus SEP-2828, SEP-2787, SEP-2809).
//!
//! The newest corpus movement models the proxy itself as an accountable,
//! signing participant: before a call it emits a client attestation, and after
//! it a paired, signed outcome record, on a best-effort path that never blocks
//! traffic. Records are canonicalized to RFC-8785 (JCS) bytes, signed detached,
//! and hash-chained so the log is tamper-evident.
//!
//! The digest is SHA-256 and the detached signature is HMAC-SHA256 over the
//! record's RFC-8785 canonical bytes (RFC 2104, keyed by the audit secret): real,
//! production-grade primitives, hand-rolled here to the exact bytes the Python
//! arm's `hashlib`/`hmac` produce, so the two arms stay byte-identical with no
//! external crypto dependency. An asymmetric Ed25519 detached signature (the other
//! construction SEP-2828 allows) is not used, because the Python arm is
//! stdlib-only and the standard library ships no Ed25519; a deployment that can
//! take a crypto dependency substitutes it behind this same interface.

use serde_json::{json, Value};

/// The chain and signature are 256-bit, so the genesis link is 64 hex zeros.
pub const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

// SHA-256 (FIPS 180-4). Pure Rust so both arms hash to identical bytes with no
// crypto crate; the constants are the standard round constants and initial state.
const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// SHA-256 digest of `data` (FIPS 180-4). The one SHA-256 in the crate; the auth
/// module's PKCE challenge reuses it so there is a single implementation.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    // Pad: 0x80, zeros, then the 64-bit big-endian bit length.
    let mut message = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for block in message.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes([block[i * 4], block[i * 4 + 1], block[i * 4 + 2], block[i * 4 + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(SHA256_K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// Lowercase hex of `bytes`, the single source for hex encoding in the Rust arm.
pub fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// SHA-256 of `data` as lowercase hex. The single source for a content digest
/// (the callout verdict-cache key, δ-ε3) as well as the audit chain link.
pub fn sha256_hex(data: &[u8]) -> String {
    to_hex(&sha256(data))
}

/// HMAC-SHA256 (RFC 2104) over `data`, keyed by `secret`.
fn hmac_sha256(secret: &[u8], data: &[u8]) -> String {
    const BLOCK: usize = 64;
    // A key longer than the block is first hashed; then padded to the block size.
    let mut key = if secret.len() > BLOCK { sha256(secret).to_vec() } else { secret.to_vec() };
    key.resize(BLOCK, 0);
    let mut ipad = vec![0u8; BLOCK];
    let mut opad = vec![0u8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] = key[i] ^ 0x36;
        opad[i] = key[i] ^ 0x5c;
    }
    let mut inner = ipad;
    inner.extend_from_slice(data);
    let inner_digest = sha256(&inner);
    let mut outer = opad;
    outer.extend_from_slice(&inner_digest);
    sha256_hex(&outer)
}

/// RFC-8785-style canonical bytes: object keys sorted at every level (serde_json
/// orders its map), no insignificant whitespace, UTF-8. Records should hold only
/// integers/strings so the two arms serialize numbers identically.
pub fn canonical(record: &Value) -> Vec<u8> {
    serde_json::to_vec(record).expect("a serde_json::Value always serializes")
}

/// A detached HMAC-SHA256 signature over the record's canonical bytes.
pub fn sign(secret: &str, record: &Value) -> String {
    hmac_sha256(secret.as_bytes(), &canonical(record))
}

pub fn verify(secret: &str, record: &Value, signature: &str) -> bool {
    constant_time_eq(sign(secret, record).as_bytes(), signature.as_bytes())
}

/// Constant-time byte equality, so signature verification does not leak match
/// progress through timing to an attacker submitting candidate signatures. This
/// is the Rust equivalent of Python's `hmac.compare_digest`.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// The next hash-chain link: SHA-256 of the previous link and this record.
pub fn chain(prev_hash: &str, record: &Value) -> String {
    let mut data = prev_hash.as_bytes().to_vec();
    data.push(b'|');
    data.extend_from_slice(&canonical(record));
    sha256_hex(&data)
}

/// A pre-call client attestation (SEP-2787).
pub fn attestation_record(principal: &str, method: &str, name: Option<&str>) -> Value {
    json!({ "type": "attestation", "principal": principal, "method": method, "name": name })
}

/// A post-call signed outcome record (SEP-2828).
pub fn outcome_record(method: &str, name: Option<&str>, ok: bool) -> Value {
    json!({ "type": "outcome", "method": method, "name": name, "ok": ok })
}

/// A tamper-evident, append-only log of signed, hash-chained records.
pub struct AuditLog {
    secret: String,
    genesis: String,
    head: String,
    pub records: Vec<Value>,
}

impl AuditLog {
    pub fn new(secret: impl Into<String>) -> Self {
        let secret = secret.into();
        Self { secret, genesis: GENESIS.to_string(), head: GENESIS.to_string(), records: Vec::new() }
    }

    pub fn append(&mut self, record: Value) -> Value {
        let entry = json!({
            "record": record,
            "prev": self.head,
            "signature": sign(&self.secret, &record),
            "hash": chain(&self.head, &record),
        });
        self.head = entry["hash"].as_str().unwrap().to_string();
        self.records.push(entry.clone());
        entry
    }

    /// Whether every record's signature and chain link is intact.
    pub fn verify(&self) -> bool {
        let mut head = self.genesis.clone();
        for entry in &self.records {
            let record = &entry["record"];
            if !verify(&self.secret, record, entry["signature"].as_str().unwrap_or("")) {
                return false;
            }
            if entry["prev"].as_str() != Some(head.as_str()) {
                return false;
            }
            if chain(&head, record).as_str() != entry["hash"].as_str().unwrap_or("") {
                return false;
            }
            head = entry["hash"].as_str().unwrap_or_default().to_string();
        }
        true
    }
}
