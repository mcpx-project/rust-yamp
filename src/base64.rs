//! Base64 (RFC 4648), the single source for the Rust arm.
//!
//! Standard alphabet with padding ([`encode`]/[`decode`]) is what MCP content
//! blocks carry (image/audio `data`, resource `blob`); the URL alphabet without
//! padding ([`encode_url_nopad`]) is what PKCE requires (RFC 7636). The Python
//! arm uses its stdlib `base64` for the same operations, so both arms agree
//! byte-for-byte; the differential corpus pins the primitive.

const STANDARD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn encode_with(alphabet: &[u8; 64], data: &[u8], pad: bool) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(alphabet[(n >> 18 & 0x3f) as usize] as char);
        out.push(alphabet[(n >> 12 & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(alphabet[(n >> 6 & 0x3f) as usize] as char);
        } else if pad {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(alphabet[(n & 0x3f) as usize] as char);
        } else if pad {
            out.push('=');
        }
    }
    out
}

/// Standard base64 with padding (RFC 4648 §4).
pub fn encode(data: &[u8]) -> String {
    encode_with(STANDARD, data, true)
}

/// base64url without padding (RFC 4648 §5), as PKCE requires.
pub fn encode_url_nopad(data: &[u8]) -> String {
    encode_with(URL, data, false)
}

fn sextet(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some((c - b'A') as u32),
        b'a'..=b'z' => Some((c - b'a' + 26) as u32),
        b'0'..=b'9' => Some((c - b'0' + 52) as u32),
        b'+' | b'-' => Some(62),
        b'/' | b'_' => Some(63),
        _ => None,
    }
}

/// Decode standard or URL base64, with or without padding. Returns `None` on any
/// character outside the alphabet or an impossible length, so a hostile blob is
/// rejected rather than silently truncated.
pub fn decode(text: &str) -> Option<Vec<u8>> {
    let mut chunk: Vec<u32> = Vec::with_capacity(4);
    let mut out: Vec<u8> = Vec::with_capacity(text.len() / 4 * 3);
    for &c in text.as_bytes() {
        if c == b'=' {
            continue;
        }
        chunk.push(sextet(c)?);
        if chunk.len() == 4 {
            out.push((chunk[0] << 2 | chunk[1] >> 4) as u8);
            out.push((chunk[1] << 4 | chunk[2] >> 2) as u8);
            out.push((chunk[2] << 6 | chunk[3]) as u8);
            chunk.clear();
        }
    }
    match chunk.len() {
        0 => {}
        1 => return None,
        2 => out.push((chunk[0] << 2 | chunk[1] >> 4) as u8),
        _ => {
            out.push((chunk[0] << 2 | chunk[1] >> 4) as u8);
            out.push((chunk[1] << 4 | chunk[2] >> 2) as u8);
        }
    }
    Some(out)
}
