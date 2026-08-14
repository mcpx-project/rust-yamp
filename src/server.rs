//! Server-origination concerns (σ0).
//!
//! yamp is also an MCP server: it originates responses from local handlers
//! rather than only routing (pure-server mode is a registry with zero backends).
//! One thing a server must do that a bare proxy need not is attach cache metadata
//! to its list results, so a downstream cache honors them exactly as yamp's own
//! [`crate::cache::ListCache`] does. The SEP-2549 directive keys are the single
//! source in [`crate::cache`]; `ttlMs` is emitted as an integer so both arms
//! agree byte-for-byte with the Python arm's `server` module.

use serde_json::{json, Map, Value};

use crate::cache;
use crate::jsonrpc;
use crate::transport::MAX_FRAME_BYTES;

/// σ5: a server originates responses, so it is accountable for the
/// size of what it emits. The ceiling is the same one the framing decoder accepts
/// on input ([`MAX_FRAME_BYTES`]), so a server never emits a frame it would itself
/// refuse to read; a routed backend response needs no separate cap, since the
/// decoder that read it already enforced this bound. Single-sourced from the
/// transport so the two limits cannot drift.
pub const MAX_OUTPUT_BYTES: usize = MAX_FRAME_BYTES;

/// Whether a server-originated `result`'s encoded (compact) form exceeds
/// `max_bytes`. A cap of zero is unbounded. Both arms encode compactly and the
/// byte length is key-order-independent, so the verdict agrees across arms for a
/// given result.
pub fn exceeds_output_cap(result: &Value, max_bytes: usize) -> bool {
    if max_bytes == 0 {
        return false;
    }
    jsonrpc::encode(result).len() > max_bytes
}

/// The SEP-2549 cache directives a server advertises on a list result.
pub fn list_directives(ttl_ms: u64, cache_scope: &str) -> Value {
    let mut out = Map::new();
    out.insert(cache::TTL_MS_KEY.to_string(), json!(ttl_ms));
    out.insert(cache::CACHE_SCOPE_KEY.to_string(), Value::String(cache_scope.to_string()));
    Value::Object(out)
}

/// Return `result` with the cache directives attached (top-level), the seam the
/// served `tools/list` and `server/discover` results use.
pub fn attach_directives(result: &Value, ttl_ms: u64, cache_scope: &str) -> Value {
    let mut out = result.as_object().cloned().unwrap_or_default();
    out.insert(cache::TTL_MS_KEY.to_string(), json!(ttl_ms));
    out.insert(cache::CACHE_SCOPE_KEY.to_string(), Value::String(cache_scope.to_string()));
    Value::Object(out)
}
