//! Server variants and variant-bound cursors (corpus SEP-2053).
//!
//! SEP-2053 lets a server expose several parallel *variants* (for example
//! `claude-optimized` and `compact`) that reshape the same capabilities. A client
//! enumerates them from the negotiated extension payload during `initialize` and
//! selects one per request via a canonical `_meta` key. All selection is
//! stateless: the variant rides in `_meta`, never in session state.
//!
//! yamp is a proxy, so its obligation is a routing one, not a variant
//! implementation. Three pieces:
//!
//! - *Enumeration*: compose the backends' `availableVariants` into the proxy's own
//!   advertised extension. A variant the proxy offers must be selectable on every
//!   backend that supports variants at all, so the composition is an
//!   intersection; a backend without the extension is variant-agnostic and
//!   imposes no constraint.
//! - *Selection*: forward the client's per-request variant to backends, and reject
//!   a variant the composed set does not contain with `-32602` before any backend
//!   is touched (SEP-2053 selection rules).
//! - *Cursor binding*: pagination cursors are variant-scoped (SEP-2053 rule 2-3).
//!   When the proxy aggregates a paginated list it mints one opaque composite
//!   cursor that binds the active variant and each paginating backend's own
//!   cursor. A continuation reverse-resolves it to exactly those backends with
//!   their own cursors, and is rejected with `-32602` if its variant differs from
//!   the one the cursor was minted under.
//!
//! The composite cursor is hex-encoded canonical JSON (opaque to clients, no new
//! dependency; SEP-2053 permits any opaque encoding), so the proxy carries no
//! per-cursor state. serde_json orders map keys, so the encoding is byte-identical
//! to the Python arm's.

use std::collections::{BTreeMap, HashSet};

use serde_json::{json, Value};

/// The negotiated extension id and the canonical per-request selection key
/// (SEP-2053 "Extension id" / "Canonical per-request _meta key").
pub const EXTENSION_ID: &str = "io.modelcontextprotocol/server-variants";
pub const SERVER_VARIANT_META_KEY: &str = "io.modelcontextprotocol/server-variant";

/// Marks a cursor this proxy minted, so a raw backend cursor is never mistaken
/// for a composite one. Versioned so the encoding can evolve.
pub const CURSOR_PREFIX: &str = "yv1:";

/// The per-request variant id from a request's `_meta`, or `None`.
pub fn selected_variant(params: &Value) -> Option<String> {
    params
        .get("_meta")?
        .get(SERVER_VARIANT_META_KEY)?
        .as_str()
        .map(str::to_string)
}

fn payload(capabilities: &Value) -> Option<&Vec<Value>> {
    capabilities
        .get("extensions")?
        .get(EXTENSION_ID)?
        .get("availableVariants")?
        .as_array()
}

/// The variant ids a backend advertises, in declared (ranked) order.
pub fn available_variants(capabilities: &Value) -> Vec<String> {
    match payload(capabilities) {
        Some(variants) => variants
            .iter()
            .filter_map(|v| v.get("id").and_then(Value::as_str).map(str::to_string))
            .collect(),
        None => Vec::new(),
    }
}

/// Compose the proxy's `availableVariants` across backends (SEP-2053).
///
/// A variant is offered only if every variant-supporting backend offers it (an
/// intersection): the proxy cannot honestly serve a variant one of its backends
/// cannot. Backends without the extension are variant-agnostic and impose no
/// constraint. Order and descriptions follow the first supporting backend, so its
/// default (the first entry) stays the proxy's default. Returns `[]` when no
/// backend supports variants or the intersection is empty.
pub fn compose_variants(backend_caps: &[Value]) -> Vec<Value> {
    let supporting: Vec<&Vec<Value>> =
        backend_caps.iter().filter_map(payload).filter(|v| !v.is_empty()).collect();
    if supporting.is_empty() {
        return Vec::new();
    }
    let id_sets: Vec<HashSet<String>> = supporting
        .iter()
        .map(|variants| {
            variants.iter().filter_map(|v| v.get("id").and_then(Value::as_str).map(str::to_string)).collect()
        })
        .collect();
    let mut common = id_sets[0].clone();
    for set in &id_sets[1..] {
        common = common.intersection(set).cloned().collect();
    }
    supporting[0]
        .iter()
        .filter(|v| v.get("id").and_then(Value::as_str).map(|id| common.contains(id)).unwrap_or(false))
        .cloned()
        .collect()
}

/// Mint one opaque composite cursor binding the active variant and each
/// paginating backend's own cursor (SEP-2053 rule 2). Canonical JSON, hex-encoded
/// so the cursor is opaque and the proxy holds no per-cursor state.
pub fn bind_cursor(variant: Option<&str>, cursors: &BTreeMap<String, String>) -> String {
    let body = json!({ "v": variant, "c": cursors });
    let bytes = serde_json::to_vec(&body).expect("a serde_json::Value always serializes");
    let mut out = String::from(CURSOR_PREFIX);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Reverse a proxy composite cursor to `(variant, {backend_id: cursor})`.
///
/// Returns `None` when the value is not a proxy-minted cursor or is malformed, so
/// a raw backend cursor or a hostile string is never mistaken for one.
pub fn resolve_cursor(cursor: &Value) -> Option<(Option<String>, BTreeMap<String, String>)> {
    let text = cursor.as_str()?;
    let hex = text.strip_prefix(CURSOR_PREFIX)?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let raw = hex.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        let hi = (raw[i] as char).to_digit(16)?;
        let lo = (raw[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    let payload: Value = serde_json::from_slice(&bytes).ok()?;
    let cursors_obj = payload.get("c").and_then(Value::as_object)?;
    let cursors: BTreeMap<String, String> = cursors_obj
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect();
    let variant = payload.get("v").and_then(Value::as_str).map(str::to_string);
    Some((variant, cursors))
}

/// The `-32602` error data for a cursor used under the wrong variant
/// (SEP-2053 rule 3).
pub fn mismatch_data(cursor_variant: Option<&str>, requested_variant: Option<&str>) -> Value {
    json!({ "cursorVariant": cursor_variant, "requestedVariant": requested_variant })
}
