//! Observability (SEP §7, §12): hop tracing and W3C Trace Context.
//!
//! Single home for the proxy-hop helpers (reused by the transparent Level 2
//! layer) and for W3C Trace Context propagation in `_meta`. Trace ids come from
//! an injected source so behavior is deterministic under test; production wires
//! a CSPRNG source.

use serde_json::{json, Map, Value};

use crate::forward::{PROXY_NAME, PROXY_VERSION};

pub const PROXY_HOPS_KEY: &str = "io.modelcontextprotocol/proxy-hops";
pub const TRACEPARENT: &str = "traceparent";
pub const TRACESTATE: &str = "tracestate";
pub const BAGGAGE: &str = "baggage";

pub fn proxy_hop(mode: &str) -> Value {
    json!({ "name": PROXY_NAME, "mode": mode, "version": PROXY_VERSION, "layers": [1, 2, 3, 4, 5] })
}

pub fn append_hop(meta: &Value, mode: &str) -> Value {
    let mut hops = meta
        .get(PROXY_HOPS_KEY)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    hops.push(proxy_hop(mode));
    let mut object = meta.as_object().cloned().unwrap_or_default();
    object.insert(PROXY_HOPS_KEY.to_string(), Value::Array(hops));
    Value::Object(object)
}

pub fn make_traceparent(trace_id: &str, span_id: &str) -> String {
    format!("00-{trace_id}-{span_id}-01")
}

/// Return `meta` with a W3C `traceparent` guaranteed present. An existing one
/// is preserved; `tracestate` and `baggage` are forwarded unchanged. When no
/// `traceparent` is present, one is generated from `new_ids`.
pub fn ensure_trace_context(meta: &Value, new_ids: impl Fn() -> (String, String)) -> Value {
    let mut object: Map<String, Value> = meta.as_object().cloned().unwrap_or_default();
    if !object.contains_key(TRACEPARENT) {
        let (trace_id, span_id) = new_ids();
        object.insert(TRACEPARENT.to_string(), Value::String(make_traceparent(&trace_id, &span_id)));
    }
    Value::Object(object)
}
