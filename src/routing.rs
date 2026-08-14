//! Routing intelligence (corpus SEP-2564, SEP-2614, SEP-2127).
//!
//! - Server-side filtering (SEP-2564): a `filter` on list methods carries
//!   `namePatterns` so an aggregating gateway can drop non-matching results and
//!   push the filter down to backends.
//! - Keyword routing (SEP-2614): a backend declares `keywords`; a filter that
//!   carries keyword hints lets the proxy pre-select only the backends that can
//!   match, cutting fan-out.
//! - Server Cards (SEP-2127): a pre-connection discovery document the proxy
//!   publishes about itself.

use std::collections::HashSet;

use serde_json::{json, Value};

use crate::forward::proxy_server_info;
use crate::version::SUPPORTED_PROTOCOL_VERSIONS;

fn glob(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    match (pattern.strip_prefix('*'), pattern.strip_suffix('*')) {
        (Some(inner), Some(_)) => name.contains(inner.strip_suffix('*').unwrap_or(inner)),
        (Some(suffix), None) => name.ends_with(suffix),
        (None, Some(prefix)) => name.starts_with(prefix),
        (None, None) => name == pattern,
    }
}

/// Whether `name` matches any glob-ish pattern. No patterns matches all. A
/// single `*` is a wildcard: `a*` prefix, `*a` suffix, `*a*` contains, `*`
/// everything, otherwise an exact match.
pub fn name_matches(name: &str, patterns: &[Value]) -> bool {
    if patterns.is_empty() {
        return true;
    }
    patterns.iter().filter_map(Value::as_str).any(|pattern| glob(pattern, name))
}

/// Whether a backend should be queried for a keyword-filtered list. A backend is
/// skipped only when it declares keywords and none intersect the filter's
/// keywords; a backend without declared keywords is always queried (SEP-2614).
pub fn backend_selected(backend_keywords: &[String], filter_keywords: &[Value]) -> bool {
    if backend_keywords.is_empty() || filter_keywords.is_empty() {
        return true;
    }
    let wanted: HashSet<&str> = filter_keywords.iter().filter_map(Value::as_str).collect();
    backend_keywords.iter().any(|k| wanted.contains(k.as_str()))
}

/// The proxy's self-description for `.well-known` discovery (SEP-2127).
pub fn server_card() -> Value {
    let info = proxy_server_info();
    json!({
        "name": info["name"],
        "version": info["version"],
        "protocolVersions": SUPPORTED_PROTOCOL_VERSIONS,
        "transports": ["stdio", "streamable-http"],
        "role": "intermediary",
    })
}
