//! δ12 list-cache unit tests (Rust arm). Mirrors the Python arm.

use serde_json::json;
use yamp::cache::{ListCache, DEFAULT_TTL_MS};

#[test]
fn hit_within_ttl_then_expires() {
    let mut cache = ListCache::default();
    cache.put("gh", "tools/list", None, json!({ "tools": [{ "name": "a" }], "ttlMs": 1000 }), 0.0);
    assert!(cache.get("gh", "tools/list", None, 0.0).is_some());
    assert!(cache.get("gh", "tools/list", None, 0.999).is_some());
    assert!(cache.get("gh", "tools/list", None, 1.0).is_none()); // ttl boundary: stale
}

#[test]
fn default_ttl_when_absent() {
    let mut cache = ListCache::default();
    cache.put("gh", "tools/list", None, json!({ "tools": [] }), 0.0);
    assert!(cache.get("gh", "tools/list", None, DEFAULT_TTL_MS / 1000.0 - 1.0).is_some());
    assert!(cache.get("gh", "tools/list", None, DEFAULT_TTL_MS / 1000.0).is_none());
}

#[test]
fn ttl_zero_or_negative_not_cached() {
    let mut cache = ListCache::default();
    cache.put("gh", "tools/list", None, json!({ "tools": [], "ttlMs": 0 }), 0.0);
    assert!(cache.get("gh", "tools/list", None, 0.0).is_none());
    cache.put("gh", "tools/list", None, json!({ "tools": [], "ttlMs": -5 }), 0.0);
    assert!(cache.get("gh", "tools/list", None, 0.0).is_none());
}

#[test]
fn ttl_zero_drops_prior_entry() {
    let mut cache = ListCache::default();
    cache.put("gh", "tools/list", None, json!({ "tools": [{ "name": "a" }], "ttlMs": 1000 }), 0.0);
    assert!(cache.get("gh", "tools/list", None, 0.0).is_some());
    cache.put("gh", "tools/list", None, json!({ "tools": [], "ttlMs": 0 }), 0.0);
    assert!(cache.get("gh", "tools/list", None, 0.0).is_none());
}

#[test]
fn non_numeric_ttl_falls_back_to_default() {
    let mut cache = ListCache::default();
    // A bool or string is not a valid ttl.
    cache.put("gh", "tools/list", None, json!({ "tools": [], "ttlMs": true }), 0.0);
    assert!(cache.get("gh", "tools/list", None, 0.0).is_some());
    cache.put("gl", "tools/list", None, json!({ "tools": [], "ttlMs": "soon" }), 0.0);
    assert!(cache.get("gl", "tools/list", None, 0.0).is_some());
}

#[test]
fn public_served_to_any_principal() {
    let mut cache = ListCache::default();
    cache.put(
        "gh",
        "tools/list",
        Some("alice"),
        json!({ "tools": [{ "name": "a" }], "ttlMs": 1000, "cacheScope": "public" }),
        0.0,
    );
    assert!(cache.get("gh", "tools/list", Some("bob"), 0.0).is_some());
    assert!(cache.get("gh", "tools/list", None, 0.0).is_some());
}

#[test]
fn private_isolated_across_principals() {
    let mut cache = ListCache::default();
    cache.put(
        "gh",
        "tools/list",
        Some("alice"),
        json!({ "tools": [{ "name": "secret" }], "ttlMs": 1000, "cacheScope": "private" }),
        0.0,
    );
    assert!(cache.get("gh", "tools/list", Some("alice"), 0.0).is_some());
    assert!(cache.get("gh", "tools/list", Some("bob"), 0.0).is_none());
    assert!(cache.get("gh", "tools/list", None, 0.0).is_none());
}

#[test]
fn unknown_scope_treated_as_public() {
    let mut cache = ListCache::default();
    cache.put(
        "gh",
        "tools/list",
        Some("alice"),
        json!({ "tools": [], "ttlMs": 1000, "cacheScope": "weird" }),
        0.0,
    );
    assert!(cache.get("gh", "tools/list", Some("bob"), 0.0).is_some());
}

#[test]
fn invalidate_backend_drops_only_that_backend() {
    let mut cache = ListCache::default();
    cache.put("gh", "tools/list", None, json!({ "tools": [], "ttlMs": 1000 }), 0.0);
    cache.put("gh", "prompts/list", None, json!({ "prompts": [], "ttlMs": 1000 }), 0.0);
    cache.put("gl", "tools/list", None, json!({ "tools": [], "ttlMs": 1000 }), 0.0);
    cache.invalidate_backend("gh");
    assert!(cache.get("gh", "tools/list", None, 0.0).is_none());
    assert!(cache.get("gh", "prompts/list", None, 0.0).is_none());
    assert!(cache.get("gl", "tools/list", None, 0.0).is_some());
}

#[test]
fn miss_on_empty_cache() {
    let mut cache = ListCache::default();
    assert!(cache.get("gh", "tools/list", None, 0.0).is_none());
}
