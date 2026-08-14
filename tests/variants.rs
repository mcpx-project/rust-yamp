//! Unit tests for the server-variants module (SEP-2053).
//!
//! Mirrors the Python arm's tests/test_variants.py.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use yamp::variants;

fn caps(ids: &[&str]) -> Value {
    let variants: Vec<Value> = ids.iter().map(|i| json!({ "id": i, "description": format!("{i} variant") })).collect();
    json!({ "extensions": { variants::EXTENSION_ID: { "availableVariants": variants } } })
}

fn ids(composed: &[Value]) -> Vec<String> {
    composed.iter().filter_map(|v| v.get("id").and_then(Value::as_str).map(str::to_string)).collect()
}

#[test]
fn selected_variant_reads_meta_key() {
    let params = json!({ "_meta": { variants::SERVER_VARIANT_META_KEY: "compact" } });
    assert_eq!(variants::selected_variant(&params), Some("compact".to_string()));
}

#[test]
fn selected_variant_absent_forms() {
    assert_eq!(variants::selected_variant(&json!({})), None);
    assert_eq!(variants::selected_variant(&json!({ "_meta": {} })), None);
    assert_eq!(variants::selected_variant(&json!({ "_meta": { variants::SERVER_VARIANT_META_KEY: 7 } })), None);
}

#[test]
fn available_variants_declared_order() {
    assert_eq!(variants::available_variants(&caps(&["a", "b", "c"])), vec!["a", "b", "c"]);
}

#[test]
fn available_variants_missing_or_malformed() {
    assert!(variants::available_variants(&json!({})).is_empty());
    assert!(variants::available_variants(&json!({ "extensions": { variants::EXTENSION_ID: {} } })).is_empty());
    assert!(variants::available_variants(&json!({ "extensions": { variants::EXTENSION_ID: { "availableVariants": "x" } } })).is_empty());
    assert!(variants::available_variants(&json!({ "extensions": { variants::EXTENSION_ID: { "availableVariants": [{ "noid": 1 }] } } })).is_empty());
}

#[test]
fn compose_variants_intersects_across_supporters() {
    let composed = variants::compose_variants(&[caps(&["a", "b", "c"]), caps(&["b", "c", "d"]), json!({})]);
    assert_eq!(ids(&composed), vec!["b", "c"]); // order from the first supporter
}

#[test]
fn compose_variants_none_supported() {
    assert!(variants::compose_variants(&[json!({}), json!({ "capabilities": {} })]).is_empty());
}

#[test]
fn compose_variants_empty_intersection() {
    assert!(variants::compose_variants(&[caps(&["a"]), caps(&["b"])]).is_empty());
}

#[test]
fn compose_variants_single_supporter_keeps_default_first() {
    let composed = variants::compose_variants(&[caps(&["claude", "compact"]), json!({})]);
    assert_eq!(ids(&composed), vec!["claude", "compact"]);
}

#[test]
fn cursor_round_trips_variant_and_backends() {
    let mut cursors = BTreeMap::new();
    cursors.insert("b0".to_string(), "p2".to_string());
    cursors.insert("b1".to_string(), "p5".to_string());
    let cursor = variants::bind_cursor(Some("compact"), &cursors);
    assert!(cursor.starts_with(variants::CURSOR_PREFIX));
    let (variant, resolved) = variants::resolve_cursor(&json!(cursor)).unwrap();
    assert_eq!(variant, Some("compact".to_string()));
    assert_eq!(resolved, cursors);
}

#[test]
fn cursor_binds_default_none_variant() {
    let mut cursors = BTreeMap::new();
    cursors.insert("b0".to_string(), "p1".to_string());
    let (variant, resolved) = variants::resolve_cursor(&json!(variants::bind_cursor(None, &cursors))).unwrap();
    assert_eq!(variant, None);
    assert_eq!(resolved, cursors);
}

#[test]
fn resolve_cursor_rejects_non_proxy_values() {
    assert!(variants::resolve_cursor(&Value::Null).is_none());
    assert!(variants::resolve_cursor(&json!(42)).is_none());
    assert!(variants::resolve_cursor(&json!("raw-backend-cursor")).is_none()); // no proxy prefix
}

#[test]
fn resolve_cursor_rejects_malformed_payloads() {
    let hex = |s: &[u8]| -> String { s.iter().map(|b| format!("{b:02x}")).collect() };
    assert!(variants::resolve_cursor(&json!(format!("{}zz", variants::CURSOR_PREFIX))).is_none()); // bad hex
    assert!(variants::resolve_cursor(&json!(format!("{}{}", variants::CURSOR_PREFIX, hex(b"not json")))).is_none());
    assert!(variants::resolve_cursor(&json!(format!("{}{}", variants::CURSOR_PREFIX, hex(br#"{"c":"x"}"#)))).is_none()); // c not object
    assert!(variants::resolve_cursor(&json!(format!("{}{}", variants::CURSOR_PREFIX, hex(b"[1,2]")))).is_none()); // not an object
    assert!(variants::resolve_cursor(&json!(format!("{}abc", variants::CURSOR_PREFIX))).is_none()); // odd hex length
}

#[test]
fn resolve_cursor_filters_non_string_entries_and_variant() {
    let hex: String = br#"{"v":9,"c":{"b0":"p1","b1":5}}"#.iter().map(|b| format!("{b:02x}")).collect();
    let (variant, cursors) = variants::resolve_cursor(&json!(format!("{}{}", variants::CURSOR_PREFIX, hex))).unwrap();
    assert_eq!(variant, None); // non-string variant normalized to None
    let mut expected = BTreeMap::new();
    expected.insert("b0".to_string(), "p1".to_string());
    assert_eq!(cursors, expected); // non-string cursor dropped
}

#[test]
fn mismatch_data_shape() {
    assert_eq!(
        variants::mismatch_data(Some("claude"), Some("compact")),
        json!({ "cursorVariant": "claude", "requestedVariant": "compact" })
    );
}
