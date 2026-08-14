//! Track U: the `yamp-config` CLI entrypoint (config explain / effective).
//!
//! Spawns the real binary over temp config files and asserts the rendered line and
//! exit code. Mirrors the Python arm's test_config_explain.py CLI cases.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::{json, Value};

fn write_config(name: &str, data: Value) -> PathBuf {
    let path = env::temp_dir().join(format!("yamp-config-{}-{name}.json", std::process::id()));
    fs::write(&path, serde_json::to_vec(&data).unwrap()).unwrap();
    path
}

fn run(args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_yamp-config")).args(args).output().unwrap();
    (out.status.code().unwrap(), String::from_utf8(out.stdout).unwrap())
}

#[test]
fn explain_config_and_default() {
    let path = write_config("explain", json!({ "listen": "127.0.0.1:9100", "resilience": { "failureThreshold": 9 } }));
    let p = path.to_str().unwrap();
    let (code, out) = run(&["explain", "--config", p, "resilience.failureThreshold"]);
    assert_eq!(code, 0);
    assert_eq!(out.trim_end(), "resilience.failureThreshold = 9 (config)");
    let (code, out) = run(&["explain", "--config", p, "resilience.resetTimeout"]);
    fs::remove_file(&path).ok();
    assert_eq!(code, 0);
    assert_eq!(out.trim_end(), "resilience.resetTimeout = 30.0 (default)");
}

#[test]
fn explain_unknown_key_exits_two() {
    let path = write_config("unknown", json!({ "listen": "127.0.0.1:9100" }));
    let (code, out) = run(&["explain", "--config", path.to_str().unwrap(), "bogus.key"]);
    fs::remove_file(&path).ok();
    assert_eq!(code, 2);
    assert_eq!(out.trim_end(), "bogus.key = null (unknown)");
}

#[test]
fn diff_reports_changed_keys_and_identical() {
    let a = write_config("diffa", json!({ "listen": "127.0.0.1:9100" }));
    let b = write_config("diffb", json!({ "listen": "127.0.0.1:9100", "resilience": { "failureThreshold": 9 } }));
    let (code, out) = run(&["diff", "--config", a.to_str().unwrap(), "--to", b.to_str().unwrap()]);
    assert_eq!(code, 1);
    assert_eq!(out.trim_end(), "resilience.failureThreshold: 5 (default) -> 9 (config)");
    // Identical documents: no differences, exit 0.
    let (code, out) = run(&["diff", "--config", a.to_str().unwrap(), "--to", a.to_str().unwrap()]);
    fs::remove_file(&a).ok();
    fs::remove_file(&b).ok();
    assert_eq!(code, 0);
    assert_eq!(out.trim_end(), "no differences");
}

#[test]
fn validate_valid_and_invalid() {
    let good = write_config("valid", json!({ "listen": "127.0.0.1:9100", "backends": { "b0": { "address": "127.0.0.1:9101" } } }));
    let (code, out) = run(&["validate", "--config", good.to_str().unwrap()]);
    assert_eq!(code, 0);
    assert_eq!(out.trim_end(), "config valid");
    // A missing 'listen' fails schema conformance: invalid, exit 1 (not 2).
    let bad = write_config("invalid", json!({ "backends": { "b0": { "address": "127.0.0.1:9101" } } }));
    let (code, out) = run(&["validate", "--config", bad.to_str().unwrap()]);
    fs::remove_file(&good).ok();
    fs::remove_file(&bad).ok();
    assert_eq!(code, 1);
    assert!(out.starts_with("config invalid:"), "stdout was: {out}");
}

#[test]
fn validate_json_carries_slug_hint_and_docs() {
    // U4: a schema error carries a slug, fix hint, and docs URL.
    let bad = write_config("u4", json!({ "listen": "127.0.0.1:9100", "namespacing": { "strategy": "nope" } }));
    let (code, out) = run(&["validate", "--config", bad.to_str().unwrap(), "--json"]);
    fs::remove_file(&bad).ok();
    assert_eq!(code, 1);
    let report: Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(report["valid"], json!(false));
    assert_eq!(report["error"]["slug"], "unknown-collision-strategy");
    assert_eq!(report["error"]["docsUrl"], "CONFIG_ERRORS.md#unknown-collision-strategy");
    assert!(!report["error"]["hint"].as_str().unwrap().is_empty());
}

#[test]
fn validate_malformed_json_carries_line_column() {
    // U4: a parse error carries line/column plus a fix hint and docs URL.
    let path = env::temp_dir().join(format!("yamp-config-{}-malformed.json", std::process::id()));
    fs::write(&path, "{\n  \"listen\": ,\n}").unwrap();
    let (code, out) = run(&["validate", "--config", path.to_str().unwrap(), "--json"]);
    fs::remove_file(&path).ok();
    assert_eq!(code, 1);
    let report: Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(report["error"]["slug"], "invalid-json");
    assert!(report["error"]["line"].as_u64().unwrap() >= 1);
    assert_eq!(report["error"]["docsUrl"], "CONFIG_ERRORS.md#invalid-json");
}

#[test]
fn adapt_emits_canonical() {
    // U9: a human shorthand normalizes to canonical JSON that re-validates.
    let path = write_config("adapt", json!({ "listen": ":9100", "backends": { "b0": "127.0.0.1:9101" } }));
    let (code, out) = run(&["adapt", "--config", path.to_str().unwrap()]);
    fs::remove_file(&path).ok();
    assert_eq!(code, 0);
    let canonical: Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(canonical["listen"], "127.0.0.1:9100");
    assert_eq!(canonical["backends"]["b0"]["addresses"], json!(["127.0.0.1:9101"]));
}

#[test]
fn effective_json_lists_every_key() {
    let path = write_config("effective", json!({ "listen": "127.0.0.1:9100" }));
    let (code, out) = run(&["effective", "--config", path.to_str().unwrap(), "--json"]);
    fs::remove_file(&path).ok();
    assert_eq!(code, 0);
    let entries: Value = serde_json::from_str(out.trim()).unwrap();
    let keys: Vec<&str> = entries.as_array().unwrap().iter().map(|e| e["key"].as_str().unwrap()).collect();
    assert!(keys.contains(&"listen") && keys.contains(&"namespacing.strategy"));
    let listen = entries.as_array().unwrap().iter().find(|e| e["key"] == "listen").unwrap();
    assert_eq!(listen["source"], "config");
    assert_eq!(listen["value"], "127.0.0.1:9100");
}
