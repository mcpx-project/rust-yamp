//! δ16 collision strategy unit tests (Rust arm). Mirrors the Python arm.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{json, Value};
use yamp::collision::{apply_priority, is_strategy, resolve_manual, STRATEGIES};
use yamp::namespace;

fn split(name: &str) -> Option<(String, String)> {
    namespace::split(name).map(|(a, b)| (a.to_string(), b.to_string()))
}

fn names(tools: &[Value]) -> Vec<String> {
    tools.iter().map(|t| t["name"].as_str().unwrap().to_string()).collect()
}

#[test]
fn strategy_set() {
    assert_eq!(STRATEGIES.len(), 4);
    assert!(is_strategy("prefix") && is_strategy("priority") && is_strategy("manual") && is_strategy("passthrough"));
    assert!(!is_strategy("bogus"));
}

#[test]
fn apply_priority_keeps_highest_and_discards_rest() {
    let tools = vec![
        json!({ "name": "gh__search" }),
        json!({ "name": "gl__search" }),
        json!({ "name": "gh__only_gh" }),
        json!({ "name": "gl__only_gl" }),
    ];
    let discarded = Mutex::new(Vec::new());
    let kept = apply_priority(tools, split, &["gh".into(), "gl".into()], "name", |t| {
        discarded.lock().unwrap().push(t["name"].as_str().unwrap().to_string())
    });
    let mut got = names(&kept);
    got.sort();
    assert_eq!(got, ["gh__only_gh", "gh__search", "gl__only_gl"]);
    assert_eq!(*discarded.lock().unwrap(), ["gl__search"]);
}

#[test]
fn apply_priority_lower_first_still_keeps_higher() {
    let tools = vec![json!({ "name": "gl__search" }), json!({ "name": "gh__search" })];
    let discarded = Mutex::new(Vec::new());
    let kept = apply_priority(tools, split, &["gh".into(), "gl".into()], "name", |t| {
        discarded.lock().unwrap().push(t["name"].as_str().unwrap().to_string())
    });
    assert_eq!(names(&kept), ["gh__search"]);
    assert_eq!(*discarded.lock().unwrap(), ["gl__search"]);
}

#[test]
fn apply_priority_unlisted_backend_ranks_lowest() {
    let tools = vec![json!({ "name": "unlisted__t" }), json!({ "name": "gh__t" })];
    let kept = apply_priority(tools, split, &["gh".into()], "name", |_| {});
    assert_eq!(names(&kept), ["gh__t"]);
}

#[test]
fn resolve_manual_applies_overrides() {
    let mut overrides = HashMap::new();
    overrides.insert("github__create_issue".to_string(), "gh_new_issue".to_string());
    let mapping = resolve_manual(
        &["github__create_issue".to_string(), "gh__search".to_string()],
        &overrides,
    )
    .unwrap();
    assert_eq!(mapping.get("github__create_issue").map(String::as_str), Some("gh_new_issue"));
    assert_eq!(mapping.get("gh__search").map(String::as_str), Some("gh__search"));
}

#[test]
fn resolve_manual_rejects_unresolved_collision() {
    let mut overrides = HashMap::new();
    overrides.insert("gh__x".to_string(), "shared".to_string());
    overrides.insert("gl__y".to_string(), "shared".to_string());
    assert!(resolve_manual(&["gh__x".to_string(), "gl__y".to_string()], &overrides).is_err());
}
