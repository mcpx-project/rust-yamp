//! δ18 routing-intelligence unit tests (Rust arm). Mirrors the Python arm.

use serde_json::json;
use yamp::routing::{backend_selected, name_matches, server_card};
use yamp::version::SUPPORTED_PROTOCOL_VERSIONS;

#[test]
fn name_matches_globs() {
    let p = |s: &str| vec![json!(s)];
    assert!(name_matches("gh__create_issue", &[])); // no patterns matches all
    assert!(name_matches("gh__create_issue", &p("gh__*"))); // prefix
    assert!(name_matches("gh__create_issue", &p("*issue"))); // suffix
    assert!(name_matches("gh__create_issue", &p("*create*"))); // contains
    assert!(name_matches("gh__create_issue", &p("*"))); // wildcard
    assert!(name_matches("gh__create_issue", &p("gh__create_issue"))); // exact
    assert!(!name_matches("gh__search", &p("gh__create*")));
    assert!(!name_matches("gh__search", &p("gl__*")));
}

#[test]
fn backend_selected_by_keywords() {
    let kw = |s: &str| vec![json!(s)];
    assert!(backend_selected(&["git".to_string()], &[])); // no filter keywords
    assert!(backend_selected(&[], &kw("git"))); // no backend keywords
    assert!(backend_selected(&["git".to_string(), "vcs".to_string()], &kw("vcs")));
    assert!(!backend_selected(&["git".to_string()], &kw("chat")));
}

#[test]
fn server_card_describes_the_proxy() {
    let card = server_card();
    assert_eq!(card["name"], "yamp");
    assert_eq!(card["role"], "intermediary");
    assert_eq!(card["protocolVersions"], json!(SUPPORTED_PROTOCOL_VERSIONS));
    assert!(card["transports"].as_array().unwrap().iter().any(|t| t == "streamable-http"));
}
