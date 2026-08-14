//! δ15 MCP media type negotiation unit tests (Rust arm). Mirrors the Python arm.

use yamp::media::{is_mcp_json, response_content_type, JSON, MCP_JSON};

#[test]
fn prefers_mcp_json_when_accepted_explicitly() {
    assert_eq!(response_content_type(Some("application/mcp+json")), MCP_JSON);
    assert_eq!(response_content_type(Some("application/json, application/mcp+json")), MCP_JSON);
}

#[test]
fn wildcards_get_mcp_json() {
    assert_eq!(response_content_type(Some("*/*")), MCP_JSON);
    assert_eq!(response_content_type(Some("application/*")), MCP_JSON);
    assert_eq!(response_content_type(Some("text/html, */*;q=0.8")), MCP_JSON);
}

#[test]
fn falls_back_to_json() {
    assert_eq!(response_content_type(Some("application/json")), JSON);
    assert_eq!(response_content_type(Some("text/plain")), JSON);
    assert_eq!(response_content_type(None), JSON);
    assert_eq!(response_content_type(Some("")), JSON);
}

#[test]
fn ignores_accept_parameters() {
    assert_eq!(response_content_type(Some("application/mcp+json;q=0.9")), MCP_JSON);
}

#[test]
fn detects_mcp_json_content_type() {
    assert!(is_mcp_json(Some("application/mcp+json")));
    assert!(is_mcp_json(Some("application/mcp+json; charset=utf-8")));
    assert!(!is_mcp_json(Some("application/json")));
    assert!(!is_mcp_json(None));
    assert!(!is_mcp_json(Some("")));
}
