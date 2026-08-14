//! MCP media type negotiation (corpus SEP-2357).
//!
//! SEP-2357 gives MCP-over-HTTP its own media type, `application/mcp+json`, so a
//! gateway, load balancer, or WAF can identify MCP traffic without parsing the
//! body. yamp emits it when the client accepts it and falls back to
//! `application/json` during the transition. The functions here are the single
//! source of that negotiation, shared by every HTTP entrypoint.

pub const MCP_JSON: &str = "application/mcp+json";
pub const JSON: &str = "application/json";

fn media_type(part: &str) -> &str {
    part.split(';').next().unwrap_or("").trim()
}

/// The Content-Type to answer with, given the request's `Accept` header. Prefer
/// `application/mcp+json` when the client accepts it explicitly or via a
/// wildcard; otherwise fall back to `application/json`.
pub fn response_content_type(accept: Option<&str>) -> &'static str {
    let accept = match accept {
        Some(value) if !value.is_empty() => value,
        _ => return JSON,
    };
    for token in accept.split(',').map(media_type) {
        if token == MCP_JSON || token == "application/*" || token == "*/*" {
            return MCP_JSON;
        }
    }
    JSON
}

/// Whether a `Content-Type` names the MCP media type (parameters ignored).
pub fn is_mcp_json(content_type: Option<&str>) -> bool {
    match content_type {
        Some(value) if !value.is_empty() => media_type(value) == MCP_JSON,
        _ => false,
    }
}
