//! Layer 7 capability (SEP §2.3, §9, draft §6.7).
//!
//! Capability composition across backends (union, intersection, curated) and
//! progressive disclosure with a `proxy__search_tools` meta-tool.

use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

pub const DEFAULT_TOOL_THRESHOLD: usize = 40;
pub const PROXY_SEARCH_TOOL: &str = "proxy__search_tools";

pub fn search_tool_definition() -> Value {
    json!({
        "name": PROXY_SEARCH_TOOL,
        "description": "Search for additional tools available through this proxy",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "Keyword to search tool names and descriptions" }
            },
            "required": ["query"],
        },
    })
}

fn tool_name(tool: &Value) -> &str {
    tool.get("name").and_then(Value::as_str).unwrap_or("")
}

pub fn compose(per_backend: &[Vec<Value>], mode: &str, curated: Option<&[String]>) -> Result<Vec<Value>, String> {
    match mode {
        "union" => Ok(per_backend.iter().flatten().cloned().collect()),
        "intersection" => {
            if per_backend.is_empty() {
                return Ok(Vec::new());
            }
            let mut common: Option<BTreeSet<String>> = None;
            for backend in per_backend {
                let names: BTreeSet<String> = backend.iter().map(|t| tool_name(t).to_string()).collect();
                common = Some(match common {
                    None => names,
                    Some(existing) => existing.intersection(&names).cloned().collect(),
                });
            }
            let common = common.unwrap_or_default();
            let mut seen = BTreeSet::new();
            let mut out = Vec::new();
            for backend in per_backend {
                for tool in backend {
                    let name = tool_name(tool).to_string();
                    if common.contains(&name) && seen.insert(name) {
                        out.push(tool.clone());
                    }
                }
            }
            Ok(out)
        }
        "curated" => {
            let allowed: BTreeSet<&str> = curated.unwrap_or(&[]).iter().map(String::as_str).collect();
            Ok(per_backend
                .iter()
                .flatten()
                .filter(|t| allowed.contains(tool_name(t)))
                .cloned()
                .collect())
        }
        other => Err(format!("unknown composition mode: {other}")),
    }
}

/// Compose the client-facing server capabilities per SEP §2.3.
///
/// Not a naive last-writer-wins union. `tools`/`resources`/`prompts` and
/// `logging`/`sampling` are advertised if ANY backend advertises them, with
/// their sub-flags merged. `elicitation` is advertised only if the CLIENT
/// supports it (the proxy elicits from the client, not a backend). `extensions`
/// are unioned across backends (SEP-2133).
pub fn compose_capabilities(backend_caps: &[Value], client_caps: Option<&Value>) -> Value {
    let mut composed = Map::new();
    for primitive in ["tools", "resources", "prompts", "logging", "sampling"] {
        let mut merged = Map::new();
        let mut present = false;
        for caps in backend_caps {
            if let Some(value) = caps.get(primitive) {
                present = true;
                if let Some(object) = value.as_object() {
                    for (key, sub) in object {
                        merged.insert(key.clone(), sub.clone());
                    }
                }
            }
        }
        if present {
            composed.insert(primitive.to_string(), Value::Object(merged));
        }
    }
    // elicitation follows the client, not the backends (SEP §2.3).
    if let Some(elicitation) = client_caps.and_then(|c| c.get("elicitation")) {
        composed.insert("elicitation".to_string(), elicitation.clone());
    }
    // extensions: union across backends.
    let mut extensions = Map::new();
    for caps in backend_caps {
        if let Some(object) = caps.get("extensions").and_then(Value::as_object) {
            for (name, value) in object {
                extensions.entry(name.clone()).or_insert_with(|| value.clone());
            }
        }
    }
    if !extensions.is_empty() {
        composed.insert("extensions".to_string(), Value::Object(extensions));
    }
    Value::Object(composed)
}

pub fn search_tools(query: &str, tools: &[Value]) -> Vec<Value> {
    let needle = query.to_lowercase();
    tools
        .iter()
        .filter(|tool| {
            tool_name(tool).to_lowercase().contains(&needle)
                || tool
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&needle)
        })
        .cloned()
        .collect()
}

/// Return the advertised surface and whether a search tool was added.
pub fn disclose(tools: &[Value], threshold: usize) -> (Vec<Value>, bool) {
    if tools.len() > threshold {
        let mut advertised = tools[..threshold].to_vec();
        advertised.push(search_tool_definition());
        (advertised, true)
    } else {
        (tools.to_vec(), false)
    }
}
