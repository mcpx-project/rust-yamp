//! δ17 handler/registry unit tests (Rust arm). Mirrors the Python arm.

use std::collections::BTreeSet;

use serde_json::{json, Value};
use yamp::handler::{BackendsHandler, CallFuture, Handler, Registry};

struct StubHandler {
    id: String,
    tools: Vec<&'static str>,
}

impl Handler for StubHandler {
    fn id(&self) -> &str {
        &self.id
    }
    fn list_tools(&self) -> Vec<Value> {
        self.tools.iter().map(|t| json!({ "name": t, "inputSchema": { "type": "object", "properties": {} } })).collect()
    }
    fn call_tool<'a>(&'a self, name: &'a str, _arguments: &'a Value) -> CallFuture<'a> {
        let text = format!("{}:{}", self.id, name);
        Box::pin(async move { json!({ "content": [{ "type": "text", "text": text }] }) })
    }
}

fn stub(id: &str, tools: Vec<&'static str>) -> Box<dyn Handler> {
    Box::new(StubHandler { id: id.to_string(), tools })
}

#[test]
fn registry_namespaces_tools_and_resolves() {
    let registry = Registry::new(vec![stub("a", vec!["x", "y"]), stub("b", vec!["z"])]).unwrap();
    let names: BTreeSet<String> = registry.list_tools().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect();
    let expected: BTreeSet<String> = ["a__x", "a__y", "b__z"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);
    assert_eq!(registry.ids(), ["a", "b"].iter().map(|s| s.to_string()).collect());
    assert_eq!(registry.handler_for("a").unwrap().id(), "a");
    assert!(registry.handler_for("missing").is_none());
}

#[test]
fn registry_rejects_invalid_and_duplicate_ids() {
    assert!(Registry::new(vec![stub("bad__id", vec!["x"])]).is_err());
    assert!(Registry::new(vec![stub("a", vec!["x"]), stub("a", vec!["y"])]).is_err());
}

#[test]
fn build_registry_from_config() {
    use std::collections::BTreeSet;
    use yamp::config::{HandlerConfig, RestHandlerConfig};
    use yamp::handler::build_registry;

    let config = HandlerConfig {
        meta_tools: true,
        rest: vec![RestHandlerConfig {
            id: "gh".to_string(),
            base_url: "https://api.example.com".to_string(),
            operations: vec![json!({ "name": "get_user" })],
        }],
    };
    let registry = build_registry(&config, || json!([{ "id": "b0" }])).unwrap();
    assert_eq!(registry.ids(), ["gh", "yamp"].iter().map(|s| s.to_string()).collect());
    let names: BTreeSet<String> = registry.list_tools().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect();
    let expected: BTreeSet<String> = ["gh__get_user", "yamp__backends"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);
}

#[test]
fn build_registry_without_meta_tools() {
    use yamp::config::HandlerConfig;
    use yamp::handler::build_registry;

    let registry = build_registry(&HandlerConfig::default(), || json!([])).unwrap();
    assert!(registry.ids().is_empty());
}

#[tokio::test]
async fn backends_handler_reports_backends() {
    let handler = BackendsHandler::new(|| json!([{ "id": "gh", "available": true }]));
    assert_eq!(handler.id(), "yamp");
    assert_eq!(handler.list_tools()[0]["name"], "backends");
    let result = handler.call_tool("backends", &json!({})).await;
    let text = result["content"][0]["text"].as_str().unwrap();
    assert_eq!(serde_json::from_str::<Value>(text).unwrap(), json!([{ "id": "gh", "available": true }]));
}
