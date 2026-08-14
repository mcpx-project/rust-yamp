//! δ7-δ9 conformance tests (Rust arm): policy, capability, observability.

use std::collections::HashMap;

use serde_json::{json, Value};

use yamp::capability::{
    compose, disclose, search_tool_definition, search_tools, PROXY_SEARCH_TOOL,
};
use yamp::observability::{
    append_hop, ensure_trace_context, make_traceparent, proxy_hop, BAGGAGE, PROXY_HOPS_KEY,
    TRACEPARENT, TRACESTATE,
};
use yamp::policy::{BearerAuthenticator, ForwardRule, PolicyLayer, AUTHORIZATION};

// ---- correctness regression: namespace delimiter collision ----

#[test]
fn backend_id_with_delimiter_rejected() {
    use yamp::namespace;
    // An id containing "__" would break reverse resolution.
    assert!(!namespace::valid_backend_id("a__b"));
    assert!(namespace::valid_backend_id("a_b")); // single underscore is fine
    assert_eq!(namespace::split(&namespace::prefix("a__b", "tool")), Some(("a", "b__tool")));
}

// ---- δ7 policy ----

fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn credential_injection_does_not_leak_client_token() {
    let mut tokens = HashMap::new();
    tokens.insert("github".to_string(), "GH_SECRET".to_string());
    let policy = PolicyLayer::new(tokens, HashMap::new(), None);
    let out = policy.backend_headers("github", &headers(&[(AUTHORIZATION, "Bearer CLIENT_TOKEN")]));
    assert_eq!(out.get(AUTHORIZATION).unwrap(), "Bearer GH_SECRET");
    assert!(!out.get(AUTHORIZATION).unwrap().contains("CLIENT_TOKEN"));
}

#[test]
fn backend_without_credentials_is_empty() {
    let mut tokens = HashMap::new();
    tokens.insert("github".to_string(), "GH".to_string());
    let policy = PolicyLayer::new(tokens, HashMap::new(), None);
    assert!(policy.backend_headers("slack", &HashMap::new()).is_empty());
}

#[test]
fn header_forwarding_scoped_and_renamed() {
    let mut forward = HashMap::new();
    forward.insert(
        "atlassian".to_string(),
        vec![
            ForwardRule::new("X-Atlassian-Token", None),
            ForwardRule::new(AUTHORIZATION, Some("X-Original-Auth")),
        ],
    );
    let policy = PolicyLayer::new(HashMap::new(), forward, None);
    let client = headers(&[("X-Atlassian-Token", "tok"), (AUTHORIZATION, "Bearer C")]);
    let atlassian = policy.backend_headers("atlassian", &client);
    assert_eq!(atlassian.get("X-Atlassian-Token").unwrap(), "tok");
    assert_eq!(atlassian.get("X-Original-Auth").unwrap(), "Bearer C");
    assert!(policy.backend_headers("github", &client).is_empty());
}

#[test]
fn client_authentication() {
    let policy = PolicyLayer::new(
        HashMap::new(),
        HashMap::new(),
        Some(Box::new(BearerAuthenticator::new(["good".to_string()]))),
    );
    assert!(policy.authorize_client(&headers(&[(AUTHORIZATION, "Bearer good")])));
    assert!(!policy.authorize_client(&headers(&[(AUTHORIZATION, "Bearer bad")])));
    assert!(!policy.authorize_client(&HashMap::new()));
    assert!(PolicyLayer::default().authorize_client(&HashMap::new()));
}

// ---- δ8 capability ----

fn tools(names: &[&str]) -> Vec<Value> {
    names.iter().map(|n| json!({ "name": n, "description": format!("{n} tool") })).collect()
}

#[test]
fn compose_modes() {
    let union = compose(&[tools(&["a", "b"]), tools(&["c"])], "union", None).unwrap();
    let union_names: Vec<&str> = union.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(union_names, vec!["a", "b", "c"]);

    let inter = compose(&[tools(&["a", "b"]), tools(&["b", "c"])], "intersection", None).unwrap();
    assert_eq!(inter.iter().map(|t| t["name"].as_str().unwrap()).collect::<Vec<_>>(), vec!["b"]);

    let curated = compose(&[tools(&["a", "b"]), tools(&["c"])], "curated", Some(&["a".into(), "c".into()])).unwrap();
    let curated_names: std::collections::BTreeSet<&str> =
        curated.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(curated_names, ["a", "c"].into_iter().collect());

    assert!(compose(&[], "intersection", None).unwrap().is_empty());
    assert!(compose(&[], "nonsense", None).is_err());
}

#[test]
fn search_matches_name_and_description() {
    let t = vec![
        json!({ "name": "gh__create_issue", "description": "open an issue" }),
        json!({ "name": "gh__search", "description": "find code" }),
    ];
    assert_eq!(search_tools("issue", &t)[0]["name"], "gh__create_issue");
    assert_eq!(search_tools("find", &t)[0]["name"], "gh__search");
}

#[test]
fn disclose_threshold_behavior() {
    let small = tools(&["a", "b"]);
    let (advertised, has_search) = disclose(&small, 40);
    assert_eq!(advertised, small);
    assert!(!has_search);

    let names: Vec<String> = (0..50).map(|i| format!("t{i}")).collect();
    let big = tools(&names.iter().map(String::as_str).collect::<Vec<_>>());
    let (advertised, has_search) = disclose(&big, 40);
    assert!(has_search);
    assert_eq!(advertised.len(), 41);
    assert_eq!(advertised.last().unwrap()["name"], PROXY_SEARCH_TOOL);
    assert_eq!(search_tool_definition()["inputSchema"]["required"], json!(["query"]));
}

// ---- δ9 observability ----

fn fixed_ids() -> (String, String) {
    ("0af7651916cd43dd8448eb211c80319c".to_string(), "b7ad6b7169203331".to_string())
}

#[test]
fn existing_trace_context_preserved() {
    let meta = json!({ TRACEPARENT: "00-aaaa-bbbb-01", TRACESTATE: "vendor=1", BAGGAGE: "k=v" });
    let out = ensure_trace_context(&meta, fixed_ids);
    assert_eq!(out[TRACEPARENT], "00-aaaa-bbbb-01");
    assert_eq!(out[TRACESTATE], "vendor=1");
    assert_eq!(out[BAGGAGE], "k=v");
}

#[test]
fn traceparent_generated_when_absent() {
    let out = ensure_trace_context(&json!({}), fixed_ids);
    let (trace_id, span_id) = fixed_ids();
    assert_eq!(out[TRACEPARENT], make_traceparent(&trace_id, &span_id));
    assert_eq!(out[TRACEPARENT], "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
}

#[test]
fn hop_append_reused() {
    let once = append_hop(&json!({}), "transparent");
    let twice = append_hop(&once, "transparent");
    assert_eq!(once[PROXY_HOPS_KEY], json!([proxy_hop("transparent")]));
    assert_eq!(twice[PROXY_HOPS_KEY], json!([proxy_hop("transparent"), proxy_hop("transparent")]));
}
