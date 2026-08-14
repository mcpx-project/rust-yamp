//! Extension filter chain (ε0): verdict set, failure policy, chain outcomes.

use serde_json::{json, Value};

use yamp::errors;
use yamp::filters::{self, Filter, FilterChain, FilterError};

#[test]
fn resolve_failure_by_policy() {
    assert_eq!(filters::resolve_failure(filters::FAIL_CLOSED), filters::DENY);
    assert_eq!(filters::resolve_failure(filters::FAIL_OPEN), filters::ALLOW);
    // An unknown policy is treated as advisory (allow), never a silent deny loop.
    assert_eq!(filters::resolve_failure("nonsense"), filters::ALLOW);
}

#[test]
fn deny_response_is_policy_error() {
    let response = filters::deny_response(&json!(9), "infected");
    assert_eq!(
        response,
        json!({"jsonrpc": "2.0", "id": 9, "error": {"code": errors::POLICY_DENIED, "message": "infected"}})
    );
}

#[test]
fn allow_forwards_unchanged() {
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "t"}});
    let out = filters::chain_outcome(&[json!({"kind": "allow"})], &req);
    assert_eq!(out, json!({"action": "forward", "message": req}));
}

#[test]
fn mutate_substitutes_arguments() {
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "t", "arguments": {"a": 1}}});
    let out = filters::chain_outcome(&[json!({"kind": "mutate", "arguments": {"a": 2}})], &req);
    assert_eq!(out["action"], "forward");
    assert_eq!(out["message"]["params"]["arguments"], json!({"a": 2}));
    assert_eq!(req["params"]["arguments"], json!({"a": 1}), "input must not be mutated in place");
}

#[test]
fn annotate_merges_provenance_into_meta() {
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "t", "_meta": {"trace": "x"}}});
    let out = filters::chain_outcome(&[json!({"kind": "annotate", "provenance": {"scanner": "clean"}})], &req);
    assert_eq!(out["message"]["params"]["_meta"], json!({"trace": "x", "scanner": "clean"}));
}

#[test]
fn deny_and_quarantine_block() {
    let req = json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {}});
    let denied = filters::chain_outcome(&[json!({"kind": "deny", "reason": "no"})], &req);
    assert_eq!(denied["action"], "block");
    assert_eq!(denied["quarantined"], json!(false));
    assert_eq!(denied["response"]["error"]["code"], json!(errors::POLICY_DENIED));
    let held = filters::chain_outcome(&[json!({"kind": "quarantine", "reason": "hold"})], &req);
    assert_eq!(held["action"], "block");
    assert_eq!(held["quarantined"], json!(true));
}

#[test]
fn deny_short_circuits_later_verdicts() {
    let req = json!({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"arguments": {"a": 1}}});
    let out = filters::chain_outcome(
        &[json!({"kind": "deny", "reason": "stop"}), json!({"kind": "mutate", "arguments": {"a": 99}})],
        &req,
    );
    assert_eq!(out["action"], "block");
}

struct Stub {
    verdict: Option<Value>,
    policy: &'static str,
}

impl Filter for Stub {
    fn name(&self) -> &str {
        "stub"
    }
    fn failure_policy(&self) -> &'static str {
        self.policy
    }
    fn evaluate(&self, _hook: &str, _message: &Value) -> Result<Value, FilterError> {
        match &self.verdict {
            Some(verdict) => Ok(verdict.clone()),
            None => Err(FilterError("scanner down".to_string())),
        }
    }
}

#[test]
fn chain_runs_filters_in_order() {
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"arguments": {"a": 0}}});
    let chain = FilterChain::new(vec![
        Box::new(Stub { verdict: Some(json!({"kind": "annotate", "provenance": {"seen": true}})), policy: filters::FAIL_CLOSED }),
        Box::new(Stub { verdict: Some(json!({"kind": "mutate", "arguments": {"a": 1}})), policy: filters::FAIL_CLOSED }),
    ]);
    let out = chain.run(filters::REQUEST, &req);
    assert_eq!(out["message"]["params"]["arguments"], json!({"a": 1}));
    assert_eq!(out["message"]["params"]["_meta"], json!({"seen": true}));
}

#[test]
fn failing_filter_is_host_resolved_by_policy() {
    let req = json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {}});
    let closed = FilterChain::new(vec![Box::new(Stub { verdict: None, policy: filters::FAIL_CLOSED })]);
    assert_eq!(closed.run(filters::REQUEST, &req)["action"], "block");
    let open = FilterChain::new(vec![Box::new(Stub { verdict: None, policy: filters::FAIL_OPEN })]);
    assert_eq!(open.run(filters::REQUEST, &req)["action"], "forward");
}

#[test]
fn hook_points_and_verdicts_are_closed_sets() {
    assert!(filters::HOOK_POINTS.contains(&filters::REQUEST));
    assert_eq!(filters::VERDICTS.len(), 5);
    assert!(filters::VERDICTS.contains(&filters::QUARANTINE));
}

// ---- ε2: interest declaration and preview ----

#[test]
fn interest_matching() {
    let ctx = json!({"method": "tools/call", "tool": "gh__x", "direction": "c2u", "content_types": ["image/png"]});
    assert!(filters::interested(&json!({}), &ctx));
    assert!(filters::interested(&json!({"methods": ["tools/call"]}), &ctx));
    assert!(!filters::interested(&json!({"methods": ["resources/read"]}), &ctx));
    assert!(filters::interested(&json!({"methods": ["*"]}), &ctx));
    assert!(filters::interested(&json!({"tools": ["gh__x"]}), &ctx));
    assert!(!filters::interested(&json!({"tools": ["other"]}), &ctx));
    assert!(filters::interested(&json!({"content_types": ["image/*"]}), &ctx));
    assert!(!filters::interested(&json!({"content_types": ["application/pdf"]}), &ctx));
    assert!(!filters::interested(&json!({"content_types": ["image/*"]}), &json!({"method": "tools/call", "content_types": []})));
}

#[test]
fn message_context_extracts_dimensions() {
    let message = json!({
        "method": "tools/call", "params": {"name": "gh__x"},
        "result": {"content": [{"type": "image", "data": "", "mimeType": "image/png"}]},
    });
    let ctx = filters::message_context(&message, "u2c");
    assert_eq!(ctx, json!({"method": "tools/call", "tool": "gh__x", "direction": "u2c", "content_types": ["image/png"]}));
}

#[test]
fn preview_slice_and_resolve() {
    assert_eq!(filters::preview(b"abcdef", 3), json!({"preview": "616263", "ieof": false}));
    assert_eq!(filters::preview(b"abc", 3), json!({"preview": "616263", "ieof": true}));
    assert_eq!(filters::preview(b"abc", 9), json!({"preview": "616263", "ieof": true}));
    assert_eq!(filters::preview_resolve("deny", false), json!({"action": "verdict", "kind": "deny"}));
    assert_eq!(filters::preview_resolve("continue", true), json!({"action": "scan_full"}));
    assert_eq!(filters::preview_resolve("continue", false), json!({"action": "need_more"}));
}

struct Scoped {
    verdict: Value,
    interest: Value,
}

impl Filter for Scoped {
    fn name(&self) -> &str {
        "scoped"
    }
    fn interest(&self) -> Value {
        self.interest.clone()
    }
    fn evaluate(&self, _hook: &str, _message: &Value) -> Result<Value, FilterError> {
        Ok(self.verdict.clone())
    }
}

#[test]
fn chain_skips_uninterested_filter() {
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "gh__x", "arguments": {}}});
    let skipped = FilterChain::new(vec![Box::new(Scoped {
        verdict: json!({"kind": "deny", "reason": "no"}),
        interest: json!({"methods": ["resources/read"]}),
    })]);
    assert_eq!(skipped.run(filters::REQUEST, &req)["action"], "forward");
    let hit = FilterChain::new(vec![Box::new(Scoped {
        verdict: json!({"kind": "deny", "reason": "no"}),
        interest: json!({"methods": ["tools/call"]}),
    })]);
    assert_eq!(hit.run(filters::REQUEST, &req)["action"], "block");
}
