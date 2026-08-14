//! Extension filter chain: hook points, the closed verdict set, and host-
//! enforced failure policy (ε0; paper §6.3/§6.4).
//!
//! A filter observes a message at a hook point and returns one of five
//! verdicts: `allow`, `deny`, `mutate`, `annotate`, `quarantine`. The chain
//! applies them in order, accumulating `mutate`/`annotate` onto the message and
//! short-circuiting on `deny`/`quarantine`. A filter that fails is resolved by
//! its declared failure policy (fail-closed -> deny, the secure default;
//! fail-open -> allow, for advisory filters), and that resolution is enforced
//! by the host here, never trusted to the filter. `deny` and `quarantine` map
//! to a clean `-32001` policy error ([`errors::POLICY_DENIED`]).
//!
//! The verdict transforms and the failure resolution are pure and
//! deterministic, mirroring the Python arm's `filters` module so the two
//! produce identical outcomes; the differential corpus pins them. This module
//! declares the whole hook-point taxonomy; ε0 wires only the request phase into
//! the router, and later increments attach the rest.

use serde_json::{json, Map, Value};

use crate::content;
use crate::errors;

// Hook points (§6.3). ε0 declares the taxonomy; the router wires REQUEST first.
pub const CONNECTION: &str = "connection";
pub const LIFECYCLE: &str = "lifecycle";
pub const REQUEST: &str = "request";
pub const RESPONSE: &str = "response";
pub const NOTIFICATION: &str = "notification";
pub const CONTENT_BLOCK: &str = "content_block";
pub const CATALOG: &str = "catalog";
pub const AUTH_SESSION: &str = "auth_session";
pub const HOOK_POINTS: [&str; 8] = [
    CONNECTION,
    LIFECYCLE,
    REQUEST,
    RESPONSE,
    NOTIFICATION,
    CONTENT_BLOCK,
    CATALOG,
    AUTH_SESSION,
];

// The closed verdict set (§6.4).
pub const ALLOW: &str = "allow";
pub const DENY: &str = "deny";
pub const MUTATE: &str = "mutate";
pub const ANNOTATE: &str = "annotate";
pub const QUARANTINE: &str = "quarantine";
pub const VERDICTS: [&str; 5] = [ALLOW, DENY, MUTATE, ANNOTATE, QUARANTINE];

// Failure policy (§6.4), enforced by the host.
pub const FAIL_OPEN: &str = "fail_open";
pub const FAIL_CLOSED: &str = "fail_closed";

// Message direction, for interest declarations (ICAP REQMOD vs RESPMOD).
pub const C2U: &str = "c2u"; // client -> upstream (a request being routed)
pub const U2C: &str = "u2c"; // upstream -> client (a result travelling back)

// The ICAP-Preview continuation signal: the filter wants the rest of the payload.
pub const CONTINUE: &str = "continue";

/// Resolve a failed filter to a verdict kind by its failure policy. fail-closed
/// denies (the secure default); fail-open allows (advisory filters only).
pub fn resolve_failure(policy: &str) -> &'static str {
    if policy == FAIL_CLOSED {
        DENY
    } else {
        ALLOW
    }
}

/// The clean JSON-RPC policy error a `deny`/`quarantine` maps to.
pub fn deny_response(id: &Value, reason: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": errors::POLICY_DENIED, "message": reason },
    })
}

/// Apply one non-terminal verdict to a request, returning the message. `allow`
/// passes it through; `mutate` substitutes the call arguments; `annotate`
/// merges provenance into `params._meta`.
fn apply_one(verdict: &Value, message: &Value) -> Value {
    let kind = verdict["kind"].as_str().unwrap_or("");
    if kind == ALLOW {
        return message.clone();
    }
    let mut params = message.get("params").and_then(Value::as_object).cloned().unwrap_or_default();
    if kind == MUTATE {
        params.insert("arguments".to_string(), verdict["arguments"].clone());
    } else if kind == ANNOTATE {
        let mut meta = params.get("_meta").and_then(Value::as_object).cloned().unwrap_or_default();
        if let Some(provenance) = verdict.get("provenance").and_then(Value::as_object) {
            for (key, value) in provenance {
                meta.insert(key.clone(), value.clone());
            }
        }
        params.insert("_meta".to_string(), Value::Object(meta));
    }
    let mut updated = message.as_object().cloned().unwrap_or_else(Map::new);
    updated.insert("params".to_string(), Value::Object(params));
    Value::Object(updated)
}

/// Reduce an ordered verdict list against a request to a single outcome.
/// Accumulates `mutate`/`annotate` and short-circuits on the first
/// `deny`/`quarantine` with a `-32001` response.
pub fn chain_outcome(verdicts: &[Value], request: &Value) -> Value {
    let mut message = request.clone();
    for verdict in verdicts {
        let kind = verdict["kind"].as_str().unwrap_or("");
        if kind == DENY || kind == QUARANTINE {
            let reason = verdict.get("reason").and_then(Value::as_str).unwrap_or("");
            let id = request.get("id").cloned().unwrap_or(Value::Null);
            return json!({
                "action": "block",
                "response": deny_response(&id, reason),
                "quarantined": kind == QUARANTINE,
            });
        }
        message = apply_one(verdict, &message);
    }
    json!({ "action": "forward", "message": message })
}

// ---- Interest declaration (§6.4): uninterested traffic pays zero cost. ----
// An interest is an object with optional list fields `methods`, `directions`,
// `tools`, `content_types`. An absent or empty list matches anything; `"*"` is
// an explicit wildcard; `content_types` also honors `type/*` prefixes.

fn matches(declared: Option<&Value>, value: Option<&str>) -> bool {
    let list = match declared.and_then(Value::as_array) {
        Some(list) if !list.is_empty() => list,
        _ => return true,
    };
    if list.iter().any(|v| v.as_str() == Some("*")) {
        return true;
    }
    match value {
        Some(value) => list.iter().any(|d| d.as_str() == Some(value)),
        None => false,
    }
}

fn mime_match(pattern: &str, mime: &str) -> bool {
    if pattern == mime || pattern == "*" {
        return true;
    }
    // "type/*" matches any mime under "type/" (mirrors Python's pattern[:-1]).
    pattern.ends_with("/*") && mime.starts_with(&pattern[..pattern.len() - 1])
}

fn matches_content(declared: Option<&Value>, present: &[String]) -> bool {
    let list = match declared.and_then(Value::as_array) {
        Some(list) if !list.is_empty() => list,
        _ => return true,
    };
    if list.iter().any(|v| v.as_str() == Some("*")) {
        return true;
    }
    list.iter()
        .filter_map(Value::as_str)
        .any(|pattern| present.iter().any(|mime| mime_match(pattern, mime)))
}

/// Whether a filter declaring `interest` cares about a message `context`. Every
/// declared dimension must match; an undeclared dimension matches all.
pub fn interested(interest: &Value, context: &Value) -> bool {
    let present: Vec<String> = context
        .get("content_types")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default();
    matches(interest.get("methods"), context.get("method").and_then(Value::as_str))
        && matches(interest.get("directions"), context.get("direction").and_then(Value::as_str))
        && matches(interest.get("tools"), context.get("tool").and_then(Value::as_str))
        && matches_content(interest.get("content_types"), &present)
}

/// The interest-matching context of a message: its method, the called tool (for
/// tools/call), the direction, and the content types it carries.
pub fn message_context(message: &Value, direction: &str) -> Value {
    let method = message.get("method").and_then(Value::as_str);
    let tool = if method == Some("tools/call") {
        message.get("params").and_then(|p| p.get("name")).and_then(Value::as_str)
    } else {
        None
    };
    let listed = content::blocks(message);
    let mut mimes: Vec<String> = listed
        .as_array()
        .map(|a| a.iter().filter_map(|b| b.get("mime").and_then(Value::as_str)).map(str::to_string).collect())
        .unwrap_or_default();
    mimes.sort();
    mimes.dedup();
    json!({ "method": method, "tool": tool, "direction": direction, "content_types": mimes })
}

// ---- Preview phase (§6.4), modeled on ICAP Preview. ----

/// The first `n` bytes offered to a filter, plus `ieof` (the preview is the
/// entire payload, so no continuation exists). Bytes are hex, JSON-safe.
pub fn preview(data: &[u8], n: usize) -> Value {
    let take = n.min(data.len());
    json!({ "preview": crate::signing::to_hex(&data[..take]), "ieof": n >= data.len() })
}

/// Resolve a filter's preview response. A terminal verdict decides early (the
/// payload is never fully buffered); `continue` proceeds to a full scan when the
/// preview already held everything, else asks the host for the rest.
pub fn preview_resolve(decision: &str, ieof: bool) -> Value {
    if decision != CONTINUE {
        json!({ "action": "verdict", "kind": decision })
    } else if ieof {
        json!({ "action": "scan_full" })
    } else {
        json!({ "action": "need_more" })
    }
}

/// A message filter. `evaluate` returns a verdict `Value` (`{"kind": ...}`) or
/// an error, which the host resolves through the failure policy. `interest`
/// declares what traffic it cares about (default: everything). Object-safe so
/// the chain holds heterogeneous filters.
pub trait Filter: Send + Sync {
    fn name(&self) -> &str;
    fn failure_policy(&self) -> &'static str {
        FAIL_CLOSED
    }
    fn interest(&self) -> Value {
        json!({})
    }
    fn evaluate(&self, hook: &str, message: &Value) -> Result<Value, FilterError>;
}

/// A filter's own failure (a scanner crash, timeout, or refusal). Carries a
/// reason for audit; the wire verdict comes from the failure policy, not this.
#[derive(Debug)]
pub struct FilterError(pub String);

/// An ordered chain of filters run at one hook point. Each filter sees the
/// message as shaped by the filters before it; a failed filter is resolved by
/// its policy; evaluation stops at the first terminal verdict. Outcome semantics
/// live in [`chain_outcome`], so the chain never re-implements them.
pub struct FilterChain {
    filters: Vec<Box<dyn Filter>>,
}

impl FilterChain {
    pub fn new(filters: Vec<Box<dyn Filter>>) -> Self {
        Self { filters }
    }

    pub fn run(&self, hook: &str, request: &Value) -> Value {
        let context = message_context(request, C2U);
        let mut verdicts: Vec<Value> = Vec::new();
        let mut message = request.clone();
        for handler in &self.filters {
            if !interested(&handler.interest(), &context) {
                continue; // uninterested traffic pays zero cost (§6.4)
            }
            let verdict = match handler.evaluate(hook, &message) {
                Ok(verdict) => verdict,
                Err(_) => json!({ "kind": resolve_failure(handler.failure_policy()) }),
            };
            let kind = verdict["kind"].as_str().unwrap_or("").to_string();
            verdicts.push(verdict);
            if kind == DENY || kind == QUARANTINE {
                break;
            }
            message = apply_one(verdicts.last().expect("just pushed"), &message);
        }
        chain_outcome(&verdicts, request)
    }
}
