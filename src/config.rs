//! Config-file loader (SEP Section 10 schema, practical subset).
//!
//! A JSON document describes the listen address, the backends (each id maps to
//! one or more addresses for failover), and optional resilience settings.
//! Servers read it with `--config file.json` instead of repeated `--backend`.

use std::collections::HashMap;
use std::fs;
use std::io;

use serde_json::{json, Value};

use crate::collision;
use crate::namespace;

/// Collision resolution config (SEP §3.4). The active strategy is declared;
/// `prefix` is the default and requires no further settings.
#[derive(Clone)]
pub struct Namespacing {
    pub strategy: String,
    pub overrides: HashMap<String, String>, // namespaced -> exposed (manual)
    pub priority: Vec<String>,              // backend ids, highest priority first
}

impl Default for Namespacing {
    fn default() -> Self {
        Self {
            strategy: collision::PREFIX.to_string(),
            overrides: HashMap::new(),
            priority: Vec::new(),
        }
    }
}

pub struct Resilience {
    pub failure_threshold: u32,
    pub reset_timeout: f64,
    pub health_interval: Option<f64>,
    pub request_timeout: Option<f64>,
    // An explicit on/off from config `resilience.enabled`; when absent the
    // meaningfully-configured heuristic decides.
    pub explicit_enabled: Option<bool>,
}

impl Default for Resilience {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: 30.0,
            health_interval: None,
            request_timeout: None,
            explicit_enabled: None,
        }
    }
}

impl Resilience {
    /// Whether to attach circuit breakers. An explicit `resilience.enabled` in
    /// config wins; otherwise a breaker is attached only when resilience is
    /// meaningfully configured (any non-default timing setting), so an operator
    /// who wants breakers with default timings can still ask for them explicitly.
    pub fn enabled(&self) -> bool {
        if let Some(explicit) = self.explicit_enabled {
            return explicit;
        }
        self.health_interval.is_some()
            || self.request_timeout.is_some()
            || self.failure_threshold != 5
            || self.reset_timeout != 30.0
    }
}

pub struct BackendConfig {
    pub id: String,
    pub addresses: Vec<String>, // tried in order for failover
    pub token: Option<String>,
}

/// One REST-to-MCP Conversion handler served locally (δ17).
pub struct RestHandlerConfig {
    pub id: String,
    pub base_url: String,
    pub operations: Vec<Value>,
}

/// Local handlers the proxy serves itself (draft §5.7 Conversion, meta-tools).
#[derive(Default)]
pub struct HandlerConfig {
    pub meta_tools: bool, // enable the yamp__backends meta-tool
    pub rest: Vec<RestHandlerConfig>,
}

pub struct ProxyConfig {
    pub listen: String,
    pub backends: Vec<BackendConfig>,
    pub resilience: Resilience,
    pub client_tokens: Vec<String>, // bearer tokens the proxy accepts
    pub namespacing: Namespacing,
    pub handlers: HandlerConfig,
    pub audit_secret: Option<String>, // enables the signed accountability log (SEP-2828)
}

pub fn parse_address(address: &str) -> io::Result<(String, u16)> {
    // Reject a missing or non-numeric port rather than silently binding port 0;
    // the Python arm raises on the same input, so both arms fail fast alike.
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| invalid(format!("address missing ':' port: {address}")))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| invalid(format!("invalid port in address: {address}")))?;
    Ok((host.to_string(), port))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Config-error catalog (Track U, U4/U8): the field-failure causes yamp diagnoses, in
/// check order. Each entry pairs a stable slug with a one-line description (for the
/// generated index) and a fix hint. The docs URL is derived from the slug so it cannot
/// drift (`CONFIG_ERRORS.md#slug`). Mirrored in the Python arm and pinned in the corpus.
pub const CONFIG_ERRORS: [(&str, &str, &str); 10] = [
    ("not-object", "the config is not a JSON object", "wrap the settings in a top-level { ... } object"),
    ("backends-not-object", "'backends' is not a JSON object", "make 'backends' a map of id to { address }"),
    ("invalid-backend-id", "a backend id is empty or contains the reserved '__' delimiter", "rename the backend so its id has no '__'"),
    ("backend-no-addresses", "a backend declares no address", "give the backend an 'address' or a non-empty 'addresses'"),
    ("missing-listen", "the config has no 'listen' address", "add \"listen\": \"127.0.0.1:PORT\""),
    ("unknown-collision-strategy", "namespacing.strategy is not a supported strategy", "set it to prefix, priority, manual, or passthrough"),
    ("invalid-handler-id", "a rest handler id is missing or invalid", "give the handler a non-empty id without '__'"),
    ("handler-backend-collision", "a handler id collides with a backend id", "rename the handler or the backend"),
    ("handler-missing-baseurl", "a rest handler has no 'baseUrl'", "add a 'baseUrl' to the handler"),
    ("invalid-json", "the config is not valid JSON", "fix the JSON syntax at the reported line and column"),
];

/// The stable in-index anchor for a config-error slug (empty if unknown).
pub fn config_docs_url(slug: &str) -> String {
    if CONFIG_ERRORS.iter().any(|(s, _, _)| *s == slug) {
        format!("CONFIG_ERRORS.md#{slug}")
    } else {
        String::new()
    }
}

/// The whole catalog as structured entries, for the generated index and cross-arm
/// pinning: each `{slug, description, hint, docsUrl}`.
pub fn error_catalog() -> Vec<Value> {
    CONFIG_ERRORS
        .iter()
        .map(|(slug, desc, hint)| json!({ "slug": slug, "description": desc, "hint": hint, "docsUrl": config_docs_url(slug) }))
        .collect()
}

/// The first schema violation in `data` as `(slug, message)`, or `None` when the
/// document conforms. One source: [`from_value`] returns an error from it and
/// [`diagnose`] reports from it, so the two never diverge.
pub fn first_error(data: &Value) -> Option<(&'static str, String)> {
    if !data.is_object() {
        return Some(("not-object", "config must be a JSON object".to_string()));
    }
    if !data["backends"].is_null() && !data["backends"].is_object() {
        return Some(("backends-not-object", "'backends' must be a JSON object".to_string()));
    }
    if let Some(map) = data["backends"].as_object() {
        for (id, spec) in map {
            if !namespace::valid_backend_id(id) {
                return Some(("invalid-backend-id", format!("invalid backend id: {id}")));
            }
            let count = spec["addresses"].as_array().map(|l| l.len()).unwrap_or(0)
                + usize::from(spec["address"].is_string());
            if count == 0 {
                return Some(("backend-no-addresses", format!("backend {id} has no addresses")));
            }
        }
    }
    if data.get("listen").is_none() {
        return Some(("missing-listen", "config is missing 'listen'".to_string()));
    }
    let strategy = data["namespacing"]["strategy"].as_str().unwrap_or(collision::PREFIX);
    if !collision::is_strategy(strategy) {
        return Some(("unknown-collision-strategy", format!("unknown collision strategy: {strategy}")));
    }
    if let Some(list) = data["handlers"]["rest"].as_array() {
        let backend_ids: std::collections::HashSet<&str> =
            data["backends"].as_object().map(|m| m.keys().map(|k| k.as_str()).collect()).unwrap_or_default();
        for spec in list {
            let id = spec["id"].as_str().unwrap_or("");
            if id.is_empty() || !namespace::valid_backend_id(id) {
                return Some(("invalid-handler-id", format!("invalid rest handler id: {id}")));
            }
            if backend_ids.contains(id) {
                return Some(("handler-backend-collision", format!("handler id {id} collides with a backend id")));
            }
            if !spec["baseUrl"].is_string() {
                return Some(("handler-missing-baseurl", format!("rest handler {id} is missing 'baseUrl'")));
            }
        }
    }
    None
}

/// Diagnose the first schema violation as a structured finding (`slug`, `message`,
/// `hint`, `docsUrl`), or `None` when the document conforms (the U4/U8 field-failure
/// identification, pure over the raw document).
pub fn diagnose(data: &Value) -> Option<Value> {
    let (slug, message) = first_error(data)?;
    let hint = CONFIG_ERRORS.iter().find(|(s, _, _)| *s == slug).map(|(_, _, h)| *h).unwrap_or("");
    Some(json!({ "slug": slug, "message": message, "hint": hint, "docsUrl": config_docs_url(slug) }))
}

/// The structured finding for a JSON *parse* failure, carrying the line and column
/// the loader recovered from the source text (U4).
pub fn parse_error_finding(message: &str, line: usize, column: usize) -> Value {
    let hint = CONFIG_ERRORS.iter().find(|(s, _, _)| *s == "invalid-json").map(|(_, _, h)| *h).unwrap_or("");
    json!({ "slug": "invalid-json", "message": message, "line": line, "column": column, "hint": hint, "docsUrl": config_docs_url("invalid-json") })
}

pub fn from_value(data: &Value) -> io::Result<ProxyConfig> {
    if let Some((_, message)) = first_error(data) {
        return Err(invalid(message));
    }
    let section = &data["resilience"];
    let resilience = Resilience {
        failure_threshold: section["failureThreshold"].as_u64().unwrap_or(5) as u32,
        reset_timeout: section["resetTimeout"].as_f64().unwrap_or(30.0),
        health_interval: section["healthInterval"].as_f64(),
        request_timeout: section["requestTimeout"].as_f64(),
        explicit_enabled: section["enabled"].as_bool(),
    };

    let mut backends = Vec::new();
    if let Some(map) = data["backends"].as_object() {
        for (id, spec) in map {
            let mut addresses = Vec::new();
            if let Some(list) = spec["addresses"].as_array() {
                for address in list {
                    if let Some(text) = address.as_str() {
                        addresses.push(text.to_string());
                    }
                }
            }
            if let Some(text) = spec["address"].as_str() {
                addresses.push(text.to_string());
            }
            backends.push(BackendConfig { id: id.clone(), addresses, token: spec["token"].as_str().map(String::from) });
        }
    }

    let listen = data["listen"].as_str().unwrap_or_default().to_string();
    let client_tokens = data["auth"]["clientTokens"]
        .as_array()
        .map(|list| list.iter().filter_map(|t| t.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let ns = &data["namespacing"];
    let strategy = ns["strategy"].as_str().unwrap_or(collision::PREFIX).to_string();
    let overrides = ns["overrides"]
        .as_object()
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let priority = ns["priority"]
        .as_array()
        .map(|list| list.iter().filter_map(|p| p.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let namespacing = Namespacing {
        strategy,
        overrides,
        priority,
    };

    let section = &data["handlers"];
    let mut rest = Vec::new();
    if let Some(list) = section["rest"].as_array() {
        for spec in list {
            let id = spec["id"].as_str().unwrap_or("");
            let base_url = spec["baseUrl"].as_str().unwrap_or_default().to_string();
            let operations = spec["operations"].as_array().cloned().unwrap_or_default();
            rest.push(RestHandlerConfig { id: id.to_string(), base_url, operations });
        }
    }
    let handlers = HandlerConfig { meta_tools: section["metaTools"].as_bool().unwrap_or(false), rest };

    // A non-empty audit secret enables the signed, hash-chained accountability
    // log on the served path (SEP-2828). An empty secret is treated as absent.
    let audit_secret = data["audit"]["secret"].as_str().filter(|s| !s.is_empty()).map(String::from);

    Ok(ProxyConfig { listen, backends, resilience, client_tokens, namespacing, handlers, audit_secret })
}

pub fn load_config(path: &str) -> io::Result<ProxyConfig> {
    let text = fs::read_to_string(path)?;
    let data: Value = serde_json::from_str(&text).map_err(|e| invalid(e.to_string()))?;
    from_value(&data)
}

/// Source of an explained config value: set by the config document, fallen back to
/// the built-in default, or an unrecognized key.
pub const SOURCE_CONFIG: &str = "config";
pub const SOURCE_DEFAULT: &str = "default";
pub const SOURCE_UNKNOWN: &str = "unknown";

/// Provenance table for `config explain` (Track U): every explainable config key in
/// dotted JSON form, paired with its built-in default. A key the loaded document sets
/// is sourced from the config; an absent key falls back to this default. Vec order is
/// the display order for `config effective`. Mirrored in the Python arm and pinned in
/// the differential corpus.
pub fn explain_keys() -> Vec<(&'static str, Value)> {
    vec![
        ("listen", Value::Null),
        ("resilience.failureThreshold", json!(5)),
        ("resilience.resetTimeout", json!(30.0)),
        ("resilience.healthInterval", Value::Null),
        ("resilience.requestTimeout", Value::Null),
        ("resilience.enabled", Value::Null),
        ("namespacing.strategy", json!(collision::PREFIX)),
        ("auth.clientTokens", json!([])),
        ("handlers.metaTools", json!(false)),
        ("audit.secret", Value::Null),
    ]
}

fn lookup<'a>(raw: &'a Value, key: &str) -> Option<&'a Value> {
    let mut node = raw;
    for part in key.split('.') {
        node = node.get(part)?;
    }
    Some(node)
}

/// Explain one config key: its effective value and where it came from. `source` is
/// `config` when the loaded document set the key, `default` when it fell back to the
/// built-in default, and `unknown` for an unrecognized key.
pub fn explain(raw: &Value, key: &str) -> Value {
    if let Some(value) = lookup(raw, key) {
        return json!({ "key": key, "value": value, "source": SOURCE_CONFIG });
    }
    for (known, default) in explain_keys() {
        if known == key {
            return json!({ "key": key, "value": default, "source": SOURCE_DEFAULT });
        }
    }
    json!({ "key": key, "value": Value::Null, "source": SOURCE_UNKNOWN })
}

/// Explain every known key in order: the resolved config with per-key provenance.
pub fn effective(raw: &Value) -> Vec<Value> {
    explain_keys().into_iter().map(|(key, _)| explain(raw, key)).collect()
}

/// One human line for an explained key, `key = <json-value> (source)`. The value is
/// compact JSON so the text is byte-identical across arms.
pub fn explain_line(entry: &Value) -> String {
    let rendered = serde_json::to_string(&entry["value"]).unwrap_or_default();
    format!(
        "{} = {} ({})",
        entry["key"].as_str().unwrap_or(""),
        rendered,
        entry["source"].as_str().unwrap_or(""),
    )
}

/// Diff two config documents over the resolved view: every known key whose effective
/// value differs between `left` and `right`, in table order. Each entry carries both
/// sides' value and provenance, so a pure default-vs-default match is omitted while a
/// real behavioral change is shown.
pub fn diff(left: &Value, right: &Value) -> Vec<Value> {
    let mut changes = Vec::new();
    for (key, _) in explain_keys() {
        let a = explain(left, key);
        let b = explain(right, key);
        if a["value"] != b["value"] {
            changes.push(json!({
                "key": key,
                "left": { "value": a["value"], "source": a["source"] },
                "right": { "value": b["value"], "source": b["source"] },
            }));
        }
    }
    changes
}

/// One human line for a diffed key, `key: <left> (source) -> <right> (source)`.
/// Values are compact JSON so the text is byte-identical across arms.
pub fn diff_line(entry: &Value) -> String {
    let left = serde_json::to_string(&entry["left"]["value"]).unwrap_or_default();
    let right = serde_json::to_string(&entry["right"]["value"]).unwrap_or_default();
    format!(
        "{}: {} ({}) -> {} ({})",
        entry["key"].as_str().unwrap_or(""),
        left,
        entry["left"]["source"].as_str().unwrap_or(""),
        right,
        entry["right"]["source"].as_str().unwrap_or(""),
    )
}

fn adapt_addresses(spec: &str) -> Vec<String> {
    spec.split(',').map(|addr| addr.trim().to_string()).collect()
}

fn adapt_listen(listen: &Value) -> Value {
    // A bare port (int) or ":port" binds the secure loopback default (U7).
    if let Some(port) = listen.as_i64() {
        return json!(format!("{}:{}", crate::security::DEFAULT_BIND_HOST, port));
    }
    if let Some(s) = listen.as_str() {
        if let Some(rest) = s.strip_prefix(':') {
            return json!(format!("{}:{}", crate::security::DEFAULT_BIND_HOST, rest));
        }
    }
    listen.clone()
}

fn adapt_backends(backends: &Value) -> Value {
    if let Some(arr) = backends.as_array() {
        // A list of "id=host:port[,host:port]" strings, like the --backend CLI flag.
        let mut out = serde_json::Map::new();
        for item in arr {
            if let Some((bid, spec)) = item.as_str().and_then(|s| s.split_once('=')) {
                out.insert(bid.to_string(), json!({ "addresses": adapt_addresses(spec) }));
            }
        }
        return Value::Object(out);
    }
    if let Some(obj) = backends.as_object() {
        // A map whose value may be a bare "host:port[,...]" string.
        let mut out = serde_json::Map::new();
        for (bid, value) in obj {
            match value.as_str() {
                Some(spec) => out.insert(bid.clone(), json!({ "addresses": adapt_addresses(spec) })),
                None => out.insert(bid.clone(), value.clone()),
            };
        }
        return Value::Object(out);
    }
    backends.clone()
}

/// Normalize a human-friendly config document to the canonical SEP schema.
///
/// Expands operator shorthands so the result loads via [`from_value`] and yields the
/// intended effective config: a `listen` given as a bare port (int) or `:port` becomes
/// `127.0.0.1:port` (the secure loopback default), a `backends` list of `id=host:port`
/// strings becomes the canonical map, and a backend value that is a bare `host:port`
/// string becomes `{"addresses": [...]}`. Every other key passes through, and a
/// canonical document is returned unchanged, so `adapt` is idempotent on its own
/// output (U9).
pub fn adapt(raw: &Value) -> Value {
    let mut out = raw.clone();
    if let Some(obj) = out.as_object_mut() {
        if let Some(listen) = obj.get("listen").cloned() {
            obj.insert("listen".to_string(), adapt_listen(&listen));
        }
        if let Some(backends) = obj.get("backends").cloned() {
            obj.insert("backends".to_string(), adapt_backends(&backends));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_backends_and_resilience() {
        let config = from_value(&json!({
            "listen": "127.0.0.1:9100",
            "backends": {
                "github": {"addresses": ["127.0.0.1:9101", "127.0.0.1:9111"], "token": "t"},
                "slack": {"address": "127.0.0.1:9102"},
            },
            "resilience": {"failureThreshold": 3, "resetTimeout": 20, "healthInterval": 5, "requestTimeout": 2},
        }))
        .unwrap();
        assert_eq!(config.listen, "127.0.0.1:9100");
        let github = config.backends.iter().find(|b| b.id == "github").unwrap();
        assert_eq!(github.addresses, ["127.0.0.1:9101", "127.0.0.1:9111"]);
        assert_eq!(github.token.as_deref(), Some("t"));
        assert!(config.resilience.enabled());
        assert_eq!(config.resilience.health_interval, Some(5.0));
    }

    #[test]
    fn defaults_are_not_resilient() {
        let config = from_value(&json!({"listen": "x:1", "backends": {"a": {"address": "h:1"}}})).unwrap();
        assert!(!config.resilience.enabled());
        assert!(config.client_tokens.is_empty());
    }

    #[test]
    fn explicit_enabled_overrides_the_heuristic() {
        // Breakers on despite default timings.
        let on = from_value(&json!({
            "listen": "x:1", "backends": {"a": {"address": "h:1"}}, "resilience": {"enabled": true},
        }))
        .unwrap();
        assert!(on.resilience.enabled());
        // Breakers off despite non-default timings.
        let off = from_value(&json!({
            "listen": "x:1", "backends": {"a": {"address": "h:1"}},
            "resilience": {"enabled": false, "failureThreshold": 3},
        }))
        .unwrap();
        assert!(!off.resilience.enabled());
    }

    #[test]
    fn parses_client_tokens() {
        let config = from_value(&json!({"listen": "x:1", "backends": {"a": {"address": "h:1"}}, "auth": {"clientTokens": ["t1", "t2"]}})).unwrap();
        assert_eq!(config.client_tokens, ["t1", "t2"]);
    }

    #[test]
    fn parse_address_splits_host_port() {
        assert_eq!(parse_address("127.0.0.1:9101").unwrap(), ("127.0.0.1".to_string(), 9101));
    }

    #[test]
    fn parse_address_rejects_missing_or_bad_port() {
        // Both arms fail fast on these rather than silently binding port 0.
        assert!(parse_address("localhost").is_err());
        assert!(parse_address("localhost:abc").is_err());
    }

    #[test]
    fn rejects_invalid_backend_id() {
        assert!(from_value(&json!({"listen": "x:1", "backends": {"a__b": {"address": "h:1"}}})).is_err());
    }

    #[test]
    fn rejects_backend_without_addresses() {
        assert!(from_value(&json!({"listen": "x:1", "backends": {"a": {}}})).is_err());
    }

    #[test]
    fn rejects_missing_listen() {
        assert!(from_value(&json!({"backends": {"a": {"address": "h:1"}}})).is_err());
    }

    #[test]
    fn namespacing_defaults_to_prefix() {
        let config = from_value(&json!({"listen": "x:1", "backends": {"a": {"address": "h:1"}}})).unwrap();
        assert_eq!(config.namespacing.strategy, "prefix");
        assert!(config.namespacing.overrides.is_empty());
        assert!(config.namespacing.priority.is_empty());
    }

    #[test]
    fn namespacing_parsed() {
        let config = from_value(&json!({
            "listen": "x:1",
            "backends": {"a": {"address": "h:1"}},
            "namespacing": {
                "strategy": "priority",
                "priority": ["gh", "gl"],
                "overrides": {"github__create_issue": "gh_new_issue"},
            },
        }))
        .unwrap();
        assert_eq!(config.namespacing.strategy, "priority");
        assert_eq!(config.namespacing.priority, ["gh", "gl"]);
        assert_eq!(config.namespacing.overrides.get("github__create_issue").map(String::as_str), Some("gh_new_issue"));
    }

    #[test]
    fn namespacing_unknown_strategy_rejected() {
        assert!(from_value(&json!({
            "listen": "x:1", "backends": {"a": {"address": "h:1"}}, "namespacing": {"strategy": "bogus"},
        }))
        .is_err());
    }

    #[test]
    fn handlers_default_empty() {
        let config = from_value(&json!({"listen": "x:1", "backends": {"a": {"address": "h:1"}}})).unwrap();
        assert!(!config.handlers.meta_tools);
        assert!(config.handlers.rest.is_empty());
    }

    #[test]
    fn handlers_parsed() {
        let config = from_value(&json!({
            "listen": "x:1",
            "backends": {"a": {"address": "h:1"}},
            "handlers": {
                "metaTools": true,
                "rest": [{"id": "gh", "baseUrl": "https://api.example.com", "operations": [{"name": "get"}]}],
            },
        }))
        .unwrap();
        assert!(config.handlers.meta_tools);
        assert_eq!(config.handlers.rest[0].id, "gh");
        assert_eq!(config.handlers.rest[0].base_url, "https://api.example.com");
        assert_eq!(config.handlers.rest[0].operations, vec![json!({"name": "get"})]);
    }

    #[test]
    fn audit_secret_parsed_and_defaults_absent() {
        let base = json!({"listen": "x:1", "backends": {"a": {"address": "h:1"}}});
        assert!(from_value(&base).unwrap().audit_secret.is_none());
        let mut with = base.clone();
        with["audit"] = json!({"secret": "s3cret"});
        assert_eq!(from_value(&with).unwrap().audit_secret.as_deref(), Some("s3cret"));
        // An empty secret is treated as absent, so it does not enable the log.
        let mut empty = base.clone();
        empty["audit"] = json!({"secret": ""});
        assert!(from_value(&empty).unwrap().audit_secret.is_none());
    }

    #[test]
    fn handler_id_validation() {
        let base = json!({"listen": "x:1", "backends": {"a": {"address": "h:1"}}});
        let with = |handlers: Value| {
            let mut data = base.clone();
            data["handlers"] = handlers;
            from_value(&data)
        };
        assert!(with(json!({"rest": [{"id": "bad__id", "baseUrl": "http://x"}]})).is_err());
        assert!(with(json!({"rest": [{"id": "a", "baseUrl": "http://x"}]})).is_err()); // collides with backend
        assert!(with(json!({"rest": [{"id": "gh"}]})).is_err()); // missing baseUrl
    }
}
