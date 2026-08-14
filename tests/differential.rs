//! Cross-arm differential harness: replay the shared golden corpus.
//!
//! Loads `conformance/differential-corpus.json` (generated from the Python arm
//! by python/tools/gen_differential_corpus.py) and asserts the Rust arm
//! reproduces every expected output. The Python arm replays the same file
//! (tests/test_differential.py), so the two implementations are pinned to
//! identical wire output for these pure, deterministic operations.

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::{json, Value};

use yamp::{auth, base64, callout, capability, config, content, doctor, errors, filters, icap, namespace, pool, schema, security, server, signing, status, subscriptions, tap, tasks, variants};

fn pair(resolved: Option<(String, String)>) -> Value {
    match resolved {
        Some((a, b)) => json!([a, b]),
        None => Value::Null,
    }
}

fn from_hex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn path_of(value: &Value) -> Vec<Value> {
    value.as_array().cloned().unwrap_or_default()
}

fn apply(op: &str, input: &Value) -> Value {
    match op {
        "namespace_prefix" => json!(namespace::prefix(input["id"].as_str().unwrap(), input["name"].as_str().unwrap())),
        "namespace_split" => pair(namespace::split(input.as_str().unwrap()).map(|(a, b)| (a.to_string(), b.to_string()))),
        "namespace_prefix_uri" => json!(namespace::prefix_uri(input["id"].as_str().unwrap(), input["uri"].as_str().unwrap())),
        "namespace_split_uri" => pair(namespace::split_uri(input.as_str().unwrap())),
        "signing_canonical" => json!(String::from_utf8(signing::canonical(input)).unwrap()),
        "signing_sign" => json!(signing::sign(input["secret"].as_str().unwrap(), &input["record"])),
        "signing_chain" => json!(signing::chain(input["prev"].as_str().unwrap(), &input["record"])),
        "capability_compose" => {
            let backends: Vec<Value> = input["backends"].as_array().unwrap().clone();
            let client = input.get("client").filter(|v| !v.is_null());
            capability::compose_capabilities(&backends, client)
        }
        "variants_compose" => {
            let backends: Vec<Value> = input["backends"].as_array().unwrap().clone();
            json!(variants::compose_variants(&backends))
        }
        "variants_bind_cursor" => {
            let cursors: BTreeMap<String, String> = input["cursors"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                .collect();
            json!(variants::bind_cursor(input["variant"].as_str(), &cursors))
        }
        "variants_selected" => match variants::selected_variant(input) {
            Some(v) => json!(v),
            None => Value::Null,
        },
        "variants_available" => json!(variants::available_variants(input)),
        "auth_code_challenge" => json!(auth::code_challenge(input.as_str().unwrap())),
        "auth_token_exchange_request" => auth::token_exchange_request(
            input["subject_token"].as_str().unwrap(),
            input.get("audience").and_then(Value::as_str),
            input.get("scope").and_then(Value::as_str),
            auth::TOKEN_TYPE_ACCESS_TOKEN,
            None,
        ),
        "filter_resolve_failure" => json!(filters::resolve_failure(input.as_str().unwrap())),
        "filter_deny_response" => filters::deny_response(&input["id"], input["reason"].as_str().unwrap()),
        "filter_chain_outcome" => {
            let verdicts: Vec<Value> = input["verdicts"].as_array().unwrap().clone();
            filters::chain_outcome(&verdicts, &input["request"])
        }
        "base64_encode" => json!(base64::encode(&from_hex(input.as_str().unwrap()))),
        "base64_decode" => json!(to_hex(&base64::decode(input.as_str().unwrap()).unwrap())),
        "content_blocks" => content::blocks(input),
        "content_set_text" => content::set_text(&input["message"], &path_of(&input["path"]), input["text"].as_str().unwrap()),
        "content_set_bytes" => content::set_bytes(&input["message"], &path_of(&input["path"]), &from_hex(input["bytes"].as_str().unwrap())),
        "interest_match" => json!(filters::interested(&input["interest"], &input["context"])),
        "message_context" => filters::message_context(&input["message"], input["direction"].as_str().unwrap()),
        "preview_slice" => filters::preview(&from_hex(input["data"].as_str().unwrap()), input["n"].as_u64().unwrap() as usize),
        "preview_resolve" => filters::preview_resolve(input["decision"].as_str().unwrap(), input["ieof"].as_bool().unwrap()),
        "callout_request" => callout::callout_request(&input["context"], input["phase"].as_str().unwrap(), &from_hex(input["content"].as_str().unwrap()), input["ieof"].as_bool().unwrap()),
        "callout_digest" => json!(callout::content_digest(&from_hex(input.as_str().unwrap()))),
        "callout_budget" => json!(callout::exceeds_budget(input["size"].as_u64().unwrap() as usize, input["max_bytes"].as_u64().unwrap() as usize)),
        "callout_parse" => callout::parse_verdict(&input["response"], input["failure_policy"].as_str().unwrap()),
        "icap_mode" => json!(icap::icap_mode(input.as_str().unwrap())),
        "icap_to_callout" => icap::icap_to_callout(input),
        "icap_should_deref" => json!(icap::should_deref(input["kind"].as_str().unwrap(), input["enabled"].as_bool().unwrap())),
        "server_list_directives" => server::list_directives(input["ttl_ms"].as_u64().unwrap(), input["cache_scope"].as_str().unwrap()),
        "server_attach_directives" => server::attach_directives(&input["result"], input["ttl_ms"].as_u64().unwrap(), input["cache_scope"].as_str().unwrap()),
        "schema_validate" => json!(schema::is_valid(&input["schema"], &input["value"])),
        "error_reason" => json!(errors::reason(input.as_i64().unwrap())),
        "error_object" => errors::error_object(input["code"].as_i64().unwrap(), input.get("detail").and_then(Value::as_str)),
        "error_describe" => errors::describe(input.as_i64().unwrap()),
        "pool_admit" => json!(pool::admit(input["in_flight"].as_u64().unwrap(), input["cap"].as_u64().unwrap())),
        "pool_deadline" => json!(pool::deadline(input["now_ms"].as_u64().unwrap(), input["idle_ms"].as_u64().unwrap())),
        "pool_expired" => json!(pool::expired(input["deadline_ms"].as_u64().unwrap(), input["now_ms"].as_u64().unwrap())),
        "pool_cancel_id" => pool::cancel_request_id(input).cloned().unwrap_or(Value::Null),
        "pool_progress_token" => pool::progress_token(input).cloned().unwrap_or(Value::Null),
        "task_augmented" => json!(tasks::is_task_augmented(input)),
        "task_new_id" => json!(tasks::new_task_id(input.as_u64().unwrap())),
        "task_handle" => tasks::task_handle(input["task_id"].as_str().unwrap(), input["status"].as_str().unwrap(), input.get("result"), input.get("error")),
        "subscription_updated" => subscriptions::updated_notification(input.as_str().unwrap()),
        "subscription_namespace_updated" => subscriptions::namespace_updated(&input["message"], input["backend_id"].as_str().unwrap()),
        "server_output_cap" => json!(server::exceeds_output_cap(&input["result"], input["max_bytes"].as_u64().unwrap() as usize)),
        "doctor_check" => {
            let tools: Vec<Value> = input["tools"].as_array().unwrap().clone();
            json!(doctor::check_server(&tools, input["protocol_version"].as_str().unwrap()))
        }
        "doctor_render" => {
            let findings: Vec<Value> = input["findings"].as_array().unwrap().clone();
            let mode = input["mode"].as_str().unwrap();
            json!({ "text": doctor::render_text(&findings, mode), "ok": doctor::servable(&findings, mode) })
        }
        "status_snapshot" => {
            let ids: Vec<String> = input["backend_ids"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_string()).collect();
            status::snapshot(&ids, input["sessions"].as_u64().unwrap() as usize)
        }
        "config_explain" => config::explain(&input["raw"], input["key"].as_str().unwrap()),
        "config_explain_line" => json!(config::explain_line(input)),
        "config_effective" => json!(config::effective(&input["raw"])),
        "config_diff" => json!(config::diff(&input["left"], &input["right"])),
        "config_diff_line" => json!(config::diff_line(input)),
        "security_is_loopback" => json!(security::is_loopback(input.as_str().unwrap())),
        "security_check_bind" => json!(security::check_bind(input["host"].as_str().unwrap(), input["has_client_auth"].as_bool().unwrap())),
        "config_adapt" => config::adapt(input),
        "tap_redact" => tap::redact(input),
        "tap_capture" => tap::capture(input["direction"].as_str().unwrap(), &input["message"]),
        "config_error_catalog" => json!(config::error_catalog()),
        "config_diagnose_slug" => config::diagnose(input).map(|d| d["slug"].clone()).unwrap_or(Value::Null),
        other => panic!("unknown op {other}"),
    }
}

fn corpus() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance/differential-corpus.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap()
}

#[test]
fn differential_corpus_matches_rust_arm() {
    let corpus = corpus();
    let cases = corpus["cases"].as_array().unwrap();
    assert!(!cases.is_empty(), "corpus is empty");
    for case in cases {
        let op = case["op"].as_str().unwrap();
        let got = apply(op, &case["in"]);
        assert_eq!(got, case["out"], "op {op} diverged");
    }
}
