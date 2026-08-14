//! σ6 server-role preflight check (Rust arm). Mirrors the Python arm.
//!
//! `doctor::check_server` inspects a server's exposed tool surface and advertised
//! protocol version and returns ordered findings (error/warning) without erroring,
//! the `nginx -t` analog for the server role. `is_ok` is false only on an error.

use serde_json::{json, Value};

use yamp::doctor;
use yamp::handler::{BackendsHandler, Registry};
use yamp::version;

fn supported() -> &'static str {
    version::STATEFUL_PROTOCOL_VERSION
}

fn good() -> Value {
    json!({ "name": "srv__do", "inputSchema": { "type": "object" } })
}

fn codes(findings: &[Value]) -> Vec<String> {
    findings.iter().map(|f| f["code"].as_str().unwrap().to_string()).collect()
}

#[test]
fn clean_surface_has_no_findings() {
    let findings = doctor::check_server(&[good()], supported());
    assert!(findings.is_empty());
    assert!(doctor::is_ok(&findings));
}

#[test]
fn no_tools_is_an_advisory_warning() {
    let findings = doctor::check_server(&[], supported());
    assert_eq!(codes(&findings), vec!["no-tools"]);
    assert_eq!(findings[0]["level"], doctor::LEVEL_WARNING);
    assert!(doctor::is_ok(&findings)); // a warning does not block serving
}

#[test]
fn unsupported_protocol_version_is_an_error() {
    let findings = doctor::check_server(&[good()], "2020-01-01");
    assert_eq!(codes(&findings), vec!["unsupported-protocol-version"]);
    assert!(!doctor::is_ok(&findings));
    assert!(findings[0]["message"].as_str().unwrap().contains("2026-07-28"));
}

#[test]
fn duplicate_tool_name_is_an_error() {
    let findings = doctor::check_server(&[good(), good()], supported());
    assert_eq!(codes(&findings), vec!["duplicate-tool"]);
    assert!(!doctor::is_ok(&findings));
}

#[test]
fn missing_input_schema_is_a_warning() {
    let findings = doctor::check_server(&[json!({ "name": "srv__x" })], supported());
    assert_eq!(codes(&findings), vec!["missing-input-schema"]);
    assert!(doctor::is_ok(&findings));
}

#[test]
fn unnamed_tool_is_an_error() {
    let findings = doctor::check_server(&[json!({ "inputSchema": { "type": "object" } })], supported());
    assert_eq!(codes(&findings), vec!["unnamed-tool"]);
    assert!(!doctor::is_ok(&findings));
}

#[test]
fn findings_are_ordered_deterministically() {
    // version, then no-tools, then per-tool in order, then sorted duplicates.
    let tools = vec![
        json!({ "name": "b__t" }),
        json!({ "name": "a__t", "inputSchema": { "type": "object" } }),
        json!({ "name": "b__t" }),
    ];
    let findings = doctor::check_server(&tools, "2020-01-01");
    assert_eq!(
        codes(&findings),
        vec!["unsupported-protocol-version", "missing-input-schema", "missing-input-schema", "duplicate-tool"]
    );
}

#[test]
fn check_registry_runs_over_the_handler_surface() {
    let registry = Registry::new(vec![Box::new(BackendsHandler::new(|| json!([])))]).unwrap();
    let findings = doctor::check_registry(&registry, supported());
    assert!(findings.is_empty()); // yamp__backends is a well-formed tool
    assert!(doctor::is_ok(&findings));
}
