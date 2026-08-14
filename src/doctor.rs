//! Server-role preflight check (σ6; the `nginx -t` analog).
//!
//! A server that originates responses should be able to say, before it accepts
//! any traffic, whether its configuration is coherent. [`check_server`] inspects
//! the composed tool surface a server would expose and the protocol version it
//! would advertise, and returns an ordered list of findings (`error`/`warning`)
//! rather than erroring, so a caller can print them all at once the way `nginx -t`
//! reports every problem in a config. The check is pure and deterministic, so its
//! verdict is pinned in the differential corpus and agrees across both arms.
//!
//! This is the diagnostic half of Track U's `doctor` verb, scoped here to the
//! server role; the CLI wiring (a `--check` flag, exit codes, `--json` output) is
//! Track U.

use serde_json::{json, Value};

use crate::handler::Registry;
use crate::version;

pub const LEVEL_OK: &str = "ok";
pub const LEVEL_WARNING: &str = "warning";
pub const LEVEL_ERROR: &str = "error";

/// One diagnostic: a severity `level`, a stable `code` a tool keys on, and a human
/// `message`.
pub fn finding(level: &str, code: &str, message: &str) -> Value {
    json!({ "level": level, "code": code, "message": message })
}

// The Python arm formats the supported set and tool names with Python's `repr`
// (single-quoted). These helpers reproduce those exact bytes so the corpus
// verdict is byte-identical across arms.
fn repr(value: &str) -> String {
    format!("'{value}'")
}

fn supported_repr() -> String {
    let items: Vec<String> = version::SUPPORTED_PROTOCOL_VERSIONS.iter().map(|v| repr(v)).collect();
    format!("[{}]", items.join(", "))
}

/// Diagnose a server's exposed tool surface and advertised protocol version.
///
/// `tools` is the composed, namespaced tool list a server would serve (for example
/// [`Registry::list_tools`]). Findings are returned in a fixed order so the result
/// is deterministic: the protocol-version check, then the empty-surface check,
/// then per-tool checks in list order, then duplicate-name errors with the names
/// sorted.
pub fn check_server(tools: &[Value], protocol_version: &str) -> Vec<Value> {
    let mut findings: Vec<Value> = Vec::new();
    if !version::is_supported(protocol_version) {
        findings.push(finding(
            LEVEL_ERROR,
            "unsupported-protocol-version",
            &format!("advertised protocol version {} is not in the supported set {}", repr(protocol_version), supported_repr()),
        ));
    }
    if tools.is_empty() {
        findings.push(finding(LEVEL_WARNING, "no-tools", "server exposes no tools"));
    }
    let mut names: Vec<String> = Vec::new();
    for tool in tools {
        match tool.get("name").and_then(Value::as_str) {
            Some(name) if !name.is_empty() => {
                names.push(name.to_string());
                if !tool.get("inputSchema").map(Value::is_object).unwrap_or(false) {
                    findings.push(finding(LEVEL_WARNING, "missing-input-schema", &format!("tool {} has no object inputSchema", repr(name))));
                }
            }
            _ => findings.push(finding(LEVEL_ERROR, "unnamed-tool", "a tool has no name")),
        }
    }
    let mut duplicates: Vec<String> = names.iter().filter(|n| names.iter().filter(|m| m == n).count() > 1).cloned().collect();
    duplicates.sort();
    duplicates.dedup();
    for name in duplicates {
        findings.push(finding(LEVEL_ERROR, "duplicate-tool", &format!("tool name {} is exposed more than once", repr(&name))));
    }
    findings
}

/// Whether the findings are clean enough to serve: no `error`. A `warning` is
/// advisory (the server can still run), matching `nginx -t`'s split between a
/// fatal config error and a warning.
pub fn is_ok(findings: &[Value]) -> bool {
    !findings.iter().any(|f| f.get("level").and_then(Value::as_str) == Some(LEVEL_ERROR))
}

/// Run [`check_server`] over a [`Registry`]'s composed tool surface, the one-call
/// server preflight.
pub fn check_registry(registry: &Registry, protocol_version: &str) -> Vec<Value> {
    check_server(&registry.list_tools(), protocol_version)
}

pub const MODE_DEFAULT: &str = "default";
pub const MODE_STRICT: &str = "strict";
pub const MODE_LENIENT: &str = "lenient";

/// Whether the composed surface is servable under the chosen strictness.
///
/// `default` follows `nginx -t`: only an `error` blocks serving, a `warning` is
/// advisory. `strict` treats any finding as blocking (a CI gate wanting a wholly
/// clean surface). `lenient` reports findings but never blocks on the surface (a
/// config that loads is accepted; only an unloadable file, handled by the caller,
/// fails).
pub fn servable(findings: &[Value], mode: &str) -> bool {
    match mode {
        MODE_LENIENT => true,
        MODE_STRICT => findings.is_empty(),
        _ => is_ok(findings),
    }
}

/// Format findings for a human, the `nginx -t` textual report: one line per finding
/// (`level: [code] message`) followed by a verdict line that reflects the active
/// `mode`. Deterministic and byte-identical across arms, so the CLI's output is
/// pinned in the corpus.
pub fn render_text(findings: &[Value], mode: &str) -> String {
    let mut lines: Vec<String> = findings
        .iter()
        .map(|f| {
            format!(
                "{}: [{}] {}",
                f.get("level").and_then(Value::as_str).unwrap_or(""),
                f.get("code").and_then(Value::as_str).unwrap_or(""),
                f.get("message").and_then(Value::as_str).unwrap_or(""),
            )
        })
        .collect();
    lines.push(if servable(findings, mode) { "config ok" } else { "config invalid" }.to_string());
    lines.join("\n")
}

/// Machine-readable preflight report (the `--json` shape): the `ok` verdict under
/// `mode` and the ordered findings.
pub fn report(findings: &[Value], mode: &str) -> Value {
    json!({ "ok": servable(findings, mode), "findings": findings })
}

/// Process exit code for the preflight: `0` when the surface is servable under
/// `mode`, `1` when a finding blocks it.
pub fn exit_code(findings: &[Value], mode: &str) -> i32 {
    if servable(findings, mode) {
        0
    } else {
        1
    }
}
