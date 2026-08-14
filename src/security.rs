//! Secure zero-config defaults (Track U, acceptance criterion U7).
//!
//! A zero-config run must be safe by default: bound to loopback, and refusing to
//! expose a non-loopback listener without client authentication. This module is the
//! single source of that policy. [`is_loopback`] classifies a bind host and
//! [`check_bind`] is the pure gate (an empty findings list means safe to bind); both
//! are pinned in the differential corpus. [`guard_bind`] is the thin entrypoint
//! adapter the servers call before binding, honoring an explicit `--insecure`
//! override.
//!
//! Token passthrough is structurally impossible independently of this module: the
//! `auth` module strips the client credential and injects the backend's own on every
//! forwarded request (confused-deputy prevention, δ20), so a client token can never
//! reach a backend regardless of bind policy.

use serde_json::{json, Value};

pub const LEVEL_ERROR: &str = "error";

/// The secure default bind host: loopback, reachable only by local processes.
pub const DEFAULT_BIND_HOST: &str = "127.0.0.1";

/// Whether `host` names a loopback interface (reachable only locally).
///
/// Loopback is `localhost`, the IPv6 loopback (`::1`, bracketed or not), and the
/// IPv4 `127.0.0.0/8` block. Everything else, including an empty host and the
/// wildcard binds (`0.0.0.0`, `::`), is treated as non-loopback (publicly reachable),
/// the conservative choice for a security gate.
pub fn is_loopback(host: &str) -> bool {
    let h = host.trim().to_lowercase();
    if h == "localhost" || h == "::1" || h == "[::1]" {
        return true;
    }
    h.starts_with("127.")
}

/// The bind-safety gate. Returns an empty list when binding `host` is safe, or a
/// single error finding when it would expose a non-loopback listener without client
/// authentication. A loopback bind is always safe; a non-loopback bind is safe only
/// when client auth is configured.
pub fn check_bind(host: &str, has_client_auth: bool) -> Vec<Value> {
    if is_loopback(host) || has_client_auth {
        return Vec::new();
    }
    vec![json!({
        "level": LEVEL_ERROR,
        "code": "insecure-bind",
        "message": format!(
            "refusing to bind non-loopback address {host} without client authentication; \
             bind {DEFAULT_BIND_HOST}, set auth.clientTokens, or pass --insecure"
        ),
    })]
}

/// Entrypoint adapter: given a `host:port` listen string, return an error message
/// when the bind must be refused, or `None` when it may proceed. An explicit
/// `insecure` override downgrades a refusal to allowed.
pub fn guard_bind(listen: &str, has_client_auth: bool, insecure: bool) -> Option<String> {
    let host = listen.rsplit_once(':').map(|(h, _)| h).unwrap_or(listen);
    let findings = check_bind(host, has_client_auth);
    if !findings.is_empty() && !insecure {
        return findings[0]["message"].as_str().map(String::from);
    }
    None
}
