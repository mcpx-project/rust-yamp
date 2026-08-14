//! Protocol version negotiation (SEP §2.1, §2.2; corpus SEP-2575).
//!
//! Single source of the MCP protocol versions this proxy can serve and of the
//! rule applied when a peer names one. Two modes negotiate differently:
//!
//! - Stateful (SEP §2.2): the intermediary always presents its own highest
//!   version and lets the MCP client-side handshake settle; it never rejects.
//!   That value is [`STATEFUL_PROTOCOL_VERSION`], re-exported as
//!   [`crate::forward::PROXY_PROTOCOL_VERSION`].
//! - Stateless (SEP-2575): each request is self-describing and carries its
//!   version in `_meta` under [`PROTOCOL_VERSION_META_KEY`]. There is no
//!   handshake to fall back on, so a request naming a version the proxy cannot
//!   serve is rejected with `-32004 UNSUPPORTED_PROTOCOL_VERSION` whose data
//!   names the supported set.
//!
//! Keeping the version set and the code here (not per module) follows the
//! repo's single-source convention, the same way JSON-RPC codes live in
//! `jsonrpc`.

use serde_json::{json, Value};

// Single source in the errors registry; re-exported for existing importers.
pub use crate::errors::UNSUPPORTED_PROTOCOL_VERSION;

/// Newest first. The head is the proxy's own highest version (SEP §2.2); every
/// member is accepted on stateless negotiation.
pub const SUPPORTED_PROTOCOL_VERSIONS: [&str; 2] = ["2026-07-28", "2025-06-18"];

/// MCP stateless semantics (SEP-2575): sessionless, per-request `_meta`.
pub const STATELESS_PROTOCOL_VERSION: &str = "2026-07-28";
/// Legacy dual-handshake semantics (SEP §2.2), the version the stateful served
/// path advertises.
pub const STATEFUL_PROTOCOL_VERSION: &str = "2025-06-18";

/// Where a stateless request carries its protocol version (SEP-2575). There is
/// no initialize handshake to pin it, so it travels per-request in `_meta`.
pub const PROTOCOL_VERSION_META_KEY: &str = "io.modelcontextprotocol/protocolVersion";

pub fn is_supported(version: &str) -> bool {
    SUPPORTED_PROTOCOL_VERSIONS.contains(&version)
}

/// Resolve the version a stateless request will run under, or `None` if it
/// cannot be served. A request that omits the version accepts `default`; one
/// that names a supported version gets exactly that version; any other named
/// version yields `None` (the caller emits `-32004`).
pub fn negotiate(requested: Option<&str>, default: &'static str) -> Option<&'static str> {
    match requested {
        None => Some(default),
        Some(version) => SUPPORTED_PROTOCOL_VERSIONS
            .into_iter()
            .find(|&supported| supported == version),
    }
}

/// The `error.data` for a `-32004`: what was asked, what would work.
pub fn unsupported_error_data(requested: Option<&str>) -> Value {
    json!({
        "requested": requested,
        "supported": SUPPORTED_PROTOCOL_VERSIONS,
    })
}
