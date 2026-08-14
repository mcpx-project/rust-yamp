//! Canonical error-code registry (single source).
//!
//! The four standard JSON-RPC codes stay defined in [`crate::jsonrpc`] (the
//! protocol layer) and are re-exported here, so this module is the one place
//! that lists every code yamp may emit. The yamp proxy codes below occupy the
//! JSON-RPC server-defined range (-32000 to -32099); each has exactly one
//! meaning (SEP-0000 §12), so no code is overloaded.
//!
//! Backend error codes are never rewritten: an error a backend returns passes
//! through the router unchanged, including codes yamp does not define. A proxy
//! must not assume a meaning it did not negotiate (SEP-2678).

use serde_json::{json, Value};

pub use crate::jsonrpc::{INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND};

// yamp proxy codes (JSON-RPC server-defined range). One fixed meaning each.
/// Missing or invalid session identifier.
pub const NO_SESSION: i64 = -32000;
/// Request denied by a policy rule (SEP §10.7).
pub const POLICY_DENIED: i64 = -32001;
/// Client authentication or authorization failure.
pub const UNAUTHORIZED: i64 = -32002;
/// Backend unavailable / circuit breaker open (SEP §8.1).
pub const SERVER_NOT_AVAILABLE: i64 = -32003;
/// Requested protocol version not supported (SEP §2.1).
pub const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32004;
/// Transport header disagrees with the body (SEP-2243).
pub const HEADER_MISMATCH: i64 = -32005;

/// Every code this module defines, standard and proxy, for registry checks.
pub const ALL_CODES: [i64; 10] = [
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    INVALID_PARAMS,
    INTERNAL_ERROR,
    NO_SESSION,
    POLICY_DENIED,
    UNAUTHORIZED,
    SERVER_NOT_AVAILABLE,
    UNSUPPORTED_PROTOCOL_VERSION,
    HEADER_MISMATCH,
];

/// The normalized registry: for each JSON-RPC numeric code, a stable string id
/// and a canonical reason phrase. This is HTTP-status-shaped: the numeric code is
/// the wire code (like `404`), the id is a yamp-stable machine key (like a
/// vendor error slug), and the reason is the fixed short phrase (like
/// `Not Found`). The id's leading digit is the error class, matching HTTP: `E4xxx`
/// is a client-caused error, `E5xxx` a server-side one, and the remaining digits
/// echo the nearest HTTP status where one exists (`E4010`≈401, `E4030`≈403,
/// `E4004`≈404, `E4260`≈426, `E5030`≈503). Backend codes are not in this table
/// (they pass through unnamed, per SEP-2678), so lookups return `""`.
///
/// Each entry also carries a one-line `cause` (why the error is emitted) and a
/// `hint` (how a caller resolves it), the human half of the registry. These feed
/// the generated error index (`ERRORS.md`, one source, cannot drift) and any
/// `explain` surface; the wire error object stays lean (id only), so cause and hint
/// never bloat the reply. Both arms carry the identical strings, pinned in the
/// differential corpus (`error_describe`).
///
/// One source: every emitter that wants a normalized error builds it through
/// [`error_object`] rather than restating an id or phrase.
pub const REGISTRY: [(i64, &str, &str, &str, &str); 10] = [
    (INVALID_REQUEST, "E4000", "Invalid Request",
     "The message is not a well-formed JSON-RPC request object.",
     "Send a JSON-RPC 2.0 request carrying jsonrpc, method, and id."),
    (INVALID_PARAMS, "E4002", "Invalid Params",
     "The request's params do not match the method's expected shape.",
     "Check the tool's inputSchema and correct the arguments."),
    (METHOD_NOT_FOUND, "E4004", "Method Not Found",
     "The requested method or namespaced tool is exposed by no backend or handler.",
     "List the available tools and call one of the exposed names."),
    (UNAUTHORIZED, "E4010", "Unauthorized",
     "The client's token failed issuer or audience validation, or none was presented.",
     "Present a token whose iss and aud match the proxy's configured values."),
    (POLICY_DENIED, "E4030", "Policy Denied",
     "A policy rule or extension filter denied the request.",
     "Review the active policy and filter chain, then adjust the rule or the request."),
    (NO_SESSION, "E4400", "No Session",
     "The request referenced a session that is missing or has expired.",
     "Reinitialize to obtain a session before sending session-scoped requests."),
    (UNSUPPORTED_PROTOCOL_VERSION, "E4260", "Unsupported Protocol Version",
     "The declared protocol version is not in the proxy's supported set.",
     "Negotiate one of the supported versions named in the error data."),
    (HEADER_MISMATCH, "E4006", "Header Mismatch",
     "A transport routing header disagrees with the request body.",
     "Make Mcp-Method and Mcp-Name agree with the body, or omit them."),
    (INTERNAL_ERROR, "E5000", "Internal Error",
     "The proxy or a local handler failed while processing the request.",
     "Retry; if it persists, check the server logs and report the error id."),
    (SERVER_NOT_AVAILABLE, "E5030", "Server Not Available",
     "The target backend is unavailable or its circuit breaker is open.",
     "Wait for the backend to recover; the proxy resumes once it is healthy."),
];

/// The stable string id for a code (`""` if the registry does not define it).
pub fn id(code: i64) -> &'static str {
    REGISTRY.iter().find(|(c, ..)| *c == code).map(|(_, id, ..)| *id).unwrap_or("")
}

/// The canonical reason phrase for a code (`""` if the registry does not define it).
pub fn reason(code: i64) -> &'static str {
    REGISTRY.iter().find(|(c, ..)| *c == code).map(|(_, _, r, ..)| *r).unwrap_or("")
}

/// The one-line cause for a code (`""` if the registry does not define it).
pub fn cause(code: i64) -> &'static str {
    REGISTRY.iter().find(|(c, ..)| *c == code).map(|(_, _, _, cause, _)| *cause).unwrap_or("")
}

/// The fix hint for a code (`""` if the registry does not define it).
pub fn hint(code: i64) -> &'static str {
    REGISTRY.iter().find(|(c, ..)| *c == code).map(|(_, _, _, _, hint)| *hint).unwrap_or("")
}

/// A stable in-index anchor for a code, derived from its id so it cannot drift
/// (`""` if the registry does not define it). The generated `ERRORS.md` gives each
/// error a heading with this fragment.
pub fn docs_url(code: i64) -> String {
    let eid = id(code);
    if eid.is_empty() {
        String::new()
    } else {
        format!("ERRORS.md#{}", eid.to_lowercase())
    }
}

/// The full human description of a registered code: id, reason, cause, hint, and the
/// docs anchor. Keyed on the yamp identity, this is what an `explain` surface or the
/// generated index renders. Pinned across arms in the corpus.
pub fn describe(code: i64) -> Value {
    json!({
        "code": code,
        "errorId": id(code),
        "reason": reason(code),
        "cause": cause(code),
        "hint": hint(code),
        "docsUrl": docs_url(code),
    })
}

/// Build a normalized JSON-RPC error object for a registered `code`. The
/// `message` is the registry's fixed reason phrase; `data.errorId` carries the
/// stable id so a client or dashboard keys on the yamp identity rather than the
/// numeric code (which the JSON-RPC standard range shares). An optional `detail`
/// string rides in `data.detail`, kept arm-independent (no library-specific text)
/// so both arms emit byte-identical errors.
pub fn error_object(code: i64, detail: Option<&str>) -> Value {
    let mut data = serde_json::Map::new();
    data.insert("errorId".to_string(), Value::String(id(code).to_string()));
    if let Some(detail) = detail {
        data.insert("detail".to_string(), Value::String(detail.to_string()));
    }
    json!({ "code": code, "message": reason(code), "data": Value::Object(data) })
}
