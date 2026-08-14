//! Minimal JSON-RPC 2.0 helpers over `serde_json::Value`.
//!
//! δ1 makes the proxy protocol-aware for the initialize handshake only;
//! payloads are still forwarded unchanged once past it.

use std::io;

use serde_json::Value;

// JSON-RPC 2.0 error codes used across the proxy layers. Centralized here (the
// JSON-RPC layer) so no module redefines them.
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

pub fn decode(bytes: &[u8]) -> io::Result<Value> {
    serde_json::from_slice(bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

pub fn encode(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("a serde_json::Value always serializes")
}

pub fn method_of(value: &Value) -> Option<&str> {
    value.get("method").and_then(Value::as_str)
}
