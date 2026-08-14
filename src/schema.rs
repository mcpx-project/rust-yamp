//! JSON Schema validation for server-originated tools (σ1).
//!
//! A server that originates responses (yamp's server role, not the proxy role)
//! is accountable for the shape of what it accepts and returns. σ1 validates a
//! local handler's `tools/call` `arguments` against the tool's `inputSchema`
//! before the handler runs, and the handler's result against `outputSchema`
//! before it leaves. A bad input is the caller's fault ([`INVALID_PARAMS`], a
//! client-class error); a bad output is the server's own fault
//! ([`INTERNAL_ERROR`], a server-class error). Both are built through the
//! normalized [`crate::errors`] registry, so each carries its stable `errorId`.
//!
//! stdlib has no JSON Schema validator and one cannot be hand-rolled to
//! byte-parity with the Python arm, so this is the single dependency exception: both arms
//! take a validator dependency (`jsonschema` here, `jsonschema` there). The
//! differential corpus therefore pins the accept/reject *verdict* only, not the
//! library's internal error text; the wire error yamp emits is arm-independent
//! (the reason phrase plus a fixed `detail`), so it stays byte-identical.
//!
//! The proxy role never validates a routed backend's calls: a transparent proxy
//! must not assume a schema it did not author. Validation is a server-role act,
//! wired only into the local-handler dispatch branch and off by default.

use serde_json::Value;

use crate::errors;

/// Validate `value` against `schema`, returning `true` iff it conforms. An
/// unparseable schema fails closed (`false`): a server that cannot understand its
/// own contract must not silently accept traffic against it. This is the pure,
/// corpus-pinned verdict.
pub fn is_valid(schema: &Value, value: &Value) -> bool {
    match jsonschema::validator_for(schema) {
        Ok(validator) => validator.is_valid(value),
        Err(_) => false,
    }
}

/// Validate a `tools/call`'s `arguments` against the tool's `inputSchema`.
/// Returns the normalized [`INVALID_PARAMS`](errors::INVALID_PARAMS) error object
/// when it fails, or `None` to proceed. An absent schema imposes no contract, so
/// any arguments pass.
pub fn validate_call_args(input_schema: Option<&Value>, arguments: &Value) -> Option<Value> {
    match input_schema {
        Some(schema) if !is_valid(schema, arguments) => {
            Some(errors::error_object(errors::INVALID_PARAMS, Some("input schema validation failed")))
        }
        _ => None,
    }
}

/// Validate a handler's result against the tool's `outputSchema`. Per MCP a tool
/// that declares an `outputSchema` returns its typed result in
/// `result.structuredContent`; that value must conform. Returns the normalized
/// [`INTERNAL_ERROR`](errors::INTERNAL_ERROR) error object when it does not
/// (the server produced output it promised not to), or `None` to proceed. An
/// absent schema imposes no contract.
pub fn validate_call_result(output_schema: Option<&Value>, result: &Value) -> Option<Value> {
    match output_schema {
        Some(schema) => {
            let structured = result.get("structuredContent").unwrap_or(&Value::Null);
            if is_valid(schema, structured) {
                None
            } else {
                Some(errors::error_object(errors::INTERNAL_ERROR, Some("output schema validation failed")))
            }
        }
        None => None,
    }
}
