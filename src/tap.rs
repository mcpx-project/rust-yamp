//! Redacting live capture (Track U `tap`, the `tcpdump`-for-the-proxy analog).
//!
//! A tap lets an operator watch live traffic, so it must never surface a credential.
//! [`redact`] is the single source of that guarantee: a pure, deterministic deep
//! masking of every sensitive value in a JSON message, pinned across arms in the
//! differential corpus. [`capture`] wraps a message into a redacted capture record.
//! The serving entrypoints print these records under `--tap`.

use serde_json::{json, Map, Value};

pub const MASK: &str = "***";

/// Whether a key's value carries a credential or identity and must be masked. Matched
/// case-insensitively, so `Authorization` / `apiKey` / `API_KEY` all redact.
pub fn is_sensitive(key: &str) -> bool {
    matches!(
        key.to_lowercase().as_str(),
        "authorization" | "token" | "secret" | "password" | "apikey" | "api_key" | "credential" | "claims"
    )
}

/// Return a copy of a JSON message with every sensitive value masked, wherever it
/// appears in the tree. Pure and deterministic, so a capture is safe to log.
pub fn redact(message: &Value) -> Value {
    match message {
        Value::Object(obj) => {
            let mut out = Map::new();
            for (key, value) in obj {
                if is_sensitive(key) {
                    out.insert(key.clone(), Value::String(MASK.to_string()));
                } else {
                    out.insert(key.clone(), redact(value));
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(redact).collect()),
        other => other.clone(),
    }
}

/// A redacted capture record for one message: its `direction` (`c2s`/`s2c`), its
/// method and id for quick scanning, and the fully redacted payload.
pub fn capture(direction: &str, message: &Value) -> Value {
    json!({
        "direction": direction,
        "method": message.get("method").cloned().unwrap_or(Value::Null),
        "id": message.get("id").cloned().unwrap_or(Value::Null),
        "message": redact(message),
    })
}
