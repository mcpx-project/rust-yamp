//! Typed content-block iterator (ε1, content-block hook).
//!
//! A single traversal that finds every content block in a message and yields a
//! normalized descriptor per block, so a scanner never reimplements MCP's JSON
//! shape. It covers the two places typed content lives: a tool result's
//! `result.content[]` (text/image/audio/resource/resource_link) and a
//! `resources/read` result's `result.contents[]`. Binary payloads (image/audio
//! `data`, resource `blob`) are base64-decoded to raw bytes, surfaced as
//! lowercase hex so the descriptor is JSON-safe and byte-comparable across arms.
//!
//! Each descriptor carries the `path` to its payload field, so a mutation writes
//! back exactly there ([`set_text`]/[`set_bytes`], the seam a CDR rewrite uses)
//! without reconstructing the wire shape. Pure and deterministic; the
//! differential corpus pins the traversal and both mutations.

use serde_json::{json, Value};

use crate::base64;

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn bytes_field(value: Option<&Value>) -> Value {
    match value.and_then(Value::as_str).and_then(base64::decode) {
        Some(bytes) => Value::String(hex(&bytes)),
        None => Value::Null,
    }
}

fn descriptor(kind: &str, path: Vec<Value>, mime: Value, uri: Value, text: Value, bytes: Value) -> Value {
    json!({ "kind": kind, "path": path, "mime": mime, "uri": uri, "text": text, "bytes": bytes })
}

fn path_of(base: &[&str], index: usize, tail: &[&str]) -> Vec<Value> {
    let mut path: Vec<Value> = base.iter().map(|s| json!(s)).collect();
    path.push(json!(index));
    path.extend(tail.iter().map(|s| json!(s)));
    path
}

/// Every content block in `message`, normalized. Empty when there is no
/// recognized content array.
pub fn blocks(message: &Value) -> Value {
    let mut out: Vec<Value> = Vec::new();
    if let Some(items) = message.pointer("/result/content").and_then(Value::as_array) {
        for (index, item) in items.iter().enumerate() {
            content_block(index, item, &mut out);
        }
    }
    if let Some(items) = message.pointer("/result/contents").and_then(Value::as_array) {
        for (index, item) in items.iter().enumerate() {
            let uri = item.get("uri").cloned().unwrap_or(Value::Null);
            let mime = item.get("mimeType").cloned().unwrap_or(Value::Null);
            if let Some(text) = item.get("text") {
                out.push(descriptor("resource", path_of(&["result", "contents"], index, &["text"]), mime, uri, text.clone(), Value::Null));
            } else if item.get("blob").is_some() {
                let bytes = bytes_field(item.get("blob"));
                out.push(descriptor("resource", path_of(&["result", "contents"], index, &["blob"]), mime, uri, Value::Null, bytes));
            }
        }
    }
    Value::Array(out)
}

fn content_block(index: usize, item: &Value, out: &mut Vec<Value>) {
    let base = &["result", "content"];
    match item.get("type").and_then(Value::as_str) {
        Some("text") => {
            let text = item.get("text").cloned().unwrap_or(Value::Null);
            out.push(descriptor("text", path_of(base, index, &["text"]), Value::Null, Value::Null, text, Value::Null));
        }
        Some(kind @ ("image" | "audio")) => {
            let mime = item.get("mimeType").cloned().unwrap_or(Value::Null);
            let bytes = bytes_field(item.get("data"));
            out.push(descriptor(kind, path_of(base, index, &["data"]), mime, Value::Null, Value::Null, bytes));
        }
        Some("resource_link") => {
            let mime = item.get("mimeType").cloned().unwrap_or(Value::Null);
            let uri = item.get("uri").cloned().unwrap_or(Value::Null);
            out.push(descriptor("resource_link", path_of(base, index, &["uri"]), mime, uri, Value::Null, Value::Null));
        }
        Some("resource") => {
            let resource = item.get("resource");
            let uri = resource.and_then(|r| r.get("uri")).cloned().unwrap_or(Value::Null);
            let mime = resource.and_then(|r| r.get("mimeType")).cloned().unwrap_or(Value::Null);
            if let Some(text) = resource.and_then(|r| r.get("text")) {
                out.push(descriptor("resource", path_of(base, index, &["resource", "text"]), mime, uri, text.clone(), Value::Null));
            } else if resource.and_then(|r| r.get("blob")).is_some() {
                let bytes = bytes_field(resource.and_then(|r| r.get("blob")));
                out.push(descriptor("resource", path_of(base, index, &["resource", "blob"]), mime, uri, Value::Null, bytes));
            }
        }
        _ => {
            out.push(descriptor("unknown", path_of(base, index, &[]), Value::Null, Value::Null, Value::Null, Value::Null));
        }
    }
}

fn navigate_mut<'a>(root: &'a mut Value, path: &[Value]) -> Option<&'a mut Value> {
    let mut current = root;
    for segment in path {
        current = match segment {
            Value::String(key) => current.as_object_mut()?.get_mut(key)?,
            Value::Number(index) => current.as_array_mut()?.get_mut(index.as_u64()? as usize)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Replace the text payload at `path` with `text`, returning a new message.
pub fn set_text(message: &Value, path: &[Value], text: &str) -> Value {
    let mut out = message.clone();
    if let Some(slot) = navigate_mut(&mut out, path) {
        *slot = Value::String(text.to_string());
    }
    out
}

/// Replace the binary payload at `path` with base64-encoded `bytes`, returning a
/// new message. This is the seam by which a CDR-rewritten payload re-enters.
pub fn set_bytes(message: &Value, path: &[Value], bytes: &[u8]) -> Value {
    let mut out = message.clone();
    if let Some(slot) = navigate_mut(&mut out, path) {
        *slot = Value::String(base64::encode(bytes));
    }
    out
}
