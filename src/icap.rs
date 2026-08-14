//! Reference ICAP bridge (ε4).
//!
//! An ordinary tier-2 extension: nothing in the core knows ICAP exists. Two
//! halves. The bridge-service side translates an ICAP response into a callout
//! response ([`icap_to_callout`]), so the same ε3 wire protocol carries it: a
//! threat quarantines; `204` passes unmodified; `200` with a modified body
//! substitutes the CDR-rewritten content and annotates provenance; `403` denies;
//! any other status denies, because a security bridge fails safe. Client-to-
//! upstream payloads are REQMOD, upstream-to-client RESPMOD ([`icap_mode`]). A
//! `resource_link` is dereferenced only on an explicit opt-in ([`should_deref`]):
//! a link fetch is an SSRF surface, never a default. These three are pure and
//! pinned in the corpus.
//!
//! The yamp side ([`ContentScanner`]) iterates a message's content blocks (ε1),
//! calls the out-of-process bridge per block (ε3), applies the block verdicts,
//! and aggregates to an outcome shaped like the filter chain's. It is
//! integration-tested end to end against a scripted bridge service.

use serde_json::{json, Map, Value};

use crate::callout::{CalloutClient, VerdictCache};
use crate::transport::{MessageRead, MessageWrite};
use crate::{content, filters};

pub const REQMOD: &str = "REQMOD";
pub const RESPMOD: &str = "RESPMOD";

/// REQMOD for client->upstream payloads, RESPMOD for upstream->client.
pub fn icap_mode(direction: &str) -> &'static str {
    if direction == filters::C2U {
        REQMOD
    } else {
        RESPMOD
    }
}

/// Whether to dereference a `resource_link` for scanning. Off by default: a link
/// fetch is an SSRF surface, so dereferencing is an explicit opt-in.
pub fn should_deref(kind: &str, enabled: bool) -> bool {
    kind == "resource_link" && enabled
}

/// Translate an ICAP response into a callout response (§6.5). `content`/`modified`
/// stay base64 on the wire; the yamp side decodes them.
pub fn icap_to_callout(response: &Value) -> Value {
    if let Some(threat) = response.get("threat").and_then(Value::as_str) {
        if !threat.is_empty() {
            return json!({ "verdict": "quarantine", "reason": threat });
        }
    }
    match response.get("status").and_then(Value::as_i64) {
        Some(204) => json!({ "verdict": "allow" }),
        Some(200) => match response.get("modified").and_then(Value::as_str) {
            Some(modified) => json!({
                "verdict": "mutate",
                "content": modified,
                "provenance": { "icap": "modified", "istag": response.get("istag").cloned().unwrap_or(Value::Null) },
            }),
            None => json!({ "verdict": "allow" }),
        },
        Some(403) => json!({ "verdict": "deny", "reason": "ICAP policy blocked" }),
        _ => json!({ "verdict": "deny", "reason": "unexpected ICAP status" }),
    }
}

fn from_hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .filter_map(|i| text.get(i..i + 2).and_then(|pair| u8::from_str_radix(pair, 16).ok()))
        .collect()
}

fn block_payload(block: &Value) -> Option<Vec<u8>> {
    if let Some(hex) = block.get("bytes").and_then(Value::as_str) {
        return Some(from_hex(hex));
    }
    if let Some(text) = block.get("text").and_then(Value::as_str) {
        return Some(text.as_bytes().to_vec());
    }
    None // resource_link/unknown: no inline bytes (deref is opt-in)
}

fn apply_mutation(message: &Value, block: &Value, new_hex: &str) -> Value {
    let new_bytes = from_hex(new_hex);
    let path = block.get("path").and_then(Value::as_array).cloned().unwrap_or_default();
    if block.get("bytes").map(|b| !b.is_null()).unwrap_or(false) {
        content::set_bytes(message, &path, &new_bytes)
    } else {
        content::set_text(message, &path, &String::from_utf8_lossy(&new_bytes))
    }
}

fn annotate(message: &Value, provenance: &Map<String, Value>) -> Value {
    let mut out = message.clone();
    let object = match out.as_object_mut() {
        Some(object) => object,
        None => return out,
    };
    let key = if object.get("result").map(Value::is_object).unwrap_or(false) { "result" } else { "params" };
    let holder = object.entry(key).or_insert_with(|| json!({}));
    if !holder.is_object() {
        *holder = json!({});
    }
    let holder = holder.as_object_mut().expect("just ensured object");
    let mut meta = holder.get("_meta").and_then(Value::as_object).cloned().unwrap_or_default();
    for (key, value) in provenance {
        meta.insert(key.clone(), value.clone());
    }
    holder.insert("_meta".to_string(), Value::Object(meta));
    out
}

/// The yamp side of the ICAP bridge: scan a message's content blocks through the
/// out-of-process bridge and apply the verdicts.
pub struct ContentScanner<R, W> {
    client: CalloutClient<R, W>,
}

impl<R: MessageRead, W: MessageWrite> ContentScanner<R, W> {
    pub fn new(client: CalloutClient<R, W>) -> Self {
        Self { client }
    }

    pub async fn scan(&mut self, message: &Value, direction: &str, mut cache: Option<&mut VerdictCache>) -> Value {
        let context = filters::message_context(message, direction);
        let blocks = content::blocks(message);
        let empty: Vec<Value> = Vec::new();
        let mut working = message.clone();
        let mut provenance: Map<String, Value> = Map::new();
        for block in blocks.as_array().unwrap_or(&empty) {
            let payload = match block_payload(block) {
                Some(payload) => payload,
                None => continue,
            };
            let verdict = self.client.scan(&context, &payload, cache.as_deref_mut()).await;
            let kind = verdict.get("kind").and_then(Value::as_str).unwrap_or("");
            if kind == filters::DENY || kind == filters::QUARANTINE {
                let reason = verdict.get("reason").and_then(Value::as_str).unwrap_or("");
                let id = message.get("id").cloned().unwrap_or(Value::Null);
                return json!({
                    "action": "block",
                    "response": filters::deny_response(&id, reason),
                    "quarantined": kind == filters::QUARANTINE,
                });
            }
            if kind == filters::MUTATE {
                if let Some(bytes_hex) = verdict.get("bytes").and_then(Value::as_str) {
                    working = apply_mutation(&working, block, bytes_hex);
                }
            }
            if let Some(prov) = verdict.get("provenance").and_then(Value::as_object) {
                for (key, value) in prov {
                    provenance.insert(key.clone(), value.clone());
                }
            }
        }
        if !provenance.is_empty() {
            working = annotate(&working, &provenance);
        }
        json!({ "action": "forward", "message": working })
    }
}
