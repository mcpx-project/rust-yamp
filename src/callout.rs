//! ext-proc callout transport (ε3).
//!
//! A tier-2 extension runs out of process (an ICAP/AV/DLP/CDR bridge, an
//! LLM-based scanner). The proxy reaches it with a framed callout that reuses
//! the existing [`crate::transport`] framing, so no gRPC dependency is pulled
//! in. A callout carries the ε2 interest context and a content chunk; the
//! service answers with one of the ε0 verdicts (or the preview `continue`
//! signal). Three protections wrap every callout: the verdict is cached by
//! content digest so a duplicate payload is not rescanned; a byte budget rejects
//! an oversize payload rather than buffering it; and a deadline bounds a slow or
//! hung scanner. On any transport failure the host applies the filter's failure
//! policy (fail-closed denies), never the scanner.
//!
//! The request/response envelope encoders and the verdict parser are pure and
//! mirror the Python arm; the differential corpus pins them. The async client is
//! integration-tested per arm against a scripted in-process service.

use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::time::timeout;

use crate::transport::{MessageRead, MessageWrite};
use crate::{base64, filters, jsonrpc, signing};

pub const CALLOUT_VERSION: &str = "1";
pub const PHASE_PREVIEW: &str = "preview";
pub const PHASE_BODY: &str = "body";

/// The verdict-cache key: SHA-256 of the content, content-addressed so identical
/// payloads share a verdict.
pub fn content_digest(content: &[u8]) -> String {
    signing::sha256_hex(content)
}

/// The callout request envelope the proxy sends to the service.
pub fn callout_request(context: &Value, phase: &str, content: &[u8], ieof: bool) -> Value {
    json!({
        "callout": CALLOUT_VERSION,
        "phase": phase,
        "context": context,
        "ieof": ieof,
        "content": base64::encode(content),
    })
}

/// Whether `size` exceeds a positive `max_bytes` budget (0 means unlimited).
pub fn exceeds_budget(size: usize, max_bytes: usize) -> bool {
    max_bytes > 0 && size > max_bytes
}

fn valid_verdict(kind: &str) -> bool {
    filters::VERDICTS.contains(&kind) || kind == filters::CONTINUE
}

/// Parse a service response into a block-scoped verdict. A `mutate` carries the
/// replacement bytes (base64 on the wire, hex in the verdict); `annotate` carries
/// provenance; `deny`/`quarantine` a reason. A malformed response is resolved by
/// the failure policy, never trusted.
pub fn parse_verdict(response: &Value, failure_policy: &str) -> Value {
    let kind = match response.get("verdict").and_then(Value::as_str) {
        Some(kind) if valid_verdict(kind) => kind,
        _ => {
            return json!({ "kind": filters::resolve_failure(failure_policy), "reason": "malformed callout response" })
        }
    };
    if kind == filters::CONTINUE {
        return json!({ "kind": filters::CONTINUE });
    }
    let mut verdict = Map::new();
    verdict.insert("kind".to_string(), json!(kind));
    if kind == filters::DENY || kind == filters::QUARANTINE {
        verdict.insert("reason".to_string(), json!(response.get("reason").and_then(Value::as_str).unwrap_or("")));
    }
    if kind == filters::MUTATE {
        let bytes = response.get("content").and_then(Value::as_str).and_then(base64::decode);
        verdict.insert("bytes".to_string(), bytes.map(|b| json!(signing::to_hex(&b))).unwrap_or(Value::Null));
        if let Some(provenance) = response.get("provenance") {
            verdict.insert("provenance".to_string(), provenance.clone()); // a modified body may also annotate (§6.5)
        }
    }
    if kind == filters::ANNOTATE {
        verdict.insert("provenance".to_string(), response.get("provenance").cloned().unwrap_or_else(|| json!({})));
    }
    Value::Object(verdict)
}

/// A content-addressed cache of callout verdicts (SEP §6.4): identical content
/// digests share a verdict, so retries and duplicate payloads are not rescanned.
#[derive(Default)]
pub struct VerdictCache {
    map: std::collections::HashMap<String, Value>,
}

impl VerdictCache {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn get(&self, digest: &str) -> Option<Value> {
        self.map.get(digest).cloned()
    }
    pub fn put(&mut self, digest: String, verdict: Value) {
        self.map.insert(digest, verdict);
    }
}

/// An out-of-process callout over a framed transport, with a preview phase, a
/// byte budget, a deadline, an optional verdict cache, and a failure policy.
pub struct CalloutClient<R, W> {
    reader: R,
    writer: W,
    failure_policy: &'static str,
    max_bytes: usize,
    preview_bytes: usize,
    deadline: Option<Duration>,
}

impl<R: MessageRead, W: MessageWrite> CalloutClient<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer, failure_policy: filters::FAIL_CLOSED, max_bytes: 0, preview_bytes: 0, deadline: None }
    }

    pub fn with_failure_policy(mut self, policy: &'static str) -> Self {
        self.failure_policy = policy;
        self
    }
    pub fn with_budget(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }
    pub fn with_preview(mut self, preview_bytes: usize) -> Self {
        self.preview_bytes = preview_bytes;
        self
    }
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    fn failed(&self, reason: &str) -> Value {
        json!({ "kind": filters::resolve_failure(self.failure_policy), "reason": reason })
    }

    /// Scan `content`, consulting `cache` first and populating it after. Runs the
    /// preview phase, escalating to a body call only when the service continues.
    pub async fn scan(&mut self, context: &Value, content: &[u8], cache: Option<&mut VerdictCache>) -> Value {
        if exceeds_budget(content.len(), self.max_bytes) {
            return self.failed("payload exceeds byte budget");
        }
        let digest = content_digest(content);
        if let Some(cache) = cache.as_deref() {
            if let Some(hit) = cache.get(&digest) {
                return hit;
            }
        }
        let mut verdict = self.exchange(context, content).await;
        if verdict.get("kind").and_then(Value::as_str) == Some(filters::CONTINUE) {
            verdict = self.call(context, PHASE_BODY, content, true).await;
        }
        if verdict.get("kind").and_then(Value::as_str) != Some(filters::CONTINUE) {
            if let Some(cache) = cache {
                cache.put(digest, verdict.clone());
            }
        }
        verdict
    }

    async fn exchange(&mut self, context: &Value, content: &[u8]) -> Value {
        let n = if self.preview_bytes > 0 { self.preview_bytes.min(content.len()) } else { content.len() };
        let ieof = n >= content.len();
        self.call(context, PHASE_PREVIEW, &content[..n], ieof).await
    }

    async fn call(&mut self, context: &Value, phase: &str, content: &[u8], ieof: bool) -> Value {
        let request = callout_request(context, phase, content, ieof);
        if self.writer.send(&jsonrpc::encode(&request)).await.is_err() {
            return self.failed("callout transport error");
        }
        let received = match self.deadline {
            Some(deadline) => match timeout(deadline, self.reader.receive()).await {
                Ok(result) => result,
                Err(_) => return self.failed("callout deadline exceeded"),
            },
            None => self.reader.receive().await,
        };
        match received {
            Ok(Some(raw)) => match jsonrpc::decode(&raw) {
                Ok(response) => parse_verdict(&response, self.failure_policy),
                Err(_) => self.failed("callout decode error"),
            },
            Ok(None) => self.failed("callout closed"),
            Err(_) => self.failed("callout transport error"),
        }
    }
}
