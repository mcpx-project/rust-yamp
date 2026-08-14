//! Layer 4 resilience (SEP §8, draft §6.4).
//!
//! A circuit breaker per backend, a resilient router that returns partial
//! results on fan-out, rejects calls to open backends with `-32003`, and never
//! retries a (possibly side-effecting) `tools/call`. The clock is passed in per
//! call so the breaker's timing is deterministic under test.

use std::io;

use serde_json::{json, Map, Value};

use crate::jsonrpc::{INTERNAL_ERROR, INVALID_PARAMS};
use crate::namespace;

// Single source in the errors registry; re-exported for existing importers.
pub use crate::errors::SERVER_NOT_AVAILABLE;
pub const PROXY_PARTIAL_KEY: &str = "io.modelcontextprotocol/proxy-partial";

/// The `_meta` payload announcing a partial list result: which backends were
/// dropped from the surface and why. One object shape, shared by every path
/// that omits a backend (circuit breaker or fan-out failure), so the wire
/// contract does not depend on which layer noticed the outage.
pub fn partial_meta(mut unavailable: Vec<String>, reason: &str) -> Value {
    unavailable.sort();
    unavailable.dedup();
    json!({ PROXY_PARTIAL_KEY: { "unavailable_backends": unavailable, "reason": reason } })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    threshold: u32,
    reset: f64,
    state: CircuitState,
    failures: u32,
    opened_at: f64,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, reset: f64) -> Self {
        Self {
            threshold,
            reset,
            state: CircuitState::Closed,
            failures: 0,
            opened_at: 0.0,
        }
    }

    pub fn state(&mut self, now: f64) -> CircuitState {
        if self.state == CircuitState::Open && now - self.opened_at >= self.reset {
            self.state = CircuitState::HalfOpen;
        }
        self.state
    }

    pub fn allow(&mut self, now: f64) -> bool {
        self.state(now) != CircuitState::Open
    }

    pub fn record_success(&mut self) {
        self.failures = 0;
        self.state = CircuitState::Closed;
    }

    pub fn record_failure(&mut self, now: f64) {
        if self.state(now) == CircuitState::HalfOpen {
            self.trip(now); // the trial failed; reopen
            return;
        }
        self.failures += 1;
        if self.failures >= self.threshold {
            self.trip(now);
        }
    }

    fn trip(&mut self, now: f64) {
        self.state = CircuitState::Open;
        self.opened_at = now;
        self.failures = self.threshold;
    }
}

/// A backend the router can call. `request` returns `Err` for a transport
/// failure (which trips the breaker) and `Ok(response)` otherwise, including
/// application errors (which do not).
pub trait BackendChannel {
    fn id(&self) -> &str;
    fn request(
        &mut self,
        method: &str,
        params: Value,
    ) -> impl std::future::Future<Output = io::Result<Value>> + Send;
}

pub struct ManagedBackend<C> {
    pub channel: C,
    pub breaker: CircuitBreaker,
}

impl<C: BackendChannel> ManagedBackend<C> {
    pub fn new(channel: C, breaker: CircuitBreaker) -> Self {
        Self { channel, breaker }
    }
}

fn error(id: Value, code: i64, message: String) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

pub struct ResilientRouter<C> {
    backends: Vec<ManagedBackend<C>>,
    now: Box<dyn Fn() -> f64>,
    last_available: Vec<String>,
}

impl<C: BackendChannel> ResilientRouter<C> {
    pub fn new(backends: Vec<ManagedBackend<C>>, now: Box<dyn Fn() -> f64>) -> Self {
        let mut router = Self {
            backends,
            now,
            last_available: Vec::new(),
        };
        router.last_available = router.available_ids();
        router
    }

    fn available_ids(&mut self) -> Vec<String> {
        let now = (self.now)();
        let mut ids = Vec::new();
        for backend in &mut self.backends {
            if backend.breaker.allow(now) {
                ids.push(backend.channel.id().to_string());
            }
        }
        ids.sort();
        ids
    }

    pub async fn tools_list(&mut self, id: Value) -> io::Result<Value> {
        let now = (self.now)();
        let mut tools = Vec::new();
        let mut unavailable = Vec::new();
        for backend in &mut self.backends {
            if !backend.breaker.allow(now) {
                unavailable.push(backend.channel.id().to_string());
                continue;
            }
            match backend.channel.request("tools/list", json!({})).await {
                Err(_) => {
                    backend.breaker.record_failure(now);
                    unavailable.push(backend.channel.id().to_string());
                }
                Ok(response) => {
                    backend.breaker.record_success();
                    let listed = response
                        .get("result")
                        .and_then(|r| r.get("tools"))
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    for tool in listed {
                        let mut entry = tool.clone();
                        let name = tool.get("name").and_then(Value::as_str).unwrap_or("");
                        if let Some(object) = entry.as_object_mut() {
                            object.insert("name".to_string(), Value::String(namespace::prefix(backend.channel.id(), name)));
                        }
                        tools.push(entry);
                    }
                }
            }
        }
        let mut result = Map::new();
        result.insert("tools".to_string(), Value::Array(tools));
        if !unavailable.is_empty() {
            result.insert("_meta".to_string(), partial_meta(unavailable, "circuit_breaker_open"));
        }
        Ok(json!({ "jsonrpc": "2.0", "id": id, "result": Value::Object(result) }))
    }

    pub async fn tools_call(&mut self, message: &Value, id: Value) -> io::Result<Value> {
        let now = (self.now)();
        let name = message
            .get("params")
            .and_then(|p| p.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let resolved = namespace::split(name);
        let index = resolved.and_then(|(bid, _)| self.backends.iter().position(|b| b.channel.id() == bid));
        let (index, original) = match (index, resolved) {
            (Some(index), Some((_, original))) => (index, original.to_string()),
            _ => return Ok(error(id, INVALID_PARAMS, format!("unknown tool: {name}"))),
        };

        if !self.backends[index].breaker.allow(now) {
            let bid = self.backends[index].channel.id().to_string();
            return Ok(error(id, SERVER_NOT_AVAILABLE, format!("backend {bid} unavailable")));
        }

        let mut params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        if let Some(object) = params.as_object_mut() {
            object.insert("name".to_string(), Value::String(original));
        }
        // Single attempt: tools/call may have side effects, so it is not retried.
        match self.backends[index].channel.request("tools/call", params).await {
            Err(_) => {
                self.backends[index].breaker.record_failure(now);
                let bid = self.backends[index].channel.id().to_string();
                Ok(error(id, SERVER_NOT_AVAILABLE, format!("backend {bid} failed")))
            }
            Ok(response) => {
                self.backends[index].breaker.record_success();
                if let Some(result) = response.get("result") {
                    Ok(json!({ "jsonrpc": "2.0", "id": id, "result": result.clone() }))
                } else {
                    let err = response
                        .get("error")
                        .cloned()
                        .unwrap_or_else(|| json!({ "code": INTERNAL_ERROR, "message": "backend error" }));
                    Ok(json!({ "jsonrpc": "2.0", "id": id, "error": err }))
                }
            }
        }
    }

    pub fn surface_changed(&mut self) -> bool {
        let current = self.available_ids();
        let changed = current != self.last_available;
        self.last_available = current;
        changed
    }

    pub fn list_changed_notification() -> Value {
        json!({ "jsonrpc": "2.0", "method": "notifications/tools/list_changed" })
    }
}
