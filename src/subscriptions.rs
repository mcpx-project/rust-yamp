//! Resource-subscription routing and origination (σ4; MCP
//! `resources/subscribe`).
//!
//! A client subscribes to a resource by URI; the party that owns the resource
//! then emits `notifications/resources/updated` when it changes. yamp handles
//! both roles from one seam, mirroring tasks (δ19 routing plus σ3 origination):
//!
//! - Proxy role: `resources/subscribe` and `resources/unsubscribe` carry a
//!   namespaced URI (`backend__uri`, like `resources/read`). yamp reverse-
//!   resolves it to the owning backend and forwards with the backend's own URI;
//!   the backend's `notifications/resources/updated` is re-namespaced so the
//!   client sees the same `backend__uri` it holds (the mirror of
//!   `tasks::namespace_event`).
//! - Server role: a subscription whose URI does not resolve to a backend is
//!   registered in a per-connection registry. When the resource changes,
//!   `publish_resource_update` fans out the notification only to the subscribed
//!   URIs, so an unsubscribed resource costs nothing.
//!
//! Registrations are per-connection node-local state (like the σ3 task store); a
//! cross-node shared registry is a fleet concern (Track F), not this increment.

use std::collections::HashSet;

use serde_json::{json, Value};

use crate::namespace;

pub const SUBSCRIBE_METHOD: &str = "resources/subscribe";
pub const UNSUBSCRIBE_METHOD: &str = "resources/unsubscribe";
// Server/backend -> client: the resource changed. One method for both roles.
pub const UPDATED_METHOD: &str = "notifications/resources/updated";

/// Whether a method is a resource subscribe/unsubscribe request.
pub fn is_subscribe_method(method: &str) -> bool {
    method == SUBSCRIBE_METHOD || method == UNSUBSCRIBE_METHOD
}

/// The `notifications/resources/updated` a server originates for a changed
/// resource (σ4). The proxy role never builds this; it re-namespaces the
/// backend's own with [`namespace_updated`].
pub fn updated_notification(uri: &str) -> Value {
    json!({ "jsonrpc": "2.0", "method": UPDATED_METHOD, "params": { "uri": uri } })
}

/// Re-namespace the `uri` in a backend's `notifications/resources/updated` so the
/// client sees the same `backend__uri` it holds (the mirror of
/// `tasks::namespace_event`). A message without a string `params.uri` is returned
/// unchanged.
pub fn namespace_updated(message: &Value, backend_id: &str) -> Value {
    let mut out = message.clone();
    if let Some(params) = out.get_mut("params").and_then(Value::as_object_mut) {
        if let Some(uri) = params.get("uri").and_then(Value::as_str) {
            let namespaced = namespace::prefix_uri(backend_id, uri);
            params.insert("uri".to_string(), Value::String(namespaced));
        }
    }
    out
}

/// The server's per-connection resource-subscription registry (σ4). A set of
/// subscribed URIs: `subscribe` adds, `unsubscribe` removes, and the update
/// fan-out consults membership so an unsubscribed resource is never sent. A
/// re-subscribe is idempotent (a set holds each URI once).
#[derive(Default)]
pub struct Subscriptions {
    uris: HashSet<String>,
}

impl Subscriptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&mut self, uri: &str) {
        self.uris.insert(uri.to_string());
    }

    /// Remove a subscription. Returns whether it was present.
    pub fn unsubscribe(&mut self, uri: &str) -> bool {
        self.uris.remove(uri)
    }

    pub fn contains(&self, uri: &str) -> bool {
        self.uris.contains(uri)
    }

    pub fn count(&self) -> usize {
        self.uris.len()
    }
}
