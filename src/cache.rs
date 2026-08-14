//! Capability-list cache (SEP §6, corpus SEP-2549).
//!
//! List methods (`tools/list`, `prompts/list`, `resources/list`,
//! `server/discover`) are cacheable keyed on backend identity. A backend result
//! may carry SEP-2549 directives: `ttlMs` (freshness in milliseconds, `0`
//! meaning immediately stale) and `cacheScope` (`"public"` shareable across
//! principals, `"private"` never served to a different principal). A shared
//! proxy cache collapses repeated sub-agent list fetches from
//! `O(subagents × backends)` to `O(backends)`.
//!
//! Invalidation (SEP §6.2): a backend's entries are dropped when it emits a
//! `list_changed` notification and when its circuit breaker opens. Callers pass
//! the current time in, so freshness is deterministic under test, mirroring the
//! circuit breaker.

use std::collections::HashMap;

use serde_json::Value;

pub const TTL_MS_KEY: &str = "ttlMs";
pub const CACHE_SCOPE_KEY: &str = "cacheScope";
pub const PUBLIC: &str = "public";
pub const PRIVATE: &str = "private";
/// Default freshness when a backend returns no `ttlMs` (draft §6.2: SHOULD
/// support a configurable default TTL of 300 seconds).
pub const DEFAULT_TTL_MS: f64 = 300_000.0;

struct Entry {
    result: Value,
    expires_at: f64, // same clock as the caller's `now`
    scope: String,
    principal: Option<String>, // set only for private entries
}

/// A shared, principal-aware cache of backend list results. One entry per
/// `(backend_id, method)`. A public entry is served to any principal; a private
/// entry is served only to the principal that stored it, so a shared gateway
/// never leaks one user's private list to another (SEP-2549).
pub struct ListCache {
    entries: HashMap<(String, String), Entry>,
    default_ttl_ms: f64,
}

impl Default for ListCache {
    fn default() -> Self {
        Self::new(DEFAULT_TTL_MS)
    }
}

impl ListCache {
    pub fn new(default_ttl_ms: f64) -> Self {
        Self {
            entries: HashMap::new(),
            default_ttl_ms,
        }
    }

    pub fn get(&mut self, backend_id: &str, method: &str, principal: Option<&str>, now: f64) -> Option<Value> {
        let key = (backend_id.to_string(), method.to_string());
        let expired = match self.entries.get(&key) {
            None => return None,
            Some(entry) => now >= entry.expires_at,
        };
        if expired {
            self.entries.remove(&key);
            return None;
        }
        let entry = self.entries.get(&key)?;
        if entry.scope == PRIVATE && entry.principal.as_deref() != principal {
            return None;
        }
        Some(entry.result.clone())
    }

    pub fn put(&mut self, backend_id: &str, method: &str, principal: Option<&str>, result: Value, now: f64) {
        let key = (backend_id.to_string(), method.to_string());
        let ttl_ms = result.get(TTL_MS_KEY).and_then(Value::as_f64).unwrap_or(self.default_ttl_ms);
        if ttl_ms <= 0.0 {
            // ttlMs = 0 means immediately stale: drop any prior entry, cache
            // nothing (SEP-2549).
            self.entries.remove(&key);
            return;
        }
        let scope = match result.get(CACHE_SCOPE_KEY).and_then(Value::as_str) {
            Some(PRIVATE) => PRIVATE,
            _ => PUBLIC,
        };
        self.entries.insert(
            key,
            Entry {
                result,
                expires_at: now + ttl_ms / 1000.0,
                scope: scope.to_string(),
                principal: if scope == PRIVATE { principal.map(str::to_string) } else { None },
            },
        );
    }

    pub fn invalidate_backend(&mut self, backend_id: &str) {
        self.entries.retain(|(bid, _), _| bid != backend_id);
    }
}
