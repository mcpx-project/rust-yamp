//! Worker-pool substrate for server-originated calls (σ2).
//!
//! A server that originates responses must bound its own concurrency, cancel a
//! call the client abandoned, and not kill a call that is still making progress.
//! This module holds the deterministic core of that: the admission decision, the
//! idle deadline arithmetic, the two message extractors (a cancellation's target
//! id, a progress notification's token), and an in-flight registry that is a pure
//! state machine over an injected clock.
//!
//! The concurrent execution itself (spawning the bounded set of tasks, racing
//! them against the client reader) is timing, not byte-matchable, and lives in
//! the router. Only the pieces below are deterministic, so only they are pinned
//! in the differential corpus; the registry is a pure state machine tested
//! identically in both arms.

use std::collections::HashMap;

use serde_json::Value;

use crate::jsonrpc;

/// Whether a new call may start given the number already `in_flight` and the
/// per-connection `cap`. A cap of zero is unbounded, so it always admits;
/// otherwise a call is admitted only while strictly under the cap.
pub fn admit(in_flight: u64, cap: u64) -> bool {
    cap == 0 || in_flight < cap
}

/// The idle deadline for a call starting (or making progress) at `now_ms` with an
/// idle budget of `idle_ms`. A budget of zero means no deadline, represented as
/// `0`; otherwise the wall-clock instant the call goes idle-expired absent
/// further progress.
pub fn deadline(now_ms: u64, idle_ms: u64) -> u64 {
    if idle_ms == 0 {
        0
    } else {
        now_ms + idle_ms
    }
}

/// Whether a call whose idle deadline is `deadline_ms` has expired at `now_ms`. A
/// deadline of zero never expires (no idle budget).
pub fn expired(deadline_ms: u64, now_ms: u64) -> bool {
    deadline_ms != 0 && now_ms >= deadline_ms
}

/// The `requestId` a `notifications/cancelled` targets, or `None` for any other
/// message (or a cancellation that names nothing). The id is returned verbatim (a
/// client's own request id may be a string or a number).
pub fn cancel_request_id(message: &Value) -> Option<&Value> {
    if jsonrpc::method_of(message) != Some("notifications/cancelled") {
        return None;
    }
    message.get("params").and_then(|p| p.get("requestId"))
}

/// The `progressToken` a `notifications/progress` carries, or `None` for any
/// other message. A progress notification for a tracked token resets that call's
/// idle deadline.
pub fn progress_token(message: &Value) -> Option<&Value> {
    if jsonrpc::method_of(message) != Some("notifications/progress") {
        return None;
    }
    message.get("params").and_then(|p| p.get("progressToken"))
}

/// The in-flight set of server-originated calls, keyed by client request id (its
/// canonical JSON string), each carrying its current idle deadline. A pure state
/// machine: it takes the clock as an argument and never reads real time, so both
/// arms and the tests are deterministic. Cancellation of the actual task and the
/// concurrency semaphore live in the router; this is the bookkeeping they agree
/// on.
#[derive(Default)]
pub struct InFlight {
    deadlines: HashMap<String, u64>,
}

/// The canonical key for a request id, so a string id and a numeric id never
/// collide and both arms key identically.
pub fn id_key(id: &Value) -> String {
    id.to_string()
}

impl InFlight {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a call as in-flight with its initial idle deadline.
    pub fn register(&mut self, id: &Value, deadline_ms: u64) {
        self.deadlines.insert(id_key(id), deadline_ms);
    }

    pub fn contains(&self, id: &Value) -> bool {
        self.deadlines.contains_key(&id_key(id))
    }

    /// Reset a call's idle deadline (a progress notification arrived). Returns
    /// whether the call was in-flight; an unknown id is a no-op.
    pub fn touch(&mut self, id: &Value, deadline_ms: u64) -> bool {
        if let Some(slot) = self.deadlines.get_mut(&id_key(id)) {
            *slot = deadline_ms;
            true
        } else {
            false
        }
    }

    /// Drop a call (it finished or was cancelled). Returns whether it was present,
    /// so a caller can tell a real cancellation from a stray id.
    pub fn remove(&mut self, id: &Value) -> bool {
        self.deadlines.remove(&id_key(id)).is_some()
    }

    pub fn count(&self) -> u64 {
        self.deadlines.len() as u64
    }

    /// Whether a new call must wait, i.e. the cap is reached.
    pub fn at_capacity(&self, cap: u64) -> bool {
        !admit(self.count(), cap)
    }

    /// The in-flight id keys whose idle deadline has passed at `now_ms`, sorted
    /// for a deterministic reap order.
    pub fn expired_ids(&self, now_ms: u64) -> Vec<String> {
        let mut ids: Vec<String> =
            self.deadlines.iter().filter(|(_, &d)| expired(d, now_ms)).map(|(k, _)| k.clone()).collect();
        ids.sort();
        ids
    }
}
