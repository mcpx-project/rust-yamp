//! σ2 worker-pool substrate unit tests (Rust arm). Mirrors the Python arm.
//!
//! The pure helpers are also pinned in the differential corpus; this covers the
//! InFlight state machine, which is deterministic (injected clock) but stateful,
//! so it is tested identically in both arms rather than corpus-pinned.

use serde_json::json;

use yamp::pool::{self, InFlight};

#[test]
fn admit_bounds() {
    assert!(pool::admit(0, 0)); // cap 0 is unbounded
    assert!(pool::admit(99, 0));
    assert!(pool::admit(0, 1));
    assert!(!pool::admit(1, 1)); // at cap
    assert!(pool::admit(3, 4));
}

#[test]
fn deadline_and_expired() {
    assert_eq!(pool::deadline(1000, 0), 0); // no idle budget
    assert_eq!(pool::deadline(1000, 5000), 6000);
    assert!(!pool::expired(0, 1_000_000_000)); // no deadline never expires
    assert!(!pool::expired(6000, 5999));
    assert!(pool::expired(6000, 6000));
}

#[test]
fn cancel_and_progress_extraction() {
    assert_eq!(pool::cancel_request_id(&json!({"method": "notifications/cancelled", "params": {"requestId": "c-7"}})), Some(&json!("c-7")));
    assert_eq!(pool::cancel_request_id(&json!({"method": "notifications/cancelled", "params": {}})), None);
    assert_eq!(pool::cancel_request_id(&json!({"method": "tools/call", "params": {"requestId": "c-7"}})), None);
    assert_eq!(pool::progress_token(&json!({"method": "notifications/progress", "params": {"progressToken": "t"}})), Some(&json!("t")));
    assert_eq!(pool::progress_token(&json!({"method": "notifications/progress", "params": {}})), None);
    assert_eq!(pool::progress_token(&json!({"method": "notifications/cancelled", "params": {"progressToken": "t"}})), None);
}

#[test]
fn inflight_state_machine() {
    let mut f = InFlight::new();
    assert_eq!(f.count(), 0);
    f.register(&json!("a"), pool::deadline(1000, 5000)); // deadline 6000
    f.register(&json!("b"), 0); // no deadline
    assert_eq!(f.count(), 2);
    assert!(f.contains(&json!("a")) && !f.contains(&json!("z")));

    // At cap 2 a third call must wait; cap 3 admits.
    assert!(f.at_capacity(2));
    assert!(!f.at_capacity(3));

    // Progress on `a` resets its deadline; an unknown id is a no-op.
    assert!(f.touch(&json!("a"), pool::deadline(4000, 5000))); // now 9000
    assert!(!f.touch(&json!("missing"), 1));

    // Only `a` had a deadline; at now=8000 nothing is expired (a moved to 9000).
    assert!(f.expired_ids(8000).is_empty());
    // At now=9000 `a` expires; `b` has no deadline so never does.
    assert_eq!(f.expired_ids(9000), vec![pool::id_key(&json!("a"))]);

    assert!(f.remove(&json!("a")));
    assert!(!f.remove(&json!("a"))); // already gone
    assert_eq!(f.count(), 1);
}

#[test]
fn numeric_and_string_ids_do_not_collide() {
    let mut f = InFlight::new();
    f.register(&json!(1), 100);
    f.register(&json!("1"), 200);
    assert_eq!(f.count(), 2); // "1" and 1 are distinct keys
    assert!(f.contains(&json!(1)) && f.contains(&json!("1")));
}
