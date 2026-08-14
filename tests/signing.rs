//! δ21 signing/attesting tests (Rust arm). Mirrors the Python arm.

use serde_json::json;
use yamp::signing::{self, AuditLog, GENESIS};

#[test]
fn canonical_is_key_order_independent() {
    let a = signing::canonical(&json!({ "b": 2, "a": 1, "nested": { "y": 1, "x": 2 } }));
    let b = signing::canonical(&json!({ "a": 1, "nested": { "x": 2, "y": 1 }, "b": 2 }));
    assert_eq!(a, b); // sorted keys at every level
    assert_eq!(a, br#"{"a":1,"b":2,"nested":{"x":2,"y":1}}"#.to_vec()); // compact, deterministic
}

#[test]
fn signature_verifies_and_detects_tamper() {
    let record = signing::outcome_record("tools/call", Some("gh__x"), true);
    let sig = signing::sign("secret", &record);
    assert!(signing::verify("secret", &record, &sig));
    assert!(!signing::verify("secret", &signing::outcome_record("tools/call", Some("gh__x"), false), &sig));
    assert!(!signing::verify("other-secret", &record, &sig));
}

#[test]
fn hash_chain_links_records() {
    let mut log = AuditLog::new("k");
    let first = log.append(signing::attestation_record("alice", "tools/call", Some("gh__x")));
    let second = log.append(signing::outcome_record("tools/call", Some("gh__x"), true));
    assert_eq!(first["prev"], GENESIS);
    assert_eq!(second["prev"], first["hash"]); // each record links to the previous
    assert!(log.verify());
}

#[test]
fn verify_fails_on_broken_chain() {
    let mut log = AuditLog::new("k");
    log.append(signing::outcome_record("tools/call", Some("a"), true));
    log.append(signing::outcome_record("tools/call", Some("b"), true));
    assert!(log.verify());
    log.records[1]["record"]["ok"] = json!(false); // tamper the record
    assert!(!log.verify());
    log.records[1]["record"]["ok"] = json!(true);
    log.records[1]["prev"] = json!("deadbeefdeadbeef"); // break the chain link
    assert!(!log.verify());
    log.records[1]["prev"] = log.records[0]["hash"].clone();
    log.records[1]["hash"] = json!("ffffffffffffffff"); // wrong stored hash
    assert!(!log.verify());
}
