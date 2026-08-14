//! Session-record and latency-budget tests (Rust arm). Uncompiled here.

use yamp::instrument::{within_budget, SessionRecord, LATENCY_BUDGET_MS};

#[test]
fn budget_boundary() {
    assert_eq!(LATENCY_BUDGET_MS, 10.0);
    assert!(within_budget(0.0));
    assert!(within_budget(LATENCY_BUDGET_MS));
    assert!(!within_budget(LATENCY_BUDGET_MS + 0.1));
}

#[test]
fn session_record_appends() {
    let dir = std::env::temp_dir().join(format!("yamp-sr-{}", std::process::id()));
    let path = dir.join("arm.jsonl");
    let _ = std::fs::remove_file(&path);

    let record = SessionRecord::new(&path, "rust").unwrap();
    record.attempt(1, 2, "advance").unwrap();
    record.attempt(2, 1, "regression").unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"kind\":\"advance\""));
    assert!(lines[0].contains("\"arm\":\"rust\""));
    assert!(lines[1].contains("\"kind\":\"regression\""));
}
