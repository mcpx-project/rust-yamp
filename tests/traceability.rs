//! Spec-traceability checker (the SEP-2484 pattern), Rust arm.
//!
//! Verifies the shared clause->test matrix at
//! conformance/sep-0000-traceability.json is well-formed and that every Rust
//! test it names actually exists in this suite, so a renamed or deleted test
//! breaks the matrix. The Python arm has a parallel checker
//! (tests/test_traceability.py) that also validates the Python references.

use std::collections::HashSet;
use std::path::Path;

use serde_json::Value;

fn matrix() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance/sep-0000-traceability.json");
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))).unwrap()
}

/// Every `fn <name>(` defined across the integration test files.
fn rust_test_names() -> HashSet<String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut names = HashSet::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        for line in std::fs::read_to_string(&path).unwrap().lines() {
            if let Some(pos) = line.find("fn ") {
                let rest = &line[pos + 3..];
                if let Some(paren) = rest.find('(') {
                    let name = rest[..paren].trim();
                    if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                        names.insert(name.to_string());
                    }
                }
            }
        }
    }
    names
}

#[test]
fn traceability_matrix_is_well_formed_and_references_existing_rust_tests() {
    let matrix = matrix();
    let clauses = matrix["clauses"].as_array().expect("clauses array");
    assert!(!clauses.is_empty(), "matrix has no clauses");
    let names = rust_test_names();
    let mut ids = HashSet::new();
    let mut missing = Vec::new();
    for clause in clauses {
        let id = clause["id"].as_str().unwrap();
        assert!(ids.insert(id.to_string()), "duplicate clause id: {id}");
        assert!(matches!(clause["level"].as_str(), Some("MUST") | Some("SHOULD")), "bad level: {id}");
        let rust_tests = clause["tests"]["rust"].as_array().expect("rust tests array");
        assert!(!rust_tests.is_empty(), "no rust tests: {id}");
        for t in rust_tests {
            let name = t.as_str().unwrap();
            if !names.contains(name) {
                missing.push(name.to_string());
            }
        }
    }
    assert!(missing.is_empty(), "traceability references unknown Rust tests: {missing:?}");
}
