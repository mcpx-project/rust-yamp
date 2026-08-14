//! δ16 capability composition unit tests (Rust arm). Mirrors the Python arm.

use serde_json::json;
use yamp::capability::compose_capabilities;

#[test]
fn any_backend_primitives() {
    let composed = compose_capabilities(
        &[
            json!({ "tools": { "listChanged": true }, "sampling": {} }),
            json!({ "tools": {}, "logging": {}, "resources": { "subscribe": true } }),
        ],
        None,
    );
    assert_eq!(composed["tools"], json!({ "listChanged": true })); // merged sub-flags
    assert_eq!(composed["sampling"], json!({})); // any backend
    assert_eq!(composed["logging"], json!({})); // any backend
    assert_eq!(composed["resources"], json!({ "subscribe": true }));
    assert!(composed.get("prompts").is_none()); // no backend advertised prompts
}

#[test]
fn elicitation_follows_client() {
    let with_client = compose_capabilities(&[json!({ "tools": {} })], Some(&json!({ "elicitation": {} })));
    assert_eq!(with_client["elicitation"], json!({}));
    let without_client = compose_capabilities(&[json!({ "tools": {}, "elicitation": {} })], None);
    assert!(without_client.get("elicitation").is_none());
}

#[test]
fn extensions_unioned() {
    let composed = compose_capabilities(
        &[
            json!({ "extensions": { "io.example/tasks": { "version": 1 } } }),
            json!({ "extensions": { "io.example/ui": { "version": 2 } } }),
        ],
        None,
    );
    assert_eq!(composed["extensions"]["io.example/tasks"], json!({ "version": 1 }));
    assert_eq!(composed["extensions"]["io.example/ui"], json!({ "version": 2 }));
}

#[test]
fn empty_backends() {
    assert_eq!(compose_capabilities(&[], None), json!({}));
}
