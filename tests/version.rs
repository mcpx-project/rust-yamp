//! δ11 protocol version negotiation unit tests (Rust arm). Mirrors the Python arm.

use serde_json::json;
use yamp::version::{
    is_supported, negotiate, unsupported_error_data, STATEFUL_PROTOCOL_VERSION,
    STATELESS_PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS,
};

#[test]
fn supported_set_is_newest_first() {
    assert_eq!(SUPPORTED_PROTOCOL_VERSIONS[0], STATELESS_PROTOCOL_VERSION);
    assert!(SUPPORTED_PROTOCOL_VERSIONS.contains(&STATEFUL_PROTOCOL_VERSION));
    assert!(is_supported(STATELESS_PROTOCOL_VERSION));
    assert!(is_supported(STATEFUL_PROTOCOL_VERSION));
    assert!(!is_supported("1999-01-01"));
}

#[test]
fn negotiate_defaults_when_omitted() {
    assert_eq!(negotiate(None, STATELESS_PROTOCOL_VERSION), Some(STATELESS_PROTOCOL_VERSION));
    assert_eq!(negotiate(None, STATEFUL_PROTOCOL_VERSION), Some(STATEFUL_PROTOCOL_VERSION));
}

#[test]
fn negotiate_echoes_supported() {
    for supported in SUPPORTED_PROTOCOL_VERSIONS {
        assert_eq!(negotiate(Some(supported), STATELESS_PROTOCOL_VERSION), Some(supported));
    }
}

#[test]
fn negotiate_rejects_unsupported() {
    assert_eq!(negotiate(Some("2024-11-05"), STATELESS_PROTOCOL_VERSION), None);
    assert_eq!(negotiate(Some(""), STATELESS_PROTOCOL_VERSION), None);
}

#[test]
fn unsupported_error_data_names_supported_set() {
    assert_eq!(
        unsupported_error_data(Some("2024-11-05")),
        json!({ "requested": "2024-11-05", "supported": SUPPORTED_PROTOCOL_VERSIONS })
    );
}
