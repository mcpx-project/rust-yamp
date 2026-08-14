//! Base64 primitive (ε1): standard round-trips, padding, URL variant, rejection.

use yamp::base64;

#[test]
fn standard_round_trips() {
    for raw in [&b""[..], b"f", b"fo", b"foo", b"foob", b"hello world", &[0u8, 1, 2, 3][..]] {
        let encoded = base64::encode(raw);
        assert_eq!(base64::decode(&encoded).unwrap(), raw, "round trip {raw:?}");
    }
}

#[test]
fn standard_encoding_is_padded() {
    assert_eq!(base64::encode(b"hello"), "aGVsbG8=");
    assert_eq!(base64::encode(b"f"), "Zg==");
    assert_eq!(base64::encode(b"fo"), "Zm8=");
    assert_eq!(base64::encode(b""), "");
}

#[test]
fn decode_accepts_missing_padding_and_url_alphabet() {
    assert_eq!(base64::decode("aGVsbG8").unwrap(), b"hello"); // no padding
    assert_eq!(base64::decode("aGVsbG8=").unwrap(), b"hello"); // padded
    // URL alphabet ('-'/'_') decodes to the same bytes as '+'/'/'.
    assert_eq!(base64::decode("-_"), base64::decode("+/"));
}

#[test]
fn url_nopad_matches_pkce_shape() {
    // No padding, URL alphabet. For bytes that avoid +// it equals standard-nopad.
    assert_eq!(base64::encode_url_nopad(b"hello"), "aGVsbG8");
    assert!(!base64::encode_url_nopad(b"any").contains('='));
}

#[test]
fn decode_rejects_invalid() {
    assert!(base64::decode("!!!!").is_none()); // non-alphabet
    assert!(base64::decode("A").is_none()); // impossible length (1 sextet)
}
