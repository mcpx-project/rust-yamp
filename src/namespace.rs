//! Namespace management (SEP §3, draft §8).
//!
//! Names are namespaced as `{backend_id}__{original}`. The backend identifier
//! is operator-assigned and drawn from `[A-Za-z0-9_-]`. Splitting uses the
//! first delimiter only, so original names containing `__` round-trip.

pub const DELIMITER: &str = "__";

pub fn valid_backend_id(id: &str) -> bool {
    // The charset allows single underscores, but an id containing the `__`
    // delimiter would break reverse resolution (split on the first `__`), so
    // ids carrying the delimiter are rejected.
    !id.is_empty()
        && !id.contains(DELIMITER)
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

pub fn prefix(id: &str, name: &str) -> String {
    format!("{id}{DELIMITER}{name}")
}

/// Reverse a namespaced name into `(backend_id, original)`, or `None` when it
/// carries no resolvable prefix.
pub fn split(name: &str) -> Option<(&str, &str)> {
    let (id, original) = name.split_once(DELIMITER)?;
    if id.is_empty() || original.is_empty() {
        None
    } else {
        Some((id, original))
    }
}

/// Namespace a resource URI by inserting the backend id as the first path
/// segment (SEP §3.2): `file:///reports/q3.md` becomes `file:///docs/reports/q3.md`.
pub fn prefix_uri(id: &str, uri: &str) -> String {
    let (scheme, rest) = match uri.split_once("://") {
        Some(parts) => parts,
        None => return uri.to_string(),
    };
    match rest.split_once('/') {
        Some((authority, path)) => format!("{scheme}://{authority}/{id}/{path}"),
        None => format!("{scheme}://{rest}/{id}"),
    }
}

/// Reverse a namespaced resource URI into `(backend_id, original_uri)`.
pub fn split_uri(uri: &str) -> Option<(String, String)> {
    let (scheme, rest) = uri.split_once("://")?;
    let (authority, path) = rest.split_once('/')?;
    match path.split_once('/') {
        Some((id, remainder)) if !id.is_empty() => {
            Some((id.to_string(), format!("{scheme}://{authority}/{remainder}")))
        }
        None if !path.is_empty() => Some((path.to_string(), format!("{scheme}://{authority}"))),
        _ => None,
    }
}

#[cfg(test)]
mod uri_tests {
    use super::*;

    #[test]
    fn prefix_and_split_uri() {
        assert_eq!(prefix_uri("docs", "file:///reports/q3.md"), "file:///docs/reports/q3.md");
        assert_eq!(prefix_uri("docs", "https://ex.com/p"), "https://ex.com/docs/p");
        assert_eq!(prefix_uri("docs", "scheme://auth"), "scheme://auth/docs");
        assert_eq!(prefix_uri("docs", "mailto:x"), "mailto:x");
        assert_eq!(split_uri("file:///docs/reports/q3.md"), Some(("docs".into(), "file:///reports/q3.md".into())));
        assert_eq!(split_uri("scheme://auth/docs"), Some(("docs".into(), "scheme://auth".into())));
        assert_eq!(split_uri("mailto:x"), None);
        assert_eq!(split_uri("scheme://auth"), None);
        assert_eq!(split_uri("scheme://auth/"), None);
    }
}
