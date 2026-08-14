//! Layer 5 policy (SEP §4, draft §6.5).
//!
//! Credential injection default: the proxy injects per-backend credentials and
//! never forwards the client's own credentials to a backend (SEP §13.1). Header
//! forwarding is scoped per backend and may rename headers. Client
//! authentication is pluggable.

use std::collections::{HashMap, HashSet};

pub const AUTHORIZATION: &str = "Authorization";
// The UNAUTHORIZED code (-32002) lives in the errors registry; consumers import
// it from there.

pub trait Authenticator {
    fn authenticate(&self, client_headers: &HashMap<String, String>) -> bool;
}

pub struct BearerAuthenticator {
    valid: HashSet<String>,
}

impl BearerAuthenticator {
    pub fn new<I: IntoIterator<Item = String>>(valid: I) -> Self {
        Self {
            valid: valid.into_iter().collect(),
        }
    }
}

impl Authenticator for BearerAuthenticator {
    fn authenticate(&self, headers: &HashMap<String, String>) -> bool {
        headers
            .get(AUTHORIZATION)
            .and_then(|h| h.strip_prefix("Bearer "))
            .map(|token| self.valid.contains(token))
            .unwrap_or(false)
    }
}

pub struct ForwardRule {
    pub name: String,
    pub backend_header: Option<String>,
}

impl ForwardRule {
    pub fn new(name: &str, backend_header: Option<&str>) -> Self {
        Self {
            name: name.to_string(),
            backend_header: backend_header.map(str::to_string),
        }
    }
}

#[derive(Default)]
pub struct PolicyLayer {
    backend_tokens: HashMap<String, String>,
    forward_headers: HashMap<String, Vec<ForwardRule>>,
    authenticator: Option<Box<dyn Authenticator + Send + Sync>>,
}

impl PolicyLayer {
    pub fn new(
        backend_tokens: HashMap<String, String>,
        forward_headers: HashMap<String, Vec<ForwardRule>>,
        authenticator: Option<Box<dyn Authenticator + Send + Sync>>,
    ) -> Self {
        Self {
            backend_tokens,
            forward_headers,
            authenticator,
        }
    }

    pub fn authorize_client(&self, client_headers: &HashMap<String, String>) -> bool {
        match &self.authenticator {
            None => true,
            Some(authenticator) => authenticator.authenticate(client_headers),
        }
    }

    /// Headers to send to `backend_id`: injected credentials plus any
    /// explicitly forwarded (and optionally renamed) client headers.
    pub fn backend_headers(
        &self,
        backend_id: &str,
        client_headers: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        if let Some(token) = self.backend_tokens.get(backend_id) {
            headers.insert(AUTHORIZATION.to_string(), format!("Bearer {token}"));
        }
        if let Some(rules) = self.forward_headers.get(backend_id) {
            for rule in rules {
                if let Some(value) = client_headers.get(&rule.name) {
                    let target = rule.backend_header.clone().unwrap_or_else(|| rule.name.clone());
                    headers.insert(target, value.clone());
                }
            }
        }
        headers
    }
}
