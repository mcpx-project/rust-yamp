//! Authentication propagation (SEP §4, draft §9; corpus SEP-2468).
//!
//! - Credential injection with confused-deputy protection (SEP §13.1): the proxy
//!   holds each backend's own credential and injects it when forwarding, and it
//!   never forwards a client's credential to a backend. The credential travels
//!   in the request `_meta` under `authorization`.
//! - Issuer/audience validation (SEP-2468): before trusting a client's token the
//!   proxy checks its `iss` and `aud` claims, so a token minted for one audience
//!   cannot be replayed against another (a confused-deputy attack).
//!
//! Two more token-propagation strategies the draft names (§9.1-9.2) have their
//! protocol-defined building blocks here. Their live network legs (an OAuth
//! redirect, a call to a token endpoint) are deployment-integration, like the
//! transparent-mode platform hook, so they are not wired into the stateless route
//! path; the deterministic, security-critical transformations are RFC 8693 token
//! exchange (swap the client's token for a backend-scoped one) and OAuth 2.1 +
//! PKCE (derive the S256 code challenge and build the authorization/token
//! requests, RFC 7636).

use serde_json::{json, Map, Value};

use crate::signing::sha256;

pub const AUTHORIZATION_META_KEY: &str = "authorization";
pub const CLAIMS_META_KEY: &str = "claims";

// RFC 8693 token exchange.
pub const GRANT_TYPE_TOKEN_EXCHANGE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
pub const TOKEN_TYPE_ACCESS_TOKEN: &str = "urn:ietf:params:oauth:token-type:access_token";
// OAuth 2.1 + PKCE (RFC 7636): S256 is the only challenge method a compliant
// client uses; the draft §9.2 SHOULD is PKCE, so the plain method is not offered.
pub const CODE_CHALLENGE_METHOD: &str = "S256";

/// The `_meta` to forward to a backend: the client's credential dropped, the
/// backend's own injected if the proxy holds one (SEP §13.1).
pub fn forward_meta(meta: &Value, backend_token: Option<&str>) -> Value {
    let mut object = meta.as_object().cloned().unwrap_or_else(Map::new);
    object.remove(AUTHORIZATION_META_KEY); // never forward the client's credential
    if let Some(token) = backend_token {
        object.insert(AUTHORIZATION_META_KEY.to_string(), Value::String(format!("Bearer {token}")));
    }
    Value::Object(object)
}

/// Whether a token's claims satisfy the configured issuer and audience. A
/// configured issuer must equal the `iss` claim; a configured audience must
/// appear in the `aud` claim (a string or a list). Unconfigured checks pass.
pub fn claims_valid(claims: &Value, issuer: Option<&str>, audience: Option<&str>) -> bool {
    if let Some(issuer) = issuer {
        if claims.get("iss").and_then(Value::as_str) != Some(issuer) {
            return false;
        }
    }
    if let Some(audience) = audience {
        let aud = claims.get("aud");
        let matches = match aud {
            Some(Value::String(s)) => s == audience,
            Some(Value::Array(list)) => list.iter().any(|a| a.as_str() == Some(audience)),
            _ => false,
        };
        if !matches {
            return false;
        }
    }
    true
}

/// The RFC 8693 §2.1 token-exchange request parameters: swap the client's
/// `subject_token` for a token the named backend `audience` accepts. The proxy
/// posts these (form-encoded) to the authorization server's token endpoint.
pub fn token_exchange_request(
    subject_token: &str,
    audience: Option<&str>,
    scope: Option<&str>,
    subject_token_type: &str,
    requested_token_type: Option<&str>,
) -> Value {
    let mut params = Map::new();
    params.insert("grant_type".to_string(), json!(GRANT_TYPE_TOKEN_EXCHANGE));
    params.insert("subject_token".to_string(), json!(subject_token));
    params.insert("subject_token_type".to_string(), json!(subject_token_type));
    if let Some(audience) = audience {
        params.insert("audience".to_string(), json!(audience));
    }
    if let Some(scope) = scope {
        params.insert("scope".to_string(), json!(scope));
    }
    if let Some(requested) = requested_token_type {
        params.insert("requested_token_type".to_string(), json!(requested));
    }
    Value::Object(params)
}

/// The issued token from an RFC 8693 §2.2 response (`access_token`), or `None`
/// when the response carries no string access token.
pub fn parse_token_exchange_response(body: &Value) -> Option<String> {
    body.get("access_token").and_then(Value::as_str).map(str::to_string)
}

/// The PKCE S256 code challenge for a caller-supplied verifier (RFC 7636 §4.2):
/// `base64url(sha256(verifier))` with the padding stripped.
pub fn code_challenge(verifier: &str) -> String {
    crate::base64::encode_url_nopad(&sha256(verifier.as_bytes()))
}

/// The OAuth 2.1 authorization-request parameters carrying the PKCE challenge
/// (RFC 7636 §4.3), for the proxy to redirect to the authorization endpoint.
pub fn authorization_request(
    client_id: &str,
    redirect_uri: &str,
    challenge: &str,
    scope: Option<&str>,
    state: Option<&str>,
) -> Value {
    let mut params = Map::new();
    params.insert("response_type".to_string(), json!("code"));
    params.insert("client_id".to_string(), json!(client_id));
    params.insert("redirect_uri".to_string(), json!(redirect_uri));
    params.insert("code_challenge".to_string(), json!(challenge));
    params.insert("code_challenge_method".to_string(), json!(CODE_CHALLENGE_METHOD));
    if let Some(scope) = scope {
        params.insert("scope".to_string(), json!(scope));
    }
    if let Some(state) = state {
        params.insert("state".to_string(), json!(state));
    }
    Value::Object(params)
}

/// The OAuth 2.1 authorization-code token-request parameters, proving the PKCE
/// verifier (RFC 7636 §4.5) to redeem the code for a token.
pub fn token_request(client_id: &str, code: &str, redirect_uri: &str, code_verifier: &str) -> Value {
    json!({
        "grant_type": "authorization_code",
        "client_id": client_id,
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    })
}
