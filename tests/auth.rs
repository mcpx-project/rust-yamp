//! δ20 auth tests (Rust arm). Mirrors the Python arm.

use std::io;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::auth;
use yamp::errors::UNAUTHORIZED;
use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

#[test]
fn forward_meta_injects_backend_and_drops_client() {
    let out = auth::forward_meta(&json!({ "authorization": "Bearer CLIENT", "traceparent": "tp" }), Some("GH_SECRET"));
    assert_eq!(out["authorization"], "Bearer GH_SECRET");
    assert_eq!(out["traceparent"], "tp");
    let dropped = auth::forward_meta(&json!({ "authorization": "Bearer CLIENT" }), None);
    assert!(dropped.get("authorization").is_none());
}

#[test]
fn claims_valid_checks_iss_and_aud() {
    assert!(auth::claims_valid(&json!({ "iss": "idp", "aud": "yamp" }), Some("idp"), Some("yamp")));
    assert!(auth::claims_valid(&json!({ "aud": ["a", "yamp"] }), None, Some("yamp")));
    assert!(auth::claims_valid(&json!({}), None, None));
    assert!(!auth::claims_valid(&json!({ "iss": "evil" }), Some("idp"), None));
    assert!(!auth::claims_valid(&json!({ "aud": "other" }), None, Some("yamp")));
}

#[test]
fn token_exchange_request_rfc8693() {
    let req = auth::token_exchange_request(
        "CLIENT_TOK",
        Some("https://gh.example"),
        Some("repo"),
        auth::TOKEN_TYPE_ACCESS_TOKEN,
        None,
    );
    assert_eq!(req["grant_type"], "urn:ietf:params:oauth:grant-type:token-exchange");
    assert_eq!(req["subject_token"], "CLIENT_TOK");
    assert_eq!(req["subject_token_type"], "urn:ietf:params:oauth:token-type:access_token");
    assert_eq!(req["audience"], "https://gh.example");
    assert_eq!(req["scope"], "repo");
    let minimal = auth::token_exchange_request("T", None, None, auth::TOKEN_TYPE_ACCESS_TOKEN, None);
    assert!(minimal.get("audience").is_none() && minimal.get("scope").is_none());
    let typed = auth::token_exchange_request("T", None, None, auth::TOKEN_TYPE_ACCESS_TOKEN, Some(auth::TOKEN_TYPE_ACCESS_TOKEN));
    assert_eq!(typed["requested_token_type"], auth::TOKEN_TYPE_ACCESS_TOKEN);
}

#[test]
fn parse_token_exchange_response_reads_access_token() {
    assert_eq!(
        auth::parse_token_exchange_response(&json!({ "access_token": "BACKEND_TOK", "token_type": "Bearer" })),
        Some("BACKEND_TOK".to_string())
    );
    assert_eq!(auth::parse_token_exchange_response(&json!({ "error": "invalid_request" })), None);
    assert_eq!(auth::parse_token_exchange_response(&json!({ "access_token": 123 })), None);
}

#[test]
fn pkce_code_challenge_rfc7636_vector() {
    // RFC 7636 Appendix B worked example.
    assert_eq!(
        auth::code_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
        "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    );
    assert!(!auth::code_challenge("short").contains('=')); // padding stripped
}

#[test]
fn oauth_request_builders() {
    let authz = auth::authorization_request("proxy", "https://cb", "CHAL", Some("mcp"), Some("xyz"));
    assert_eq!(authz["response_type"], "code");
    assert_eq!(authz["code_challenge"], "CHAL");
    assert_eq!(authz["code_challenge_method"], "S256");
    assert_eq!(authz["scope"], "mcp");
    assert_eq!(authz["state"], "xyz");
    let bare = auth::authorization_request("proxy", "https://cb", "CHAL", None, None);
    assert!(bare.get("scope").is_none() && bare.get("state").is_none());
    let tok = auth::token_request("proxy", "AUTH_CODE", "https://cb", "VERIFIER");
    assert_eq!(tok["grant_type"], "authorization_code");
    assert_eq!(tok["code"], "AUTH_CODE");
    assert_eq!(tok["code_verifier"], "VERIFIER");
}

async fn echo_meta_backend(
    mut reader: LineReader<BufReader<DuplexStream>>,
    mut writer: LineWriter<DuplexStream>,
    seen: Arc<Mutex<Vec<Value>>>,
) -> io::Result<()> {
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    seen.lock().unwrap().push(init.get("params").and_then(|p| p.get("_meta")).cloned().unwrap_or_else(|| json!({})));
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": "gh" } },
        })))
        .await?;
    reader.receive().await?;
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let m = jsonrpc::decode(&raw)?;
                if jsonrpc::method_of(&m) == Some("tools/call") {
                    seen.lock().unwrap().push(m.get("params").and_then(|p| p.get("_meta")).cloned().unwrap_or_else(|| json!({})));
                    writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": { "content": [{ "type": "text", "text": "ok" }] } }))).await?;
                }
            }
        }
    }
}

#[tokio::test]
async fn backend_credential_injected_and_client_not_leaked() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);

    let backend = Backend::new("gh", LineReader::new(BufReader::new(router_reads_gh)), LineWriter::new(router_to_gh))
        .unwrap()
        .with_token("GH_SECRET".to_string());
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        vec![backend],
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let gh = echo_meta_backend(LineReader::new(BufReader::new(gh_reads)), LineWriter::new(gh_writes), seen.clone());

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c1", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        // The client smuggles its own credential in _meta.
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "s", "method": "tools/call", "params": { "name": "gh__x", "arguments": {}, "_meta": { "authorization": "Bearer CLIENT_TOKEN" } } }))).await?;
        cr.receive().await?;
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(router.serve(), gh, client).unwrap();
    let seen = seen.lock().unwrap();
    let call_meta = &seen[seen.len() - 1];
    assert_eq!(call_meta["authorization"], "Bearer GH_SECRET"); // backend's own injected
    assert!(seen.iter().all(|m| !m.to_string().contains("CLIENT_TOKEN"))); // client's never leaked
}

#[tokio::test]
async fn handshake_rejects_invalid_issuer_audience() {
    async fn reject(claims: Value) -> Value {
        let (client_w, router_reads_client) = duplex(CAP);
        let (router_writes_client, client_r) = duplex(CAP);
        let (router_to_gh, _gh_reads) = duplex(CAP);
        let (_gh_writes, router_reads_gh) = duplex(CAP);
        // The router rejects before touching the backend, so no responder needed.
        let backend = Backend::new("gh", LineReader::new(BufReader::new(router_reads_gh)), LineWriter::new(router_to_gh)).unwrap();
        let router = ForwardRouter::new(
            LineReader::new(BufReader::new(router_reads_client)),
            LineWriter::new(router_writes_client),
            vec![backend],
        )
        .set_auth(Some("idp".to_string()), Some("yamp".to_string()));
        let server = router.serve();
        let client = async {
            let mut cw = LineWriter::new(client_w);
            let mut cr = LineReader::new(BufReader::new(client_r));
            cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c1", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {}, "_meta": { "claims": claims } } }))).await?;
            let response = jsonrpc::decode(&cr.receive().await?.unwrap())?;
            cw.send_eof().await?;
            Ok::<Value, io::Error>(response)
        };
        // The server errors on rejection; ignore its Err, keep the client response.
        let (_s, response) = tokio::join!(server, client);
        response.unwrap()
    }

    assert_eq!(reject(json!({ "iss": "evil", "aud": "yamp" })).await["error"]["code"], UNAUTHORIZED);
    assert_eq!(reject(json!({ "iss": "idp", "aud": "other" })).await["error"]["code"], UNAUTHORIZED);
}
