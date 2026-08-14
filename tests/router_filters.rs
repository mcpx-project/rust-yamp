//! ε0 filter-chain seam in the served router (Rust arm). Mirrors the Python arm.
//!
//! A filter chain runs on each client call request before routing: a deny
//! returns a clean -32001 and the backend is never touched; a mutate substitutes
//! the call arguments the backend receives; absent a chain the request routes
//! unchanged.

use std::io;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader};

use yamp::errors;
use yamp::filters::{Filter, FilterChain, FilterError};
use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::signing::AuditLog;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

/// A filter that returns a fixed verdict at every hook.
struct Fixed(Value);

impl Filter for Fixed {
    fn name(&self) -> &str {
        "fixed"
    }
    fn evaluate(&self, _hook: &str, _message: &Value) -> Result<Value, FilterError> {
        Ok(self.0.clone())
    }
}

/// Echoes the arguments it received, so the client result reveals exactly what
/// reached the backend (used to observe a mutation, or its absence).
async fn echo_backend<R, W>(mut reader: R, mut writer: W) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": "gh" } },
        })))
        .await?;
    reader.receive().await?; // notifications/initialized
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let message = jsonrpc::decode(&raw)?;
                match jsonrpc::method_of(&message) {
                    Some("tools/list") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": [{ "name": "echo" }] } }))).await?;
                    }
                    Some("tools/call") => {
                        let args = message["params"].get("arguments").cloned().unwrap_or(Value::Null);
                        let text = serde_json::to_string(&args).unwrap();
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "content": [{ "type": "text", "text": text }] } }))).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Drive one filtered `tools/call` through the router and return the response.
async fn drive(chain: FilterChain, audit: Option<Arc<StdMutex<AuditLog>>>) -> Value {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);

    let backend = Backend::new("gh", LineReader::new(BufReader::new(router_reads_gh)), LineWriter::new(router_to_gh)).unwrap();
    let mut router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        vec![backend],
    )
    .set_filters(chain);
    if let Some(audit) = audit {
        router = router.set_audit(audit);
    }

    let gh = echo_backend(LineReader::new(BufReader::new(gh_reads)), LineWriter::new(gh_writes));

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "s", "method": "tools/call",
            "params": { "name": "gh__echo", "arguments": { "secret": "raw" } },
        })))
        .await?;
        let response = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(response)
    };

    let (_r, _gh, response) = tokio::try_join!(router.serve(), gh, client).unwrap();
    response
}

#[tokio::test]
async fn deny_returns_policy_error_and_audits() {
    let audit = Arc::new(StdMutex::new(AuditLog::new("secret")));
    let chain = FilterChain::new(vec![Box::new(Fixed(json!({ "kind": "deny", "reason": "blocked by dlp" })))]);
    let response = drive(chain, Some(audit.clone())).await;
    assert_eq!(response["error"]["code"], json!(errors::POLICY_DENIED));
    assert_eq!(response["error"]["message"], "blocked by dlp");
    let log = audit.lock().unwrap();
    assert!(log
        .records
        .iter()
        .any(|e| e["record"]["type"] == "outcome" && e["record"]["ok"] == json!(false)));
}

#[tokio::test]
async fn mutate_substitutes_arguments_reaching_backend() {
    let chain = FilterChain::new(vec![Box::new(Fixed(json!({ "kind": "mutate", "arguments": { "secret": "[redacted]" } })))]);
    let response = drive(chain, None).await;
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(serde_json::from_str::<Value>(text).unwrap(), json!({ "secret": "[redacted]" }));
}

#[tokio::test]
async fn allow_routes_unchanged() {
    let chain = FilterChain::new(vec![Box::new(Fixed(json!({ "kind": "allow" })))]);
    let response = drive(chain, None).await;
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(serde_json::from_str::<Value>(text).unwrap(), json!({ "secret": "raw" }));
}
