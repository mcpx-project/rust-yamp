//! δ21: the served ForwardRouter appends attestation/outcome audit records.
//!
//! A routed call emits a pre-call attestation and a post-call outcome to a
//! shared, tamper-evident AuditLog (SEP-2828/2787), best-effort and off the
//! reply path. A resilient backend failure still records an outcome (ok=false).
//! Mirrors the Python arm's test_router_audit.py.

use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc;
use yamp::resilience::CircuitBreaker;
use yamp::router::{Backend, ForwardRouter};
use yamp::signing::AuditLog;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

async fn mock(
    mut reader: LineReader<BufReader<DuplexStream>>,
    mut writer: LineWriter<DuplexStream>,
    name: &'static str,
    tools: Vec<&'static str>,
    drop_calls: bool,
) -> io::Result<()> {
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": name } },
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
                        let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": listed } }))).await?;
                    }
                    Some("tools/call") => {
                        if drop_calls {
                            continue; // never respond: the router's request times out
                        }
                        writer
                            .send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "content": [ { "type": "text", "text": "ok" } ] } })))
                            .await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn run(audit: Arc<StdMutex<AuditLog>>, resilient: bool, drop_calls: bool, call_name: &str) -> Value {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_b, b_reads) = duplex(CAP);
    let (b_writes, router_reads_b) = duplex(CAP);

    let reader = LineReader::new(BufReader::new(router_reads_b));
    let writer = LineWriter::new(router_to_b);
    let backend = if resilient {
        Backend::resilient("b", reader, writer, CircuitBreaker::new(3, 30.0), Some(Duration::from_millis(300))).unwrap()
    } else {
        Backend::new("b", reader, writer).unwrap()
    };
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        vec![backend],
    )
    .set_audit(audit);
    let backend_mock = mock(LineReader::new(BufReader::new(b_reads)), LineWriter::new(b_writes), "b", vec!["search"], drop_calls);

    let call_name = call_name.to_string();
    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "s", "method": "tools/call", "params": { "name": call_name, "arguments": {} } }))).await?;
        let resp = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(resp)
    };
    let (_r, _b, resp) = tokio::try_join!(router.serve(), backend_mock, client).unwrap();
    resp
}

#[tokio::test]
async fn audit_records_attestation_and_outcome() {
    let audit = Arc::new(StdMutex::new(AuditLog::new("secret")));
    let resp = run(audit.clone(), false, false, "search").await;
    assert_eq!(resp["result"]["content"][0]["text"], "ok");
    let log = audit.lock().unwrap();
    let kinds: Vec<String> = log.records.iter().map(|e| e["record"]["type"].as_str().unwrap().to_string()).collect();
    assert_eq!(kinds, vec!["attestation".to_string(), "outcome".to_string()]);
    assert_eq!(log.records[0]["record"]["name"], "search");
    assert_eq!(log.records[0]["record"]["principal"], "anonymous");
    assert_eq!(log.records[1]["record"]["ok"], true);
    assert!(log.verify()); // signatures and hash chain intact
}

#[tokio::test]
async fn audit_records_failure_outcome_on_resilient_backend() {
    let audit = Arc::new(StdMutex::new(AuditLog::new("secret")));
    let resp = run(audit.clone(), true, true, "search").await;
    assert!(resp["error"].is_object()); // backend timed out -> SERVER_NOT_AVAILABLE
    let log = audit.lock().unwrap();
    let kinds: Vec<String> = log.records.iter().map(|e| e["record"]["type"].as_str().unwrap().to_string()).collect();
    assert_eq!(kinds, vec!["attestation".to_string(), "outcome".to_string()]);
    assert_eq!(log.records[1]["record"]["ok"], false);
    assert!(log.verify());
}
