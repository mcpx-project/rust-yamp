//! δ4 transparent L1 integration tests (Rust arm). Mirrors the Python arm.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader};

use yamp::instrument::within_budget;
use yamp::jsonrpc;
use yamp::errors::POLICY_DENIED;
use yamp::transparent::{
    encode_envelope, peek_headers, recover_original_destination, select_backend, AllowAll,
    HeaderPolicy, Policy, TransparentL1,
};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

#[test]
fn peek_headers_ignores_body() {
    let raw = encode_envelope(&json!({ "Mcp-Method": "tools/call" }), "NOT-JSON <<{[");
    assert_eq!(peek_headers(&raw).unwrap(), json!({ "Mcp-Method": "tools/call" }));
}

#[test]
fn policies() {
    assert!(AllowAll.allow(&json!({ "Mcp-Method": "anything" })));
    let policy = HeaderPolicy::new(["tools/call".to_string()], ["danger".to_string()]);
    assert!(policy.allow(&json!({ "Mcp-Method": "tools/list" })));
    assert!(!policy.allow(&json!({ "Mcp-Method": "tools/call" })));
    assert!(!policy.allow(&json!({ "Mcp-Name": "danger" })));
    assert!(policy.allow(&json!({})));
}

#[test]
fn select_backend_and_recovery_stub() {
    let mut table: HashMap<(String, u16), &str> = HashMap::new();
    table.insert(("10.0.0.1".to_string(), 443), "a");
    table.insert(("10.0.0.2".to_string(), 443), "b");
    assert_eq!(*select_backend(&("10.0.0.2".to_string(), 443), &table).unwrap(), "b");
    assert!(select_backend(&("10.0.0.9".to_string(), 443), &table).is_err());
    assert!(recover_original_destination().is_err());
}

async fn mock_backend<R, W>(
    mut reader: R,
    mut writer: W,
    received: Arc<Mutex<Vec<Vec<u8>>>>,
) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                received.lock().unwrap().push(raw.clone());
                let envelope: Value = jsonrpc::decode(&raw)?;
                let body = envelope["body"].as_str().unwrap_or("");
                let response = encode_envelope(&json!({ "Mcp-From": "backend" }), &format!("RESP:{body}"));
                writer.send(&response).await?;
            }
        }
    }
}

async fn run_case<P: Policy + Send + 'static>(
    policy: P,
    envelopes: Vec<Vec<u8>>,
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, u64) {
    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_backend, backend_r) = duplex(CAP);
    let (backend_w, proxy_reads_backend) = duplex(CAP);

    let received = Arc::new(Mutex::new(Vec::new()));
    let backend = mock_backend(
        LineReader::new(BufReader::new(backend_r)),
        LineWriter::new(backend_w),
        received.clone(),
    );
    let proxy = TransparentL1::run(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        LineReader::new(BufReader::new(proxy_reads_backend)),
        LineWriter::new(proxy_to_backend),
        policy,
    );

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        let mut responses = Vec::new();
        for envelope in &envelopes {
            cw.send(envelope).await?;
            responses.push(cr.receive().await?.unwrap());
        }
        cw.send_eof().await?;
        Ok::<Vec<Vec<u8>>, io::Error>(responses)
    };

    let (blocked, _backend, responses) = tokio::try_join!(proxy, backend, client).unwrap();
    let received = received.lock().unwrap().clone();
    (received, responses, blocked)
}

#[tokio::test]
async fn standard_headers_forwarded_byte_identical() {
    // SEP-2792: an intermediary must not rewrite mirrored standard headers in
    // place. TransparentL1 forwards raw bytes, so traceparent and Accept-Language
    // arrive byte-for-byte, never normalized.
    let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let sent = encode_envelope(&json!({ "traceparent": tp, "Accept-Language": "fr-CH, fr;q=0.9" }), "payload");
    let (received, _responses, _blocked) = run_case(AllowAll, vec![sent.clone()]).await;
    assert_eq!(received[0], sent); // byte-for-byte, no normalization
    assert_eq!(peek_headers(&received[0]).unwrap()["traceparent"], tp);
}

#[tokio::test]
async fn stateful_passthrough_unmodified() {
    let init = encode_envelope(&json!({}), r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#);
    let initialized = encode_envelope(&json!({}), r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#);

    let (received, responses, blocked) =
        run_case(AllowAll, vec![init.clone(), initialized.clone()]).await;

    assert_eq!(received, vec![init, initialized]); // unmodified, nothing injected
    assert_eq!(blocked, 0);
    let expected = encode_envelope(
        &json!({ "Mcp-From": "backend" }),
        r#"RESP:{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    );
    assert_eq!(responses[0], expected);
}

#[tokio::test]
async fn header_filter_blocks_without_body_parse() {
    let policy = HeaderPolicy::new(["tools/call".to_string()], []);
    let allowed = encode_envelope(&json!({ "Mcp-Method": "tools/list" }), "OPAQUE<<{[");
    let blocked_env = encode_envelope(&json!({ "Mcp-Method": "tools/call", "Mcp-Name": "x" }), "ALSO }}");

    let (received, responses, blocked) =
        run_case(policy, vec![allowed.clone(), blocked_env]).await;

    assert_eq!(received, vec![allowed]); // only the allowed message reached the backend
    assert_eq!(blocked, 1);
    let body: Value =
        serde_json::from_str(jsonrpc::decode(&responses[1]).unwrap()["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["error"]["code"], POLICY_DENIED);
}

#[tokio::test]
async fn both_modes_detected_per_connection() {
    let policy = HeaderPolicy::new(["tools/call".to_string()], []);
    let stateful = encode_envelope(&json!({}), r#"{"jsonrpc":"2.0","id":1,"method":"tools/call"}"#);
    let stateless = encode_envelope(&json!({ "Mcp-Method": "tools/call" }), "opaque");

    let (received, _responses, blocked) =
        run_case(policy, vec![stateful.clone(), stateless]).await;

    assert_eq!(received, vec![stateful]); // stateful body is opaque, passes through
    assert_eq!(blocked, 1); // the header-tagged stateless call is blocked
}

#[tokio::test]
async fn transparent_latency_within_budget() {
    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_backend, backend_r) = duplex(CAP);
    let (backend_w, proxy_reads_backend) = duplex(CAP);

    let received = Arc::new(Mutex::new(Vec::new()));
    let backend = mock_backend(
        LineReader::new(BufReader::new(backend_r)),
        LineWriter::new(backend_w),
        received.clone(),
    );
    let proxy = TransparentL1::run(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        LineReader::new(BufReader::new(proxy_reads_backend)),
        LineWriter::new(proxy_to_backend),
        AllowAll,
    );

    let driver = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        let envelope = encode_envelope(&json!({ "Mcp-Method": "tools/list" }), "x");
        for _ in 0..50 {
            cw.send(&envelope).await?;
            cr.receive().await?;
        }
        let mut samples = Vec::new();
        for _ in 0..300 {
            let start = Instant::now();
            cw.send(&envelope).await?;
            cr.receive().await?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        cw.send_eof().await?;
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[samples.len() / 2];
        let under = samples.iter().filter(|&&x| within_budget(x)).count() as f64 / samples.len() as f64;
        println!("[latency δ4 transparent] median={median:.4}ms within={under:.3}");
        assert!(within_budget(median));
        assert!(under >= 0.99);
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(proxy, backend, driver).unwrap();
}
