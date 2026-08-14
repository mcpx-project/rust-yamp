//! δ5 transparent L2 integration tests (Rust arm). Mirrors the Python arm.

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::instrument::within_budget;
use yamp::jsonrpc;
use yamp::stateless::{
    decode_request, decode_response, encode_request, encode_response, StatelessBackend,
    StatelessRequest, StatelessResponse,
};
use yamp::transparent::encode_envelope;
use yamp::transparent_l2::{proxy_hop, TransparentL2Stateful, TransparentL2Stateless, PROXY_HOPS_KEY};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type LineBackend =
    StatelessBackend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>>;

fn line_backend(id: &str, reader: DuplexStream, writer: DuplexStream) -> LineBackend {
    StatelessBackend::new(id, LineReader::new(BufReader::new(reader)), LineWriter::new(writer)).unwrap()
}

async fn mock_stateless_backend<R, W>(
    mut reader: R,
    mut writer: W,
    tools: Vec<&'static str>,
    log: Arc<Mutex<Vec<StatelessRequest>>>,
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
                let request = decode_request(&raw)?;
                log.lock().unwrap().push(request.clone());
                let response = match request.method.as_str() {
                    "server/discover" => {
                        let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                        StatelessResponse { meta: json!({}), body: json!({ "tools": listed }).to_string() }
                    }
                    "tools/call" => StatelessResponse {
                        meta: json!({}),
                        body: format!("RESULT:{}", request.name.clone().unwrap_or_default()),
                    },
                    _ => StatelessResponse { meta: json!({}), body: String::new() },
                };
                writer.send(&encode_response(&response)).await?;
            }
        }
    }
}

#[tokio::test]
async fn hop_appended_not_replaced() {
    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_b, b_reads) = duplex(CAP);
    let (b_writes, proxy_reads_b) = duplex(CAP);

    let backends = vec![line_backend("b", proxy_reads_b, proxy_to_b)];
    let proxy = TransparentL2Stateless::new(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        backends,
        None,
    );
    let log = Arc::new(Mutex::new(Vec::new()));
    let backend = mock_stateless_backend(
        LineReader::new(BufReader::new(b_reads)),
        LineWriter::new(b_writes),
        vec!["x"],
        log.clone(),
    );

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        let with_hop = StatelessRequest::new(
            "tools/call",
            Some("b__x".into()),
            json!({ PROXY_HOPS_KEY: [ { "name": "upstream" } ] }),
            "p",
        );
        cw.send(&encode_request(&with_hop)).await?;
        cr.receive().await?;
        let no_hop = StatelessRequest::new("tools/call", Some("b__x".into()), json!({}), "p");
        cw.send(&encode_request(&no_hop)).await?;
        cr.receive().await?;
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(proxy.serve(), backend, client).unwrap();

    let guard = log.lock().unwrap();
    assert_eq!(guard[0].meta[PROXY_HOPS_KEY], json!([ { "name": "upstream" }, proxy_hop("transparent") ]));
    assert_eq!(guard[1].meta[PROXY_HOPS_KEY], json!([ proxy_hop("transparent") ]));
}

#[tokio::test]
async fn discover_filter_and_namespace() {
    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, proxy_reads_gh) = duplex(CAP);
    let (proxy_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, proxy_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", proxy_reads_gh, proxy_to_gh),
        line_backend("gl", proxy_reads_gl, proxy_to_gl),
    ];
    let filter: Box<dyn Fn(&str) -> bool> = Box::new(|name| name != "secret");
    let proxy = TransparentL2Stateless::new(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        backends,
        Some(filter),
    );
    let gh_log = Arc::new(Mutex::new(Vec::new()));
    let gh = mock_stateless_backend(
        LineReader::new(BufReader::new(gh_reads)),
        LineWriter::new(gh_writes),
        vec!["a", "secret"],
        gh_log.clone(),
    );
    let gl = mock_stateless_backend(
        LineReader::new(BufReader::new(gl_reads)),
        LineWriter::new(gl_writes),
        vec!["c"],
        Arc::new(Mutex::new(Vec::new())),
    );

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&encode_request(&StatelessRequest::new("server/discover", None, json!({}), "")))
            .await?;
        let response = decode_response(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<StatelessResponse, io::Error>(response)
    };

    let (_p, _gh, _gl, response) = tokio::try_join!(proxy.serve(), gh, gl, client).unwrap();

    let names: BTreeSet<String> = serde_json::from_str::<Value>(&response.body)
        .unwrap()["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = ["gh__a", "gl__c"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected); // 'secret' filtered, others namespaced
    assert_eq!(gh_log.lock().unwrap()[0].meta[PROXY_HOPS_KEY], json!([ proxy_hop("transparent") ]));
}

#[test]
fn header_body_mismatch_ignores_non_object_and_opaque_bodies() {
    use yamp::transparent_l2::header_body_mismatch;
    let req = |body: &str| StatelessRequest::new("tools/call", Some("b__x".into()), json!({}), body);
    // Valid JSON that is not a JSON-RPC object, plus empty and opaque bodies.
    assert_eq!(header_body_mismatch(&req("[1,2,3]")), None);
    assert_eq!(header_body_mismatch(&req("42")), None);
    assert_eq!(header_body_mismatch(&req("")), None);
    assert_eq!(header_body_mismatch(&req("NOT-JSON")), None);
    assert_eq!(header_body_mismatch(&req(r#"{"jsonrpc":"2.0"}"#)), None);
    // Divergent method and tool name are caught.
    assert_eq!(header_body_mismatch(&req(r#"{"method":"resources/read"}"#)), Some("method"));
    assert_eq!(
        header_body_mismatch(&req(r#"{"method":"tools/call","params":{"name":"b__evil"}}"#)),
        Some("name")
    );
}

#[tokio::test]
async fn header_body_validation_rejects_divergent_body() {
    use yamp::errors::HEADER_MISMATCH;

    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_b, b_reads) = duplex(CAP);
    let (b_writes, proxy_reads_b) = duplex(CAP);

    let backends = vec![line_backend("b", proxy_reads_b, proxy_to_b)];
    let proxy = TransparentL2Stateless::new(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        backends,
        None,
    );
    let log = Arc::new(Mutex::new(Vec::new()));
    let backend = mock_stateless_backend(LineReader::new(BufReader::new(b_reads)), LineWriter::new(b_writes), vec!["x"], log.clone());

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        let agree = StatelessRequest::new("tools/call", Some("b__x".into()), json!({}), r#"{"method":"tools/call","params":{"name":"b__x"}}"#);
        cw.send(&encode_request(&agree)).await?;
        let routed = decode_response(&cr.receive().await?.unwrap())?;
        let bad = StatelessRequest::new("tools/call", Some("b__x".into()), json!({}), r#"{"method":"tools/call","params":{"name":"b__evil"}}"#);
        cw.send(&encode_request(&bad)).await?;
        let mismatch = decode_response(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<(StatelessResponse, StatelessResponse), io::Error>((routed, mismatch))
    };

    let (_p, _b, (routed, mismatch)) = tokio::try_join!(proxy.serve(), backend, client).unwrap();
    assert_eq!(routed.body, "RESULT:x"); // agreeing call routed, prefix stripped
    let error: Value = serde_json::from_str(&mismatch.body).unwrap();
    assert_eq!(error["error"]["code"], HEADER_MISMATCH);
    assert_eq!(log.lock().unwrap().len(), 1); // mismatched call never reached the backend
}

// ---- stateful L2: dual-handshake toggle ----

async fn jsonrpc_backend<R, W>(mut reader: R, mut writer: W) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {}, "serverInfo": { "name": "backend" } },
        })))
        .await?;
    reader.receive().await?; // notifications/initialized
    while reader.receive().await?.is_some() {}
    writer.send_eof().await
}

#[tokio::test]
async fn dual_handshake_follows_forward_and_records() {
    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_backend, backend_r) = duplex(CAP);
    let (backend_w, proxy_reads_backend) = duplex(CAP);

    let proxy = TransparentL2Stateful::new(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        LineReader::new(BufReader::new(proxy_reads_backend)),
        LineWriter::new(proxy_to_backend),
        true,
    );
    let backend = jsonrpc_backend(
        LineReader::new(BufReader::new(backend_r)),
        LineWriter::new(backend_w),
    );

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        let init = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })))
            .await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(init)
    };

    let (performed, _backend, init) = tokio::try_join!(proxy.serve(), backend, client).unwrap();
    assert!(performed);
    assert_eq!(init["result"]["serverInfo"]["name"], "yamp");
}

#[tokio::test]
async fn passthrough_follows_level1() {
    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_backend, backend_r) = duplex(CAP);
    let (backend_w, proxy_reads_backend) = duplex(CAP);

    let proxy = TransparentL2Stateful::new(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        LineReader::new(BufReader::new(proxy_reads_backend)),
        LineWriter::new(proxy_to_backend),
        false,
    );

    let received = Arc::new(Mutex::new(Vec::new()));
    let received_backend = received.clone();
    let backend = async move {
        let mut reader = LineReader::new(BufReader::new(backend_r));
        let mut writer = LineWriter::new(backend_w);
        loop {
            match reader.receive().await? {
                None => {
                    writer.send_eof().await?;
                    return Ok::<(), io::Error>(());
                }
                Some(raw) => {
                    received_backend.lock().unwrap().push(raw.clone());
                    writer.send(&encode_envelope(&json!({ "Mcp-From": "backend" }), "ok")).await?;
                }
            }
        }
    };

    let envelope = encode_envelope(&json!({}), r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#);
    let env_for_client = envelope.clone();
    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&env_for_client).await?;
        cr.receive().await?;
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    let (performed, _backend, _client) = tokio::try_join!(proxy.serve(), backend, client).unwrap();
    assert!(!performed);
    assert_eq!(*received.lock().unwrap(), vec![envelope]); // unmodified passthrough
}

#[tokio::test]
async fn l2_stateless_latency_within_budget() {
    let (client_w, proxy_reads_client) = duplex(CAP);
    let (proxy_writes_client, client_r) = duplex(CAP);
    let (proxy_to_b, b_reads) = duplex(CAP);
    let (b_writes, proxy_reads_b) = duplex(CAP);

    let backends = vec![line_backend("b", proxy_reads_b, proxy_to_b)];
    let proxy = TransparentL2Stateless::new(
        LineReader::new(BufReader::new(proxy_reads_client)),
        LineWriter::new(proxy_writes_client),
        backends,
        None,
    );
    let backend = mock_stateless_backend(
        LineReader::new(BufReader::new(b_reads)),
        LineWriter::new(b_writes),
        vec!["x"],
        Arc::new(Mutex::new(Vec::new())),
    );

    let driver = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        let request = encode_request(&StatelessRequest::new("tools/call", Some("b__x".into()), json!({}), "p"));
        for _ in 0..50 {
            cw.send(&request).await?;
            cr.receive().await?;
        }
        let mut samples = Vec::new();
        for _ in 0..300 {
            let start = Instant::now();
            cw.send(&request).await?;
            cr.receive().await?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        cw.send_eof().await?;
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[samples.len() / 2];
        let under = samples.iter().filter(|&&x| within_budget(x)).count() as f64 / samples.len() as f64;
        println!("[latency δ5 L2 stateless] median={median:.4}ms within={under:.3}");
        assert!(within_budget(median));
        assert!(under >= 0.99);
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(proxy.serve(), backend, driver).unwrap();
}
