//! δ1 forward-proxy integration tests (Rust arm). Mirrors the Python arm.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader};

use yamp::forward::{ForwardProxy, PROXY_PROTOCOL_VERSION};
use yamp::instrument::within_budget;
use yamp::jsonrpc;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

#[derive(Default)]
struct BackendLog {
    methods: Vec<String>,
    saw_client: Option<String>,
}

async fn mock_backend<R, W>(mut reader: R, mut writer: W, log: Arc<Mutex<BackendLog>>) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    {
        let mut guard = log.lock().unwrap();
        guard.methods.push(jsonrpc::method_of(&init).unwrap().to_string());
        guard.saw_client = init["params"]["clientInfo"]["name"].as_str().map(str::to_string);
    }
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0",
            "id": init["id"],
            "result": {
                "protocolVersion": PROXY_PROTOCOL_VERSION,
                "capabilities": { "tools": { "listChanged": true } },
                "serverInfo": { "name": "backend-xyz", "version": "9.9" },
            },
        })))
        .await?;
    let note = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    log.lock()
        .unwrap()
        .methods
        .push(jsonrpc::method_of(&note).unwrap().to_string());

    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let message = jsonrpc::decode(&raw)?;
                if jsonrpc::method_of(&message) == Some("tools/list") {
                    writer
                        .send(&jsonrpc::encode(&json!({
                            "jsonrpc": "2.0",
                            "id": message["id"],
                            "result": { "tools": [ { "name": "echo" } ] },
                        })))
                        .await?;
                }
            }
        }
    }
}

type LineProxy = ForwardProxy<
    LineReader<BufReader<tokio::io::DuplexStream>>,
    LineWriter<tokio::io::DuplexStream>,
    LineReader<BufReader<tokio::io::DuplexStream>>,
    LineWriter<tokio::io::DuplexStream>,
>;

fn proxy_over(
    client_r: tokio::io::DuplexStream,
    client_w: tokio::io::DuplexStream,
    backend_r: tokio::io::DuplexStream,
    backend_w: tokio::io::DuplexStream,
) -> LineProxy {
    ForwardProxy::new(
        LineReader::new(BufReader::new(client_r)),
        LineWriter::new(client_w),
        LineReader::new(BufReader::new(backend_r)),
        LineWriter::new(backend_w),
    )
}

#[tokio::test]
async fn full_forward_session() {
    let (client_w, relay_reads_client) = duplex(CAP);
    let (relay_writes_client, client_r) = duplex(CAP);
    let (relay_writes_backend, backend_r) = duplex(CAP);
    let (backend_w, relay_reads_backend) = duplex(CAP);

    let proxy = proxy_over(
        relay_reads_client,
        relay_writes_client,
        relay_reads_backend,
        relay_writes_backend,
    );

    let log = Arc::new(Mutex::new(BackendLog::default()));
    let backend = mock_backend(
        LineReader::new(BufReader::new(backend_r)),
        LineWriter::new(backend_w),
        log.clone(),
    );

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "2024-11-05", "capabilities": {},
                        "clientInfo": { "name": "test-client" } },
        })))
        .await?;
        let init_raw = cr.receive().await?.unwrap();
        let init = jsonrpc::decode(&init_raw)?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })))
            .await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/list" })))
            .await?;
        let tools_raw = cr.receive().await?.unwrap();
        let tools = jsonrpc::decode(&tools_raw)?;
        cw.send_eof().await?;
        Ok::<(Value, Value, Vec<u8>, Vec<u8>), io::Error>((init, tools, init_raw, tools_raw))
    };

    let (backend_info, _backend_done, (init, tools, init_raw, tools_raw)) =
        tokio::try_join!(proxy.serve(), backend, client).unwrap();

    // gate 2: the client sees the proxy's serverInfo, backend identity is held.
    assert_eq!(init["result"]["serverInfo"]["name"], "yamp");
    assert_eq!(backend_info.as_ref().unwrap()["name"], "backend-xyz");
    // gate 3: the proxy's own highest protocol version, not the client's.
    assert_eq!(init["result"]["protocolVersion"], PROXY_PROTOCOL_VERSION);
    // gate 4: capabilities pass through, tools/list is forwarded, no leak.
    assert_eq!(init["result"]["capabilities"]["tools"]["listChanged"], true);
    assert_eq!(tools["result"]["tools"][0]["name"], "echo");
    assert!(!init_raw.windows(11).any(|w| w == b"backend-xyz"));
    assert!(!tools_raw.windows(11).any(|w| w == b"backend-xyz"));
    // gate 1: dual handshake, proxy presented itself to the backend.
    let guard = log.lock().unwrap();
    assert_eq!(guard.methods, vec!["initialize", "notifications/initialized"]);
    assert_eq!(guard.saw_client.as_deref(), Some("yamp"));
}

#[tokio::test]
async fn rejects_non_initialize_first_message() {
    let (client_w, relay_reads_client) = duplex(CAP);
    let (relay_writes_client, client_r) = duplex(CAP);
    let (relay_writes_backend, _backend_r) = duplex(CAP);
    let (_backend_w, relay_reads_backend) = duplex(CAP);

    let proxy = proxy_over(
        relay_reads_client,
        relay_writes_client,
        relay_reads_backend,
        relay_writes_backend,
    );

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "x", "method": "tools/list" })))
            .await?;
        let reply = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        Ok::<Value, io::Error>(reply)
    };

    let (serve_result, reply) = tokio::join!(proxy.serve(), client);
    assert!(serve_result.is_err());
    assert_eq!(reply.unwrap()["error"]["code"], -32600);
}

#[tokio::test]
async fn forward_path_latency_within_budget() {
    let (client_w, relay_reads_client) = duplex(CAP);
    let (relay_writes_client, client_r) = duplex(CAP);
    let (relay_writes_backend, backend_r) = duplex(CAP);
    let (backend_w, relay_reads_backend) = duplex(CAP);

    let proxy = proxy_over(
        relay_reads_client,
        relay_writes_client,
        relay_reads_backend,
        relay_writes_backend,
    );
    let log = Arc::new(Mutex::new(BackendLog::default()));
    let backend = mock_backend(
        LineReader::new(BufReader::new(backend_r)),
        LineWriter::new(backend_w),
        log,
    );

    let driver = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "2024-11-05", "capabilities": {},
                        "clientInfo": { "name": "test-client" } },
        })))
        .await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })))
            .await?;

        let request = jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "t", "method": "tools/list" }));
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
        let under = samples.iter().filter(|&&x| within_budget(x)).count() as f64
            / samples.len() as f64;
        println!("[latency δ1 forward] median={median:.4}ms within={under:.3}");
        assert!(within_budget(median));
        assert!(under >= 0.99);
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(proxy.serve(), backend, driver).unwrap();
}
