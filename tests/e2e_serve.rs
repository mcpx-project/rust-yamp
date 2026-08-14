//! δ-testinfra: automated per-mode e2e for the served stdio (TCP) entrypoint.
//!
//! Spawns the real `yamp-serve` binary against two live stub backends over TCP
//! and drives a full initialize -> tools/list -> tools/call as a client,
//! asserting the composed handshake, the namespaced surface, and a routed call.
//! Mirrors the Python arm's test_e2e_serve.py.

use std::io;
use std::net::TcpListener as StdListener;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

fn free_port() -> u16 {
    StdListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn serve_stub(stream: TcpStream) -> io::Result<()> {
    let (r, w) = stream.into_split();
    let mut reader = LineReader::new(BufReader::new(r));
    let mut writer = LineWriter::new(w);
    let init = jsonrpc::decode(&reader.receive().await?.unwrap_or_default())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": "stub" } },
        })))
        .await?;
    reader.receive().await?; // notifications/initialized
    loop {
        match reader.receive().await? {
            None => return Ok(()),
            Some(raw) => {
                let message = jsonrpc::decode(&raw)?;
                match jsonrpc::method_of(&message) {
                    Some("tools/list") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": [ { "name": "echo" } ] } }))).await?;
                    }
                    Some("tools/call") => {
                        let text = format!("echoed:{}", message["params"]["name"].as_str().unwrap_or(""));
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "content": [ { "type": "text", "text": text } ] } }))).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn run_backend(listener: TcpListener) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let _ = serve_stub(stream).await;
        });
    }
}

async fn client_call(
    cw: &mut LineWriter<tokio::net::tcp::OwnedWriteHalf>,
    cr: &mut LineReader<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    id: &str,
    method: &str,
    params: Value,
) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap_or_default())
}

#[tokio::test]
async fn e2e_stdio_initialize_list_call() {
    let b0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b0_port = b0.local_addr().unwrap().port();
    let b1_port = b1.local_addr().unwrap().port();
    let backend_tasks = vec![tokio::spawn(run_backend(b0)), tokio::spawn(run_backend(b1))];

    let proxy_port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve"))
        .args([
            "--listen",
            &format!("127.0.0.1:{proxy_port}"),
            "--backend",
            &format!("b0=127.0.0.1:{b0_port}"),
            "--backend",
            &format!("b1=127.0.0.1:{b1_port}"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Wait for the proxy to accept.
    let stream = {
        let mut connected = None;
        for _ in 0..100 {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", proxy_port)).await {
                connected = Some(stream);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        connected.expect("proxy did not start")
    };
    let (r, w) = stream.into_split();
    let mut cr = LineReader::new(BufReader::new(r));
    let mut cw = LineWriter::new(w);

    cw.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": "c1", "method": "initialize",
        "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
    })))
    .await
    .unwrap();
    let init = jsonrpc::decode(&cr.receive().await.unwrap().unwrap()).unwrap();
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await.unwrap();
    let listing = client_call(&mut cw, &mut cr, "l", "tools/list", json!({})).await.unwrap();
    let called = client_call(&mut cw, &mut cr, "s", "tools/call", json!({ "name": "b0__echo", "arguments": {} })).await.unwrap();

    let _ = child.kill();
    let _ = child.wait();
    for task in backend_tasks {
        task.abort();
    }

    assert_eq!(init["result"]["protocolVersion"], PROXY_PROTOCOL_VERSION);
    assert!(init["result"]["capabilities"].get("tools").is_some());
    let names: std::collections::BTreeSet<String> = listing["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    let expected: std::collections::BTreeSet<String> = ["b0__echo", "b1__echo"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);
    assert_eq!(called["result"]["content"][0]["text"], "echoed:echo");
}
