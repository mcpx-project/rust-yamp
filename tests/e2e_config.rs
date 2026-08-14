//! δ-testinfra: automated e2e for the --config handler path.
//!
//! Spawns the real `yamp-serve` binary from a config that declares a routed MCP
//! backend, a REST Conversion handler (served locally, δ17), and the
//! yamp__backends meta-tool. Drives a full initialize -> tools/list -> tools/call
//! and asserts all three surfaces appear on one namespaced tools/list and that a
//! tools/call reaches each: the routed backend, the local REST layer, and the
//! local meta-tool. Mirrors the Python arm's test_e2e_config.py.

use std::io;
use std::net::TcpListener as StdListener;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
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

/// A minimal HTTP/1.1 endpoint for the REST handler: any request -> 200 pong.
async fn run_http_stub(listener: TcpListener) {
    while let Ok((mut stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let mut buf = Vec::new();
            let mut tmp = [0u8; 256];
            loop {
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
                match stream.read(&mut tmp).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => buf.extend_from_slice(&tmp[..n]),
                }
            }
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\npong").await;
            let _ = stream.flush().await;
        });
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
async fn e2e_config_routes_backend_rest_and_meta_tool() {
    let b0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b0_port = b0.local_addr().unwrap().port();
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http.local_addr().unwrap().port();
    let tasks = vec![tokio::spawn(run_backend(b0)), tokio::spawn(run_http_stub(http))];

    let proxy_port = free_port();
    let config = json!({
        "listen": format!("127.0.0.1:{proxy_port}"),
        "backends": { "b0": { "address": format!("127.0.0.1:{b0_port}") } },
        "handlers": {
            "metaTools": true,
            "rest": [{
                "id": "api",
                "baseUrl": format!("http://127.0.0.1:{http_port}"),
                "operations": [{ "name": "ping", "method": "GET", "path": "/ping" }],
            }],
        },
    });
    let config_path = std::env::temp_dir().join(format!("yamp-e2e-config-{proxy_port}.json"));
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve"))
        .args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

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
    cr.receive().await.unwrap();
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await.unwrap();

    let listing = client_call(&mut cw, &mut cr, "l", "tools/list", json!({})).await.unwrap();
    let rest_call = client_call(&mut cw, &mut cr, "r", "tools/call", json!({ "name": "api__ping", "arguments": {} })).await.unwrap();
    let meta_call = client_call(&mut cw, &mut cr, "m", "tools/call", json!({ "name": "yamp__backends", "arguments": {} })).await.unwrap();
    let backend_call = client_call(&mut cw, &mut cr, "s", "tools/call", json!({ "name": "b0__echo", "arguments": {} })).await.unwrap();

    let _ = child.kill();
    let _ = child.wait();
    for task in tasks {
        task.abort();
    }
    let _ = std::fs::remove_file(&config_path);

    let names: std::collections::BTreeSet<String> = listing["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    for expected in ["b0__echo", "api__ping", "yamp__backends"] {
        assert!(names.contains(expected), "missing {expected} in {names:?}");
    }
    assert_eq!(rest_call["result"]["content"][0]["text"], "pong");
    assert!(meta_call["result"]["content"][0]["text"].as_str().unwrap().contains("b0"));
    assert_eq!(backend_call["result"]["content"][0]["text"], "echoed:echo");
}
