//! δ-testinfra: automated full-flow e2e for the Streamable HTTP entrypoint.
//!
//! Spawns the real `yamp-serve-streamable` binary against two live stub backends
//! and drives a full initialize (mints a session) -> notifications/initialized ->
//! tools/list -> tools/call over HTTP, asserting the minted Mcp-Session-Id, the
//! composed namespaced surface, and a routed call. Mirrors the Python arm's
//! test_serve_streamable.py::test_e2e_streamable_initialize_list_call.

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

async fn run_backend(listener: TcpListener) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let _ = serve_stub(stream).await;
        });
    }
}

/// One HTTP POST /mcp on a fresh connection; returns (status line, headers, body).
async fn http_post(port: u16, body: &[u8], session_id: Option<&str>) -> (String, std::collections::HashMap<String, String>, Vec<u8>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut head = format!("POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n", body.len());
    if let Some(sid) = session_id {
        head.push_str(&format!("Mcp-Session-Id: {sid}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut tmp).await.unwrap();
        assert!(n > 0, "eof before headers");
        buf.extend_from_slice(&tmp[..n]);
    };
    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = header_text.split("\r\n");
    let status = lines.next().unwrap_or("").to_string();
    let mut headers = std::collections::HashMap::new();
    let mut clen = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_lowercase();
            let value = v.trim().to_string();
            if key == "content-length" {
                clen = value.parse().unwrap_or(0);
            }
            headers.insert(key, value);
        }
    }
    while buf.len() < header_end + clen {
        let n = stream.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    (status, headers, buf[header_end..header_end + clen].to_vec())
}

#[tokio::test]
async fn e2e_streamable_initialize_list_call() {
    let b0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b0_port = b0.local_addr().unwrap().port();
    let b1_port = b1.local_addr().unwrap().port();
    let backend_tasks = vec![tokio::spawn(run_backend(b0)), tokio::spawn(run_backend(b1))];

    let proxy_port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
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
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", proxy_port)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let init_body = jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": "c1", "method": "initialize",
        "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
    }));
    let (status, headers, body) = http_post(proxy_port, &init_body, None).await;
    let sid = headers.get("mcp-session-id").cloned();
    let init: Value = serde_json::from_slice(&body).unwrap();

    let sid_ref = sid.as_deref();
    let note = jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
    let (note_status, _h, _b) = http_post(proxy_port, &note, sid_ref).await;

    let list_body = jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "l", "method": "tools/list", "params": {} }));
    let (_s, _h, listing_raw) = http_post(proxy_port, &list_body, sid_ref).await;
    let listing: Value = serde_json::from_slice(&listing_raw).unwrap();

    let call_body = jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "s", "method": "tools/call", "params": { "name": "b0__echo", "arguments": {} } }));
    let (_s, _h, called_raw) = http_post(proxy_port, &call_body, sid_ref).await;
    let called: Value = serde_json::from_slice(&called_raw).unwrap();

    let _ = child.kill();
    let _ = child.wait();
    for task in backend_tasks {
        task.abort();
    }

    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(sid.is_some(), "a session id was minted");
    assert_eq!(init["result"]["protocolVersion"], PROXY_PROTOCOL_VERSION);
    assert_eq!(note_status, "HTTP/1.1 202 Accepted");
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
