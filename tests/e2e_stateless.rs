//! δ-testinfra: automated e2e for the stateless served entrypoint.
//!
//! Spawns the real `yamp-serve-stateless` binary against two live stub stateless
//! backends over TCP and drives a full server/discover -> tools/call as a client,
//! asserting the composed namespaced surface, a routed call with the prefix
//! stripped, and per-request protocol-version negotiation (SEP-2575). Mirrors the
//! Python arm's test_e2e_stateless.py.

use std::io;
use std::net::TcpListener as StdListener;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::json;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};

use yamp::stateless::{decode_request, decode_response, encode_response, StatelessResponse};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};
use yamp::version::{PROTOCOL_VERSION_META_KEY, STATELESS_PROTOCOL_VERSION};

fn free_port() -> u16 {
    StdListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn stateless_stub(stream: TcpStream, name: &str) -> io::Result<()> {
    let (r, w) = stream.into_split();
    let mut reader = LineReader::new(BufReader::new(r));
    let mut writer = LineWriter::new(w);
    loop {
        match reader.receive().await? {
            None => return Ok(()),
            Some(raw) => {
                let request = decode_request(&raw)?;
                let response = if request.method == "server/discover" {
                    StatelessResponse {
                        meta: json!({ "backend": name }),
                        body: json!({ "tools": [ { "name": "echo" } ] }).to_string(),
                    }
                } else {
                    // Echo the (prefix-stripped) name and the pinned version.
                    let pinned = request.meta.get(PROTOCOL_VERSION_META_KEY).and_then(|v| v.as_str()).unwrap_or("");
                    StatelessResponse {
                        meta: json!({ "backend": name }),
                        body: format!("echoed:{}:{}", request.name.unwrap_or_default(), pinned),
                    }
                };
                writer.send(&encode_response(&response)).await?;
            }
        }
    }
}

async fn run_backend(listener: TcpListener, name: String) {
    while let Ok((stream, _)) = listener.accept().await {
        let name = name.clone();
        tokio::spawn(async move {
            let _ = stateless_stub(stream, &name).await;
        });
    }
}

#[tokio::test]
async fn e2e_stateless_discover_and_call() {
    let b0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b0_port = b0.local_addr().unwrap().port();
    let b1_port = b1.local_addr().unwrap().port();
    let backend_tasks = vec![
        tokio::spawn(run_backend(b0, "b0".to_string())),
        tokio::spawn(run_backend(b1, "b1".to_string())),
    ];

    let proxy_port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-stateless"))
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

    let version_meta = json!({ PROTOCOL_VERSION_META_KEY: STATELESS_PROTOCOL_VERSION });
    // server/discover: the proxy composes the backends' tool surfaces, prefixed.
    let discover_req = json!({ "method": "server/discover", "name": null, "meta": version_meta.clone(), "body": "" });
    cw.send(&yamp::jsonrpc::encode(&discover_req)).await.unwrap();
    let discover = decode_response(&cr.receive().await.unwrap().unwrap()).unwrap();
    let tools: serde_json::Value = serde_json::from_str(&discover.body).unwrap();
    let names: std::collections::BTreeSet<String> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();

    // tools/call routes on the Mcp-Name header, stripping the prefix.
    let call_req = json!({ "method": "tools/call", "name": "b0__echo", "meta": version_meta.clone(), "body": "{}" });
    cw.send(&yamp::jsonrpc::encode(&call_req)).await.unwrap();
    let call_resp = decode_response(&cr.receive().await.unwrap().unwrap()).unwrap();

    let _ = child.kill();
    let _ = child.wait();
    for task in backend_tasks {
        task.abort();
    }

    let expected: std::collections::BTreeSet<String> = ["b0__echo", "b1__echo"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);
    assert_eq!(call_resp.body, format!("echoed:echo:{STATELESS_PROTOCOL_VERSION}"));
}
