//! δ18: the Streamable HTTP entrypoint publishes the Server Card (SEP-2127).
//!
//! Spawns the real `yamp-serve-streamable` binary on an ephemeral port and
//! asserts a plain GET /.well-known/mcp returns the proxy's self-description,
//! with no session and no backends involved. Mirrors the Python arm's
//! test_serve_streamable.py.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::Duration;

/// Reserve an ephemeral port, then release it so the child can bind it.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn connect_with_retry(port: u16) -> TcpStream {
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            return stream;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server on port {port} did not start");
}

/// Read one keep-alive HTTP response: header block, then Content-Length bytes.
fn read_http(stream: &mut TcpStream) -> (String, String) {
    stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut tmp).unwrap();
        assert!(n > 0, "eof before headers");
        buf.extend_from_slice(&tmp[..n]);
    };
    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let clen = header_text
        .lines()
        .find_map(|line| line.to_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap()))
        .unwrap_or(0);
    while buf.len() < header_end + clen {
        let n = stream.read(&mut tmp).unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let body = String::from_utf8_lossy(&buf[header_end..header_end + clen]).to_string();
    (header_text, body)
}

#[test]
fn well_known_server_card() {
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stream = connect_with_retry(port);
    stream.write_all(b"GET /.well-known/mcp HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let (headers, body) = read_http(&mut stream);

    let _ = child.kill();
    let _ = child.wait();

    assert!(headers.starts_with("HTTP/1.1 200 OK"), "unexpected status: {headers}");
    assert!(headers.to_lowercase().contains("content-type: application/json"));
    let card: serde_json::Value = serde_json::from_str(body.trim()).unwrap();
    assert_eq!(card["role"], "intermediary");
    assert_eq!(card["transports"], serde_json::json!(["stdio", "streamable-http"]));
    assert!(card["protocolVersions"].is_array());
}

fn status_backends(port: u16) -> Vec<String> {
    let mut stream = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(stream) => stream,
        Err(_) => return vec![],
    };
    if stream.write_all(b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n").is_err() {
        return vec![];
    }
    let (_headers, body) = read_http(&mut stream);
    serde_json::from_str::<serde_json::Value>(body.trim())
        .ok()
        .and_then(|v| v["backends"].as_array().map(|a| a.iter().filter_map(|b| b["id"].as_str().map(String::from)).collect()))
        .unwrap_or_default()
}

fn status_ok(port: u16) -> bool {
    let mut stream = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(stream) => stream,
        Err(_) => return false,
    };
    if stream.write_all(b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n").is_err() {
        return false;
    }
    let (_headers, body) = read_http(&mut stream);
    serde_json::from_str::<serde_json::Value>(body.trim()).map(|v| v["status"] == "ok").unwrap_or(false)
}

#[test]
fn cold_start_is_fast() {
    // U3: the server binds and answers quickly. The target is < 100ms for the
    // release binary (measured by bench/check_ux_gates.sh); this sanity ceiling keeps
    // the check non-flaky on the debug binary.
    let port = free_port();
    let path = std::env::temp_dir().join(format!("yamp-cold-{}.json", std::process::id()));
    std::fs::write(&path, format!("{{\"listen\":\"127.0.0.1:{port}\",\"backends\":{{}}}}")).unwrap();
    let start = std::time::Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args(["--config", path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut ready = false;
    for _ in 0..200 {
        if status_ok(port) {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let elapsed = start.elapsed();
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);
    assert!(ready, "server never became ready");
    assert!(elapsed < Duration::from_secs(2), "cold start took {elapsed:?}");
}

fn wait_backends(port: u16, want: &[&str]) -> bool {
    let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
    for _ in 0..100 {
        if status_backends(port) == want {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn sighup_reload_swaps_config_and_rejects_bad() {
    // U5: SIGHUP reloads --config for new connections without restarting the server; a
    // valid reload is applied and a bad one is rejected with the running config kept.
    let port = free_port();
    let path = std::env::temp_dir().join(format!("yamp-reload-{}.json", std::process::id()));
    let write_cfg = |extra: &str| std::fs::write(&path, format!("{{\"listen\":\"127.0.0.1:{port}\",{extra}}}")).unwrap();
    write_cfg("\"backends\":{\"b0\":{\"address\":\"127.0.0.1:1\"}}");
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args(["--config", path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let hup = |child: &std::process::Child| {
        let _ = Command::new("kill").args(["-HUP", &child.id().to_string()]).status();
    };

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert!(wait_backends(port, &["b0"]), "server did not come up with b0");
        // Valid reload adds b1.
        write_cfg("\"backends\":{\"b0\":{\"address\":\"127.0.0.1:1\"},\"b1\":{\"address\":\"127.0.0.1:2\"}}");
        hup(&child);
        assert!(wait_backends(port, &["b0", "b1"]), "reload did not add b1");
        // Bad reload (unknown strategy) is rejected; the running config is kept.
        write_cfg("\"namespacing\":{\"strategy\":\"nope\"},\"backends\":{\"b0\":{\"address\":\"127.0.0.1:1\"},\"b1\":{\"address\":\"127.0.0.1:2\"}}");
        hup(&child);
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(status_backends(port), vec!["b0".to_string(), "b1".to_string()], "bad reload changed config");
    }));

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);
    result.unwrap();
}

#[test]
fn tap_redacts_client_capture() {
    // With --tap, each client request is captured to stderr with credentials masked.
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args(["--listen", &format!("127.0.0.1:{port}"), "--tap"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stream = connect_with_retry(port);
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"_meta":{"authorization":"Bearer SECRET"}}}"#;
    let req = format!("POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}", body.len());
    stream.write_all(req.as_bytes()).unwrap();
    let _ = read_http(&mut stream); // no session: 400, but tapped first

    std::thread::sleep(Duration::from_millis(100));
    let _ = child.kill();
    let _ = child.wait();
    let mut err = String::new();
    child.stderr.take().unwrap().read_to_string(&mut err).unwrap();
    assert!(!err.contains("Bearer SECRET"), "stderr leaked credential: {err}");
    assert!(err.contains("\"authorization\":\"***\""), "stderr was: {err}");
    assert!(err.contains("\"direction\":\"c2s\""), "stderr was: {err}");
}

#[test]
fn refuses_public_bind_without_auth() {
    // Secure default (U7): binding a non-loopback address with no client auth must be
    // refused with exit code 2, before the listener is opened.
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args(["--listen", &format!("0.0.0.0:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Bounded wait so a regression (server binds instead of refusing) fails fast
    // rather than hanging the suite.
    let mut code = None;
    for _ in 0..50 {
        if let Some(status) = child.try_wait().unwrap() {
            code = status.code();
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if code.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("server did not exit; it bound a public address without auth");
    }
    assert_eq!(code, Some(2));
}

#[test]
fn status_endpoint() {
    // GET /status returns the read-only operational snapshot: proxy identity plus
    // the configured backends and live session count. No session required.
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args([
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--backend",
            "b0=127.0.0.1:1",
            "--backend",
            "b1=127.0.0.1:2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stream = connect_with_retry(port);
    stream.write_all(b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let (headers, body) = read_http(&mut stream);

    let _ = child.kill();
    let _ = child.wait();

    assert!(headers.starts_with("HTTP/1.1 200 OK"), "unexpected status: {headers}");
    let snap: serde_json::Value = serde_json::from_str(body.trim()).unwrap();
    assert_eq!(snap["status"], "ok");
    assert_eq!(snap["role"], "intermediary");
    assert_eq!(snap["backends"], serde_json::json!([{ "id": "b0" }, { "id": "b1" }]));
    assert_eq!(snap["sessions"], 0);
}

#[test]
fn unknown_path_is_404() {
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stream = connect_with_retry(port);
    stream.write_all(b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let (headers, _body) = read_http(&mut stream);

    let _ = child.kill();
    let _ = child.wait();

    assert!(headers.starts_with("HTTP/1.1 404 Not Found"), "unexpected status: {headers}");
}
