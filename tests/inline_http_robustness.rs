//! Adversarial-input tests for the inline HTTP request parsers in the served
//! entrypoints (test-infra gap #4).
//!
//! `yamp-serve-http` and `yamp-serve-streamable` parse Content-Length themselves
//! rather than through the framing decoder, so they carry their own copy of the
//! allocation-amplification vector the framing decoder closed. These boot each
//! real binary and confirm a hostile Content-Length forces neither an unbounded
//! read nor a crash: the connection closes cleanly and the server keeps serving.
//! Mirrors the Python arm's test_inline_http_parser.py.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

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

/// Send a POST declaring a hostile Content-Length and assert the server closes
/// the connection (a clean EOF) rather than hanging to read the declared bytes.
fn assert_hostile_length_closes(port: u16) {
    let mut stream = connect_with_retry(port);
    stream
        .write_all(b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 999999999999\r\n\r\nX")
        .unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut tmp = [0u8; 64];
    // A hang would surface as a read timeout (Err); a clean reject reads 0 (EOF).
    let n = stream.read(&mut tmp).expect("server hung on a hostile Content-Length");
    assert_eq!(n, 0, "expected a clean close on a hostile Content-Length");
}

fn read_status_line(stream: &mut TcpStream) -> String {
    stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 256];
    loop {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            return String::from_utf8_lossy(&buf[..pos]).to_string();
        }
        let n = stream.read(&mut tmp).unwrap();
        assert!(n > 0, "eof before status line");
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[test]
fn streamable_rejects_hostile_content_length_and_survives() {
    let port = free_port();
    let mut child: Child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-streamable"))
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    assert_hostile_length_closes(port);

    // The server survived the hostile request and still serves a fresh client.
    let mut ok = connect_with_retry(port);
    ok.write_all(b"GET /.well-known/mcp HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let status = read_status_line(&mut ok);

    let _ = child.kill();
    let _ = child.wait();
    assert!(status.starts_with("HTTP/1.1 200 OK"), "server did not survive: {status}");
}

#[test]
fn http_rejects_hostile_content_length_and_survives() {
    let port = free_port();
    // No backend is needed: the hostile Content-Length is rejected while parsing
    // the client request, before any routing.
    let mut child: Child = Command::new(env!("CARGO_BIN_EXE_yamp-serve-http"))
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    assert_hostile_length_closes(port);

    // A well-formed request on a fresh connection still gets a response, proving
    // the server process survived (no backend, so it is a routed error).
    let mut ok = connect_with_retry(port);
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x"}}"#;
    let request = format!(
        "POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    ok.write_all(request.as_bytes()).unwrap();
    ok.write_all(body).unwrap();
    let status = read_status_line(&mut ok);

    let _ = child.kill();
    let _ = child.wait();
    assert!(status.starts_with("HTTP/1.1 200 OK"), "server did not survive: {status}");
}
