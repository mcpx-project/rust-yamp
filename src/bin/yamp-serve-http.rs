//! Run a yamp stateless proxy over HTTP (Streamable HTTP request/response).
//!
//! Mirrors python/serve_http.py. Each POST to /mcp carries one JSON-RPC
//! message. The proxy reads the tool name, resolves the namespace prefix to a
//! backend, forwards the call over HTTP with a pooled keep-alive connection,
//! and returns the backend's response. Stateless: no session, no handshake.
//! This is the entrypoint the Go HTTP harness (`bench/httpbench`) drives.
//!
//! Usage:
//!   yamp-serve-http --listen 127.0.0.1:9100 --backend b0=http://127.0.0.1:9101/mcp

use std::collections::HashMap;
use std::env;
use std::io;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use yamp::errors::{INVALID_PARAMS, SERVER_NOT_AVAILABLE};
use yamp::transport::MAX_FRAME_BYTES;
use yamp::{auth, media, namespace};

#[derive(Clone)]
struct Target {
    host: String,
    port: u16,
    path: String,
}

type Pool = Arc<Mutex<HashMap<(String, u16), Vec<BufReader<TcpStream>>>>>;

fn parse_backend(spec: &str) -> (String, Target) {
    let (id, url) = spec.split_once('=').expect("backend as id=url");
    let rest = url.strip_prefix("http://").unwrap_or(url);
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    let (host, port) = authority.split_once(':').expect("host:port");
    (id.to_string(), Target { host: host.to_string(), port: port.parse().unwrap_or(80), path })
}

async fn read_message<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> io::Result<Option<(Vec<u8>, Option<String>)>> {
    let mut length = 0usize;
    let mut accept: Option<String> = None;
    loop {
        let mut line = Vec::new();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Ok(None);
        }
        if line.ends_with(b"\n") {
            line.pop();
        }
        if line.ends_with(b"\r") {
            line.pop();
        }
        if line.is_empty() {
            break;
        }
        let text = String::from_utf8_lossy(&line);
        if let Some((k, v)) = text.split_once(':') {
            let key = k.trim();
            if key.eq_ignore_ascii_case("content-length") {
                // Bounded, defensive parse (this entrypoint parses Content-Length
                // itself rather than through the framing decoder). A non-numeric,
                // negative, or over-MAX_FRAME_BYTES value closes the connection
                // rather than coercing to 0 and desyncing the keep-alive stream,
                // matching the Python arm which raises and returns None.
                match v.trim().parse::<usize>() {
                    Ok(n) if n <= MAX_FRAME_BYTES => length = n,
                    _ => return Ok(None),
                }
            } else if key.eq_ignore_ascii_case("accept") {
                accept = Some(v.trim().to_string());
            }
        }
    }
    let mut body = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut body).await?;
    }
    Ok(Some((body, accept)))
}

async fn backend_post(pool: &Pool, target: &Target, body: &[u8]) -> io::Result<Vec<u8>> {
    let key = (target.host.clone(), target.port);
    let mut conn = {
        let mut guard = pool.lock().await;
        guard.get_mut(&key).and_then(Vec::pop)
    };
    if conn.is_none() {
        conn = Some(BufReader::new(TcpStream::connect((target.host.as_str(), target.port)).await?));
    }
    let mut reader = conn.unwrap();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        target.path, target.host, body.len()
    );
    reader.get_mut().write_all(request.as_bytes()).await?;
    reader.get_mut().write_all(body).await?;
    reader.get_mut().flush().await?;

    match read_message(&mut reader).await? {
        Some((body, _)) => {
            pool.lock().await.entry(key).or_default().push(reader);
            Ok(body)
        }
        // The backend closed the keep-alive connection: drop it rather than
        // returning it to the pool where the next request reuses a dead socket.
        None => Ok(Vec::new()),
    }
}

fn error_body(code: i64) -> Vec<u8> {
    json!({ "jsonrpc": "2.0", "error": { "code": code } }).to_string().into_bytes()
}

async fn route(body: &[u8], backends: &HashMap<String, Target>, pool: &Pool) -> Vec<u8> {
    let message: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return error_body(INVALID_PARAMS),
    };
    let name = message.get("params").and_then(|p| p.get("name")).and_then(Value::as_str).unwrap_or("");
    let (bid, original) = match namespace::split(name) {
        Some(pair) if backends.contains_key(pair.0) => pair,
        _ => return error_body(INVALID_PARAMS),
    };
    let mut forwarded = message.clone();
    if let Some(params) = forwarded.get_mut("params").and_then(Value::as_object_mut) {
        params.insert("name".to_string(), Value::String(original.to_string()));
        // Confused-deputy defense (SEP §13.1): drop the client's credential from
        // the forwarded _meta so it never reaches a backend, like the router.
        if let Some(meta) = params.get("_meta") {
            let stripped = auth::forward_meta(meta, None);
            params.insert("_meta".to_string(), stripped);
        }
    }
    match backend_post(pool, &backends[bid], &serde_json::to_vec(&forwarded).unwrap()).await {
        Ok(resp) => resp,
        Err(_) => error_body(SERVER_NOT_AVAILABLE),
    }
}

async fn handle(stream: TcpStream, backends: Arc<HashMap<String, Target>>, pool: Pool) -> io::Result<()> {
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    while let Some((body, accept)) = read_message(&mut reader).await? {
        let out = route(&body, &backends, &pool).await;
        // SEP-2357: answer in application/mcp+json when the client accepts it,
        // else application/json.
        let content_type = media::response_content_type(accept.as_deref());
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            out.len()
        );
        w.write_all(head.as_bytes()).await?;
        w.write_all(&out).await?;
        w.flush().await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut listen = String::new();
    let mut backends: HashMap<String, Target> = HashMap::new();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--listen" => listen = args[i + 1].clone(),
            "--backend" => {
                let (id, target) = parse_backend(&args[i + 1]);
                backends.insert(id, target);
            }
            _ => {}
        }
        i += 2;
    }

    let listener = TcpListener::bind(&listen).await?;
    println!("listening on {listen}");
    let backends = Arc::new(backends);
    let pool: Pool = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (stream, _) = listener.accept().await?;
        let backends = backends.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let _ = handle(stream, backends, pool).await;
        });
    }
}
