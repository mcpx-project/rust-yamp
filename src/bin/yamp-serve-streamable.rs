//! Serve yamp as an MCP Streamable HTTP proxy (Rust arm).
//!
//! Mirrors python/serve_streamable.py. A client POSTs JSON-RPC to /mcp. On
//! initialize the server creates a session running the real ForwardRouter, fed
//! request-by-request through an in-memory duplex pipe, assigns an
//! Mcp-Session-Id, and returns the composed response. Requests get a JSON
//! response; notifications get 202. DELETE ends the session. Backends connect
//! over TCP with stdio (newline) framing. Server-initiated SSE is not handled.
//!
//! Usage:
//!   yamp-serve-streamable --listen 127.0.0.1:9100 --backend b0=127.0.0.1:9101

use std::collections::HashMap;
use std::env;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{
    duplex, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    DuplexStream,
};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

use yamp::config::{from_value, load_config, parse_address, BackendConfig, ProxyConfig, Resilience};
use yamp::errors::{INVALID_PARAMS, NO_SESSION, UNAUTHORIZED};
use yamp::policy::{BearerAuthenticator, PolicyLayer, AUTHORIZATION};
use yamp::resilience::CircuitBreaker;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite, MAX_FRAME_BYTES};

const CAP: usize = 1 << 16;

async fn connect_failover(addresses: &[String]) -> Option<(OwnedReadHalf, OwnedWriteHalf)> {
    for address in addresses {
        if let Ok(stream) = TcpStream::connect(address).await {
            return Some(stream.into_split());
        }
    }
    None
}

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn new_session_id() -> String {
    format!("{:016x}", SESSION_COUNTER.fetch_add(1, Ordering::Relaxed))
}

struct SessionIo {
    tx: LineWriter<DuplexStream>,
    rx: LineReader<BufReader<DuplexStream>>,
}

struct Session {
    io: Mutex<SessionIo>,
    outbound: Mutex<Option<mpsc::Receiver<Value>>>,
}

impl Session {
    async fn create(configs: &[BackendConfig], resilience: &Resilience) -> io::Result<Session> {
        let (to_router, router_reads) = duplex(CAP);
        let (router_writes, from_router) = duplex(CAP);
        let resilient = resilience.enabled();
        let mut backends = Vec::new();
        for config in configs {
            let (br, bw) = match connect_failover(&config.addresses).await {
                Some(pair) => pair,
                None => {
                    // Cannot reach any address. In resilient mode leave it out.
                    if resilient {
                        continue;
                    }
                    return Err(io::Error::new(io::ErrorKind::NotConnected, format!("backend {}: all addresses failed", config.id)));
                }
            };
            let reader = LineReader::new(BufReader::new(br));
            let writer = LineWriter::new(bw);
            let backend = if resilient {
                let timeout = resilience.request_timeout.map(Duration::from_secs_f64);
                let breaker = CircuitBreaker::new(resilience.failure_threshold, resilience.reset_timeout);
                Backend::resilient(config.id.clone(), reader, writer, breaker, timeout)
            } else {
                Backend::new(config.id.clone(), reader, writer)
            }
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            backends.push(backend);
        }
        // Backend-initiated messages go to this channel; the GET SSE stream drains it.
        let (outbound_tx, outbound_rx) = mpsc::channel(256);
        let health = if resilient { resilience.health_interval.map(Duration::from_secs_f64) } else { None };
        let router = ForwardRouter::configure(
            LineReader::new(BufReader::new(router_reads)),
            LineWriter::new(router_writes),
            backends,
            Some(outbound_tx),
            health,
        );
        tokio::spawn(router.serve());
        Ok(Session {
            io: Mutex::new(SessionIo {
                tx: LineWriter::new(to_router),
                rx: LineReader::new(BufReader::new(from_router)),
            }),
            outbound: Mutex::new(Some(outbound_rx)),
        })
    }

    async fn request(&self, body: &[u8]) -> io::Result<Vec<u8>> {
        let mut io = self.io.lock().await;
        io.tx.send(body).await?;
        Ok(io.rx.receive().await?.unwrap_or_default())
    }

    async fn notify(&self, body: &[u8]) -> io::Result<()> {
        self.io.lock().await.tx.send(body).await
    }
}

type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;
// The live config new connections read, swappable by a SIGHUP reload (U5). A std
// RwLock keeps the per-request read cheap and lock-free of the async runtime.
type ConfigHolder = Arc<std::sync::RwLock<Arc<ProxyConfig>>>;

async fn read_request<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<(String, String, HashMap<String, String>, Vec<u8>)>> {
    let mut header_lines: Vec<Vec<u8>> = Vec::new();
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
        header_lines.push(line);
    }
    if header_lines.is_empty() {
        return Ok(None);
    }
    let request_line = String::from_utf8_lossy(&header_lines[0]).into_owned();
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut headers = HashMap::new();
    let mut length = 0usize;
    for raw in &header_lines[1..] {
        let text = String::from_utf8_lossy(raw);
        if let Some((k, v)) = text.split_once(':') {
            let key = k.trim().to_lowercase();
            let value = v.trim().to_string();
            if key == "content-length" {
                // Bounded, defensive parse (this entrypoint parses Content-Length
                // itself rather than through the framing decoder). A non-numeric
                // or negative value is treated as no body; a value over
                // MAX_FRAME_BYTES is rejected so a hostile header cannot force an
                // unbounded allocation, matching the framing decoder's cap.
                length = value.parse().unwrap_or(0);
                if length > MAX_FRAME_BYTES {
                    return Ok(None);
                }
            }
            headers.insert(key, value);
        }
    }
    let mut body = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut body).await?;
    }
    Ok(Some((method, path, headers, body)))
}

async fn write_response<W: AsyncWrite + Unpin>(
    w: &mut W,
    status: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> io::Result<()> {
    let mut head = format!("HTTP/1.1 {status}\r\n");
    for (k, v) in extra {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!(
        "Content-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        body.len()
    ));
    w.write_all(head.as_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}

async fn handle(stream: TcpStream, holder: ConfigHolder, sessions: Sessions, tap: bool) -> io::Result<()> {
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    loop {
        let (method, path, headers, body) = match read_request(&mut reader).await? {
            Some(req) => req,
            None => break,
        };
        // Read the current config per request so a SIGHUP reload takes effect for new
        // requests while in-flight sessions keep serving (U5, zero dropped).
        let config = holder.read().unwrap().clone();
        if method == "GET" && path == "/.well-known/mcp" {
            // Publish the proxy's Server Card for discovery (SEP-2127).
            let card = serde_json::to_vec(&yamp::routing::server_card()).unwrap_or_default();
            write_response(&mut w, "200 OK", &[("Content-Type", "application/json")], &card).await?;
            continue;
        }
        if method == "GET" && path == "/status" {
            // Read-only operational status (Track U). serde_json serializes the map
            // with sorted keys, matching the Python arm's sort_keys output.
            let ids: Vec<String> = config.backends.iter().map(|b| b.id.clone()).collect();
            let count = sessions.lock().await.len();
            let snap = serde_json::to_vec(&yamp::status::snapshot(&ids, count)).unwrap_or_default();
            write_response(&mut w, "200 OK", &[("Content-Type", "application/json")], &snap).await?;
            continue;
        }
        if path != "/mcp" {
            write_response(&mut w, "404 Not Found", &[], b"").await?;
            continue;
        }
        let sid = headers.get("mcp-session-id").cloned();
        if method == "GET" {
            // Server-to-client SSE stream (Streamable HTTP GET /mcp).
            let session = match &sid {
                Some(id) => sessions.lock().await.get(id).cloned(),
                None => None,
            };
            match session {
                None => {
                    write_response(&mut w, "404 Not Found", &[], b"").await?;
                    continue;
                }
                Some(session) => {
                    w.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                          Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
                    )
                    .await?;
                    w.flush().await?;
                    if let Some(mut rx) = session.outbound.lock().await.take() {
                        while let Some(message) = rx.recv().await {
                            let event = format!("data: {}\n\n", serde_json::to_string(&message).unwrap_or_default());
                            if w.write_all(event.as_bytes()).await.is_err() || w.flush().await.is_err() {
                                break;
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }
        if method == "DELETE" {
            if let Some(id) = &sid {
                sessions.lock().await.remove(id);
            }
            write_response(&mut w, "200 OK", &[], b"").await?;
            continue;
        }

        let message: Value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            // An empty body is a bodyless POST (e.g. a bare notification frame);
            // a non-empty malformed body gets a 400, not a silent null message,
            // matching the Python arm.
            Err(_) if body.is_empty() => Value::Null,
            Err(_) => {
                let body = format!("{{\"error\":{{\"code\":{INVALID_PARAMS},\"message\":\"invalid JSON\"}}}}");
                write_response(&mut w, "400 Bad Request", &[("Content-Type", "application/json")], body.as_bytes()).await?;
                continue;
            }
        };
        if tap {
            // Redacting live capture (Track U): never surface a credential.
            eprintln!("{}", serde_json::to_string(&yamp::tap::capture("c2s", &message)).unwrap_or_default());
        }
        let is_request = message.get("id").is_some() && message.get("method").is_some();
        let is_initialize = message.get("method").and_then(Value::as_str) == Some("initialize");

        if sid.is_none() && is_initialize {
            if !config.client_tokens.is_empty() {
                let policy = PolicyLayer::new(
                    HashMap::new(),
                    HashMap::new(),
                    Some(Box::new(BearerAuthenticator::new(config.client_tokens.clone()))),
                );
                let mut client_headers = HashMap::new();
                client_headers.insert(AUTHORIZATION.to_string(), headers.get("authorization").cloned().unwrap_or_default());
                if !policy.authorize_client(&client_headers) {
                    let body = format!("{{\"error\":{{\"code\":{UNAUTHORIZED},\"message\":\"unauthorized\"}}}}");
                    write_response(&mut w, "401 Unauthorized", &[("Content-Type", "application/json")], body.as_bytes()).await?;
                    continue;
                }
            }
            let session = Arc::new(Session::create(&config.backends, &config.resilience).await?);
            let id = new_session_id();
            sessions.lock().await.insert(id.clone(), session.clone());
            let resp = session.request(&body).await.unwrap_or_default();
            write_response(
                &mut w,
                "200 OK",
                &[("Content-Type", "application/json"), ("Mcp-Session-Id", &id)],
                &resp,
            )
            .await?;
            continue;
        }

        let session = match &sid {
            Some(id) => sessions.lock().await.get(id).cloned(),
            None => None,
        };
        match session {
            Some(session) if is_request => {
                let resp = session.request(&body).await.unwrap_or_default();
                write_response(&mut w, "200 OK", &[("Content-Type", "application/json")], &resp).await?;
            }
            Some(session) => {
                let _ = session.notify(&body).await;
                write_response(&mut w, "202 Accepted", &[], b"").await?;
            }
            None => {
                let body = format!("{{\"error\":{{\"code\":{NO_SESSION},\"message\":\"no session\"}}}}");
                write_response(
                    &mut w,
                    "400 Bad Request",
                    &[("Content-Type", "application/json")],
                    body.as_bytes(),
                )
                .await?;
            }
        }
    }
    Ok(())
}

fn config_from_args(args: &[String]) -> io::Result<ProxyConfig> {
    let mut listen = String::new();
    let mut backends = serde_json::Map::new();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--config" => return load_config(&args[i + 1]),
            "--listen" => listen = args[i + 1].clone(),
            "--backend" => {
                let (id, addr) = args[i + 1].split_once('=').expect("backend as id=addr");
                backends.insert(id.to_string(), serde_json::json!({ "address": addr }));
            }
            _ => {}
        }
        i += 2;
    }
    from_value(&serde_json::json!({ "listen": listen, "backends": backends }))
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let config = config_from_args(&args)?;

    // Secure default (U7): refuse a non-loopback bind without client auth unless the
    // operator explicitly opts out with --insecure.
    let insecure = args.iter().any(|a| a == "--insecure");
    if let Some(refusal) = yamp::security::guard_bind(&config.listen, !config.client_tokens.is_empty(), insecure) {
        eprintln!("error: {refusal}");
        std::process::exit(2);
    }

    let tap = args.iter().any(|a| a == "--tap");
    let config_path = args.windows(2).find(|w| w[0] == "--config").map(|w| w[1].clone());
    let (host, port) = parse_address(&config.listen)?;
    let listener = TcpListener::bind((host.as_str(), port)).await?;
    println!("listening on {}", config.listen);
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let holder: ConfigHolder = Arc::new(std::sync::RwLock::new(Arc::new(config)));

    // Hot reload (U5): on SIGHUP re-read --config, validate it (a bad reload is
    // rejected and the running config kept), and swap it for new connections. In-flight
    // sessions hold their own config and are never dropped.
    if let Some(path) = config_path {
        let holder = holder.clone();
        tokio::spawn(async move {
            let mut hup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                Ok(signal) => signal,
                Err(_) => return,
            };
            while hup.recv().await.is_some() {
                let raw = std::fs::read_to_string(&path).ok().and_then(|text| serde_json::from_str::<Value>(&text).ok());
                match raw {
                    None => eprintln!("reload rejected: config unreadable or not JSON"),
                    Some(raw) => {
                        if let Some(diagnosis) = yamp::config::diagnose(&raw) {
                            eprintln!("reload rejected: {}: {}", diagnosis["slug"].as_str().unwrap_or(""), diagnosis["message"].as_str().unwrap_or(""));
                        } else if let Ok(new_config) = from_value(&raw) {
                            *holder.write().unwrap() = Arc::new(new_config);
                            println!("config reloaded");
                        }
                    }
                }
            }
        });
    }

    loop {
        let (stream, _) = listener.accept().await?;
        let holder = holder.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            let _ = handle(stream, holder, sessions, tap).await;
        });
    }
}
