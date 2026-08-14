//! Run a yamp forward proxy over TCP.
//!
//! Each accepted client connection gets its own ForwardRouter, which opens its
//! own connections to the backends and runs the MCP handshake. This is the
//! entrypoint the external load harness (../bench) drives.
//!
//! Usage:
//!   yamp-serve --listen 127.0.0.1:9100 --backend b0=127.0.0.1:9101 --backend b1=127.0.0.1:9102

use std::env;
use std::io;
use std::sync::{Arc, Mutex};

use serde_json::json;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};

use yamp::cache::ListCache;
use yamp::config::{load_config, ProxyConfig};
use yamp::handler::build_registry;
use yamp::router::{Backend, ForwardRouter};
use yamp::signing::AuditLog;
use yamp::transport::{LineReader, LineWriter};

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut listen = String::new();
    let mut backends: Vec<(String, String)> = Vec::new();
    let mut config_path: Option<String> = None;
    let mut insecure = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" if i + 1 < args.len() => {
                listen = args[i + 1].clone();
                i += 1;
            }
            "--backend" if i + 1 < args.len() => {
                let (id, addr) = args[i + 1].split_once('=').expect("backend as id=addr");
                backends.push((id.to_string(), addr.to_string()));
                i += 1;
            }
            "--config" if i + 1 < args.len() => {
                config_path = Some(args[i + 1].clone());
                i += 1;
            }
            "--insecure" => insecure = true,
            _ => {}
        }
        i += 1;
    }

    // A --config file supplies backends, handlers, and the collision strategy;
    // --backend flags are the simpler alternative with no local handlers.
    let config = match &config_path {
        Some(path) => {
            let loaded = load_config(path)?;
            if listen.is_empty() {
                listen = loaded.listen.clone();
            }
            backends = loaded
                .backends
                .iter()
                .map(|b| (b.id.clone(), b.addresses[0].clone()))
                .collect();
            Some(Arc::new(loaded))
        }
        None => None,
    };

    // Secure default (U7): refuse a non-loopback bind without client auth unless the
    // operator explicitly opts out with --insecure.
    let has_client_auth = config.as_ref().map(|c| !c.client_tokens.is_empty()).unwrap_or(false);
    if let Some(refusal) = yamp::security::guard_bind(&listen, has_client_auth, insecure) {
        eprintln!("error: {refusal}");
        std::process::exit(2);
    }

    let listener = TcpListener::bind(&listen).await?;
    println!("listening on {listen}");
    // One cache shared across every client connection, so repeated list fetches
    // by many clients collapse to O(backends) (SEP §6).
    let cache = Arc::new(Mutex::new(ListCache::default()));
    // A config audit secret enables one accountability log shared across every
    // connection, so the hash chain spans the whole proxy (SEP-2828, δ21).
    let audit = config
        .as_ref()
        .and_then(|c| c.audit_secret.clone())
        .map(|secret| Arc::new(Mutex::new(AuditLog::new(secret))));
    if audit.is_some() {
        println!("accountability log enabled");
    }
    loop {
        let (client, _) = listener.accept().await?;
        let specs = backends.clone();
        let cache = cache.clone();
        let config = config.clone();
        let audit = audit.clone();
        tokio::spawn(async move {
            let _ = handle(client, specs, cache, config, audit).await;
        });
    }
}

async fn handle(
    client: TcpStream,
    specs: Vec<(String, String)>,
    cache: Arc<Mutex<ListCache>>,
    config: Option<Arc<ProxyConfig>>,
    audit: Option<Arc<Mutex<AuditLog>>>,
) -> io::Result<()> {
    let (client_read, client_write) = client.into_split();
    let mut backends = Vec::new();
    for (id, addr) in &specs {
        let (backend_read, backend_write) = TcpStream::connect(addr).await?.into_split();
        let mut backend = Backend::new(
            id.clone(),
            LineReader::new(BufReader::new(backend_read)),
            LineWriter::new(backend_write),
        )
        .expect("valid backend id");
        // Inject the backend's own credential from config (SEP §13.1).
        if let Some(config) = &config {
            if let Some(token) = config.backends.iter().find(|b| &b.id == id).and_then(|b| b.token.clone()) {
                backend = backend.with_token(token);
            }
        }
        backends.push(backend);
    }
    let mut router = ForwardRouter::new(
        LineReader::new(BufReader::new(client_read)),
        LineWriter::new(client_write),
        backends,
    )
    .set_cache(cache, None);
    if let Some(audit) = audit {
        router = router.set_audit(audit);
    }
    if let Some(config) = &config {
        let ids: Vec<String> = specs.iter().map(|(id, _)| id.clone()).collect();
        let provider = move || json!(ids.iter().map(|id| json!({ "id": id })).collect::<Vec<_>>());
        let registry = build_registry(&config.handlers, provider).expect("valid handler config");
        router = router.set_registry(registry).set_namespacing(config.namespacing.clone());
    }
    router.serve().await
}
