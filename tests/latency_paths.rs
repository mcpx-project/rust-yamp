//! Latency-tier coverage for paths added after the original δ0-δ5 tier.
//!
//! Extends the ≤10 ms per-message budget to a many-backend (40, 100) fan-out and
//! to the signing/audit hot path, which the original tier did not exercise.
//! Mirrors the Python arm's test_latency_paths.py.

use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::cache::ListCache;
use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::handler::{BackendsHandler, Registry};
use yamp::instrument::within_budget;
use yamp::jsonrpc::{self, method_of};
use yamp::router::{Backend, ForwardRouter};
use yamp::signing::AuditLog;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;
const WARMUP: usize = 20;
const SAMPLES: usize = 200;

async fn mock(
    mut reader: LineReader<BufReader<DuplexStream>>,
    mut writer: LineWriter<DuplexStream>,
    name: String,
) -> io::Result<()> {
    let init = jsonrpc::decode(&reader.receive().await?.unwrap_or_default())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": name } },
        })))
        .await?;
    reader.receive().await?; // notifications/initialized
    let tool = format!("{name}_tool");
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let message = jsonrpc::decode(&raw)?;
                match method_of(&message) {
                    Some("tools/list") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": [ { "name": tool } ] } }))).await?;
                    }
                    Some("tools/call") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "content": [ { "type": "text", "text": "ok" } ] } }))).await?;
                    }
                    Some("tasks/get") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "status": "completed" } }))).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

#[derive(Default)]
struct Opts {
    audit: Option<Arc<StdMutex<AuditLog>>>,
    cache: Option<Arc<StdMutex<ListCache>>>,
    registry: Option<Registry>,
    token: Option<String>,
}

async fn measure(n: usize, request: Value, opts: Opts) -> Vec<f64> {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let mut backends = Vec::new();
    let mut mocks = Vec::new();
    for i in 0..n {
        let (router_to_b, b_reads) = duplex(CAP);
        let (b_writes, router_reads_b) = duplex(CAP);
        let id = format!("b{i}");
        let mut backend =
            Backend::new(id.clone(), LineReader::new(BufReader::new(router_reads_b)), LineWriter::new(router_to_b)).unwrap();
        if let Some(token) = &opts.token {
            backend = backend.with_token(token.clone());
        }
        backends.push(backend);
        mocks.push(mock(LineReader::new(BufReader::new(b_reads)), LineWriter::new(b_writes), id));
    }
    let mut router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );
    if let Some(a) = opts.audit {
        router = router.set_audit(a);
    }
    if let Some(c) = opts.cache {
        router = router.set_cache(c, None);
    }
    if let Some(r) = opts.registry {
        router = router.set_registry(r);
    }

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let req = jsonrpc::encode(&request);
        for _ in 0..WARMUP {
            cw.send(&req).await?;
            cr.receive().await?;
        }
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let start = Instant::now();
            cw.send(&req).await?;
            cr.receive().await?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        cw.send_eof().await?;
        Ok::<Vec<f64>, io::Error>(samples)
    };

    let mocks_fut = futures::future::join_all(mocks);
    let (_router, _mocks, samples) = tokio::join!(router.serve(), mocks_fut, client);
    samples.unwrap()
}

fn assert_budget(label: &str, mut latencies: Vec<f64>) {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = latencies[latencies.len() / 2];
    let under = latencies.iter().filter(|&&x| within_budget(x)).count() as f64 / latencies.len() as f64;
    println!("[latency {label}] median={median:.4}ms within={under:.3}");
    assert!(within_budget(median));
    assert!(under >= 0.99);
}

#[tokio::test]
async fn fanout_40_backends_within_budget() {
    let request = json!({ "jsonrpc": "2.0", "id": "l", "method": "tools/list", "params": {} });
    assert_budget("fanout-40", measure(40, request, Opts::default()).await);
}

#[tokio::test]
async fn fanout_100_backends_within_budget() {
    let request = json!({ "jsonrpc": "2.0", "id": "l", "method": "tools/list", "params": {} });
    assert_budget("fanout-100", measure(100, request, Opts::default()).await);
}

#[tokio::test]
async fn audited_call_within_budget() {
    // A single backend, but every tools/call appends a signed attestation and
    // outcome to the audit log: the signing hot path must stay within budget.
    let request = json!({ "jsonrpc": "2.0", "id": "s", "method": "tools/call", "params": { "name": "b0_tool", "arguments": {} } });
    let audit = Arc::new(StdMutex::new(AuditLog::new("secret")));
    assert_budget("audited-call", measure(1, request, Opts { audit: Some(audit), ..Opts::default() }).await);
}

#[tokio::test]
async fn cache_hit_within_budget() {
    // A shared list cache over 40 backends: the warmup fills the cache, so every
    // sampled tools/list is a fresh hit that skips the backend fan-out entirely
    // (SEP §6). The cache-hit path must stay within budget.
    let request = json!({ "jsonrpc": "2.0", "id": "l", "method": "tools/list", "params": {} });
    let cache = Arc::new(StdMutex::new(ListCache::default()));
    assert_budget("cache-hit", measure(40, request, Opts { cache: Some(cache), ..Opts::default() }).await);
}

#[tokio::test]
async fn dispatch_handler_within_budget() {
    // A tools/call to the yamp__backends meta-tool is served in-process by the
    // local handler registry (draft §5.3), never touching a backend. The dispatch
    // path must stay within budget.
    let registry = Registry::new(vec![Box::new(BackendsHandler::new(|| json!([{ "id": "b0", "available": true }])))]).unwrap();
    let request = json!({ "jsonrpc": "2.0", "id": "d", "method": "tools/call", "params": { "name": "yamp__backends", "arguments": {} } });
    assert_budget("dispatch-handler", measure(1, request, Opts { registry: Some(registry), ..Opts::default() }).await);
}

#[tokio::test]
async fn tasks_get_within_budget() {
    // A tasks/get reverse-resolves its namespaced taskId to the originating
    // backend and forwards with the backend's own id (SEP-2663). Task routing is
    // stateless, so the namespaced id is served directly. This path must stay
    // within budget.
    let request = json!({ "jsonrpc": "2.0", "id": "t", "method": "tasks/get", "params": { "taskId": "b0__task-1" } });
    assert_budget("tasks-get", measure(1, request, Opts::default()).await);
}

#[tokio::test]
async fn auth_injection_within_budget() {
    // Every request the backend forwards has the client's credential stripped and
    // the backend's own token injected into _meta (SEP §13.1, confused deputy).
    // The credential-injection path must stay within budget.
    let request = json!({ "jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": { "name": "b0_tool", "arguments": {} } });
    assert_budget("auth-injection", measure(1, request, Opts { token: Some("backend-token".to_string()), ..Opts::default() }).await);
}
