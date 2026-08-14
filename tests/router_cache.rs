//! δ12 list-cache integration tests (Rust arm). Mirrors the Python arm.
//!
//! Exercises the cache through the real ForwardRouter fan-out: a fresh hit
//! skips the backend request, a backend list_changed invalidates, and an
//! opening breaker invalidates. The hit-collapse claim is measured by counting
//! backend requests, not asserted.

use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::cache::ListCache;
use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc;
use yamp::resilience::CircuitBreaker;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type SharedCache = Arc<StdMutex<ListCache>>;

#[derive(Default)]
struct MockOpts {
    emit_list_changed_after_first: bool,
    die_on_call: bool,
}

async fn mock_backend<R, W>(
    mut reader: R,
    mut writer: W,
    name: &'static str,
    tools: Vec<&'static str>,
    count: Arc<StdMutex<usize>>,
    opts: MockOpts,
) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": name } },
        })))
        .await?;
    reader.receive().await?; // notifications/initialized
    let mut first = true;
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let message = jsonrpc::decode(&raw)?;
                match jsonrpc::method_of(&message) {
                    Some("tools/list") => {
                        *count.lock().unwrap() += 1;
                        let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                        writer
                            .send(&jsonrpc::encode(&json!({
                                "jsonrpc": "2.0", "id": message["id"],
                                "result": { "tools": listed, "ttlMs": 60000, "cacheScope": "public" },
                            })))
                            .await?;
                        if opts.emit_list_changed_after_first && first {
                            first = false;
                            // Delay so the fetch is cached before the
                            // notification invalidates it (no put/invalidate race).
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            writer
                                .send(&jsonrpc::encode(&json!({
                                    "jsonrpc": "2.0", "method": "notifications/tools/list_changed",
                                })))
                                .await?;
                        }
                    }
                    Some("tools/call") if opts.die_on_call => {
                        writer.send_eof().await?;
                        return Ok(());
                    }
                    Some("tools/call") => {
                        writer
                            .send(&jsonrpc::encode(&json!({
                                "jsonrpc": "2.0", "id": message["id"],
                                "result": { "content": [{ "type": "text", "text": "ok" }] },
                            })))
                            .await?;
                    }
                    Some("ping") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": {} }))).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn handshake(cw: &mut LineWriter<DuplexStream>, cr: &mut LineReader<BufReader<DuplexStream>>) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": "c1", "method": "initialize",
        "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
    })))
    .await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
    Ok(())
}

async fn call(
    cw: &mut LineWriter<DuplexStream>,
    cr: &mut LineReader<BufReader<DuplexStream>>,
    method: &str,
    params: Value,
) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "r", "method": method, "params": params }))).await?;
    // Drain any server-initiated notification (e.g. a list_changed emitted on a
    // breaker transition, or one forwarded from a backend) until our response.
    loop {
        let message = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        if message.get("id").and_then(Value::as_str) == Some("r") {
            return Ok(message);
        }
    }
}

/// One client connection wired to backends built from `(name, tools, opts)`,
/// sharing `cache`. Returns the client duplex halves, per-backend counters, and
/// the joined server future (router + all backend mocks).
#[allow(clippy::type_complexity)]
fn build(
    cache: SharedCache,
    backends_spec: Vec<(&'static str, Vec<&'static str>, MockOpts)>,
    resilient: bool,
) -> (
    LineWriter<DuplexStream>,
    LineReader<BufReader<DuplexStream>>,
    Vec<(&'static str, Arc<StdMutex<usize>>)>,
    impl std::future::Future<Output = io::Result<()>>,
) {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);

    let mut backend_objs = Vec::new();
    let mut counters = Vec::new();
    let mut mocks: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>> = Vec::new();
    for (name, tools, opts) in backends_spec {
        let (router_to_b, b_reads) = duplex(CAP);
        let (b_writes, router_reads_b) = duplex(CAP);
        let backend = if resilient {
            Backend::resilient(name, LineReader::new(BufReader::new(router_reads_b)), LineWriter::new(router_to_b), CircuitBreaker::new(1, 100.0), None)
        } else {
            Backend::new(name, LineReader::new(BufReader::new(router_reads_b)), LineWriter::new(router_to_b))
        }
        .unwrap();
        backend_objs.push(backend);
        let count = Arc::new(StdMutex::new(0usize));
        counters.push((name, count.clone()));
        mocks.push(Box::pin(mock_backend(
            LineReader::new(BufReader::new(b_reads)),
            LineWriter::new(b_writes),
            name,
            tools,
            count,
            opts,
        )));
    }

    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backend_objs,
    )
    .set_cache(cache, None);

    let server = async move {
        let mocks_future = futures::future::try_join_all(mocks);
        tokio::try_join!(router.serve(), mocks_future)?;
        Ok(())
    };
    (LineWriter::new(client_w), LineReader::new(BufReader::new(client_r)), counters, server)
}

#[tokio::test]
async fn cache_hit_collapses_fetches() {
    let cache = Arc::new(StdMutex::new(ListCache::default()));
    let (mut cw, mut cr, counters, server) = build(
        cache,
        vec![("gh", vec!["a"], MockOpts::default()), ("gl", vec!["b"], MockOpts::default())],
        false,
    );

    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        let mut last = Value::Null;
        for _ in 0..5 {
            last = call(&mut cw, &mut cr, "tools/list", json!({})).await?;
        }
        cw.send_eof().await?;
        Ok::<Value, io::Error>(last)
    };

    let (_server, last) = tokio::try_join!(server, client).unwrap();
    // Five client list calls, but each backend was queried exactly once.
    for (name, count) in &counters {
        assert_eq!(*count.lock().unwrap(), 1, "backend {name} should be queried once");
    }
    let names: std::collections::BTreeSet<String> = last["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, ["gh__a", "gl__b"].iter().map(|s| s.to_string()).collect());
}

#[tokio::test]
async fn shared_cache_collapses_across_connections() {
    let cache = Arc::new(StdMutex::new(ListCache::default()));

    // First connection primes the shared cache.
    let (mut aw, mut ar, a_counters, a_server) =
        build(cache.clone(), vec![("gh", vec!["a"], MockOpts::default())], false);
    let a_client = async move {
        handshake(&mut aw, &mut ar).await?;
        call(&mut aw, &mut ar, "tools/list", json!({})).await?;
        aw.send_eof().await?;
        Ok::<(), io::Error>(())
    };
    tokio::try_join!(a_server, a_client).unwrap();

    // Second connection should serve entirely from the shared cache.
    let (mut bw, mut br, b_counters, b_server) =
        build(cache.clone(), vec![("gh", vec!["a"], MockOpts::default())], false);
    let b_client = async move {
        handshake(&mut bw, &mut br).await?;
        call(&mut bw, &mut br, "tools/list", json!({})).await?;
        bw.send_eof().await?;
        Ok::<(), io::Error>(())
    };
    tokio::try_join!(b_server, b_client).unwrap();

    assert_eq!(*a_counters[0].1.lock().unwrap(), 1); // first connection queried the backend
    assert_eq!(*b_counters[0].1.lock().unwrap(), 0); // second served from the shared cache
}

#[tokio::test]
async fn list_changed_invalidates_cache() {
    let cache = Arc::new(StdMutex::new(ListCache::default()));
    let (mut cw, mut cr, counters, server) = build(
        cache,
        vec![("gh", vec!["a"], MockOpts { emit_list_changed_after_first: true, die_on_call: false })],
        false,
    );

    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        call(&mut cw, &mut cr, "tools/list", json!({})).await?; // miss: fetch and cache
        // Let the backend's delayed list_changed arrive and invalidate.
        tokio::time::sleep(Duration::from_millis(60)).await;
        call(&mut cw, &mut cr, "tools/list", json!({})).await?; // miss again after invalidation
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(server, client).unwrap();
    // Without invalidation this would be 1; the list_changed forces a re-fetch.
    assert_eq!(*counters[0].1.lock().unwrap(), 2);
}

#[tokio::test]
async fn breaker_open_invalidates_cache() {
    let cache = Arc::new(StdMutex::new(ListCache::default()));
    let (mut cw, mut cr, _counters, server) = build(
        cache.clone(),
        vec![
            ("gh", vec!["a"], MockOpts { emit_list_changed_after_first: false, die_on_call: true }),
            ("gl", vec!["b"], MockOpts::default()),
        ],
        true,
    );

    let inspect = cache.clone();
    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        call(&mut cw, &mut cr, "tools/list", json!({})).await?; // prime both backends
        let primed = inspect.lock().unwrap().get("gh", "tools/list", None, 0.0).is_some();
        // A call to gh makes it die, opening its breaker; the router then runs
        // its surface check and must drop gh's cached list.
        call(&mut cw, &mut cr, "tools/call", json!({ "name": "gh__a", "arguments": {} })).await?;
        // A follow-up list is sequenced after the router's post-call surface
        // check, so invalidation has definitely run by the time it returns.
        call(&mut cw, &mut cr, "tools/list", json!({})).await?;
        let gh_after = inspect.lock().unwrap().get("gh", "tools/list", None, 0.0);
        let gl_after = inspect.lock().unwrap().get("gl", "tools/list", None, 0.0);
        cw.send_eof().await?;
        Ok::<(bool, Option<Value>, Option<Value>), io::Error>((primed, gh_after, gl_after))
    };

    let (_server, (primed, gh_after, gl_after)) = tokio::try_join!(server, client).unwrap();
    assert!(primed);
    assert!(gh_after.is_none()); // invalidated on breaker-open
    assert!(gl_after.is_some()); // the healthy backend keeps its cache
}
