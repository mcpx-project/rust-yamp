//! σ2 worker pool for server-originated calls (Rust arm). Mirrors the Python arm.
//!
//! With `set_worker_pool(cap, idle_ms)` a tools/call that resolves to a local
//! handler runs as a bounded, cancellable spawned task: the cap serializes beyond
//! it, `notifications/cancelled` stops a running call (and sends no response), the
//! idle deadline kills a stalled one, and shutdown drains the in-flight set. Off
//! by default. The `Semaphore` gates make concurrency deterministic (the only
//! timing is the idle deadline, a must-fire timeout).

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};
use tokio::sync::Semaphore;

use yamp::errors::INTERNAL_ERROR;
use yamp::handler::{CallFuture, Handler, Registry};
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

/// `block` signals its per-key entry semaphore then waits on its per-key gate the
/// test releases; `fast` returns at once.
struct PoolHandler {
    enter: HashMap<String, Arc<Semaphore>>,
    gate: HashMap<String, Arc<Semaphore>>,
}

impl Handler for PoolHandler {
    fn id(&self) -> &str {
        "srv"
    }

    fn list_tools(&self) -> Vec<Value> {
        vec![
            json!({ "name": "block", "inputSchema": { "type": "object" } }),
            json!({ "name": "fast", "inputSchema": { "type": "object" } }),
        ]
    }

    fn call_tool<'a>(&'a self, name: &'a str, arguments: &'a Value) -> CallFuture<'a> {
        if name == "fast" {
            return Box::pin(async { json!({ "content": [{ "type": "text", "text": "fast" }] }) });
        }
        let key = arguments.get("k").and_then(Value::as_str).unwrap_or("").to_string();
        let enter = self.enter.get(&key).cloned();
        let gate = self.gate.get(&key).cloned();
        Box::pin(async move {
            if let Some(enter) = enter {
                enter.add_permits(1);
            }
            if let Some(gate) = gate {
                let _ = gate.acquire().await;
            }
            json!({ "content": [{ "type": "text", "text": key }] })
        })
    }
}

type CR = LineReader<BufReader<DuplexStream>>;
type CW = LineWriter<DuplexStream>;

fn router_with(handler: PoolHandler, cap: u64, idle_ms: u64, reads: DuplexStream, writes: DuplexStream) -> ForwardRouter<CR, CW, CR, CW> {
    let registry = Registry::new(vec![Box::new(handler)]).unwrap();
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    ForwardRouter::new(LineReader::new(BufReader::new(reads)), LineWriter::new(writes), backends)
        .set_registry(registry)
        .set_worker_pool(cap, idle_ms)
}

async fn handshake(cw: &mut CW, cr: &mut CR) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "i", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
    Ok(())
}

async fn send_call(cw: &mut CW, id: &str, name: &str, arguments: Value, meta: Option<Value>) -> io::Result<()> {
    let mut params = json!({ "name": name, "arguments": arguments });
    if let Some(meta) = meta {
        params.as_object_mut().unwrap().insert("_meta".to_string(), meta);
    }
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": params }))).await
}

async fn send_notification(cw: &mut CW, method: &str, params: Value) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": method, "params": params }))).await
}

fn one(pairs: &[(&str, &Arc<Semaphore>)]) -> HashMap<String, Arc<Semaphore>> {
    pairs.iter().map(|(k, s)| (k.to_string(), (*s).clone())).collect()
}

#[tokio::test]
async fn cap_serializes_beyond_the_limit() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter_a, gate_a, enter_b, gate_b) =
        (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)));
    let handler = PoolHandler {
        enter: one(&[("a", &enter_a), ("b", &enter_b)]),
        gate: one(&[("a", &gate_a), ("b", &gate_b)]),
    };
    let router = router_with(handler, 1, 0, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        send_call(&mut cw, "A", "srv__block", json!({ "k": "a" }), None).await?;
        enter_a.acquire().await.unwrap().forget(); // A holds the only slot
        send_call(&mut cw, "B", "srv__block", json!({ "k": "b" }), None).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(enter_b.try_acquire().is_err()); // B cannot enter while A holds the slot
        gate_a.add_permits(1); // free the slot; B proceeds
        enter_b.acquire().await.unwrap().forget();
        gate_b.add_permits(1);
        let r1 = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        let r2 = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<(Value, Value), io::Error>((r1, r2))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (r1, r2) = client_result.unwrap();
    assert_eq!(r1["id"], "A");
    assert_eq!(r1["result"]["content"][0]["text"], "a");
    assert_eq!(r2["id"], "B");
    assert_eq!(r2["result"]["content"][0]["text"], "b");
}

#[tokio::test]
async fn cancellation_stops_a_running_call_with_no_response() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter_a, gate_a) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)));
    let handler = PoolHandler { enter: one(&[("a", &enter_a)]), gate: one(&[("a", &gate_a)]) };
    let router = router_with(handler, 0, 0, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        send_call(&mut cw, "A", "srv__block", json!({ "k": "a" }), None).await?;
        enter_a.acquire().await.unwrap().forget();
        send_notification(&mut cw, "notifications/cancelled", json!({ "requestId": "A" })).await?;
        send_call(&mut cw, "B", "srv__fast", json!({}), None).await?;
        send_call(&mut cw, "C", "srv__fast", json!({}), None).await?;
        // A was cancelled and sends nothing; only B and C respond.
        let r1 = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        let r2 = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<(Value, Value), io::Error>((r1, r2))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (r1, r2) = client_result.unwrap();
    let ids: std::collections::BTreeSet<&str> = [r1["id"].as_str().unwrap(), r2["id"].as_str().unwrap()].into_iter().collect();
    assert_eq!(ids, ["B", "C"].into_iter().collect()); // never "A"
}

#[tokio::test]
async fn idle_deadline_kills_a_stalled_call() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter_a, gate_a) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0))); // gate never released
    let handler = PoolHandler { enter: one(&[("a", &enter_a)]), gate: one(&[("a", &gate_a)]) };
    let router = router_with(handler, 0, 50, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        send_call(&mut cw, "A", "srv__block", json!({ "k": "a" }), None).await?;
        let r = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(r)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let r = client_result.unwrap();
    assert_eq!(r["id"], "A");
    assert_eq!(r["error"]["code"].as_i64(), Some(INTERNAL_ERROR));
    assert_eq!(r["error"]["data"]["errorId"], "E5000");
}

#[tokio::test]
async fn progress_notifications_touch_the_inflight_call() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter_a, gate_a) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)));
    let handler = PoolHandler { enter: one(&[("a", &enter_a)]), gate: one(&[("a", &gate_a)]) };
    let router = router_with(handler, 0, 0, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        send_call(&mut cw, "A", "srv__block", json!({ "k": "a" }), Some(json!({ "progressToken": "tok" }))).await?;
        enter_a.acquire().await.unwrap().forget();
        // A tracked token resets the deadline; an unknown token and a token-less
        // progress are no-ops. All three exercise the receive-side touch path.
        send_notification(&mut cw, "notifications/progress", json!({ "progressToken": "tok", "progress": 1 })).await?;
        send_notification(&mut cw, "notifications/progress", json!({ "progressToken": "nope" })).await?;
        send_notification(&mut cw, "notifications/progress", json!({})).await?;
        gate_a.add_permits(1);
        let r = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(r)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let r = client_result.unwrap();
    assert_eq!(r["id"], "A");
    assert_eq!(r["result"]["content"][0]["text"], "a");
}

#[tokio::test]
async fn shutdown_drains_in_flight_calls() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter_a, gate_a) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0))); // never released
    let handler = PoolHandler { enter: one(&[("a", &enter_a)]), gate: one(&[("a", &gate_a)]) };
    let router = router_with(handler, 0, 0, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        send_call(&mut cw, "A", "srv__block", json!({ "k": "a" }), None).await?;
        enter_a.acquire().await.unwrap().forget();
        // Close with a call still running: the drain aborts it, no response.
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    // Must not hang: the drain aborts the in-flight task at shutdown.
    let run = async {
        let (router_result, client_result) = tokio::join!(router.serve(), client);
        router_result.unwrap();
        client_result.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(5), run).await.expect("serve drained and returned");
}
