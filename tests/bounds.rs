//! σ5 output-size bounds and graceful drain (Rust arm). Mirrors the Python arm.
//!
//! `set_output_limit(max_bytes)` caps a server-originated result: a local
//! handler's result whose encoded form exceeds the cap is rejected with a
//! server-class error instead of emitted. `set_drain_timeout(ms)` gives in-flight
//! server-originated work a bounded window to finish (and send its response) on
//! shutdown before it is cancelled; the default of 0 cancels immediately.

use std::io;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};
use tokio::sync::Semaphore;

use yamp::errors::INTERNAL_ERROR;
use yamp::handler::{CallFuture, Handler, Registry};
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::server;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite, MAX_FRAME_BYTES};

const CAP: usize = 1 << 16;

type CR = LineReader<BufReader<DuplexStream>>;
type CW = LineWriter<DuplexStream>;

#[test]
fn output_cap_helpers() {
    assert_eq!(server::MAX_OUTPUT_BYTES, MAX_FRAME_BYTES);
    let small = json!({ "content": [{ "type": "text", "text": "ok" }] });
    assert!(!server::exceeds_output_cap(&small, 1000));
    assert!(server::exceeds_output_cap(&small, 10));
    assert!(!server::exceeds_output_cap(&small, 0)); // unbounded
}

struct BoundsHandler;

impl Handler for BoundsHandler {
    fn id(&self) -> &str {
        "srv"
    }

    fn list_tools(&self) -> Vec<Value> {
        vec![
            json!({ "name": "big", "inputSchema": { "type": "object" } }),
            json!({ "name": "small", "inputSchema": { "type": "object" } }),
        ]
    }

    fn call_tool<'a>(&'a self, name: &'a str, _arguments: &'a Value) -> CallFuture<'a> {
        let text = if name == "big" { "x".repeat(500) } else { "ok".to_string() };
        Box::pin(async move { json!({ "content": [{ "type": "text", "text": text }] }) })
    }
}

async fn handshake(cw: &mut CW, cr: &mut CR) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "i", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
    Ok(())
}

async fn call(cw: &mut CW, cr: &mut CR, id: &str, name: &str) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": { "name": name, "arguments": {} } }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

fn bounds_router(limit: usize, reads: DuplexStream, writes: DuplexStream) -> ForwardRouter<CR, CW, CR, CW> {
    let registry = Registry::new(vec![Box::new(BoundsHandler)]).unwrap();
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    ForwardRouter::new(LineReader::new(BufReader::new(reads)), LineWriter::new(writes), backends)
        .set_registry(registry)
        .set_output_limit(limit)
}

#[tokio::test]
async fn oversize_local_result_is_a_server_error() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let router = bounds_router(100, r_reads, r_writes); // small cap

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let big = call(&mut cw, &mut cr, "A", "srv__big").await?;
        let small = call(&mut cw, &mut cr, "B", "srv__small").await?;
        cw.send_eof().await?;
        Ok::<(Value, Value), io::Error>((big, small))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (big, small) = client_result.unwrap();
    // The 500-byte result exceeds the 100-byte cap: a server-class error.
    assert!(big.get("result").is_none());
    assert_eq!(big["error"]["code"], INTERNAL_ERROR);
    assert_eq!(big["error"]["data"]["errorId"], "E5000");
    // The small result is under the cap and served normally.
    assert_eq!(small["result"]["content"][0]["text"], "ok");
}

#[tokio::test]
async fn default_limit_does_not_trip() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let registry = Registry::new(vec![Box::new(BoundsHandler)]).unwrap();
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    let router = ForwardRouter::new(LineReader::new(BufReader::new(r_reads)), LineWriter::new(r_writes), backends).set_registry(registry); // default cap

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let big = call(&mut cw, &mut cr, "A", "srv__big").await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(big)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let big = client_result.unwrap();
    assert_eq!(big["result"]["content"][0]["text"], "x".repeat(500)); // served, well under 64 MiB
}

struct GateHandler {
    enter: Arc<Semaphore>,
    gate: Arc<Semaphore>,
}

impl Handler for GateHandler {
    fn id(&self) -> &str {
        "srv"
    }

    fn list_tools(&self) -> Vec<Value> {
        vec![json!({ "name": "block", "inputSchema": { "type": "object" } })]
    }

    fn call_tool<'a>(&'a self, _name: &'a str, _arguments: &'a Value) -> CallFuture<'a> {
        let enter = self.enter.clone();
        let gate = self.gate.clone();
        Box::pin(async move {
            enter.add_permits(1);
            let _ = gate.acquire().await;
            json!({ "content": [{ "type": "text", "text": "done" }] })
        })
    }
}

#[tokio::test]
async fn graceful_drain_lets_in_flight_call_finish() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter, gate) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)));
    let handler = GateHandler { enter: enter.clone(), gate: gate.clone() };
    let registry = Registry::new(vec![Box::new(handler)]).unwrap();
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    let router = ForwardRouter::new(LineReader::new(BufReader::new(r_reads)), LineWriter::new(r_writes), backends)
        .set_registry(registry)
        .set_worker_pool(0, 0)
        .set_drain_timeout(5000); // generous window

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "A", "method": "tools/call", "params": { "name": "srv__block", "arguments": {} } }))).await?;
        enter.acquire().await.unwrap().forget(); // the call is in flight
        cw.send_eof().await?; // shutdown begins; drain waits for the call
        gate.add_permits(1); // let it finish inside the drain window
        let r = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        Ok::<Value, io::Error>(r)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let r = client_result.unwrap();
    assert_eq!(r["id"], "A");
    assert_eq!(r["result"]["content"][0]["text"], "done"); // completed, not cancelled
}

#[tokio::test]
async fn zero_drain_cancels_in_flight_call() {
    // The default (0) drains by aborting at once: the call never responds, and
    // serve still returns promptly (no hang).
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter, gate) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0))); // gate never released
    let handler = GateHandler { enter: enter.clone(), gate };
    let registry = Registry::new(vec![Box::new(handler)]).unwrap();
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    let router = ForwardRouter::new(LineReader::new(BufReader::new(r_reads)), LineWriter::new(r_writes), backends)
        .set_registry(registry)
        .set_worker_pool(0, 0);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "A", "method": "tools/call", "params": { "name": "srv__block", "arguments": {} } }))).await?;
        enter.acquire().await.unwrap().forget();
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap(); // must not hang
    client_result.unwrap();
}
