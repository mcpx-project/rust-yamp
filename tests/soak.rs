//! Soak / leak coverage for the per-backend demux reader under churn.
//!
//! The router spawns one reader task per backend that demultiplexes backend
//! replies into per-request oneshot senders held in a pending map. A bug in the
//! teardown path (`Backend::close` aborts the reader; the reader clears its
//! pending map on EOF) would leak a reader task on every session, growing without
//! bound under churn. Nothing else in the suite runs enough sessions to surface
//! that.
//!
//! This drives many full router sessions back to back on one current-thread
//! runtime. The only tasks the router spawns are the per-backend demux readers
//! (the mocks and the client run as joined futures, not spawned tasks), so the
//! runtime's alive-task count returning to its pre-churn baseline is a direct
//! signal that every reader was reaped. A per-session leak would grow the count
//! by O(sessions). Mirrors the Python arm's test_soak.py.

use std::io;

use serde_json::json;
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc::{self, method_of};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;
const CYCLES: usize = 100;
const REQUESTS_PER_CYCLE: usize = 5;
const BACKENDS: usize = 2;

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
                    _ => {}
                }
            }
        }
    }
}

async fn one_session() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let mut backends = Vec::new();
    let mut mocks = Vec::new();
    for i in 0..BACKENDS {
        let (router_to_b, b_reads) = duplex(CAP);
        let (b_writes, router_reads_b) = duplex(CAP);
        let id = format!("b{i}");
        backends.push(
            Backend::new(id.clone(), LineReader::new(BufReader::new(router_reads_b)), LineWriter::new(router_to_b)).unwrap(),
        );
        mocks.push(mock(LineReader::new(BufReader::new(b_reads)), LineWriter::new(b_writes), id));
    }
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );

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
        for i in 0..REQUESTS_PER_CYCLE {
            cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": format!("l{i}"), "method": "tools/list", "params": {} }))).await?;
            cr.receive().await?;
            cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": format!("k{i}"), "method": "tools/call", "params": { "name": "b0__b0_tool", "arguments": {} } }))).await?;
            cr.receive().await?;
        }
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    let mocks_fut = futures::future::join_all(mocks);
    let (_router, _mocks, client) = tokio::join!(router.serve(), mocks_fut, client);
    client.unwrap();
}

async fn settle() {
    // Let aborted reader tasks be reaped before sampling the alive-task count.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn demux_reader_no_leak_under_churn() {
    let metrics = tokio::runtime::Handle::current().metrics();
    settle().await;
    let baseline = metrics.num_alive_tasks();
    for _ in 0..CYCLES {
        one_session().await;
    }
    settle().await;
    let grown = metrics.num_alive_tasks().saturating_sub(baseline);
    // Every per-backend reader must be reaped when its session ends; a leak would
    // grow this by O(CYCLES). Allow a small slack for runtime bookkeeping.
    assert!(grown <= 1, "leaked {grown} tasks over {CYCLES} churn cycles");
}
