//! Bidirectional router test (Rust arm): backend-initiated messages reach the
//! sink while normal request/response demuxes by id.

use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};
use tokio::sync::mpsc;

use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

async fn backend_with_push<R, W>(mut r: R, mut w: W) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&r.receive().await?.unwrap())?;
    w.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": init["id"],
        "result": { "capabilities": { "tools": {} }, "serverInfo": { "name": "b" } },
    })))
    .await?;
    r.receive().await?; // notifications/initialized
    // A server-initiated notification, not a response to any request.
    w.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "method": "notifications/message", "params": { "level": "info", "data": "hi" },
    })))
    .await?;
    loop {
        match r.receive().await? {
            None => {
                w.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let m = jsonrpc::decode(&raw)?;
                if jsonrpc::method_of(&m) == Some("tools/call") {
                    w.send(&jsonrpc::encode(&json!({
                        "jsonrpc": "2.0", "id": m["id"], "result": { "content": [ { "type": "text", "text": "ok" } ] },
                    })))
                    .await?;
                }
            }
        }
    }
}

fn line_backend(
    id: &str,
    reader: DuplexStream,
    writer: DuplexStream,
) -> Backend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>> {
    Backend::new(id, LineReader::new(BufReader::new(reader)), LineWriter::new(writer)).unwrap()
}

#[tokio::test]
async fn backend_initiated_message_reaches_sink() {
    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (r_to_b, b_reads) = duplex(CAP);
    let (b_writes, r_reads_b) = duplex(CAP);

    let (sink_tx, mut sink_rx) = mpsc::channel(16);
    let router = ForwardRouter::with_server_sink(
        LineReader::new(BufReader::new(r_reads_client)),
        LineWriter::new(r_writes_client),
        vec![line_backend("b", r_reads_b, r_to_b)],
        Some(sink_tx),
    );
    let backend = backend_with_push(LineReader::new(BufReader::new(b_reads)), LineWriter::new(b_writes));

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        cr.receive().await?; // initialize response
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/call", "params": { "name": "b__x" } }))).await?;
        let called = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(called)
    };

    let (_router, _backend, called) = tokio::try_join!(router.serve(), backend, client).unwrap();
    assert_eq!(called["result"]["content"][0]["text"], "ok");

    let pushed = sink_rx.recv().await.unwrap();
    assert_eq!(pushed["method"], "notifications/message");
    assert_eq!(pushed["params"]["data"], "hi");
}
