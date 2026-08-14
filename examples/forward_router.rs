//! Runnable example: a forward router aggregating two backends.
//!
//! Wires two in-process backends behind a `ForwardRouter`, runs the MCP
//! handshake and a tools/list, and prints the aggregated, namespaced surface.
//!
//! Run:  cd rust && cargo run --example forward_router

use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

async fn backend<R, W>(mut reader: R, mut writer: W, name: &'static str, tools: Vec<&'static str>) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} },
                        "serverInfo": { "name": format!("{name}-server") } },
        })))
        .await?;
    reader.receive().await?; // notifications/initialized
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let msg = jsonrpc::decode(&raw)?;
                if jsonrpc::method_of(&msg) == Some("tools/list") {
                    let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                    writer
                        .send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": msg["id"], "result": { "tools": listed } })))
                        .await?;
                }
            }
        }
    }
}

fn line_backend(id: &str, reader: DuplexStream, writer: DuplexStream) -> Backend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>> {
    Backend::new(id, LineReader::new(BufReader::new(reader)), LineWriter::new(writer)).unwrap()
}

#[tokio::main]
async fn main() {
    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (r_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, r_reads_gh) = duplex(CAP);
    let (r_to_slack, slack_reads) = duplex(CAP);
    let (slack_writes, r_reads_slack) = duplex(CAP);

    let backends = vec![
        line_backend("github", r_reads_gh, r_to_gh),
        line_backend("slack", r_reads_slack, r_to_slack),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(r_reads_client)),
        LineWriter::new(r_writes_client),
        backends,
    );
    let gh = backend(LineReader::new(BufReader::new(gh_reads)), LineWriter::new(gh_writes), "github", vec!["create_issue", "search"]);
    let slack = backend(LineReader::new(BufReader::new(slack_reads)), LineWriter::new(slack_writes), "slack", vec!["post_message"]);

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "1", "method": "initialize",
            "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": { "name": "demo" } },
        })))
        .await?;
        let init = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        println!("client sees serverInfo: {}", init["result"]["serverInfo"]);

        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "2", "method": "tools/list" }))).await?;
        let listing = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        let names: Vec<&str> = listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
        println!("aggregated, namespaced tools: {}", serde_json::to_string_pretty(&names).unwrap());
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(router.serve(), gh, slack, client).unwrap();
}
