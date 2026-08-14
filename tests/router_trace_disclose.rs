//! G6 hop tracing on the forward path and G4 progressive disclosure (Rust arm).

use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::capability::PROXY_SEARCH_TOOL;
use yamp::jsonrpc;
use yamp::observability::{PROXY_HOPS_KEY, TRACEPARENT};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;
type LR = LineReader<BufReader<DuplexStream>>;
type LW = LineWriter<DuplexStream>;

fn line(reader: DuplexStream, writer: DuplexStream) -> (LR, LW) {
    (LineReader::new(BufReader::new(reader)), LineWriter::new(writer))
}

async fn backend(mut r: LR, mut w: LW, tool_count: usize) -> io::Result<()> {
    let init = jsonrpc::decode(&r.receive().await?.unwrap())?;
    w.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": init["id"], "result": { "capabilities": { "tools": {} }, "serverInfo": { "name": "b" } },
    })))
    .await?;
    r.receive().await?;
    loop {
        match r.receive().await? {
            None => {
                w.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let m = jsonrpc::decode(&raw)?;
                let body = match jsonrpc::method_of(&m) {
                    Some("tools/list") => {
                        let tools: Vec<Value> = (0..tool_count).map(|i| json!({ "name": format!("tool{i}"), "description": format!("desc{i}") })).collect();
                        json!({ "tools": tools })
                    }
                    Some("tools/call") => json!({ "received_meta": m["params"].get("_meta").cloned().unwrap_or(json!({})) }),
                    _ => json!({}),
                };
                w.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": body }))).await?;
            }
        }
    }
}

async fn handshake(cw: &mut LW, cr: &mut LR) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c1", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await
}

async fn call(cw: &mut LW, cr: &mut LR, id: &str, method: &str, params: Value) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

fn wire() -> (LR, LW, LR, LW) {
    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (crr, crw) = line(r_reads_client, r_writes_client);
    let (cr, cw) = line(client_r, client_w);
    (crr, crw, cr, cw)
}

#[tokio::test]
async fn forward_path_adds_hop_and_trace() {
    let (crr, crw, mut cr, mut cw) = wire();
    let (r_to_b, b_reads) = duplex(CAP);
    let (b_writes, r_reads_b) = duplex(CAP);
    let (bckr, bckw) = line(r_reads_b, r_to_b);
    let router = ForwardRouter::new(crr, crw, vec![Backend::new("b", bckr, bckw).unwrap()]);
    let b = { let (r, w) = line(b_reads, b_writes); backend(r, w, 1) };

    let client = async {
        handshake(&mut cw, &mut cr).await?;
        let listing = call(&mut cw, &mut cr, "2", "tools/list", json!({})).await?;
        let called = call(&mut cw, &mut cr, "3", "tools/call", json!({ "name": "tool0" })).await?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((listing, called))
    };
    let (_r, _b, out) = tokio::join!(router.serve(), b, client);
    let (listing, called) = out.unwrap();
    assert_eq!(listing["result"]["_meta"][PROXY_HOPS_KEY][0]["mode"], "forward");
    assert_eq!(called["result"]["_meta"][PROXY_HOPS_KEY][0]["mode"], "forward");
    let forwarded = &called["result"]["received_meta"];
    assert_eq!(forwarded[PROXY_HOPS_KEY][0]["mode"], "forward");
    assert!(forwarded.get(TRACEPARENT).is_some());
}

#[tokio::test]
async fn trace_can_be_disabled() {
    let (crr, crw, mut cr, mut cw) = wire();
    let (r_to_b, b_reads) = duplex(CAP);
    let (b_writes, r_reads_b) = duplex(CAP);
    let (bckr, bckw) = line(r_reads_b, r_to_b);
    let router = ForwardRouter::new(crr, crw, vec![Backend::new("b", bckr, bckw).unwrap()]).set_trace(false);
    let b = { let (r, w) = line(b_reads, b_writes); backend(r, w, 1) };

    let client = async {
        handshake(&mut cw, &mut cr).await?;
        let called = call(&mut cw, &mut cr, "2", "tools/call", json!({ "name": "tool0" })).await?;
        cw.send_eof().await?;
        Ok::<_, io::Error>(called)
    };
    let (_r, _b, out) = tokio::join!(router.serve(), b, client);
    let called = out.unwrap();
    assert!(called["result"].get("_meta").is_none());
    assert_eq!(called["result"]["received_meta"], json!({}));
}

#[tokio::test]
async fn progressive_disclosure_and_search() {
    let (crr, crw, mut cr, mut cw) = wire();
    let (r_to_b, b_reads) = duplex(CAP);
    let (b_writes, r_reads_b) = duplex(CAP);
    let (bckr, bckw) = line(r_reads_b, r_to_b);
    let router = ForwardRouter::new(crr, crw, vec![Backend::new("b", bckr, bckw).unwrap()]).set_disclose(3);
    let b = { let (r, w) = line(b_reads, b_writes); backend(r, w, 5) };

    let client = async {
        handshake(&mut cw, &mut cr).await?;
        let listing = call(&mut cw, &mut cr, "2", "tools/list", json!({})).await?;
        let searched = call(&mut cw, &mut cr, "3", "tools/call", json!({ "name": PROXY_SEARCH_TOOL, "arguments": { "query": "tool4" } })).await?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((listing, searched))
    };
    let (_r, _b, out) = tokio::join!(router.serve(), b, client);
    let (listing, searched) = out.unwrap();
    let names: Vec<&str> = listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["tool0", "tool1", "tool2", PROXY_SEARCH_TOOL]);
    let text = searched["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(text, "[\"tool4\"]");
}
