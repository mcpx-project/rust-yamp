//! Resilient router tests (Rust arm): breaker-driven partial lists, -32003 on
//! calls to unavailable backends, and timeout tripping the breaker.

use std::io;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::jsonrpc;
use yamp::resilience::{CircuitBreaker, PROXY_PARTIAL_KEY, SERVER_NOT_AVAILABLE};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type LR = LineReader<BufReader<DuplexStream>>;
type LW = LineWriter<DuplexStream>;

fn line(reader: DuplexStream, writer: DuplexStream) -> (LR, LW) {
    (LineReader::new(BufReader::new(reader)), LineWriter::new(writer))
}

async fn responder(mut r: LR, mut w: LW, tools: &[&str], answer_calls: bool) -> io::Result<()> {
    let init = jsonrpc::decode(&r.receive().await?.unwrap())?;
    w.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": init["id"], "result": { "capabilities": { "tools": {} }, "serverInfo": { "name": "b" } },
    })))
    .await?;
    r.receive().await?; // initialized
    loop {
        match r.receive().await? {
            None => {
                w.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let m = jsonrpc::decode(&raw)?;
                match jsonrpc::method_of(&m) {
                    Some("tools/list") => {
                        let listed: Vec<Value> = tools.iter().map(|n| json!({ "name": n })).collect();
                        w.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": { "tools": listed } }))).await?;
                    }
                    Some("tools/call") if answer_calls => {
                        w.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": { "content": [ { "type": "text", "text": "ok" } ] } }))).await?;
                    }
                    _ => {} // ignore (used to force a timeout)
                }
            }
        }
    }
}

async fn dead_on_handshake(mut r: LR, mut w: LW) -> io::Result<()> {
    r.receive().await?; // initialize
    w.send_eof().await // die without responding
}

async fn client_handshake(cw: &mut LW, cr: &mut LR) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": "c1", "method": "initialize",
        "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
    })))
    .await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await
}

#[tokio::test]
async fn resilient_partial_list_and_unavailable_call() {
    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (r_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, r_reads_gh) = duplex(CAP);
    let (r_to_sl, sl_reads) = duplex(CAP);
    let (sl_writes, r_reads_sl) = duplex(CAP);

    let (ghr, ghw) = line(r_reads_gh, r_to_gh);
    let github = Backend::resilient("github", ghr, ghw, CircuitBreaker::new(5, 100.0), None).unwrap();
    let (slr, slw) = line(r_reads_sl, r_to_sl);
    let slack = Backend::resilient("slack", slr, slw, CircuitBreaker::new(1, 100.0), None).unwrap();
    let (crr, crw) = line(r_reads_client, r_writes_client);
    let router = ForwardRouter::new(crr, crw, vec![github, slack]);

    let gh = { let (r, w) = line(gh_reads, gh_writes); responder(r, w, &["search"], true) };
    let sl = { let (r, w) = line(sl_reads, sl_writes); dead_on_handshake(r, w) };

    let client = async {
        let (mut cr, mut cw) = line(client_r, client_w);
        client_handshake(&mut cw, &mut cr).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/list", "params": {} }))).await?;
        let listing = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c3", "method": "tools/call", "params": { "name": "slack__post" } }))).await?;
        let blocked = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c4", "method": "tools/call", "params": { "name": "github__search" } }))).await?;
        let allowed = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((listing, blocked, allowed))
    };

    let (_r, _g, _s, out) = tokio::join!(router.serve(), gh, sl, client);
    let (listing, blocked, allowed) = out.unwrap();
    let names: Vec<&str> = listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["github__search"]);
    assert_eq!(listing["result"]["_meta"][PROXY_PARTIAL_KEY]["unavailable_backends"][0], "slack");
    assert_eq!(blocked["error"]["code"], SERVER_NOT_AVAILABLE);
    assert_eq!(allowed["result"]["content"][0]["text"], "ok");
}

#[tokio::test]
async fn timeout_trips_breaker() {
    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (r_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, r_reads_gh) = duplex(CAP);

    let (ghr, ghw) = line(r_reads_gh, r_to_gh);
    let github = Backend::resilient("github", ghr, ghw, CircuitBreaker::new(1, 100.0), Some(Duration::from_millis(50))).unwrap();
    let (crr, crw) = line(r_reads_client, r_writes_client);
    let router = ForwardRouter::new(crr, crw, vec![github]);

    // answer_calls=false: handshake works, tools/call is ignored and times out
    let gh = { let (r, w) = line(gh_reads, gh_writes); responder(r, w, &["search"], false) };

    let client = async {
        let (mut cr, mut cw) = line(client_r, client_w);
        client_handshake(&mut cw, &mut cr).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/call", "params": { "name": "github__search" } }))).await?;
        let first = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        let changed = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((first, changed))
    };

    let (_r, _g, out) = tokio::join!(router.serve(), gh, client);
    let (first, changed) = out.unwrap();
    assert_eq!(first["error"]["code"], SERVER_NOT_AVAILABLE);
    assert_eq!(changed["method"], "notifications/tools/list_changed");
}
