//! Prompts and resources are namespaced and routed like tools; a single backend
//! passes names through unmodified (SEP §5.3).

use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;
type LR = LineReader<BufReader<DuplexStream>>;
type LW = LineWriter<DuplexStream>;

fn line(reader: DuplexStream, writer: DuplexStream) -> (LR, LW) {
    (LineReader::new(BufReader::new(reader)), LineWriter::new(writer))
}

async fn backend(mut r: LR, mut w: LW, name: &str) -> io::Result<()> {
    let init = jsonrpc::decode(&r.receive().await?.unwrap())?;
    w.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": init["id"],
        "result": { "capabilities": { "tools": {}, "prompts": {}, "resources": {} }, "serverInfo": { "name": name } },
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
                let params = &m["params"];
                let body = match jsonrpc::method_of(&m) {
                    Some("tools/list") => json!({ "tools": [{ "name": "search" }] }),
                    Some("prompts/list") => json!({ "prompts": [{ "name": "greet" }] }),
                    Some("resources/list") => json!({ "resources": [{ "uri": format!("file:///{name}.md") }] }),
                    Some("tools/call") | Some("prompts/get") => json!({ "echo_name": params["name"] }),
                    Some("resources/read") => json!({ "echo_uri": params["uri"] }),
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

#[tokio::test]
async fn prompts_and_resources_namespaced_and_routed() {
    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (r_to_a, a_reads) = duplex(CAP);
    let (a_writes, r_reads_a) = duplex(CAP);
    let (r_to_b, b_reads) = duplex(CAP);
    let (b_writes, r_reads_b) = duplex(CAP);

    let (ar, aw) = line(r_reads_a, r_to_a);
    let (br, bw) = line(r_reads_b, r_to_b);
    let (crr, crw) = line(r_reads_client, r_writes_client);
    let router = ForwardRouter::new(crr, crw, vec![Backend::new("a", ar, aw).unwrap(), Backend::new("b", br, bw).unwrap()]);

    let a = { let (r, w) = line(a_reads, a_writes); backend(r, w, "a") };
    let b = { let (r, w) = line(b_reads, b_writes); backend(r, w, "b") };

    let client = async {
        let (mut cr, mut cw) = line(client_r, client_w);
        handshake(&mut cw, &mut cr).await?;
        let prompts = call(&mut cw, &mut cr, "2", "prompts/list", json!({})).await?;
        let resources = call(&mut cw, &mut cr, "3", "resources/list", json!({})).await?;
        let got = call(&mut cw, &mut cr, "4", "prompts/get", json!({ "name": "a__greet" })).await?;
        let read = call(&mut cw, &mut cr, "5", "resources/read", json!({ "uri": "file:///b/b.md" })).await?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((prompts, resources, got, read))
    };

    let (_r, _a, _b, out) = tokio::join!(router.serve(), a, b, client);
    let (prompts, resources, got, read) = out.unwrap();

    let mut names: Vec<&str> = prompts["result"]["prompts"].as_array().unwrap().iter().map(|p| p["name"].as_str().unwrap()).collect();
    names.sort();
    assert_eq!(names, ["a__greet", "b__greet"]);
    let mut uris: Vec<&str> = resources["result"]["resources"].as_array().unwrap().iter().map(|r| r["uri"].as_str().unwrap()).collect();
    uris.sort();
    assert_eq!(uris, ["file:///a/a.md", "file:///b/b.md"]);
    assert_eq!(got["result"]["echo_name"], "greet");
    assert_eq!(read["result"]["echo_uri"], "file:///b.md");
}

#[tokio::test]
async fn single_backend_passes_names_through() {
    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (r_to_s, s_reads) = duplex(CAP);
    let (s_writes, r_reads_s) = duplex(CAP);

    let (sr, sw) = line(r_reads_s, r_to_s);
    let (crr, crw) = line(r_reads_client, r_writes_client);
    let router = ForwardRouter::new(crr, crw, vec![Backend::new("solo", sr, sw).unwrap()]);
    let solo = { let (r, w) = line(s_reads, s_writes); backend(r, w, "solo") };

    let client = async {
        let (mut cr, mut cw) = line(client_r, client_w);
        handshake(&mut cw, &mut cr).await?;
        let listing = call(&mut cw, &mut cr, "2", "tools/list", json!({})).await?;
        let called = call(&mut cw, &mut cr, "3", "tools/call", json!({ "name": "search" })).await?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((listing, called))
    };

    let (_r, _s, out) = tokio::join!(router.serve(), solo, client);
    let (listing, called) = out.unwrap();
    let names: Vec<&str> = listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["search"]); // no prefix
    assert_eq!(called["result"]["echo_name"], "search"); // forwarded unchanged
}
