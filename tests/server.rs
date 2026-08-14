//! Server spine (σ0): pure-server mode is a registry with zero backends.
//!
//! The router originates `server/discover` and `tools/list` from the handler
//! surface (no backends), and attaches the server's SEP-2549 cache directives to
//! those list results.

use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::cache::{CACHE_SCOPE_KEY, TTL_MS_KEY};
use yamp::handler::{BackendsHandler, Registry};
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};
use yamp::{doctor, version};

const CAP: usize = 1 << 16;

async fn call(cw: &mut LineWriter<DuplexStream>, cr: &mut LineReader<BufReader<DuplexStream>>, id: &str, method: &str, params: Value) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

#[tokio::test]
async fn pure_server_mode_serves_from_handlers_with_cache_directives() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);

    let registry = Registry::new(vec![Box::new(BackendsHandler::new(|| json!([])))]).unwrap();
    let backends: Vec<Backend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>>> = Vec::new();
    let router = ForwardRouter::new(LineReader::new(BufReader::new(router_reads_client)), LineWriter::new(router_writes_client), backends)
        .set_registry(registry)
        .set_list_directives(300000, "public");

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "i", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        let init = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let discover = call(&mut cw, &mut cr, "d", "server/discover", json!({})).await?;
        let listing = call(&mut cw, &mut cr, "l", "tools/list", json!({})).await?;
        let called = call(&mut cw, &mut cr, "c", "tools/call", json!({ "name": "yamp__backends", "arguments": {} })).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value, Value, Value), io::Error>((init, discover, listing, called))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (init, discover, listing, called) = client_result.unwrap();

    // The handshake succeeds with zero backends.
    assert_eq!(init["result"]["serverInfo"]["name"], "yamp");

    // tools/list is composed from the handler surface alone, with cache directives.
    let names: Vec<&str> = listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["yamp__backends"]);
    assert_eq!(listing["result"][TTL_MS_KEY].as_u64(), Some(300000));
    assert_eq!(listing["result"][CACHE_SCOPE_KEY], "public");

    // server/discover answers from the same surface, also with directives.
    let discover_names: Vec<&str> = discover["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(discover_names, vec!["yamp__backends"]);
    assert_eq!(discover["result"][TTL_MS_KEY].as_u64(), Some(300000));

    // A local handler originates its response (server behavior, no backend).
    assert!(called["result"].get("content").is_some());
}

#[tokio::test]
async fn server_advertises_a_supported_protocol_version() {
    // σ6 per-revision conformance: the pure-server handshake advertises a protocol
    // version drawn from the single supported set, whatever the client requested,
    // and a doctor preflight on that surface and version is clean.
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let registry = Registry::new(vec![Box::new(BackendsHandler::new(|| json!([])))]).unwrap();
    let backends: Vec<Backend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>>> = Vec::new();
    let router = ForwardRouter::new(LineReader::new(BufReader::new(router_reads_client)), LineWriter::new(router_writes_client), backends).set_registry(registry);

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "i", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        let init = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let listing = call(&mut cw, &mut cr, "l", "tools/list", json!({})).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value), io::Error>((init, listing))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (init, listing) = client_result.unwrap();
    let advertised = init["result"]["protocolVersion"].as_str().unwrap();
    assert!(version::SUPPORTED_PROTOCOL_VERSIONS.contains(&advertised));
    let tools: Vec<Value> = listing["result"]["tools"].as_array().unwrap().clone();
    assert!(doctor::is_ok(&doctor::check_server(&tools, advertised)));
}
