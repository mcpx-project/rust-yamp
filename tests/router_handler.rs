//! δ17 dispatch seam integration (Rust arm). Mirrors the Python arm.
//!
//! Local handlers and routed backends share one namespaced surface: tools/list
//! merges both, a tools/call whose prefix names a handler is served locally
//! (including RestToMcp as a Conversion-mode handler), and an unknown method
//! still returns -32601.

use std::io;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::handler::{BackendsHandler, Registry};
use yamp::jsonrpc::{self, METHOD_NOT_FOUND};
use yamp::rest::{HttpClient, RestToMcp};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type Recorded = Arc<Mutex<Vec<(String, String)>>>;

struct FakeHttp {
    calls: Recorded,
}

impl HttpClient for FakeHttp {
    async fn call(&self, method: &str, url: &str, _body: Option<&[u8]>) -> io::Result<(u16, Vec<u8>)> {
        self.calls.lock().unwrap().push((method.to_string(), url.to_string()));
        Ok((200, b"{\"name\": \"ada\"}".to_vec()))
    }
}

async fn mock_backend<R, W>(mut reader: R, mut writer: W, name: &'static str, tools: Vec<&'static str>) -> io::Result<()>
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
                        let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": listed } }))).await?;
                    }
                    Some("tools/call") => {
                        let tool = message["params"]["name"].as_str().unwrap();
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "content": [{ "type": "text", "text": format!("{name}:{tool}") } ] } }))).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn call(cw: &mut LineWriter<DuplexStream>, cr: &mut LineReader<BufReader<DuplexStream>>, id: &str, method: &str, params: Value) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

#[tokio::test]
async fn dispatch_merges_and_serves_local_handlers() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);

    let spec = json!({
        "baseUrl": "https://api.example.com",
        "operations": [
            {"name": "get_user", "method": "GET", "path": "/users/{id}", "parameters": [{"name": "id", "in": "path"}]},
        ],
    });
    let http_calls: Recorded = Arc::new(Mutex::new(Vec::new()));
    let rest = RestToMcp::new(&spec, FakeHttp { calls: http_calls.clone() }); // Conversion-mode handler, id "rest"
    let backends_handler = BackendsHandler::new(|| json!([{ "id": "gh" }])); // meta-tool, id "yamp"
    let registry = Registry::new(vec![Box::new(rest), Box::new(backends_handler)]).unwrap();

    let backend = Backend::new("gh", LineReader::new(BufReader::new(router_reads_gh)), LineWriter::new(router_to_gh)).unwrap();
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        vec![backend],
    )
    .set_registry(registry);

    let gh = mock_backend(LineReader::new(BufReader::new(gh_reads)), LineWriter::new(gh_writes), "gh", vec!["search"]);

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        let init = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let listing = call(&mut cw, &mut cr, "l", "tools/list", json!({})).await?;
        let backends_tool = call(&mut cw, &mut cr, "b", "tools/call", json!({ "name": "yamp__backends", "arguments": {} })).await?;
        let rest_tool = call(&mut cw, &mut cr, "r", "tools/call", json!({ "name": "rest__get_user", "arguments": { "id": "42" } })).await?;
        let routed = call(&mut cw, &mut cr, "s", "tools/call", json!({ "name": "gh__search", "arguments": {} })).await?;
        let unknown = call(&mut cw, &mut cr, "u", "does/not-exist", json!({})).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value, Value, Value, Value, Value), io::Error>((init, listing, backends_tool, rest_tool, routed, unknown))
    };

    let (_r, _gh, (init, listing, backends_tool, rest_tool, routed, unknown)) = tokio::try_join!(router.serve(), gh, client).unwrap();

    // tools/list merges routed backend + both local handlers, all namespaced.
    let names: std::collections::BTreeSet<String> =
        listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect();
    let expected: std::collections::BTreeSet<String> =
        ["gh__search", "rest__get_user", "yamp__backends"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);
    assert!(init["result"]["capabilities"]["tools"].is_object());

    // A meta-tool call is served entirely inside yamp.
    let text = backends_tool["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(serde_json::from_str::<Value>(text).unwrap(), json!([{ "id": "gh" }]));

    // RestToMcp is served directly (Conversion mode): the call reached the fake
    // HTTP layer with the path parameter substituted, no backend process.
    assert_eq!(rest_tool["result"]["content"][0]["text"], "{\"name\": \"ada\"}");
    assert_eq!(*http_calls.lock().unwrap(), [("GET".to_string(), "https://api.example.com/users/42".to_string())]);

    // A real backend tool still routes normally.
    assert_eq!(routed["result"]["content"][0]["text"], "gh:search");

    // An unknown method is still rejected.
    assert_eq!(unknown["error"]["code"], METHOD_NOT_FOUND);
}
