//! REST-to-MCP conversion test (Rust arm): an MCP client reaches a REST API
//! through the proxy; tools/call becomes an HTTP request.

use std::io;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::jsonrpc;
use yamp::rest::{HttpClient, RestToMcp};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type Recorded = Arc<Mutex<Vec<(String, String, Option<Vec<u8>>)>>>;

struct FakeHttp {
    calls: Recorded,
}

impl HttpClient for FakeHttp {
    async fn call(&self, method: &str, url: &str, body: Option<&[u8]>) -> io::Result<(u16, Vec<u8>)> {
        self.calls.lock().unwrap().push((method.to_string(), url.to_string(), body.map(<[u8]>::to_vec)));
        Ok((200, b"{\"ok\": true}".to_vec()))
    }
}

fn line(reader: DuplexStream, writer: DuplexStream) -> (LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>) {
    (LineReader::new(BufReader::new(reader)), LineWriter::new(writer))
}

#[tokio::test]
async fn rest_backend_translates_calls() {
    let spec = json!({
        "baseUrl": "https://api.example.com",
        "operations": [
            {"name": "get_user", "method": "GET", "path": "/users/{id}",
             "parameters": [{"name": "id", "in": "path"}, {"name": "verbose", "in": "query"}]},
            {"name": "create_issue", "method": "POST", "path": "/issues", "body": ["title"]},
        ],
    });
    let calls: Recorded = Arc::new(Mutex::new(Vec::new()));
    let rest = RestToMcp::new(&spec, FakeHttp { calls: calls.clone() });

    let (client_w, r_reads_client) = duplex(CAP);
    let (r_writes_client, client_r) = duplex(CAP);
    let (r_to_rest, rest_reads) = duplex(CAP);
    let (rest_writes, r_reads_rest) = duplex(CAP);

    let (br, bw) = line(r_reads_rest, r_to_rest);
    let backend = Backend::new("api", br, bw).unwrap();
    let (crr, crw) = line(r_reads_client, r_writes_client);
    let router = ForwardRouter::new(crr, crw, vec![backend]);

    let (rr, rw) = line(rest_reads, rest_writes);
    let rest_task = rest.serve(rr, rw);

    let client = async {
        let (mut cr, mut cw) = line(client_r, client_w);
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "1", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;

        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "2", "method": "tools/list", "params": {} }))).await?;
        let listing = jsonrpc::decode(&cr.receive().await?.unwrap())?;

        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "3", "method": "tools/call", "params": { "name": "get_user", "arguments": { "id": 5, "verbose": "true" } } }))).await?;
        let got = jsonrpc::decode(&cr.receive().await?.unwrap())?;

        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "4", "method": "tools/call", "params": { "name": "create_issue", "arguments": { "title": "hi" } } }))).await?;
        cr.receive().await?;

        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "5", "method": "tools/call", "params": { "name": "missing", "arguments": {} } }))).await?;
        let unknown = jsonrpc::decode(&cr.receive().await?.unwrap())?;

        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "6", "method": "tools/call", "params": { "name": "get_user", "arguments": { "id": 7 } } }))).await?;
        cr.receive().await?;

        cw.send_eof().await?;
        Ok::<_, io::Error>((listing, got, unknown))
    };

    let (_router, _rest, out) = tokio::join!(router.serve(), rest_task, client);
    let (listing, got, unknown) = out.unwrap();

    let names: Vec<&str> = listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["get_user", "create_issue"]);
    assert_eq!(got["result"]["content"][0]["text"], "{\"ok\": true}");
    assert_eq!(unknown["result"]["isError"], Value::Bool(true));

    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded[0], ("GET".into(), "https://api.example.com/users/5?verbose=true".into(), None));
    assert_eq!(recorded[1], ("POST".into(), "https://api.example.com/issues".into(), Some(b"{\"title\":\"hi\"}".to_vec())));
    assert_eq!(recorded[2], ("GET".into(), "https://api.example.com/users/7".into(), None));
}
