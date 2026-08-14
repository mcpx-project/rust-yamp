//! Schema validation of server-originated calls (σ1). Mirrors the Python arm.
//!
//! With `set_validate_schemas(true)` the router validates a local handler's
//! `tools/call` arguments against the tool's `inputSchema` (a bad input is a
//! client-class `-32602`) and its result against `outputSchema` before it leaves
//! (a bad output is a server-class `-32603`). Both errors carry the normalized
//! `errorId`. Validation is off by default, so a bad input then reaches the
//! handler unchecked. The proxy role is untouched: only the local-handler branch
//! validates.

use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::errors::{INTERNAL_ERROR, INVALID_PARAMS};
use yamp::handler::{CallFuture, Handler, Registry};
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

/// A server tool with declared schemas. `add` requires an integer `n` and
/// promises an integer `doubled`; `bad_out` accepts anything but returns a
/// result that violates its own `outputSchema`.
struct SchemaHandler;

impl Handler for SchemaHandler {
    fn id(&self) -> &str {
        "srv"
    }

    fn list_tools(&self) -> Vec<Value> {
        vec![
            json!({
                "name": "add",
                "inputSchema": { "type": "object", "properties": { "n": { "type": "integer" } }, "required": ["n"] },
                "outputSchema": { "type": "object", "properties": { "doubled": { "type": "integer" } }, "required": ["doubled"] },
            }),
            json!({
                "name": "bad_out",
                "inputSchema": { "type": "object" },
                "outputSchema": { "type": "object", "properties": { "doubled": { "type": "integer" } }, "required": ["doubled"] },
            }),
        ]
    }

    fn call_tool<'a>(&'a self, name: &'a str, arguments: &'a Value) -> CallFuture<'a> {
        let name = name.to_string();
        let n = arguments.get("n").and_then(Value::as_i64).unwrap_or(0);
        Box::pin(async move {
            if name == "bad_out" {
                json!({ "content": [], "structuredContent": { "doubled": "not-an-int" } })
            } else {
                json!({ "content": [{ "type": "text", "text": "ok" }], "structuredContent": { "doubled": n * 2 } })
            }
        })
    }
}

async fn call(cw: &mut LineWriter<DuplexStream>, cr: &mut LineReader<BufReader<DuplexStream>>, id: &str, name: &str, args: Value) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": { "name": name, "arguments": args } }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

async fn drive(validate: bool) -> (Value, Value, Value) {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);

    let registry = Registry::new(vec![Box::new(SchemaHandler)]).unwrap();
    let backends: Vec<Backend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>>> = Vec::new();
    let router = ForwardRouter::new(LineReader::new(BufReader::new(router_reads_client)), LineWriter::new(router_writes_client), backends)
        .set_registry(registry)
        .set_validate_schemas(validate);

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "i", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let good = call(&mut cw, &mut cr, "g", "srv__add", json!({ "n": 3 })).await?;
        let bad_in = call(&mut cw, &mut cr, "b", "srv__add", json!({})).await?;
        let bad_out = call(&mut cw, &mut cr, "o", "srv__bad_out", json!({})).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value, Value), io::Error>((good, bad_in, bad_out))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    client_result.unwrap()
}

#[tokio::test]
async fn validates_input_and_output_when_enabled() {
    let (good, bad_in, bad_out) = drive(true).await;

    // Valid input, valid output: the call succeeds and the typed result crosses.
    assert_eq!(good["result"]["structuredContent"]["doubled"].as_i64(), Some(6));

    // Missing required `n`: rejected before the handler runs, client-class.
    assert_eq!(bad_in["error"]["code"].as_i64(), Some(INVALID_PARAMS));
    assert_eq!(bad_in["error"]["data"]["errorId"], "E4002");
    assert!(bad_in.get("result").is_none());

    // Handler produced output violating its own outputSchema: server-class.
    assert_eq!(bad_out["error"]["code"].as_i64(), Some(INTERNAL_ERROR));
    assert_eq!(bad_out["error"]["data"]["errorId"], "E5000");
}

#[tokio::test]
async fn off_by_default_passes_bad_input_through() {
    let (good, bad_in, _bad_out) = drive(false).await;

    // With validation off the handler runs unchecked: the good call still works,
    assert_eq!(good["result"]["structuredContent"]["doubled"].as_i64(), Some(6));
    // and a call the schema would reject reaches the handler and returns a result.
    assert!(bad_in.get("error").is_none());
    assert_eq!(bad_in["result"]["structuredContent"]["doubled"].as_i64(), Some(0));
}
