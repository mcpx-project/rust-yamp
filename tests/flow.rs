//! Golden-flow corpus (M-T1): the Rust arm replays every recorded flow.
//!
//! Reads `conformance/flow-corpus.json` (generated from the Python arm by
//! python/tools/gen_flow_corpus.py) and drives each scenario through the Rust
//! `ForwardRouter` against scripted in-process backends, asserting it produces
//! the identical client-facing message sequence. The two data planes are thus
//! pinned to behave identically across whole exchanges, not just per pure
//! function.

use std::io;
use std::path::Path;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader};

use yamp::filters::{Filter, FilterChain, FilterError};
use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

// Reference filters (ε5), built from the same declarative spec as the Python arm.
struct DenyTool {
    tool: String,
    reason: String,
}
impl Filter for DenyTool {
    fn name(&self) -> &str {
        "deny_tool"
    }
    fn evaluate(&self, _hook: &str, message: &Value) -> Result<Value, FilterError> {
        if message.get("params").and_then(|p| p.get("name")).and_then(Value::as_str) == Some(self.tool.as_str()) {
            Ok(json!({ "kind": "deny", "reason": self.reason }))
        } else {
            Ok(json!({ "kind": "allow" }))
        }
    }
}

struct RedactArg {
    arg: String,
    to: String,
}
impl Filter for RedactArg {
    fn name(&self) -> &str {
        "redact_arg"
    }
    fn evaluate(&self, _hook: &str, message: &Value) -> Result<Value, FilterError> {
        let mut args = message.get("params").and_then(|p| p.get("arguments")).and_then(Value::as_object).cloned().unwrap_or_default();
        args.insert(self.arg.clone(), json!(self.to));
        Ok(json!({ "kind": "mutate", "arguments": Value::Object(args) }))
    }
}

fn build_filter(spec: &Value) -> FilterChain {
    match spec["kind"].as_str().unwrap() {
        "deny_tool" => FilterChain::new(vec![Box::new(DenyTool {
            tool: spec["tool"].as_str().unwrap().to_string(),
            reason: spec.get("reason").and_then(Value::as_str).unwrap_or("").to_string(),
        })]),
        "redact_arg" => FilterChain::new(vec![Box::new(RedactArg {
            arg: spec["arg"].as_str().unwrap().to_string(),
            to: spec["to"].as_str().unwrap().to_string(),
        })]),
        other => panic!("unknown filter {other}"),
    }
}

fn init_params() -> Value {
    json!({ "protocolVersion": "x", "capabilities": {}, "clientInfo": { "name": "flow" } })
}

async fn mock_backend<R, W>(mut reader: R, mut writer: W, spec: Value) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    let caps = spec.get("capabilities").cloned().unwrap_or_else(|| json!({ "tools": {} }));
    let id = spec["id"].as_str().unwrap();
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": caps, "serverInfo": { "name": id } },
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
                        let tools: Vec<Value> = spec
                            .get("tools")
                            .and_then(Value::as_array)
                            .map(|a| a.iter().map(|t| json!({ "name": t })).collect())
                            .unwrap_or_default();
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": tools } }))).await?;
                    }
                    Some("tools/call") => {
                        let name = message["params"]["name"].as_str().unwrap();
                        let result = if spec.get("echo_args").and_then(Value::as_bool).unwrap_or(false) {
                            json!({ "content": [{ "type": "text", "text": name }], "arguments": message["params"].get("arguments").cloned().unwrap_or_else(|| json!({})) })
                        } else {
                            spec.get("responses").and_then(|r| r.get(name)).cloned().unwrap_or_else(|| json!({ "content": [{ "type": "text", "text": name }] }))
                        };
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": result }))).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn drive(scenario_in: &Value) -> Vec<Value> {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);

    let mut backends = Vec::new();
    let mut mocks = Vec::new();
    for spec in scenario_in["backends"].as_array().unwrap() {
        let (router_to_b, b_reads) = duplex(CAP);
        let (b_writes, router_reads_b) = duplex(CAP);
        backends.push(
            Backend::new(spec["id"].as_str().unwrap(), LineReader::new(BufReader::new(router_reads_b)), LineWriter::new(router_to_b)).unwrap(),
        );
        mocks.push(mock_backend(LineReader::new(BufReader::new(b_reads)), LineWriter::new(b_writes), spec.clone()));
    }
    let mut router = ForwardRouter::new(LineReader::new(BufReader::new(router_reads_client)), LineWriter::new(router_writes_client), backends);
    if let Some(spec) = scenario_in.get("filter") {
        router = router.set_filters(build_filter(spec));
    }

    let requests: Vec<Value> = scenario_in["client"].as_array().unwrap().clone();
    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        let mut out = Vec::new();
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "init", "method": "initialize", "params": init_params() }))).await.unwrap();
        out.push(jsonrpc::decode(&cr.receive().await.unwrap().unwrap()).unwrap());
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await.unwrap();
        for (index, request) in requests.iter().enumerate() {
            let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
            cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": format!("r{index}"), "method": request["method"], "params": params }))).await.unwrap();
            out.push(jsonrpc::decode(&cr.receive().await.unwrap().unwrap()).unwrap());
        }
        cw.send_eof().await.unwrap();
        out
    };

    let (router_result, _mocks, out) = tokio::join!(router.serve(), futures::future::join_all(mocks), client);
    router_result.unwrap();
    out
}

fn corpus() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance/flow-corpus.json");
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))).unwrap()
}

#[tokio::test]
async fn flow_corpus_matches_rust_arm() {
    let corpus = corpus();
    let flows = corpus["flows"].as_array().unwrap();
    assert!(!flows.is_empty(), "flow corpus is empty");
    for flow in flows {
        let out = drive(&flow["in"]).await;
        assert_eq!(Value::Array(out), flow["out"], "flow {} diverged", flow["name"]);
    }
}
