//! σ3 server-side task origination (Rust arm). Mirrors the Python arm.
//!
//! With `set_server_tasks(true)` a task-augmented tools/call that resolves to a
//! local handler returns a `working` handle at once, runs in the background, and
//! its later `tasks/get`/`tasks/cancel` are served from the store: a finished
//! call is `completed`, a schema-rejected one `failed`, a cancelled one
//! `cancelled`. Off by default, so a task-augmented call is answered
//! synchronously. (A handler cannot itself fail in Rust, so the failure path is
//! exercised through schema rejection rather than a raising handler.)

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};
use tokio::sync::Semaphore;

use yamp::handler::{CallFuture, Handler, Registry};
use yamp::jsonrpc::{self, INVALID_PARAMS};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

struct TaskHandler {
    gate: HashMap<String, Arc<Semaphore>>,
    enter: HashMap<String, Arc<Semaphore>>,
}

impl Handler for TaskHandler {
    fn id(&self) -> &str {
        "srv"
    }

    fn list_tools(&self) -> Vec<Value> {
        vec![
            json!({ "name": "fast", "inputSchema": { "type": "object" } }),
            json!({ "name": "block", "inputSchema": { "type": "object" } }),
            json!({ "name": "strict", "inputSchema": { "type": "object", "properties": { "n": { "type": "integer" } }, "required": ["n"] } }),
        ]
    }

    fn call_tool<'a>(&'a self, name: &'a str, arguments: &'a Value) -> CallFuture<'a> {
        let name = name.to_string();
        let key = arguments.get("k").and_then(Value::as_str).unwrap_or("").to_string();
        let enter = self.enter.get(&key).cloned();
        let gate = self.gate.get(&key).cloned();
        Box::pin(async move {
            if name == "block" {
                if let Some(enter) = enter {
                    enter.add_permits(1);
                }
                if let Some(gate) = gate {
                    let _ = gate.acquire().await;
                }
            }
            json!({ "content": [{ "type": "text", "text": name }] })
        })
    }
}

type CR = LineReader<BufReader<DuplexStream>>;
type CW = LineWriter<DuplexStream>;

fn router_with(handler: TaskHandler, validate: bool, reads: DuplexStream, writes: DuplexStream) -> ForwardRouter<CR, CW, CR, CW> {
    let registry = Registry::new(vec![Box::new(handler)]).unwrap();
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    ForwardRouter::new(LineReader::new(BufReader::new(reads)), LineWriter::new(writes), backends)
        .set_registry(registry)
        .set_server_tasks(true)
        .set_validate_schemas(validate)
}

async fn handshake(cw: &mut CW, cr: &mut CR) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "i", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
    Ok(())
}

async fn augmented_call(cw: &mut CW, cr: &mut CR, id: &str, name: &str, arguments: Value) -> io::Result<Value> {
    let params = json!({ "name": name, "arguments": arguments, "_meta": { "io.modelcontextprotocol/task": {} } });
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": params }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

async fn task_req(cw: &mut CW, cr: &mut CR, id: &str, method: &str, task_id: &str) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": { "taskId": task_id } }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

async fn poll_done(cw: &mut CW, cr: &mut CR, task_id: &str) -> io::Result<Value> {
    for i in 0..100 {
        let r = task_req(cw, cr, &format!("g{i}"), "tasks/get", task_id).await?;
        if r["result"]["status"] != "working" {
            return Ok(r["result"].clone());
        }
    }
    panic!("task never left working");
}

fn empty() -> HashMap<String, Arc<Semaphore>> {
    HashMap::new()
}

#[tokio::test]
async fn originate_then_complete() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let handler = TaskHandler { gate: empty(), enter: empty() };
    let router = router_with(handler, false, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let handle = augmented_call(&mut cw, &mut cr, "A", "srv__fast", json!({})).await?;
        let task_id = handle["result"]["taskId"].as_str().unwrap().to_string();
        let done = poll_done(&mut cw, &mut cr, &task_id).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value), io::Error>((handle, done))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (handle, done) = client_result.unwrap();
    assert_eq!(handle["result"]["resultType"], "task");
    assert_eq!(handle["result"]["status"], "working");
    assert!(!handle["result"]["taskId"].as_str().unwrap().contains("__"));
    assert_eq!(done["status"], "completed");
    assert_eq!(done["result"]["content"][0]["text"], "fast");
}

#[tokio::test]
async fn working_then_cancel() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let (enter_a, gate_a) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0))); // never released
    let handler = TaskHandler {
        gate: [("a".to_string(), gate_a.clone())].into_iter().collect(),
        enter: [("a".to_string(), enter_a.clone())].into_iter().collect(),
    };
    let router = router_with(handler, false, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let handle = augmented_call(&mut cw, &mut cr, "A", "srv__block", json!({ "k": "a" })).await?;
        let task_id = handle["result"]["taskId"].as_str().unwrap().to_string();
        enter_a.acquire().await.unwrap().forget();
        let pending = task_req(&mut cw, &mut cr, "g", "tasks/get", &task_id).await?;
        let cancelled = task_req(&mut cw, &mut cr, "c", "tasks/cancel", &task_id).await?;
        let after = task_req(&mut cw, &mut cr, "g2", "tasks/get", &task_id).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value, Value), io::Error>((pending, cancelled, after))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (pending, cancelled, after) = client_result.unwrap();
    assert_eq!(pending["result"]["status"], "working");
    assert_eq!(cancelled["result"]["status"], "cancelled");
    assert_eq!(after["result"]["status"], "cancelled");
}

#[tokio::test]
async fn schema_rejected_call_fails_the_task() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let handler = TaskHandler { gate: empty(), enter: empty() };
    let router = router_with(handler, true, r_reads, r_writes); // validate on

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let handle = augmented_call(&mut cw, &mut cr, "A", "srv__strict", json!({})).await?; // missing required n
        let task_id = handle["result"]["taskId"].as_str().unwrap().to_string();
        let done = poll_done(&mut cw, &mut cr, &task_id).await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(done)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let done = client_result.unwrap();
    assert_eq!(done["status"], "failed");
    assert_eq!(done["error"]["data"]["errorId"], "E4002");
}

#[tokio::test]
async fn cancel_after_completion_returns_terminal_handle() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let handler = TaskHandler { gate: empty(), enter: empty() };
    let router = router_with(handler, false, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let handle = augmented_call(&mut cw, &mut cr, "A", "srv__fast", json!({})).await?;
        let task_id = handle["result"]["taskId"].as_str().unwrap().to_string();
        poll_done(&mut cw, &mut cr, &task_id).await?; // ensure completed
        let cancelled = task_req(&mut cw, &mut cr, "c", "tasks/cancel", &task_id).await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(cancelled)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    // Cancel on an already-completed task is a no-op: the completed handle stands.
    assert_eq!(client_result.unwrap()["result"]["status"], "completed");
}

#[tokio::test]
async fn unknown_task_id_is_rejected() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let handler = TaskHandler { gate: empty(), enter: empty() };
    let router = router_with(handler, false, r_reads, r_writes);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let r = task_req(&mut cw, &mut cr, "g", "tasks/get", "task-999").await?; // never created
        cw.send_eof().await?;
        Ok::<Value, io::Error>(r)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    assert_eq!(client_result.unwrap()["error"]["code"].as_i64(), Some(INVALID_PARAMS));
}

#[tokio::test]
async fn off_by_default_answers_synchronously() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let handler = TaskHandler { gate: empty(), enter: empty() };
    let registry = Registry::new(vec![Box::new(handler)]).unwrap();
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    // server tasks OFF (default): the augmentation is ignored.
    let router = ForwardRouter::new(LineReader::new(BufReader::new(r_reads)), LineWriter::new(r_writes), backends).set_registry(registry);

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let r = augmented_call(&mut cw, &mut cr, "A", "srv__fast", json!({})).await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(r)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let r = client_result.unwrap();
    assert_ne!(r["result"]["resultType"], "task"); // answered directly, no handle
    assert_eq!(r["result"]["content"][0]["text"], "fast");
}
