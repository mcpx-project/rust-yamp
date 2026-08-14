//! δ19 tasks-routing tests (Rust arm). Mirrors the Python arm.

use std::io;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc::{self, INVALID_PARAMS};
use yamp::router::{Backend, ForwardRouter};
use yamp::tasks;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

#[test]
fn task_helpers() {
    assert!(tasks::is_task_result(&json!({ "resultType": "task", "taskId": "t1" })));
    assert!(!tasks::is_task_result(&json!({ "resultType": "complete" })));
    let out = tasks::namespace_task_id(&json!({ "resultType": "task", "taskId": "t1", "task": { "taskId": "t1", "status": "working" } }), "gh");
    assert_eq!(out["taskId"], "gh__t1");
    assert_eq!(out["task"]["taskId"], "gh__t1");
    assert_eq!(out["task"]["status"], "working");
    assert_eq!(tasks::resolve_task("gh__t1"), Some(("gh".to_string(), "t1".to_string())));
    assert_eq!(tasks::resolve_task("nodelim"), None);
}

#[test]
fn stream_helpers() {
    // SEP-2694: tasks/stream routes like the other task methods and is a read.
    assert!(tasks::is_task_method("tasks/stream"));
    assert!(tasks::TASK_READ_METHODS.contains(&"tasks/stream"));
    // Re-namespacing a task event renames the taskId, preserving other fields.
    let event = json!({ "method": tasks::TASK_EVENT_METHOD, "params": { "taskId": "T-9", "seq": 0, "type": "log" } });
    let out = tasks::namespace_event(&event, "gh");
    assert_eq!(out["params"]["taskId"], "gh__T-9");
    assert_eq!(out["params"]["seq"], 0);
    assert_eq!(out["params"]["type"], "log");
    assert_eq!(event["params"]["taskId"], "T-9"); // input not mutated
    // A message with no string taskId is returned unchanged.
    let no_task = json!({ "method": tasks::TASK_EVENT_METHOD, "params": { "seq": 3 } });
    assert_eq!(tasks::namespace_event(&no_task, "gh"), no_task);
    assert_eq!(tasks::namespace_event(&json!({ "method": tasks::TASK_EVENT_METHOD }), "gh"), json!({ "method": tasks::TASK_EVENT_METHOD }));
    // SEP-2848: an approval-gated handle is an ordinary task the proxy namespaces.
    let handle = json!({ "resultType": "task", "taskId": "A-1", "status": "working" });
    assert!(tasks::is_task_result(&handle));
    assert_eq!(tasks::namespace_task_id(&handle, "gh")["taskId"], "gh__A-1");
}

async fn call(
    cw: &mut LineWriter<DuplexStream>,
    cr: &mut LineReader<BufReader<DuplexStream>>,
    id: &str,
    method: &str,
    params: Value,
) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

async fn task_backend(
    mut reader: LineReader<BufReader<DuplexStream>>,
    mut writer: LineWriter<DuplexStream>,
    name: &'static str,
    log: Arc<Mutex<Vec<Value>>>,
) -> io::Result<()> {
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": name } },
        })))
        .await?;
    reader.receive().await?;
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let m = jsonrpc::decode(&raw)?;
                log.lock().unwrap().push(m.clone());
                let reply = match jsonrpc::method_of(&m) {
                    Some("tools/call") => json!({ "jsonrpc": "2.0", "id": m["id"], "result": { "resultType": "task", "taskId": "T-99", "task": { "taskId": "T-99", "status": "working" } } }),
                    Some("tasks/get") => json!({ "jsonrpc": "2.0", "id": m["id"], "result": { "resultType": "complete", "taskId": m["params"]["taskId"], "handledBy": name } }),
                    _ => continue,
                };
                writer.send(&jsonrpc::encode(&reply)).await?;
            }
        }
    }
}

#[tokio::test]
async fn task_creation_namespaced_and_get_routes_back() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);
    let (router_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, router_reads_gl) = duplex(CAP);

    let backends = vec![
        Backend::new("gh", LineReader::new(BufReader::new(router_reads_gh)), LineWriter::new(router_to_gh)).unwrap(),
        Backend::new("gl", LineReader::new(BufReader::new(router_reads_gl)), LineWriter::new(router_to_gl)).unwrap(),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );
    let gh_log = Arc::new(Mutex::new(Vec::new()));
    let gl_log = Arc::new(Mutex::new(Vec::new()));
    let gh = task_backend(LineReader::new(BufReader::new(gh_reads)), LineWriter::new(gh_writes), "gh", gh_log.clone());
    let gl = task_backend(LineReader::new(BufReader::new(gl_reads)), LineWriter::new(gl_writes), "gl", gl_log.clone());

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c1", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let created = call(&mut cw, &mut cr, "t", "tools/call", json!({ "name": "gl__run", "arguments": {} })).await?;
        let task_id = created["result"]["taskId"].as_str().unwrap().to_string();
        let got = call(&mut cw, &mut cr, "g", "tasks/get", json!({ "taskId": task_id })).await?;
        let unknown = call(&mut cw, &mut cr, "u", "tasks/get", json!({ "taskId": "zz__nope" })).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value, Value), io::Error>((created, got, unknown))
    };

    let (_r, _gh, _gl, (created, got, unknown)) = tokio::try_join!(router.serve(), gh, gl, client).unwrap();
    assert_eq!(created["result"]["taskId"], "gl__T-99");
    assert_eq!(created["result"]["task"]["taskId"], "gl__T-99");
    assert_eq!(got["result"]["handledBy"], "gl");
    assert_eq!(got["result"]["taskId"], "gl__T-99"); // re-namespaced for the client
    let gl_gets: Vec<Value> = gl_log.lock().unwrap().iter().filter(|m| jsonrpc::method_of(m) == Some("tasks/get")).cloned().collect();
    assert_eq!(gl_gets[0]["params"]["taskId"], "T-99"); // backend saw its own id
    assert!(gh_log.lock().unwrap().iter().all(|m| jsonrpc::method_of(m) != Some("tasks/get"))); // gh untouched
    assert_eq!(unknown["error"]["code"], INVALID_PARAMS);
}

async fn stream_backend(
    mut reader: LineReader<BufReader<DuplexStream>>,
    mut writer: LineWriter<DuplexStream>,
    log: Arc<Mutex<Vec<Value>>>,
) -> io::Result<()> {
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": "gh" } },
        })))
        .await?;
    reader.receive().await?;
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let m = jsonrpc::decode(&raw)?;
                log.lock().unwrap().push(m.clone());
                match jsonrpc::method_of(&m) {
                    Some("tools/call") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": { "resultType": "task", "taskId": "T-7" } }))).await?;
                    }
                    Some("tasks/stream") => {
                        let tid = m["params"]["taskId"].clone();
                        for seq in 0..2 {
                            writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": tasks::TASK_EVENT_METHOD, "params": { "taskId": tid, "seq": seq, "type": "log" } }))).await?;
                        }
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": {} }))).await?; // stream closed
                    }
                    _ => {}
                }
            }
        }
    }
}

#[tokio::test]
async fn tasks_stream_routes_and_renames_events() {
    // SEP-2694: tasks/stream routes to the task's backend with the backend's own
    // id and the resume cursor preserved; the events the backend then emits are
    // re-namespaced so the client sees the backend__taskId it holds.
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);

    let backends =
        vec![Backend::new("gh", LineReader::new(BufReader::new(router_reads_gh)), LineWriter::new(router_to_gh)).unwrap()];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );
    let gh_log = Arc::new(Mutex::new(Vec::new()));
    let gh = stream_backend(LineReader::new(BufReader::new(gh_reads)), LineWriter::new(gh_writes), gh_log.clone());

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c1", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let created = call(&mut cw, &mut cr, "t", "tools/call", json!({ "name": "run", "arguments": {} })).await?;
        let task_id = created["result"]["taskId"].as_str().unwrap().to_string();
        // Open the stream from the last seen sequence (resume cursor).
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "s", "method": "tasks/stream", "params": { "taskId": task_id, "after": 4 } }))).await?;
        let mut events = Vec::new();
        let response;
        loop {
            let msg = jsonrpc::decode(&cr.receive().await?.unwrap())?;
            if jsonrpc::method_of(&msg) == Some(tasks::TASK_EVENT_METHOD) {
                events.push(msg);
            } else if msg.get("id").and_then(Value::as_str) == Some("s") {
                response = msg;
                break;
            }
        }
        cw.send_eof().await?;
        Ok::<(Value, Vec<Value>, Value), io::Error>((created, events, response))
    };

    let (_r, _gh, (created, events, response)) = tokio::try_join!(router.serve(), gh, client).unwrap();
    assert_eq!(created["result"]["taskId"], "gh__T-7"); // single backend still namespaces task ids
    let streams: Vec<Value> = gh_log.lock().unwrap().iter().filter(|m| jsonrpc::method_of(m) == Some("tasks/stream")).cloned().collect();
    assert_eq!(streams[0]["params"]["taskId"], "T-7"); // backend saw its own id
    assert_eq!(streams[0]["params"]["after"], 4); // resume cursor preserved
    let event_ids: Vec<&Value> = events.iter().map(|e| &e["params"]["taskId"]).collect();
    assert_eq!(event_ids, vec![&json!("gh__T-7"), &json!("gh__T-7")]); // re-namespaced for the client
    assert_eq!(events.iter().map(|e| e["params"]["seq"].clone()).collect::<Vec<_>>(), vec![json!(0), json!(1)]);
    assert!(response.get("result").is_some()); // stream closed with an empty result
}

// --- Server-side task store (σ3). Mirrors the Python arm's store unit test. ---

#[test]
fn server_tasks_store_lifecycle() {
    use yamp::tasks::ServerTasks;

    let mut store = ServerTasks::new();
    let a = store.create();
    let b = store.create();
    assert_eq!((a.as_str(), b.as_str()), ("task-1", "task-2"));
    assert!(store.contains(&a) && !store.contains("task-9"));
    assert_eq!(store.get(&a), Some(json!({ "status": "working" })));

    store.complete(&a, json!({ "content": [] }));
    assert_eq!(store.get(&a), Some(json!({ "status": "completed", "result": { "content": [] } })));

    // Terminal states are final: a completed task does not transition again.
    assert!(!store.cancel(&a));
    assert_eq!(store.get(&a).unwrap()["status"], "completed");

    // cancel a working task transitions; cancel a terminal one returns false.
    assert!(store.cancel(&b));
    assert_eq!(store.get(&b), Some(json!({ "status": "cancelled" })));
    assert!(!store.cancel(&b));

    let c = store.create();
    store.fail(&c, json!({ "code": -32603 }));
    assert_eq!(store.get(&c), Some(json!({ "status": "failed", "error": { "code": -32603 } })));
    store.complete(&c, json!({ "x": 1 })); // no transition from failed
    assert_eq!(store.get(&c).unwrap()["status"], "failed");
}
