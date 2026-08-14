//! δ14 bidirectional reply-routing and notification forwarding (Rust arm).
//!
//! Mirrors the Python arm. Pins two bugs the increment fixes: a client's reply
//! to a backend-initiated request must route back to the originating backend,
//! and a client notification must be forwarded onward rather than dropped.

use std::io;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::jsonrpc;
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

/// A backend that handshakes, optionally pushes one server-initiated message on
/// ready, answers tools/call with `tool_result`, and logs every message it
/// receives after the handshake.
async fn mock_backend<R, W>(
    mut reader: R,
    mut writer: W,
    on_ready: Option<Value>,
    tool_result: Value,
    log: Arc<StdMutex<Vec<Value>>>,
) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "capabilities": { "tools": {} }, "serverInfo": { "name": "b" } },
        })))
        .await?;
    reader.receive().await?; // notifications/initialized
    if let Some(message) = on_ready {
        writer.send(&jsonrpc::encode(&message)).await?;
    }
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let message = jsonrpc::decode(&raw)?;
                log.lock().unwrap().push(message.clone());
                if jsonrpc::method_of(&message) == Some("tools/call") {
                    writer
                        .send(&jsonrpc::encode(&json!({
                            "jsonrpc": "2.0", "id": message["id"], "result": tool_result,
                        })))
                        .await?;
                }
            }
        }
    }
}

async fn handshake(cw: &mut LineWriter<DuplexStream>, cr: &mut LineReader<BufReader<DuplexStream>>) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": "c1", "method": "initialize",
        "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
    })))
    .await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
    Ok(())
}

/// Wire one client, one backend, and the router; returns the client duplex
/// halves, the backend log, and the joined server future.
#[allow(clippy::type_complexity)]
fn build(
    on_ready: Option<Value>,
    tool_result: Value,
) -> (
    LineWriter<DuplexStream>,
    LineReader<BufReader<DuplexStream>>,
    Arc<StdMutex<Vec<Value>>>,
    impl std::future::Future<Output = io::Result<()>>,
) {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_b, b_reads) = duplex(CAP);
    let (b_writes, router_reads_b) = duplex(CAP);

    let backend = Backend::new("b", LineReader::new(BufReader::new(router_reads_b)), LineWriter::new(router_to_b)).unwrap();
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        vec![backend],
    );

    let log = Arc::new(StdMutex::new(Vec::new()));
    let backend_fut = mock_backend(
        LineReader::new(BufReader::new(b_reads)),
        LineWriter::new(b_writes),
        on_ready,
        tool_result,
        log.clone(),
    );
    let server = async move {
        tokio::try_join!(router.serve(), backend_fut)?;
        Ok(())
    };
    (LineWriter::new(client_w), LineReader::new(BufReader::new(client_r)), log, server)
}

#[tokio::test]
async fn server_initiated_request_reply_routes_back_to_backend() {
    let sampling = json!({ "jsonrpc": "2.0", "id": "bkreq-1", "method": "sampling/createMessage", "params": { "prompt": "hi" } });
    let (mut cw, mut cr, log, server) = build(Some(sampling), json!({ "content": [{ "type": "text", "text": "ok" }] }));

    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        let pushed = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        assert_eq!(pushed["method"], "sampling/createMessage");
        let srv_id = pushed["id"].as_str().unwrap().to_string();
        assert!(srv_id.starts_with("srv-")); // proxy minted a unique client id
        assert_ne!(srv_id, "bkreq-1");
        // Reply with the proxy's id; it must reach the backend as "bkreq-1".
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": srv_id, "result": { "content": "sampled" } }))).await?;
        // A normal call afterwards proves the channel is still healthy.
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/call", "params": { "name": "b__x" } }))).await?;
        let called = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(called)
    };

    let (_server, called) = tokio::try_join!(server, client).unwrap();
    let log = log.lock().unwrap();
    let reply = log.iter().find(|m| m.get("id") == Some(&json!("bkreq-1")) && m.get("result").is_some());
    assert_eq!(reply.unwrap()["result"], json!({ "content": "sampled" }));
    assert_eq!(called["result"]["content"][0]["text"], "ok");
}

#[tokio::test]
async fn client_notification_forwarded_to_backend() {
    let (mut cw, mut cr, log, server) = build(None, json!({ "content": [{ "type": "text", "text": "ok" }] }));

    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/progress", "params": { "progress": 0.5 } }))).await?;
        // Follow with a call so the notification has certainly been processed.
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/call", "params": { "name": "b__x" } }))).await?;
        cr.receive().await?;
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(server, client).unwrap();
    let log = log.lock().unwrap();
    let progress = log.iter().find(|m| jsonrpc::method_of(m) == Some("notifications/progress"));
    assert_eq!(progress.unwrap()["params"]["progress"], 0.5);
}

#[tokio::test]
async fn client_cancellation_routes_to_holding_backend_with_original_id() {
    // The backend initiates a request; the client cancels it. The proxy must
    // deliver the cancellation to that backend with the backend's own id
    // restored, not broadcast it (SEP §5.1, SEP-2260/2322).
    let sampling = json!({ "jsonrpc": "2.0", "id": "bkreq-1", "method": "sampling/createMessage", "params": { "prompt": "hi" } });
    let (mut cw, mut cr, log, server) = build(Some(sampling), json!({ "content": [{ "type": "text", "text": "ok" }] }));

    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        let pushed = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        let srv_id = pushed["id"].as_str().unwrap().to_string();
        // Cancel the proxy-facing id; the backend must see its own "bkreq-1".
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "method": "notifications/cancelled",
            "params": { "requestId": srv_id, "reason": "user aborted" },
        })))
        .await?;
        // A cancel for an id the proxy never minted must be dropped, not routed.
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "method": "notifications/cancelled",
            "params": { "requestId": "never-seen" },
        })))
        .await?;
        // A trailing call flushes the pipeline so both notifications are handled.
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/call", "params": { "name": "b__x" } }))).await?;
        cr.receive().await?;
        cw.send_eof().await?;
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(server, client).unwrap();
    let log = log.lock().unwrap();
    let cancels: Vec<&Value> = log.iter().filter(|m| jsonrpc::method_of(m) == Some("notifications/cancelled")).collect();
    // Exactly one cancellation reached the backend: the tracked one, translated.
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0]["params"]["requestId"], json!("bkreq-1"));
    assert_eq!(cancels[0]["params"]["reason"], json!("user aborted"));
}

#[tokio::test]
async fn stray_client_response_is_dropped_not_errored() {
    let (mut cw, mut cr, _log, server) = build(None, json!({ "content": [{ "type": "text", "text": "ok" }] }));

    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        // A response with an id the proxy never minted: dropped silently.
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "no-such-id", "result": { "x": 1 } }))).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/call", "params": { "name": "b__x" } }))).await?;
        let called = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(called)
    };

    let (_server, called) = tokio::try_join!(server, client).unwrap();
    assert_eq!(called["result"]["content"][0]["text"], "ok");
}

#[tokio::test]
async fn mrtr_result_and_request_state_pass_through_verbatim() {
    let opaque = json!({ "token": "abc123", "nested": [1, 2, { "k": "v" }] });
    let mrtr = json!({ "resultType": "input_required", "requestState": opaque, "content": [] });
    let (mut cw, mut cr, log, server) = build(None, mrtr);

    let opaque_client = opaque.clone();
    let client = async move {
        handshake(&mut cw, &mut cr).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c2", "method": "tools/call", "params": { "name": "b__x" } }))).await?;
        let first = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        // Client echoes requestState verbatim in a follow-up request.
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c3", "method": "tools/call",
            "params": { "name": "b__x", "arguments": { "requestState": opaque_client } },
        })))
        .await?;
        cr.receive().await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(first)
    };

    let (_server, first) = tokio::try_join!(server, client).unwrap();
    assert_eq!(first["result"]["resultType"], "input_required");
    assert_eq!(first["result"]["requestState"], opaque);
    let log = log.lock().unwrap();
    let follow = log
        .iter()
        .find(|m| m.pointer("/params/arguments/requestState").is_some());
    assert_eq!(follow.unwrap().pointer("/params/arguments/requestState").unwrap(), &opaque);
}
