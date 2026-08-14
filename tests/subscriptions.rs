//! σ4 resource subscriptions (Rust arm). Mirrors the Python arm.
//!
//! Two roles from one seam (like tasks: δ19 routing + σ3 origination):
//! - Proxy role: resources/subscribe|unsubscribe reverse-resolve their namespaced
//!   URI to the owning backend and forward with the backend's own URI; the
//!   backend's notifications/resources/updated is re-namespaced so the client sees
//!   the same backend__uri it holds.
//! - Server role: a subscribe whose URI resolves to no backend is registered in a
//!   per-connection registry, and a ResourcePublisher fans out the notification
//!   only to the subscribed URIs.

use std::io;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc::{self, INVALID_PARAMS};
use yamp::router::{Backend, ForwardRouter};
use yamp::subscriptions::{self, Subscriptions};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type CR = LineReader<BufReader<DuplexStream>>;
type CW = LineWriter<DuplexStream>;

#[test]
fn subscription_helpers() {
    assert!(subscriptions::is_subscribe_method("resources/subscribe"));
    assert!(subscriptions::is_subscribe_method("resources/unsubscribe"));
    assert!(!subscriptions::is_subscribe_method("resources/read"));
    assert_eq!(
        subscriptions::updated_notification("file:///x"),
        json!({ "jsonrpc": "2.0", "method": "notifications/resources/updated", "params": { "uri": "file:///x" } })
    );
    // Re-namespacing renames the uri, preserving other fields and not mutating input.
    let msg = json!({ "jsonrpc": "2.0", "method": subscriptions::UPDATED_METHOD, "params": { "uri": "file:///reports/q3.md", "title": "Q3" } });
    let out = subscriptions::namespace_updated(&msg, "docs");
    assert_eq!(out["params"]["uri"], "file:///docs/reports/q3.md");
    assert_eq!(out["params"]["title"], "Q3");
    assert_eq!(msg["params"]["uri"], "file:///reports/q3.md");
    // A message with no string uri is returned unchanged.
    let no_uri = json!({ "method": subscriptions::UPDATED_METHOD, "params": { "seq": 3 } });
    assert_eq!(subscriptions::namespace_updated(&no_uri, "docs"), no_uri);
    assert_eq!(
        subscriptions::namespace_updated(&json!({ "method": subscriptions::UPDATED_METHOD }), "docs"),
        json!({ "method": subscriptions::UPDATED_METHOD })
    );
}

#[test]
fn subscriptions_registry() {
    let mut reg = Subscriptions::new();
    assert_eq!(reg.count(), 0);
    assert!(!reg.contains("u1"));
    reg.subscribe("u1");
    reg.subscribe("u1"); // idempotent
    assert!(reg.contains("u1"));
    assert_eq!(reg.count(), 1);
    assert!(reg.unsubscribe("u1"));
    assert!(!reg.unsubscribe("u1")); // already gone
    assert!(!reg.contains("u1"));
}

async fn call(cw: &mut CW, cr: &mut CR, id: &str, method: &str, params: Value) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))).await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

async fn resource_backend(
    mut reader: CR,
    mut writer: CW,
    name: &'static str,
    log: Arc<Mutex<Vec<Value>>>,
) -> io::Result<()> {
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "resources": { "subscribe": true } }, "serverInfo": { "name": name } },
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
                    Some("resources/subscribe") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": {} }))).await?;
                        // Emit a change for the just-subscribed resource, with the
                        // backend's own (un-prefixed) uri.
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": subscriptions::UPDATED_METHOD, "params": { "uri": m["params"]["uri"] } }))).await?;
                    }
                    Some("resources/unsubscribe") => {
                        writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": m["id"], "result": {} }))).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

#[tokio::test]
async fn subscribe_routes_and_updated_is_renamespaced() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_docs, docs_reads) = duplex(CAP);
    let (docs_writes, router_reads_docs) = duplex(CAP);
    let (router_to_wiki, wiki_reads) = duplex(CAP);
    let (wiki_writes, router_reads_wiki) = duplex(CAP);

    let backends = vec![
        Backend::new("docs", LineReader::new(BufReader::new(router_reads_docs)), LineWriter::new(router_to_docs)).unwrap(),
        Backend::new("wiki", LineReader::new(BufReader::new(router_reads_wiki)), LineWriter::new(router_to_wiki)).unwrap(),
    ];
    let router = ForwardRouter::new(LineReader::new(BufReader::new(router_reads_client)), LineWriter::new(router_writes_client), backends);
    let docs_log = Arc::new(Mutex::new(Vec::new()));
    let wiki_log = Arc::new(Mutex::new(Vec::new()));
    let docs = resource_backend(LineReader::new(BufReader::new(docs_reads)), LineWriter::new(docs_writes), "docs", docs_log.clone());
    let wiki = resource_backend(LineReader::new(BufReader::new(wiki_reads)), LineWriter::new(wiki_writes), "wiki", wiki_log.clone());

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "c1", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "s", "method": "resources/subscribe", "params": { "uri": "file:///docs/reports/q3.md" } }))).await?;
        let mut ack = Value::Null;
        let mut updated = Value::Null;
        while ack.is_null() || updated.is_null() {
            let msg = jsonrpc::decode(&cr.receive().await?.unwrap())?;
            if msg.get("id").and_then(Value::as_str) == Some("s") {
                ack = msg;
            } else if jsonrpc::method_of(&msg) == Some(subscriptions::UPDATED_METHOD) {
                updated = msg;
            }
        }
        // An unknown resource (no backend, server subs off) is rejected.
        let unknown = call(&mut cw, &mut cr, "u", "resources/subscribe", json!({ "uri": "file:///ghost/x" })).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value, Value), io::Error>((ack, updated, unknown))
    };

    let (_r, _docs, _wiki, (ack, updated, unknown)) = tokio::try_join!(router.serve(), docs, wiki, client).unwrap();
    assert!(ack.get("result").is_some());
    // docs saw the subscribe with its own un-prefixed uri; wiki untouched.
    let docs_subs: Vec<Value> = docs_log.lock().unwrap().iter().filter(|m| jsonrpc::method_of(m) == Some("resources/subscribe")).cloned().collect();
    assert_eq!(docs_subs[0]["params"]["uri"], "file:///reports/q3.md");
    assert!(wiki_log.lock().unwrap().iter().all(|m| jsonrpc::method_of(m) != Some("resources/subscribe")));
    assert_eq!(updated["params"]["uri"], "file:///docs/reports/q3.md"); // re-namespaced for the client
    assert_eq!(unknown["error"]["code"], INVALID_PARAMS);
}

fn server_router(on: bool, reads: DuplexStream, writes: DuplexStream) -> ForwardRouter<CR, CW, CR, CW> {
    let backends: Vec<Backend<CR, CW>> = Vec::new();
    ForwardRouter::new(LineReader::new(BufReader::new(reads)), LineWriter::new(writes), backends).set_resource_subscriptions(on)
}

async fn handshake(cw: &mut CW, cr: &mut CR) -> io::Result<()> {
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "i", "method": "initialize", "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} } }))).await?;
    cr.receive().await?;
    cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
    Ok(())
}

#[tokio::test]
async fn server_subscribe_registers_and_publish_fans_out() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let router = server_router(true, r_reads, r_writes);
    let publisher = router.resource_publisher();

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let ack = call(&mut cw, &mut cr, "s", "resources/subscribe", json!({ "uri": "mem://counter" })).await?;
        let sent_sub = publisher.publish("mem://counter").await?;
        let sent_other = publisher.publish("mem://unseen").await?;
        let note = jsonrpc::decode(&cr.receive().await?.unwrap())?; // only the subscribed one arrives
        cw.send_eof().await?;
        Ok::<(Value, bool, bool, Value), io::Error>((ack, sent_sub, sent_other, note))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (ack, sent_sub, sent_other, note) = client_result.unwrap();
    assert!(ack["result"].as_object().map(|o| o.keys().all(|k| k == "_meta")).unwrap_or(false)); // empty result (only the proxy trace hop)
    assert!(sent_sub && !sent_other); // only subscribed uris fan out
    assert_eq!(jsonrpc::method_of(&note), Some(subscriptions::UPDATED_METHOD));
    assert_eq!(note["params"]["uri"], "mem://counter");
}

#[tokio::test]
async fn server_unsubscribe_stops_updates() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let router = server_router(true, r_reads, r_writes);
    let publisher = router.resource_publisher();

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        call(&mut cw, &mut cr, "s", "resources/subscribe", json!({ "uri": "mem://x" })).await?;
        call(&mut cw, &mut cr, "u", "resources/unsubscribe", json!({ "uri": "mem://x" })).await?;
        let sent = publisher.publish("mem://x").await?;
        cw.send_eof().await?;
        Ok::<bool, io::Error>(sent)
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    assert!(!client_result.unwrap()); // no longer subscribed
}

#[tokio::test]
async fn server_off_by_default_rejects_local_subscribe() {
    let (client_w, r_reads) = duplex(CAP);
    let (r_writes, client_r) = duplex(CAP);
    let router = server_router(false, r_reads, r_writes);
    let publisher = router.resource_publisher();

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        handshake(&mut cw, &mut cr).await?;
        let r = call(&mut cw, &mut cr, "s", "resources/subscribe", json!({ "uri": "mem://x" })).await?;
        let sent = publisher.publish("mem://x").await?; // nothing was registered
        cw.send_eof().await?;
        Ok::<(Value, bool), io::Error>((r, sent))
    };

    let (router_result, client_result) = tokio::join!(router.serve(), client);
    router_result.unwrap();
    let (r, sent) = client_result.unwrap();
    assert_eq!(r["error"]["code"], INVALID_PARAMS);
    assert!(!sent);
}
