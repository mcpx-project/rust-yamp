//! Reference ICAP bridge (ε4): pure ICAP mapping plus the end-to-end scanner.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::base64;
use yamp::callout::CalloutClient;
use yamp::icap::{self, ContentScanner};
use yamp::jsonrpc;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

// ---- pure functions ----

#[test]
fn mode_and_deref() {
    assert_eq!(icap::icap_mode("c2u"), "REQMOD");
    assert_eq!(icap::icap_mode("u2c"), "RESPMOD");
    assert!(icap::should_deref("resource_link", true));
    assert!(!icap::should_deref("resource_link", false));
    assert!(!icap::should_deref("image", true));
}

#[test]
fn icap_to_callout_mapping() {
    assert_eq!(icap::icap_to_callout(&json!({"status": 204})), json!({"verdict": "allow"}));
    let modified = base64::encode(b"clean");
    assert_eq!(
        icap::icap_to_callout(&json!({"status": 200, "modified": modified, "istag": "av-1"})),
        json!({"verdict": "mutate", "content": base64::encode(b"clean"), "provenance": {"icap": "modified", "istag": "av-1"}})
    );
    assert_eq!(icap::icap_to_callout(&json!({"status": 200})), json!({"verdict": "allow"}));
    assert_eq!(icap::icap_to_callout(&json!({"status": 403})), json!({"verdict": "deny", "reason": "ICAP policy blocked"}));
    assert_eq!(icap::icap_to_callout(&json!({"status": 200, "threat": "eicar"})), json!({"verdict": "quarantine", "reason": "eicar"}));
    assert_eq!(icap::icap_to_callout(&json!({"status": 500}))["reason"], "unexpected ICAP status");
}

// ---- end-to-end: ContentScanner against a scripted bridge service ----

fn message() -> Value {
    json!({
        "jsonrpc": "2.0", "id": 5,
        "result": {"content": [
            {"type": "text", "text": "hello"},
            {"type": "image", "data": base64::encode(b"rawimg"), "mimeType": "image/png"},
            {"type": "resource_link", "uri": "file:///x", "mimeType": "text/plain"},
        ]}
    })
}

async fn service<R, W>(mut reader: R, mut writer: W, icap_responses: Vec<Value>, log: Arc<Mutex<Vec<String>>>)
where
    R: MessageRead,
    W: MessageWrite,
{
    let mut index = 0;
    while let Ok(Some(raw)) = reader.receive().await {
        let request = jsonrpc::decode(&raw).unwrap();
        log.lock().unwrap().push(request["context"]["direction"].as_str().unwrap().to_string());
        let response = icap_responses.get(index).cloned().unwrap_or_else(|| json!({"status": 204}));
        index += 1;
        let _ = writer.send(&jsonrpc::encode(&icap::icap_to_callout(&response))).await;
    }
    let _ = writer.send_eof().await;
}

async fn scan(message: Value, icap_responses: Vec<Value>) -> (Value, Vec<String>) {
    let (client_w, service_r) = duplex(CAP);
    let (service_w, client_r) = duplex(CAP);
    let log = Arc::new(Mutex::new(Vec::new()));
    let svc = service(LineReader::new(BufReader::new(service_r)), LineWriter::new(service_w), icap_responses, log.clone());

    let driver = async {
        let client: CalloutClient<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>> =
            CalloutClient::new(LineReader::new(BufReader::new(client_r)), LineWriter::new(client_w));
        let mut scanner = ContentScanner::new(client);
        scanner.scan(&message, "u2c", None).await
    };

    let (_svc, outcome) = tokio::join!(svc, driver);
    let log = log.lock().unwrap().clone();
    (outcome, log)
}

#[tokio::test]
async fn clean_content_passes_unchanged() {
    let msg = message();
    let (outcome, log) = scan(msg.clone(), vec![json!({"status": 204}), json!({"status": 204})]).await;
    assert_eq!(outcome, json!({"action": "forward", "message": msg}));
    assert_eq!(log, vec!["u2c", "u2c"]); // text + image scanned; resource_link skipped
}

#[tokio::test]
async fn infected_block_is_quarantined() {
    let (outcome, _) = scan(message(), vec![json!({"status": 200, "threat": "eicar"})]).await;
    assert_eq!(outcome["action"], "block");
    assert_eq!(outcome["quarantined"], json!(true));
    assert_eq!(outcome["response"]["error"]["message"], "eicar");
}

#[tokio::test]
async fn modified_body_is_substituted_and_annotated() {
    let cleaned = base64::encode(b"cleaned");
    let (outcome, _) = scan(message(), vec![json!({"status": 204}), json!({"status": 200, "modified": cleaned, "istag": "av-1"})]).await;
    assert_eq!(outcome["action"], "forward");
    let image = outcome["message"]["result"]["content"][1]["data"].as_str().unwrap();
    assert_eq!(base64::decode(image).unwrap(), b"cleaned");
    assert_eq!(outcome["message"]["result"]["_meta"], json!({"icap": "modified", "istag": "av-1"}));
}

#[tokio::test]
async fn resource_link_is_skipped_without_deref() {
    let (_outcome, log) = scan(message(), vec![json!({"status": 204}), json!({"status": 204})]).await;
    assert_eq!(log.len(), 2);
}

#[tokio::test]
async fn text_block_mutation_rewrites_text() {
    let cleaned = base64::encode(b"scrubbed");
    let msg = json!({"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": "dirty"}]}});
    let (outcome, _) = scan(msg, vec![json!({"status": 200, "modified": cleaned})]).await;
    assert_eq!(outcome["message"]["result"]["content"][0]["text"], "scrubbed");
}
