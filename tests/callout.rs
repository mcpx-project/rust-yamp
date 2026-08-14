//! ext-proc callout transport (ε3): pure envelope/parse plus the async client.

use std::io;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader};

use yamp::callout::{self, CalloutClient, VerdictCache};
use yamp::jsonrpc;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

fn ctx() -> Value {
    json!({"method": "tools/call", "tool": "gh__x", "direction": "u2c", "content_types": ["image/png"]})
}

#[test]
fn request_envelope_and_digest_and_budget() {
    let req = callout::callout_request(&ctx(), "preview", b"scan", true);
    assert_eq!(req["callout"], "1");
    assert_eq!(req["phase"], "preview");
    assert_eq!(req["ieof"], json!(true));
    assert_eq!(req["content"], "c2Nhbg==");
    assert_eq!(callout::content_digest(b""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    assert!(callout::exceeds_budget(30, 20) && !callout::exceeds_budget(10, 20));
    assert!(!callout::exceeds_budget(30, 0));
}

#[test]
fn parse_verdict_kinds_and_failure_policy() {
    use yamp::base64;
    assert_eq!(callout::parse_verdict(&json!({"verdict": "allow"}), "fail_closed"), json!({"kind": "allow"}));
    assert_eq!(callout::parse_verdict(&json!({"verdict": "deny", "reason": "x"}), "fail_closed"), json!({"kind": "deny", "reason": "x"}));
    let mutated = callout::parse_verdict(&json!({"verdict": "mutate", "content": base64::encode(b"ok")}), "fail_closed");
    assert_eq!(mutated, json!({"kind": "mutate", "bytes": "6f6b"}));
    assert_eq!(callout::parse_verdict(&json!({"verdict": "mutate"}), "fail_closed"), json!({"kind": "mutate", "bytes": Value::Null}));
    assert_eq!(callout::parse_verdict(&json!({"verdict": "annotate", "provenance": {"s": 1}}), "fail_closed"), json!({"kind": "annotate", "provenance": {"s": 1}}));
    assert_eq!(callout::parse_verdict(&json!({"verdict": "continue"}), "fail_closed"), json!({"kind": "continue"}));
    assert_eq!(callout::parse_verdict(&json!({"x": 1}), "fail_closed")["kind"], "deny");
    assert_eq!(callout::parse_verdict(&json!({"x": 1}), "fail_open")["kind"], "allow");
    assert_eq!(callout::parse_verdict(&json!("not a dict"), "fail_closed")["kind"], "deny");
}

// ---- async client against a scripted in-process service ----

use std::sync::{Arc, Mutex};
use tokio::io::DuplexStream;

/// A scripted service: for each request it logs the phase, then acts on the
/// scripted reply (a verdict, or a control string `close`/`garbage`/`noreply`).
/// Always signals EOF on exit so the client's receive() unblocks.
async fn service<R, W>(mut reader: R, mut writer: W, replies: Vec<Value>, log: Arc<Mutex<Vec<String>>>)
where
    R: MessageRead,
    W: MessageWrite,
{
    let mut index = 0;
    while let Ok(Some(raw)) = reader.receive().await {
        let request = jsonrpc::decode(&raw).unwrap();
        log.lock().unwrap().push(request["phase"].as_str().unwrap().to_string());
        let reply = replies.get(index).cloned().unwrap_or_else(|| json!("close"));
        index += 1;
        match reply.as_str() {
            Some("close") => break,
            Some("garbage") => {
                let _ = writer.send(b"{not json").await;
            }
            Some("noreply") => {}
            _ => {
                let _ = writer.send(&jsonrpc::encode(&reply)).await;
            }
        }
    }
    let _ = writer.send_eof().await;
}

struct Driven {
    results: Vec<Value>,
    log: Vec<String>,
}

async fn drive(replies: Vec<Value>, contents: Vec<&'static [u8]>, use_cache: bool, preview: usize, budget: usize, deadline: Option<Duration>) -> Driven {
    let (client_w, service_r) = duplex(CAP);
    let (service_w, client_r) = duplex(CAP);
    let log = Arc::new(Mutex::new(Vec::new()));
    let svc = service(LineReader::new(BufReader::new(service_r)), LineWriter::new(service_w), replies, log.clone());

    let driver = async {
        // The client owns client_w; dropping it at block end EOFs the service.
        let mut client: CalloutClient<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>> =
            CalloutClient::new(LineReader::new(BufReader::new(client_r)), LineWriter::new(client_w))
                .with_preview(preview)
                .with_budget(budget);
        if let Some(deadline) = deadline {
            client = client.with_deadline(deadline);
        }
        let mut cache = VerdictCache::new();
        let mut results = Vec::new();
        for content in &contents {
            results.push(if use_cache {
                client.scan(&ctx(), content, Some(&mut cache)).await
            } else {
                client.scan(&ctx(), content, None).await
            });
        }
        results
    };

    let (_svc, results) = tokio::join!(svc, driver);
    let log = log.lock().unwrap().clone();
    Driven { results, log }
}

#[tokio::test]
async fn allow_via_service() {
    let out = drive(vec![json!({"verdict": "allow"})], vec![b"payload"], false, 0, 0, None).await;
    assert_eq!(out.results, vec![json!({"kind": "allow"})]);
    assert_eq!(out.log, vec!["preview"]);
}

#[tokio::test]
async fn early_deny_in_preview_skips_body() {
    let out = drive(vec![json!({"verdict": "deny", "reason": "bad"})], vec![b"hello world"], false, 3, 0, None).await;
    assert_eq!(out.results[0]["kind"], "deny");
    assert_eq!(out.log, vec!["preview"]);
}

#[tokio::test]
async fn continue_escalates_to_body() {
    let out = drive(vec![json!({"verdict": "continue"}), json!({"verdict": "allow"})], vec![b"hello world"], false, 3, 0, None).await;
    assert_eq!(out.results[0], json!({"kind": "allow"}));
    assert_eq!(out.log, vec!["preview", "body"]);
}

#[tokio::test]
async fn cache_avoids_rescan() {
    let out = drive(vec![json!({"verdict": "allow"})], vec![b"same", b"same"], true, 0, 0, None).await;
    assert_eq!(out.results, vec![json!({"kind": "allow"}), json!({"kind": "allow"})]);
    assert_eq!(out.log, vec!["preview"]);
}

#[tokio::test]
async fn budget_rejects_without_calling() {
    let out = drive(vec![json!({"verdict": "allow"})], vec![b"too big"], false, 0, 2, None).await;
    assert_eq!(out.results[0]["kind"], "deny");
    assert!(out.log.is_empty());
}

#[tokio::test]
async fn service_close_is_failure_policy() {
    let out = drive(vec![json!("close")], vec![b"payload"], false, 0, 0, None).await;
    assert_eq!(out.results[0], json!({"kind": "deny", "reason": "callout closed"}));
}

#[tokio::test]
async fn garbage_response_is_decode_error() {
    let out = drive(vec![json!("garbage")], vec![b"payload"], false, 0, 0, None).await;
    assert_eq!(out.results[0], json!({"kind": "deny", "reason": "callout decode error"}));
}

#[tokio::test]
async fn deadline_bounds_a_hung_scanner() {
    let out = drive(vec![json!("noreply")], vec![b"payload"], false, 0, 0, Some(Duration::from_millis(50))).await;
    assert_eq!(out.results[0], json!({"kind": "deny", "reason": "callout deadline exceeded"}));
}

#[tokio::test]
async fn transport_send_error_is_failure_policy() {
    struct Broken;
    impl MessageRead for Broken {
        async fn receive(&mut self) -> io::Result<Option<Vec<u8>>> {
            Ok(None)
        }
    }
    impl MessageWrite for Broken {
        async fn send(&mut self, _payload: &[u8]) -> io::Result<()> {
            Err(io::Error::other("boom"))
        }
        async fn send_eof(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut client = CalloutClient::new(Broken, Broken).with_failure_policy(yamp::filters::FAIL_OPEN);
    let verdict = client.scan(&ctx(), b"payload", None).await;
    assert_eq!(verdict, json!({"kind": "allow", "reason": "callout transport error"}));
}
