//! Router integration for server variants and variant-bound cursors (SEP-2053).
//!
//! Drives the served ForwardRouter against mock backends that advertise variants
//! and paginate, asserting the four proxy obligations: compose the offered
//! variants, forward the selection, mint a variant-bound composite cursor, and
//! reject a continuation used under the wrong variant. The composite cursor is
//! deterministic, so a paginating test precomputes the page-2 cursor rather than
//! driving the client interactively. Mirrors the Python arm's
//! test_router_variants.py.

use std::collections::{BTreeMap, BTreeSet};
use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc::{self, INVALID_PARAMS};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};
use yamp::variants;

const CAP: usize = 1 << 16;

struct Spec {
    id: &'static str,
    variants: Vec<&'static str>,
    paginates: bool,
}

async fn variant_mock(
    mut reader: LineReader<BufReader<DuplexStream>>,
    mut writer: LineWriter<DuplexStream>,
    name: String,
    variant_ids: Vec<String>,
    paginates: bool,
) -> io::Result<()> {
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    let mut caps = json!({ "tools": {} });
    if !variant_ids.is_empty() {
        let listed: Vec<Value> = variant_ids.iter().map(|v| json!({ "id": v })).collect();
        caps.as_object_mut().unwrap().insert(
            "extensions".to_string(),
            json!({ variants::EXTENSION_ID: { "availableVariants": listed } }),
        );
    }
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": caps, "serverInfo": { "name": name } },
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
                if jsonrpc::method_of(&message) == Some("tools/list") {
                    let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
                    let variant = params
                        .get("_meta")
                        .and_then(|m| m.get(variants::SERVER_VARIANT_META_KEY))
                        .and_then(Value::as_str)
                        .unwrap_or("none");
                    let cursor = params.get("cursor").and_then(Value::as_str);
                    let body = if paginates && cursor.is_none() {
                        json!({ "tools": [ { "name": format!("{name}_{variant}_p1") } ], "nextCursor": "backend-p2" })
                    } else if paginates && cursor == Some("backend-p2") {
                        json!({ "tools": [ { "name": format!("{name}_{variant}_p2") } ] })
                    } else {
                        json!({ "tools": [ { "name": format!("{name}_{variant}") } ] })
                    };
                    writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": body }))).await?;
                }
            }
        }
    }
}

/// Run one router session: initialize, then send each request in order and
/// collect the responses. Returns `(init_result, responses)`.
async fn run(specs: Vec<Spec>, requests: Vec<Value>) -> (Value, Vec<Value>) {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let mut backends = Vec::new();
    let mut mocks = Vec::new();
    for spec in &specs {
        let (router_to_b, b_reads) = duplex(CAP);
        let (b_writes, router_reads_b) = duplex(CAP);
        backends.push(Backend::new(spec.id, LineReader::new(BufReader::new(router_reads_b)), LineWriter::new(router_to_b)).unwrap());
        mocks.push(variant_mock(
            LineReader::new(BufReader::new(b_reads)),
            LineWriter::new(b_writes),
            spec.id.to_string(),
            spec.variants.iter().map(|s| s.to_string()).collect(),
            spec.paginates,
        ));
    }
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );
    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        let init = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let mut responses = Vec::new();
        for params in &requests {
            cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "r", "method": "tools/list", "params": params }))).await?;
            responses.push(jsonrpc::decode(&cr.receive().await?.unwrap())?);
        }
        cw.send_eof().await?;
        Ok::<(Value, Vec<Value>), io::Error>((init, responses))
    };
    let mocks_fut = futures::future::join_all(mocks);
    let (_r, _m, out) = tokio::join!(router.serve(), mocks_fut, client);
    out.unwrap()
}

fn spec(id: &'static str, variants: &[&'static str], paginates: bool) -> Spec {
    Spec { id, variants: variants.to_vec(), paginates }
}

fn offered_ids(init: &Value) -> Vec<String> {
    init["result"]["capabilities"]["extensions"][variants::EXTENSION_ID]["availableVariants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect()
}

fn names(response: &Value) -> BTreeSet<String> {
    response["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect()
}

fn meta(variant: &str) -> Value {
    let mut m = serde_json::Map::new();
    m.insert(variants::SERVER_VARIANT_META_KEY.to_string(), json!(variant));
    json!({ "_meta": m })
}

fn meta_cursor(variant: &str, cursor: &str) -> Value {
    let mut m = serde_json::Map::new();
    m.insert(variants::SERVER_VARIANT_META_KEY.to_string(), json!(variant));
    json!({ "_meta": m, "cursor": cursor })
}

fn page2_cursor(variant: &str) -> String {
    let mut cursors = BTreeMap::new();
    cursors.insert("b0".to_string(), "backend-p2".to_string());
    variants::bind_cursor(Some(variant), &cursors)
}

#[tokio::test]
async fn handshake_composes_offered_variants_by_intersection() {
    let (init, _) = run(vec![spec("b0", &["a", "b", "c"], false), spec("b1", &["b", "c", "d"], false)], vec![]).await;
    assert_eq!(offered_ids(&init), vec!["b", "c"]);
}

#[tokio::test]
async fn no_variant_extension_when_disjoint() {
    let (init, _) = run(vec![spec("b0", &["a"], false), spec("b1", &["b"], false)], vec![]).await;
    let extensions = &init["result"]["capabilities"]["extensions"];
    assert!(extensions.get(variants::EXTENSION_ID).is_none());
}

#[tokio::test]
async fn selected_variant_forwarded_to_backends() {
    let (_, responses) = run(
        vec![spec("b0", &["a", "b"], false), spec("b1", &["a", "b"], false)],
        vec![meta("b")],
    )
    .await;
    let got = names(&responses[0]);
    assert!(got.contains("b0__b0_b") && got.contains("b1__b1_b"));
}

#[tokio::test]
async fn unknown_variant_rejected() {
    let (_, responses) = run(vec![spec("b0", &["a", "b"], false)], vec![meta("zzz")]).await;
    assert_eq!(responses[0]["error"]["code"], INVALID_PARAMS);
    assert_eq!(responses[0]["error"]["data"]["availableVariants"], json!(["a", "b"]));
}

#[tokio::test]
async fn variant_selected_but_unsupported_rejected() {
    let (_, responses) = run(vec![spec("b0", &[], false)], vec![meta("b")]).await;
    assert!(responses[0]["error"]["message"].as_str().unwrap().contains("not supported"));
}

#[tokio::test]
async fn composite_cursor_paginates_and_binds_variant() {
    let cursor = page2_cursor("a");
    let (_, responses) = run(
        vec![spec("b0", &["a", "b"], true), spec("b1", &["a", "b"], false)],
        vec![meta("a"), meta_cursor("a", &cursor)],
    )
    .await;
    let minted = responses[0]["result"]["nextCursor"].as_str().unwrap();
    assert_eq!(minted, page2_cursor("a"));
    let (variant, backends) = variants::resolve_cursor(&responses[0]["result"]["nextCursor"]).unwrap();
    assert_eq!(variant, Some("a".to_string()));
    assert_eq!(backends.keys().cloned().collect::<Vec<_>>(), vec!["b0".to_string()]);
    assert_eq!(names(&responses[1]), BTreeSet::from(["b0__b0_a_p2".to_string()]));
    assert!(responses[1]["result"].get("nextCursor").is_none());
}

#[tokio::test]
async fn cursor_rejected_under_wrong_variant() {
    let cursor = page2_cursor("a");
    let (_, responses) = run(
        vec![spec("b0", &["a", "b"], true)],
        vec![meta("a"), meta_cursor("b", &cursor)],
    )
    .await;
    assert_eq!(responses[1]["error"]["message"], "Cursor invalid for requested variant");
    assert_eq!(responses[1]["error"]["data"], json!({ "cursorVariant": "a", "requestedVariant": "b" }));
}

#[tokio::test]
async fn unknown_cursor_rejected_by_aggregator() {
    let (_, responses) =
        run(vec![spec("b0", &[], false), spec("b1", &[], false)], vec![json!({ "cursor": "not-a-proxy-cursor" })]).await;
    assert_eq!(responses[0]["error"]["message"], "unknown cursor");
}

#[tokio::test]
async fn single_backend_passes_raw_cursor_through() {
    // With one backend there is nothing to disambiguate (SEP §5.3), so a raw
    // backend cursor is forwarded straight through rather than rejected.
    let (_, responses) = run(vec![spec("b0", &[], true)], vec![json!({ "cursor": "backend-p2" })]).await;
    assert_eq!(names(&responses[0]), BTreeSet::from(["b0_none_p2".to_string()]));
}
