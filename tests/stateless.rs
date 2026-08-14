//! δ3 stateless forwarder integration tests (Rust arm). Mirrors the Python arm.

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::instrument::within_budget;
use yamp::jsonrpc::{INVALID_PARAMS, METHOD_NOT_FOUND};
use yamp::stateless::{
    decode_request, decode_response, encode_request, encode_response, StatelessBackend,
    StatelessForwarder, StatelessRequest, StatelessResponse, CLIENT_INFO_META_KEY,
};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type LineBackend =
    StatelessBackend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>>;

fn line_backend(id: &str, reader: DuplexStream, writer: DuplexStream) -> LineBackend {
    StatelessBackend::new(id, LineReader::new(BufReader::new(reader)), LineWriter::new(writer)).unwrap()
}

fn proxy_info() -> Value {
    json!({ "name": "yamp", "version": "0.0.0" })
}

async fn mock_backend<R, W>(
    mut reader: R,
    mut writer: W,
    name: &'static str,
    tools: Vec<&'static str>,
    log: Arc<Mutex<Vec<StatelessRequest>>>,
) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(raw) => {
                let request = decode_request(&raw)?;
                log.lock().unwrap().push(request.clone());
                let response = match request.method.as_str() {
                    "server/discover" => {
                        let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                        StatelessResponse {
                            meta: json!({ "backend": name }),
                            body: json!({ "tools": listed }).to_string(),
                        }
                    }
                    "tools/call" => StatelessResponse {
                        meta: json!({
                            "backend": name,
                            "echoed_client": request.meta.get(CLIENT_INFO_META_KEY),
                        }),
                        body: format!("RESULT:{}:{}", request.name.clone().unwrap_or_default(), request.body),
                    },
                    _ => StatelessResponse { meta: json!({}), body: String::new() },
                };
                writer.send(&encode_response(&response)).await?;
            }
        }
    }
}

struct Harness {
    client_w: DuplexStream,
    client_r: DuplexStream,
}

#[tokio::test]
async fn wire_round_trips() {
    let req = StatelessRequest::new("tools/call", Some("gh__x".into()), json!({ "k": 1 }), "body");
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    let resp = StatelessResponse { meta: json!({ "m": 2 }), body: "b".into() };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
}

/// Build a forwarder over two backends (gh: a,b / gl: c) and return the client
/// duplex plus the backend logs and the joined server future. The return type
/// is inherently a tuple of generic transport handles plus an opaque future;
/// aliasing an `impl Future` is not possible, so the lint is suppressed here.
#[allow(clippy::type_complexity)]
fn build() -> (
    Harness,
    Arc<Mutex<Vec<StatelessRequest>>>,
    Arc<Mutex<Vec<StatelessRequest>>>,
    impl std::future::Future<Output = io::Result<()>>,
) {
    let (client_w, forwarder_reads_client) = duplex(CAP);
    let (forwarder_writes_client, client_r) = duplex(CAP);
    let (fwd_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, fwd_reads_gh) = duplex(CAP);
    let (fwd_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, fwd_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", fwd_reads_gh, fwd_to_gh),
        line_backend("gl", fwd_reads_gl, fwd_to_gl),
    ];
    let forwarder = StatelessForwarder::new(
        LineReader::new(BufReader::new(forwarder_reads_client)),
        LineWriter::new(forwarder_writes_client),
        backends,
    );

    let gh_log = Arc::new(Mutex::new(Vec::new()));
    let gl_log = Arc::new(Mutex::new(Vec::new()));
    let gh = mock_backend(
        LineReader::new(BufReader::new(gh_reads)),
        LineWriter::new(gh_writes),
        "gh",
        vec!["a", "b"],
        gh_log.clone(),
    );
    let gl = mock_backend(
        LineReader::new(BufReader::new(gl_reads)),
        LineWriter::new(gl_writes),
        "gl",
        vec!["c"],
        gl_log.clone(),
    );

    let server = async move {
        tokio::try_join!(forwarder.serve(), gh, gl)?;
        Ok(())
    };
    (Harness { client_w, client_r }, gh_log, gl_log, server)
}

async fn exchange(
    cw: &mut LineWriter<DuplexStream>,
    cr: &mut LineReader<BufReader<DuplexStream>>,
    request: &StatelessRequest,
) -> io::Result<StatelessResponse> {
    cw.send(&encode_request(request)).await?;
    decode_response(&cr.receive().await?.unwrap())
}

#[tokio::test]
async fn discover_meta_routing_gates() {
    let (harness, gh_log, gl_log, server) = build();
    let Harness { client_w, client_r } = harness;

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));

        let discover = exchange(&mut cw, &mut cr, &StatelessRequest::new("server/discover", None, json!({}), "")).await?;
        let call = exchange(
            &mut cw,
            &mut cr,
            &StatelessRequest::new("tools/call", Some("gh__a".into()), json!({ "trace": "t1" }), "PAYLOAD"),
        )
        .await?;
        let opaque = "NOT-JSON <<{[}>>";
        let opaque_call = exchange(
            &mut cw,
            &mut cr,
            &StatelessRequest::new("tools/call", Some("gl__c".into()), json!({}), opaque),
        )
        .await?;
        let bad = exchange(&mut cw, &mut cr, &StatelessRequest::new("tools/call", Some("nope".into()), json!({}), "")).await?;
        let other = exchange(&mut cw, &mut cr, &StatelessRequest::new("resources/read", None, json!({}), "")).await?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((discover, call, opaque_call, bad, other, opaque.to_string()))
    };

    let (_server, (discover, call, opaque_call, bad, other, opaque)) =
        tokio::try_join!(server, client).unwrap();

    // gate 1: discover composition, namespaced across backends.
    let names: BTreeSet<String> = serde_json::from_str::<Value>(&discover.body)
        .unwrap()["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> =
        ["gh__a", "gh__b", "gl__c"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);

    // gate 2: proxy identity injected on forward, client meta preserved,
    // backend meta forwarded back. (discover fans out first, so select the
    // tools/call entry rather than index 0.)
    let gh_guard = gh_log.lock().unwrap();
    let gh_call = gh_guard.iter().find(|r| r.method == "tools/call").unwrap();
    assert_eq!(gh_call.meta[CLIENT_INFO_META_KEY], proxy_info());
    assert_eq!(gh_call.meta["trace"], "t1");
    assert_eq!(call.meta["backend"], "gh");
    assert_eq!(call.meta["echoed_client"], proxy_info());

    // gate 3: routed by header, body forwarded unchanged (never parsed).
    let gl_guard = gl_log.lock().unwrap();
    let gl_call = gl_guard.iter().find(|r| r.method == "tools/call").unwrap();
    assert_eq!(gl_call.body, opaque);
    assert_eq!(opaque_call.body, format!("RESULT:c:{opaque}"));
    drop(gh_guard);
    drop(gl_guard);

    // gate 4: no handshake was ever performed against the backends.
    assert!(gh_log.lock().unwrap().iter().all(|r| r.method != "initialize"));
    assert!(gl_log.lock().unwrap().iter().all(|r| r.method != "initialize"));

    // error paths
    assert_eq!(
        serde_json::from_str::<Value>(&bad.body).unwrap()["error"]["code"],
        INVALID_PARAMS
    );
    assert_eq!(
        serde_json::from_str::<Value>(&other.body).unwrap()["error"]["code"],
        METHOD_NOT_FOUND
    );
}

#[tokio::test]
async fn version_negotiation_matrix() {
    use yamp::version::{
        PROTOCOL_VERSION_META_KEY, STATELESS_PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS,
        UNSUPPORTED_PROTOCOL_VERSION,
    };

    let (harness, gh_log, _gl_log, server) = build();
    let Harness { client_w, client_r } = harness;

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        // Omitted: accepted, defaulted, pinned in the forwarded _meta.
        let omitted =
            exchange(&mut cw, &mut cr, &StatelessRequest::new("tools/call", Some("gh__a".into()), json!({}), "x")).await?;
        // Supported: echoed through.
        let supported = exchange(
            &mut cw,
            &mut cr,
            &StatelessRequest::new(
                "tools/call",
                Some("gh__b".into()),
                json!({ PROTOCOL_VERSION_META_KEY: STATELESS_PROTOCOL_VERSION }),
                "y",
            ),
        )
        .await?;
        // Unsupported: rejected before routing.
        let unsupported = exchange(
            &mut cw,
            &mut cr,
            &StatelessRequest::new(
                "tools/call",
                Some("gh__a".into()),
                json!({ PROTOCOL_VERSION_META_KEY: "2024-11-05" }),
                "z",
            ),
        )
        .await?;
        cw.send_eof().await?;
        Ok::<_, io::Error>((omitted, supported, unsupported))
    };

    let (_server, (omitted, supported, unsupported)) = tokio::try_join!(server, client).unwrap();

    // Both accepted calls reached the backend with the version pinned in _meta.
    let gh_guard = gh_log.lock().unwrap();
    let calls: Vec<&StatelessRequest> = gh_guard.iter().filter(|r| r.method == "tools/call").collect();
    assert_eq!(calls.len(), 2);
    for call in &calls {
        assert_eq!(call.meta[PROTOCOL_VERSION_META_KEY], STATELESS_PROTOCOL_VERSION);
    }
    assert!(omitted.body.starts_with("RESULT:a"));
    assert!(supported.body.starts_with("RESULT:b"));

    // The unsupported request never reached a backend and carries -32004 with data.
    let error = &serde_json::from_str::<Value>(&unsupported.body).unwrap()["error"];
    assert_eq!(error["code"], UNSUPPORTED_PROTOCOL_VERSION);
    assert_eq!(error["data"]["requested"], "2024-11-05");
    let supported_set: Vec<String> = error["data"]["supported"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(supported_set, SUPPORTED_PROTOCOL_VERSIONS.to_vec());
}

#[tokio::test]
async fn invalid_backend_id_rejected() {
    let (a, _b) = duplex(CAP);
    let (c, _d) = duplex(CAP);
    let result = StatelessBackend::new("bad id!", LineReader::new(BufReader::new(a)), LineWriter::new(c));
    assert!(result.is_err());
}

#[tokio::test]
async fn stateless_latency_within_budget() {
    let (harness, _gh, _gl, server) = build();
    let Harness { client_w, client_r } = harness;

    let driver = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        let request = encode_request(&StatelessRequest::new("tools/call", Some("gh__a".into()), json!({}), "x"));
        for _ in 0..50 {
            cw.send(&request).await?;
            cr.receive().await?;
        }
        let mut samples = Vec::new();
        for _ in 0..300 {
            let start = Instant::now();
            cw.send(&request).await?;
            cr.receive().await?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        cw.send_eof().await?;
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[samples.len() / 2];
        let under = samples.iter().filter(|&&x| within_budget(x)).count() as f64 / samples.len() as f64;
        println!("[latency δ3 stateless] median={median:.4}ms within={under:.3}");
        assert!(within_budget(median));
        assert!(under >= 0.99);
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(server, driver).unwrap();
}
