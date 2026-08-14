//! δ2 router integration tests (Rust arm). Mirrors the Python arm.

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader, DuplexStream};

use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::instrument::within_budget;
use yamp::jsonrpc::{self, INVALID_PARAMS};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

type LineBackend = Backend<LineReader<BufReader<DuplexStream>>, LineWriter<DuplexStream>>;

fn line_backend(id: &str, reader: DuplexStream, writer: DuplexStream) -> LineBackend {
    Backend::new(id, LineReader::new(BufReader::new(reader)), LineWriter::new(writer)).unwrap()
}

async fn mock_backend<R, W>(
    mut reader: R,
    mut writer: W,
    name: &'static str,
    tools: Vec<&'static str>,
    calls: Arc<Mutex<Vec<String>>>,
) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0",
            "id": init["id"],
            "result": {
                "protocolVersion": PROXY_PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": format!("{name}-server") },
            },
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
                        let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                        writer
                            .send(&jsonrpc::encode(&json!({
                                "jsonrpc": "2.0", "id": message["id"], "result": { "tools": listed },
                            })))
                            .await?;
                    }
                    Some("tools/call") => {
                        let tool = message["params"]["name"].as_str().unwrap().to_string();
                        calls.lock().unwrap().push(tool.clone());
                        writer
                            .send(&jsonrpc::encode(&json!({
                                "jsonrpc": "2.0", "id": message["id"],
                                "result": { "content": [ { "type": "text", "text": format!("{name}:{tool}") } ] },
                            })))
                            .await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn client_call(
    cw: &mut LineWriter<DuplexStream>,
    cr: &mut LineReader<BufReader<DuplexStream>>,
    id: &str,
    method: &str,
    params: Value,
) -> io::Result<Value> {
    cw.send(&jsonrpc::encode(&json!({
        "jsonrpc": "2.0", "id": id, "method": method, "params": params,
    })))
    .await?;
    jsonrpc::decode(&cr.receive().await?.unwrap())
}

#[tokio::test]
async fn full_router_session() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);
    let (router_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, router_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", router_reads_gh, router_to_gh),
        line_backend("gl", router_reads_gl, router_to_gl),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );

    let gh_calls = Arc::new(Mutex::new(Vec::new()));
    let gl_calls = Arc::new(Mutex::new(Vec::new()));
    let gh = mock_backend(
        LineReader::new(BufReader::new(gh_reads)),
        LineWriter::new(gh_writes),
        "gh",
        vec!["create_issue", "search"],
        gh_calls.clone(),
    );
    let gl = mock_backend(
        LineReader::new(BufReader::new(gl_reads)),
        LineWriter::new(gl_writes),
        "gl",
        vec!["create_issue"],
        gl_calls.clone(),
    );

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));

        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": { "name": "c" } },
        })))
        .await?;
        let init = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })))
            .await?;

        let listing = client_call(&mut cw, &mut cr, "l", "tools/list", json!({})).await?;
        let routed = client_call(&mut cw, &mut cr, "s", "tools/call", json!({ "name": "gh__search", "arguments": {} })).await?;
        let no_delim = client_call(&mut cw, &mut cr, "u", "tools/call", json!({ "name": "nope", "arguments": {} })).await?;
        let bad_backend = client_call(&mut cw, &mut cr, "z", "tools/call", json!({ "name": "zz__x", "arguments": {} })).await?;

        cw.send_eof().await?;
        Ok::<(Value, Value, Value, Value, Value), io::Error>((init, listing, routed, no_delim, bad_backend))
    };

    let (_r, _gh, _gl, (init, listing, routed, no_delim, bad_backend)) =
        tokio::try_join!(router.serve(), gh, gl, client).unwrap();

    let names: BTreeSet<String> = listing["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = ["gh__create_issue", "gh__search", "gl__create_issue"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(names, expected); // gates 1, 3, 5

    assert_eq!(*gh_calls.lock().unwrap(), vec!["search".to_string()]); // gate 2
    assert!(gl_calls.lock().unwrap().is_empty());
    assert_eq!(routed["result"]["content"][0]["text"], "gh:search");

    assert_eq!(no_delim["error"]["code"], INVALID_PARAMS); // gate 4
    assert_eq!(bad_backend["error"]["code"], INVALID_PARAMS);

    assert!(init["result"]["capabilities"]["tools"].is_object());
}

#[tokio::test]
async fn server_discover_composes_backend_tools() {
    // SEP §2.1: the router answers server/discover by composing the same
    // namespaced tool surface as tools/list, from all healthy backends.
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);
    let (router_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, router_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", router_reads_gh, router_to_gh),
        line_backend("gl", router_reads_gl, router_to_gl),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );

    let gh = mock_backend(
        LineReader::new(BufReader::new(gh_reads)),
        LineWriter::new(gh_writes),
        "gh",
        vec!["create_issue", "search"],
        Arc::new(Mutex::new(Vec::new())),
    );
    let gl = mock_backend(
        LineReader::new(BufReader::new(gl_reads)),
        LineWriter::new(gl_writes),
        "gl",
        vec!["create_issue"],
        Arc::new(Mutex::new(Vec::new())),
    );

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": { "name": "c" } },
        })))
        .await?;
        cr.receive().await?; // init response
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })))
            .await?;
        let discover = client_call(&mut cw, &mut cr, "d", "server/discover", json!({})).await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(discover)
    };

    let (_r, _gh, _gl, discover) = tokio::try_join!(router.serve(), gh, gl, client).unwrap();

    let names: BTreeSet<String> = discover["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = ["gh__create_issue", "gh__search", "gl__create_issue"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(names, expected);
}

#[tokio::test]
async fn priority_strategy_drops_lower_priority_collision() {
    // gh and gl both offer 'create_issue'; with priority [gh, gl] only gh's copy
    // survives (SEP §3.4). Names stay prefixed, so reverse resolution still works.
    use yamp::config::Namespacing;

    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);
    let (router_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, router_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", router_reads_gh, router_to_gh),
        line_backend("gl", router_reads_gl, router_to_gl),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    )
    .set_namespacing(Namespacing {
        strategy: "priority".to_string(),
        overrides: Default::default(),
        priority: vec!["gh".to_string(), "gl".to_string()],
    });

    let gh = mock_backend(
        LineReader::new(BufReader::new(gh_reads)),
        LineWriter::new(gh_writes),
        "gh",
        vec!["create_issue", "search"],
        Arc::new(Mutex::new(Vec::new())),
    );
    let gl = mock_backend(
        LineReader::new(BufReader::new(gl_reads)),
        LineWriter::new(gl_writes),
        "gl",
        vec!["create_issue"],
        Arc::new(Mutex::new(Vec::new())),
    );

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let listing = client_call(&mut cw, &mut cr, "l", "tools/list", json!({})).await?;
        let routed = client_call(&mut cw, &mut cr, "s", "tools/call", json!({ "name": "gh__create_issue", "arguments": {} })).await?;
        cw.send_eof().await?;
        Ok::<(Value, Value), io::Error>((listing, routed))
    };

    let (_r, _gh, _gl, (listing, routed)) = tokio::try_join!(router.serve(), gh, gl, client).unwrap();
    let names: BTreeSet<String> = listing["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = ["gh__create_issue", "gh__search"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected); // gl__create_issue dropped by priority
    assert_eq!(routed["result"]["content"][0]["text"], "gh:create_issue");
}

async fn list_counting_backend(
    mut reader: LineReader<BufReader<DuplexStream>>,
    mut writer: LineWriter<DuplexStream>,
    name: &'static str,
    tools: Vec<&'static str>,
    count: Arc<Mutex<usize>>,
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
                let message = jsonrpc::decode(&raw)?;
                if jsonrpc::method_of(&message) == Some("tools/list") {
                    *count.lock().unwrap() += 1;
                    let listed: Vec<Value> = tools.iter().map(|t| json!({ "name": t })).collect();
                    writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": listed } }))).await?;
                }
            }
        }
    }
}

#[tokio::test]
async fn filter_keyword_preselect_and_name_patterns() {
    // gh declares keyword "git", gl declares "chat". A filtered tools/list with
    // keyword "git" must skip gl (fan-out drops) and name patterns trim the
    // composed surface (SEP-2564/2614).
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);
    let (router_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, router_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", router_reads_gh, router_to_gh).with_keywords(vec!["git".to_string()]),
        line_backend("gl", router_reads_gl, router_to_gl).with_keywords(vec!["chat".to_string()]),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );
    let gh_count = Arc::new(Mutex::new(0));
    let gl_count = Arc::new(Mutex::new(0));
    let gh = list_counting_backend(LineReader::new(BufReader::new(gh_reads)), LineWriter::new(gh_writes), "gh", vec!["create_issue", "search"], gh_count.clone());
    let gl = list_counting_backend(LineReader::new(BufReader::new(gl_reads)), LineWriter::new(gl_writes), "gl", vec!["create_issue"], gl_count.clone());

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let listing = client_call(&mut cw, &mut cr, "l", "tools/list", json!({ "filter": { "keywords": ["git"], "namePatterns": ["gh__create*"] } })).await?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(listing)
    };

    let (_r, _gh, _gl, listing) = tokio::try_join!(router.serve(), gh, gl, client).unwrap();
    assert_eq!(*gh_count.lock().unwrap(), 1);
    assert_eq!(*gl_count.lock().unwrap(), 0); // gl skipped by keyword pre-select
    let names: BTreeSet<String> = listing["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect();
    assert_eq!(names, ["gh__create_issue"].iter().map(|s| s.to_string()).collect());
}

#[tokio::test]
async fn invalid_backend_id_rejected() {
    let (a, _b) = duplex(CAP);
    let (c, _d) = duplex(CAP);
    let result = Backend::new("bad id!", LineReader::new(BufReader::new(a)), LineWriter::new(c));
    assert!(result.is_err());
}

#[tokio::test]
async fn fanout_latency_within_budget() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);
    let (router_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, router_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", router_reads_gh, router_to_gh),
        line_backend("gl", router_reads_gl, router_to_gl),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    );
    let gh = mock_backend(
        LineReader::new(BufReader::new(gh_reads)),
        LineWriter::new(gh_writes),
        "gh",
        vec!["a"],
        Arc::new(Mutex::new(Vec::new())),
    );
    let gl = mock_backend(
        LineReader::new(BufReader::new(gl_reads)),
        LineWriter::new(gl_writes),
        "gl",
        vec!["b"],
        Arc::new(Mutex::new(Vec::new())),
    );

    let driver = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })))
            .await?;

        let request = jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "t", "method": "tools/list", "params": {} }));
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
        println!("[latency δ2 fanout] median={median:.4}ms within={under:.3}");
        assert!(within_budget(median));
        assert!(under >= 0.99);
        Ok::<(), io::Error>(())
    };

    tokio::try_join!(router.serve(), gh, gl, driver).unwrap();
}

/// Spin up gh (create_issue, search) + gl (create_issue) under a collision
/// strategy, replay client actions, and return the responses plus each backend's
/// recorded tools/call names. Mirrors the Python `_run_namespacing` helper.
async fn run_namespacing(
    namespacing: yamp::config::Namespacing,
    actions: Vec<(&'static str, &'static str, Value)>,
) -> (Vec<Value>, Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<String>>>) {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_gh, gh_reads) = duplex(CAP);
    let (gh_writes, router_reads_gh) = duplex(CAP);
    let (router_to_gl, gl_reads) = duplex(CAP);
    let (gl_writes, router_reads_gl) = duplex(CAP);

    let backends = vec![
        line_backend("gh", router_reads_gh, router_to_gh),
        line_backend("gl", router_reads_gl, router_to_gl),
    ];
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        backends,
    )
    .set_namespacing(namespacing);

    let gh_calls = Arc::new(Mutex::new(Vec::new()));
    let gl_calls = Arc::new(Mutex::new(Vec::new()));
    let gh = mock_backend(
        LineReader::new(BufReader::new(gh_reads)),
        LineWriter::new(gh_writes),
        "gh",
        vec!["create_issue", "search"],
        gh_calls.clone(),
    );
    let gl = mock_backend(
        LineReader::new(BufReader::new(gl_reads)),
        LineWriter::new(gl_writes),
        "gl",
        vec!["create_issue"],
        gl_calls.clone(),
    );

    let client = async move {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        cw.send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": "c1", "method": "initialize",
            "params": { "protocolVersion": "x", "capabilities": {}, "clientInfo": {} },
        })))
        .await?;
        cr.receive().await?;
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))).await?;
        let mut out = Vec::new();
        for (id, method, params) in &actions {
            out.push(client_call(&mut cw, &mut cr, id, method, params.clone()).await?);
        }
        cw.send_eof().await?;
        Ok::<Vec<Value>, io::Error>(out)
    };

    let (_r, _gh, _gl, responses) = tokio::try_join!(router.serve(), gh, gl, client).unwrap();
    (responses, gh_calls, gl_calls)
}

fn manual_ns(overrides: &[(&str, &str)]) -> yamp::config::Namespacing {
    yamp::config::Namespacing {
        strategy: "manual".to_string(),
        overrides: overrides.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        priority: Vec::new(),
    }
}

#[tokio::test]
async fn manual_strategy_renames_and_routes() {
    // gh__create_issue renamed to new_issue; a tools/call by the exposed name
    // reverse-resolves to gh, and an unknown exposed name is rejected (SEP §3.4).
    let (responses, gh_calls, _gl) = run_namespacing(
        manual_ns(&[("gh__create_issue", "new_issue")]),
        vec![
            ("l", "tools/list", json!({})),
            ("s", "tools/call", json!({ "name": "new_issue", "arguments": {} })),
            ("g", "tools/call", json!({ "name": "gl__create_issue", "arguments": {} })),
            ("u", "tools/call", json!({ "name": "missing", "arguments": {} })),
        ],
    )
    .await;
    let names: BTreeSet<String> = responses[0]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = ["new_issue", "gh__search", "gl__create_issue"].iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);
    assert_eq!(responses[1]["result"]["content"][0]["text"], "gh:create_issue");
    assert_eq!(*gh_calls.lock().unwrap(), vec!["create_issue".to_string()]);
    assert_eq!(responses[2]["result"]["content"][0]["text"], "gl:create_issue");
    assert_eq!(responses[3]["error"]["code"], INVALID_PARAMS);
}

#[tokio::test]
async fn manual_strategy_rejects_unresolved_collision() {
    // Two names mapping to one exposed name is an unresolved collision: tools/list
    // is rejected rather than served as a silent duplicate (SEP §3.4).
    let (responses, _gh, _gl) = run_namespacing(
        manual_ns(&[("gh__search", "dup"), ("gl__create_issue", "dup")]),
        vec![("l", "tools/list", json!({}))],
    )
    .await;
    assert!(responses[0]["error"].is_object());
    assert!(responses[0]["error"]["message"].as_str().unwrap().contains("manual collision"));
}

fn passthrough_ns() -> yamp::config::Namespacing {
    yamp::config::Namespacing {
        strategy: "passthrough".to_string(),
        overrides: Default::default(),
        priority: Vec::new(),
    }
}

#[tokio::test]
async fn passthrough_strategy_keeps_originals_and_routes() {
    // passthrough keeps original names (duplicates and all); a tools/call resolves
    // through the reverse map to the first backend offering the name (SEP §3.4).
    let (responses, _gh, _gl) = run_namespacing(
        passthrough_ns(),
        vec![
            ("l", "tools/list", json!({})),
            ("a", "tools/call", json!({ "name": "search", "arguments": {} })),
            ("b", "tools/call", json!({ "name": "create_issue", "arguments": {} })),
            ("u", "tools/call", json!({ "name": "missing", "arguments": {} })),
        ],
    )
    .await;
    let mut names: Vec<String> = responses[0]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["create_issue".to_string(), "create_issue".to_string(), "search".to_string()]);
    assert_eq!(responses[1]["result"]["content"][0]["text"], "gh:search");
    assert_eq!(responses[2]["result"]["content"][0]["text"], "gh:create_issue"); // first backend wins
    assert_eq!(responses[3]["error"]["code"], INVALID_PARAMS);
}

#[tokio::test]
async fn passthrough_strategy_warms_reverse_map_on_cold_call() {
    // A tools/call under passthrough before any tools/list still resolves: the
    // reverse map is warmed by an on-demand list fan-out.
    let (responses, gh_calls, _gl) = run_namespacing(
        passthrough_ns(),
        vec![("a", "tools/call", json!({ "name": "search", "arguments": {} }))],
    )
    .await;
    assert_eq!(responses[0]["result"]["content"][0]["text"], "gh:search");
    assert_eq!(*gh_calls.lock().unwrap(), vec!["search".to_string()]);
}
