//! δ13 canonical error-code registry tests (Rust arm). Mirrors the Python arm.

use std::collections::BTreeSet;
use std::io;

use serde_json::{json, Value};
use tokio::io::{duplex, BufReader};

use yamp::errors::{
    ALL_CODES, HEADER_MISMATCH, NO_SESSION, POLICY_DENIED, SERVER_NOT_AVAILABLE, UNAUTHORIZED,
    UNSUPPORTED_PROTOCOL_VERSION,
};
use yamp::forward::PROXY_PROTOCOL_VERSION;
use yamp::jsonrpc::{self, INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND};
use yamp::router::{Backend, ForwardRouter};
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite};

const CAP: usize = 1 << 16;

#[test]
fn canonical_assignments() {
    assert_eq!(NO_SESSION, -32000);
    assert_eq!(POLICY_DENIED, -32001);
    assert_eq!(UNAUTHORIZED, -32002);
    assert_eq!(SERVER_NOT_AVAILABLE, -32003);
    assert_eq!(UNSUPPORTED_PROTOCOL_VERSION, -32004);
    assert_eq!(HEADER_MISMATCH, -32005);
}

#[test]
fn proxy_codes_distinct_and_in_server_range() {
    let proxy = [
        NO_SESSION,
        POLICY_DENIED,
        UNAUTHORIZED,
        SERVER_NOT_AVAILABLE,
        UNSUPPORTED_PROTOCOL_VERSION,
        HEADER_MISMATCH,
    ];
    let distinct: BTreeSet<i64> = proxy.into_iter().collect();
    assert_eq!(distinct.len(), proxy.len()); // no overload
    for code in proxy {
        assert!((-32099..=-32000).contains(&code)); // JSON-RPC server-defined range
    }
}

#[test]
fn registry_lists_standard_and_proxy_codes() {
    for code in [
        INVALID_REQUEST,
        METHOD_NOT_FOUND,
        INVALID_PARAMS,
        INTERNAL_ERROR,
        NO_SESSION,
        POLICY_DENIED,
        UNAUTHORIZED,
        SERVER_NOT_AVAILABLE,
        UNSUPPORTED_PROTOCOL_VERSION,
    ] {
        assert!(ALL_CODES.contains(&code));
    }
}

#[test]
fn modules_source_codes_from_registry() {
    assert_eq!(yamp::resilience::SERVER_NOT_AVAILABLE, SERVER_NOT_AVAILABLE);
    assert_eq!(yamp::version::UNSUPPORTED_PROTOCOL_VERSION, UNSUPPORTED_PROTOCOL_VERSION);
    assert_eq!(yamp::transparent::POLICY_DENIED, POLICY_DENIED);
}

async fn error_backend<R, W>(mut reader: R, mut writer: W, code: i64) -> io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    let init = jsonrpc::decode(&reader.receive().await?.unwrap())?;
    writer
        .send(&jsonrpc::encode(&json!({
            "jsonrpc": "2.0", "id": init["id"],
            "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": { "name": "b" } },
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
                if jsonrpc::method_of(&message) == Some("tools/call") {
                    writer
                        .send(&jsonrpc::encode(&json!({
                            "jsonrpc": "2.0", "id": message["id"],
                            "error": { "code": code, "message": "backend-specific" },
                        })))
                        .await?;
                }
            }
        }
    }
}

#[tokio::test]
async fn unknown_backend_error_code_passes_through_unchanged() {
    let (client_w, router_reads_client) = duplex(CAP);
    let (router_writes_client, client_r) = duplex(CAP);
    let (router_to_b, b_reads) = duplex(CAP);
    let (b_writes, router_reads_b) = duplex(CAP);

    let backend = Backend::new(
        "b",
        LineReader::new(BufReader::new(router_reads_b)),
        LineWriter::new(router_to_b),
    )
    .unwrap();
    let router = ForwardRouter::new(
        LineReader::new(BufReader::new(router_reads_client)),
        LineWriter::new(router_writes_client),
        vec![backend],
    );
    let backend_task = error_backend(LineReader::new(BufReader::new(b_reads)), LineWriter::new(b_writes), -31234);

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
        // Single backend passes names through, so the tool name is unprefixed.
        cw.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": "t", "method": "tools/call", "params": { "name": "do", "arguments": {} } }))).await?;
        let response = jsonrpc::decode(&cr.receive().await?.unwrap())?;
        cw.send_eof().await?;
        Ok::<Value, io::Error>(response)
    };

    let (_r, _b, response) = tokio::try_join!(router.serve(), backend_task, client).unwrap();
    // The proxy must not rewrite a code it did not negotiate (SEP-2678).
    assert_eq!(response["error"]["code"], -31234);
    assert_eq!(response["error"]["message"], "backend-specific");
    assert!(!ALL_CODES.contains(&-31234)); // genuinely unknown to the proxy
}
