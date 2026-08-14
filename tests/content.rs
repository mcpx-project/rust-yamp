//! Typed content-block iterator (ε1): traversal, decode, write-back.

use serde_json::{json, Value};

use yamp::{base64, content};

fn tool_result() -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1,
        "result": { "content": [
            {"type": "text", "text": "hello"},
            {"type": "image", "data": base64::encode(b"\x89PNG"), "mimeType": "image/png"},
            {"type": "resource_link", "uri": "file:///a.txt", "mimeType": "text/plain"},
            {"type": "resource", "resource": {"uri": "file:///b.bin", "mimeType": "application/octet-stream", "blob": base64::encode(&[0u8, 1, 2])}},
            {"type": "widget", "foo": 1},
        ]}
    })
}

#[test]
fn traversal_normalizes_every_block() {
    let blocks = content::blocks(&tool_result());
    let list = blocks.as_array().unwrap();
    assert_eq!(list.len(), 5);

    assert_eq!(list[0]["kind"], "text");
    assert_eq!(list[0]["text"], "hello");
    assert_eq!(list[0]["path"], json!(["result", "content", 0, "text"]));

    assert_eq!(list[1]["kind"], "image");
    assert_eq!(list[1]["mime"], "image/png");
    assert_eq!(list[1]["bytes"], "89504e47"); // hex of \x89PNG
    assert_eq!(list[1]["path"], json!(["result", "content", 1, "data"]));

    assert_eq!(list[2]["kind"], "resource_link");
    assert_eq!(list[2]["uri"], "file:///a.txt");

    assert_eq!(list[3]["kind"], "resource");
    assert_eq!(list[3]["bytes"], "000102");
    assert_eq!(list[3]["path"], json!(["result", "content", 3, "resource", "blob"]));

    assert_eq!(list[4]["kind"], "unknown");
}

#[test]
fn resources_read_contents_are_covered() {
    let message = json!({
        "jsonrpc": "2.0", "id": 2,
        "result": { "contents": [
            {"uri": "file:///c.txt", "mimeType": "text/plain", "text": "doc"},
            {"uri": "file:///d.bin", "mimeType": "application/octet-stream", "blob": base64::encode(&[9u8, 9])},
        ]}
    });
    let list = content::blocks(&message);
    let list = list.as_array().unwrap();
    assert_eq!(list[0]["kind"], "resource");
    assert_eq!(list[0]["text"], "doc");
    assert_eq!(list[1]["bytes"], "0909");
    assert_eq!(list[1]["path"], json!(["result", "contents", 1, "blob"]));
}

#[test]
fn no_content_yields_empty() {
    let request = json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "t"}});
    assert_eq!(content::blocks(&request), json!([]));
}

#[test]
fn set_text_writes_back_at_path() {
    let message = tool_result();
    let path = vec![json!("result"), json!("content"), json!(0), json!("text")];
    let out = content::set_text(&message, &path, "[redacted]");
    assert_eq!(out["result"]["content"][0]["text"], "[redacted]");
    assert_eq!(message["result"]["content"][0]["text"], "hello", "input untouched");
}

#[test]
fn set_bytes_reencodes_at_path() {
    let message = tool_result();
    let path = vec![json!("result"), json!("content"), json!(1), json!("data")];
    let out = content::set_bytes(&message, &path, b"NEW");
    let encoded = out["result"]["content"][1]["data"].as_str().unwrap();
    assert_eq!(base64::decode(encoded).unwrap(), b"NEW");
}
