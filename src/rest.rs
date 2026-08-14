//! REST-to-MCP conversion (Conversion mode, draft Section 5.7).
//!
//! A `RestToMcp` adapter is an MCP server that fronts a REST API described by an
//! operation manifest. The proxy connects to it as an ordinary backend, so an
//! MCP client reaches a REST API through the proxy: each operation shows up as a
//! tool on `tools/list`, and `tools/call` translates the arguments into an HTTP
//! request. The HTTP client is injectable so the translation is testable without
//! a network. `HttpTransport` is a default client for plain `http://` targets.

use std::io;

use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::forward::{PROXY_NAME, PROXY_PROTOCOL_VERSION, PROXY_VERSION};
use crate::handler::{CallFuture, Handler};
use crate::jsonrpc;
use crate::namespace;
use crate::transport::{MessageRead, MessageWrite};

/// Performs one HTTP request. Returns (status, body).
pub trait HttpClient {
    fn call(
        &self,
        method: &str,
        url: &str,
        body: Option<&[u8]>,
    ) -> impl std::future::Future<Output = io::Result<(u16, Vec<u8>)>> + Send;
}

fn rest_server_info() -> Value {
    json!({ "name": format!("{PROXY_NAME}-rest"), "version": PROXY_VERSION })
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Percent-encode a string per RFC 3986 with no safe characters, so a
/// client-supplied argument cannot inject path segments (`../`) or smuggle
/// extra query pairs. Matches the Python arm's `quote(value, safe="")`
/// (uppercase hex, space -> %20) so the built URL is byte-identical.
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub struct RestToMcp<H> {
    id: String,
    base: String,
    ops: Vec<Value>,
    http: H,
}

impl<H: HttpClient> RestToMcp<H> {
    pub fn new(spec: &Value, http: H) -> Self {
        let base = spec["baseUrl"].as_str().unwrap_or("").trim_end_matches('/').to_string();
        let ops = spec["operations"].as_array().cloned().unwrap_or_default();
        Self { id: "rest".to_string(), base, ops, http }
    }

    /// Set the reserved namespace id used when this adapter is served directly as
    /// a [`Handler`] (Conversion mode). Panics on an id carrying the delimiter.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        let id = id.into();
        assert!(namespace::valid_backend_id(&id), "invalid rest handler id: {id}");
        self.id = id;
        self
    }

    fn find(&self, name: &str) -> Option<&Value> {
        self.ops.iter().find(|op| op["name"].as_str() == Some(name))
    }

    fn tools(&self) -> Vec<Value> {
        self.ops
            .iter()
            .map(|op| {
                let mut properties = Map::new();
                if let Some(parameters) = op["parameters"].as_array() {
                    for parameter in parameters {
                        if let Some(name) = parameter["name"].as_str() {
                            properties.insert(name.to_string(), json!({ "type": "string" }));
                        }
                    }
                }
                if let Some(fields) = op["body"].as_array() {
                    for field in fields {
                        if let Some(name) = field.as_str() {
                            properties.insert(name.to_string(), json!({ "type": "string" }));
                        }
                    }
                }
                json!({
                    "name": op["name"],
                    "description": op["description"].as_str().unwrap_or(""),
                    "inputSchema": { "type": "object", "properties": properties },
                })
            })
            .collect()
    }

    async fn call(&self, params: &Value) -> Value {
        let name = params["name"].as_str().unwrap_or("");
        let op = match self.find(name) {
            Some(op) => op,
            None => return json!({ "content": [{ "type": "text", "text": "unknown operation" }], "isError": true }),
        };
        let args = &params["arguments"];
        let mut path = op["path"].as_str().unwrap_or("").to_string();
        let mut query = Vec::new();
        if let Some(parameters) = op["parameters"].as_array() {
            for parameter in parameters {
                let pname = parameter["name"].as_str().unwrap_or("");
                let value = &args[pname];
                if value.is_null() {
                    continue;
                }
                let text = value_to_string(value);
                match parameter["in"].as_str() {
                    Some("path") => path = path.replace(&format!("{{{pname}}}"), &percent_encode(&text)),
                    Some("query") => query.push(format!("{}={}", percent_encode(pname), percent_encode(&text))),
                    _ => {}
                }
            }
        }
        let mut url = format!("{}{}", self.base, path);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query.join("&"));
        }
        let mut body = None;
        if let Some(fields) = op["body"].as_array() {
            let mut object = Map::new();
            for field in fields {
                if let Some(name) = field.as_str() {
                    if !args[name].is_null() {
                        object.insert(name.to_string(), args[name].clone());
                    }
                }
            }
            body = Some(serde_json::to_vec(&Value::Object(object)).unwrap());
        }
        let method = op["method"].as_str().unwrap_or("GET");
        match self.http.call(method, &url, body.as_deref()).await {
            Ok((status, response)) => json!({
                "content": [{ "type": "text", "text": String::from_utf8_lossy(&response) }],
                "isError": status >= 400,
            }),
            Err(e) => json!({ "content": [{ "type": "text", "text": e.to_string() }], "isError": true }),
        }
    }

    pub async fn serve<R, W>(&self, mut reader: R, mut writer: W) -> io::Result<()>
    where
        R: MessageRead,
        W: MessageWrite,
    {
        let init = jsonrpc::decode(&reader.receive().await?.unwrap_or_default())?;
        writer
            .send(&jsonrpc::encode(&json!({
                "jsonrpc": "2.0", "id": init["id"],
                "result": { "protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": rest_server_info() },
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
                            writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "tools": self.tools() } }))).await?;
                        }
                        Some("tools/call") => {
                            let result = self.call(&message["params"]).await;
                            writer.send(&jsonrpc::encode(&json!({ "jsonrpc": "2.0", "id": message["id"], "result": result }))).await?;
                        }
                        other => {
                            // An unknown request (carries an id) must get a reply;
                            // otherwise the client blocks forever. Notifications
                            // (no id) are ignored.
                            if !message["id"].is_null() {
                                writer.send(&jsonrpc::encode(&json!({
                                    "jsonrpc": "2.0", "id": message["id"],
                                    "error": { "code": jsonrpc::METHOD_NOT_FOUND, "message": format!("unknown method: {}", other.unwrap_or("")) },
                                }))).await?;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Served directly by the proxy (Conversion mode), reusing the same `tools` /
/// `call` surface as the backend form.
impl<H: HttpClient + Send + Sync> Handler for RestToMcp<H> {
    fn id(&self) -> &str {
        &self.id
    }

    fn list_tools(&self) -> Vec<Value> {
        self.tools()
    }

    fn call_tool<'a>(&'a self, name: &'a str, arguments: &'a Value) -> CallFuture<'a> {
        let params = json!({ "name": name, "arguments": arguments });
        Box::pin(async move { self.call(&params).await })
    }
}

/// A default HTTP client for plain `http://` targets. TLS is not handled; a
/// production deployment behind HTTPS backends supplies its own `HttpClient`.
pub struct HttpTransport;

impl HttpClient for HttpTransport {
    async fn call(&self, method: &str, url: &str, body: Option<&[u8]>) -> io::Result<(u16, Vec<u8>)> {
        let rest = url.strip_prefix("http://").ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "only http:// is supported"))?;
        let (authority, path) = match rest.split_once('/') {
            Some((a, p)) => (a, format!("/{p}")),
            None => (rest, "/".to_string()),
        };
        let host = authority.to_string();
        let mut stream = TcpStream::connect(&host).await?;
        let body = body.unwrap_or(b"");
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.flush().await?;

        let mut reader = BufReader::new(stream);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).await?;
        let status: u16 = status_line.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
        let mut length: Option<usize> = None;
        let mut chunked = false;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await? == 0 {
                break;
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                let k = k.trim();
                if k.eq_ignore_ascii_case("content-length") {
                    length = v.trim().parse().ok();
                } else if k.eq_ignore_ascii_case("transfer-encoding") && v.trim().eq_ignore_ascii_case("chunked") {
                    chunked = true;
                }
            }
        }
        let response = if chunked {
            read_chunked(&mut reader).await?
        } else if let Some(length) = length {
            let mut buf = vec![0u8; length];
            reader.read_exact(&mut buf).await?;
            buf
        } else {
            // No framing header: the backend closes the connection after the body
            // (we send `Connection: close`), so read to EOF rather than assuming
            // an empty body.
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            buf
        };
        Ok((status, response))
    }
}

/// Decode an HTTP/1.1 `Transfer-Encoding: chunked` body into the assembled
/// bytes, so a chunked backend response is not silently read as empty.
async fn read_chunked<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncBufReadExt + AsyncReadExt + Unpin,
{
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        if reader.read_line(&mut size_line).await? == 0 {
            break;
        }
        // A chunk size line may carry `;ext` extensions after the hex size.
        let hex = size_line.trim().split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(hex, 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk).await?;
        body.extend_from_slice(&chunk);
        // Consume the CRLF that terminates the chunk data.
        let mut crlf = String::new();
        reader.read_line(&mut crlf).await?;
    }
    Ok(body)
}
