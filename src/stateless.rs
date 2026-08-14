//! Forward proxy, stateless mode (SEP §2.1, MCP 2026-07-28 and later).
//!
//! No handshake, no session identifiers. Routing uses the transport-level
//! `Mcp-Method` / `Mcp-Name` headers (modeled here as the envelope's `method`
//! and `name`), so the application `body` is never parsed to decide routing.
//! Per-request `_meta` carries client identity; the proxy injects its own when
//! forwarding to a backend and forwards the backend's `_meta` back.

use std::io;

use serde_json::{json, Map, Value};

use crate::forward::proxy_server_info;
use crate::jsonrpc::{self, INVALID_PARAMS, METHOD_NOT_FOUND};
use crate::namespace;
use crate::transport::{MessageRead, MessageWrite};
use crate::version::{self, PROTOCOL_VERSION_META_KEY, UNSUPPORTED_PROTOCOL_VERSION};

pub const CLIENT_INFO_META_KEY: &str = "io.modelcontextprotocol/clientInfo";

fn proxy_meta() -> Value {
    let mut meta = Map::new();
    meta.insert(CLIENT_INFO_META_KEY.to_string(), proxy_server_info());
    Value::Object(meta)
}

fn error_body(code: i64, message: String) -> String {
    json!({ "error": { "code": code, "message": message } }).to_string()
}

fn error_body_with_data(code: i64, message: String, data: Value) -> String {
    json!({ "error": { "code": code, "message": message, "data": data } }).to_string()
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatelessRequest {
    pub method: String,
    pub name: Option<String>,
    pub meta: Value,
    pub body: String,
}

impl StatelessRequest {
    pub fn new(method: impl Into<String>, name: Option<String>, meta: Value, body: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            name,
            meta,
            body: body.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatelessResponse {
    pub meta: Value,
    pub body: String,
}

pub fn encode_request(request: &StatelessRequest) -> Vec<u8> {
    jsonrpc::encode(&json!({
        "method": request.method,
        "name": request.name,
        "meta": request.meta,
        "body": request.body,
    }))
}

pub fn decode_request(raw: &[u8]) -> io::Result<StatelessRequest> {
    let value = jsonrpc::decode(raw)?;
    Ok(StatelessRequest {
        method: value["method"].as_str().unwrap_or("").to_string(),
        name: value.get("name").and_then(Value::as_str).map(String::from),
        meta: value.get("meta").cloned().unwrap_or_else(|| json!({})),
        body: value.get("body").and_then(Value::as_str).unwrap_or("").to_string(),
    })
}

pub fn encode_response(response: &StatelessResponse) -> Vec<u8> {
    jsonrpc::encode(&json!({ "meta": response.meta, "body": response.body }))
}

pub fn decode_response(raw: &[u8]) -> io::Result<StatelessResponse> {
    let value = jsonrpc::decode(raw)?;
    Ok(StatelessResponse {
        meta: value.get("meta").cloned().unwrap_or_else(|| json!({})),
        body: value.get("body").and_then(Value::as_str).unwrap_or("").to_string(),
    })
}

pub struct StatelessBackend<R, W> {
    id: String,
    reader: R,
    writer: W,
}

impl<R, W> StatelessBackend<R, W>
where
    R: MessageRead,
    W: MessageWrite,
{
    pub fn new(id: impl Into<String>, reader: R, writer: W) -> io::Result<Self> {
        let id = id.into();
        if !namespace::valid_backend_id(&id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid backend id: {id}"),
            ));
        }
        Ok(Self { id, reader, writer })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn exchange(&mut self, request: &StatelessRequest) -> io::Result<StatelessResponse> {
        self.writer.send(&encode_request(request)).await?;
        let raw = self.reader.receive().await?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::ConnectionAborted, format!("backend {} closed", self.id))
        })?;
        decode_response(&raw)
    }

    pub async fn close(&mut self) -> io::Result<()> {
        self.writer.send_eof().await
    }
}

pub struct StatelessForwarder<CR, CW, BR, BW> {
    client_read: CR,
    client_write: CW,
    backends: Vec<StatelessBackend<BR, BW>>,
}

impl<CR, CW, BR, BW> StatelessForwarder<CR, CW, BR, BW>
where
    CR: MessageRead,
    CW: MessageWrite,
    BR: MessageRead,
    BW: MessageWrite,
{
    pub fn new(client_read: CR, client_write: CW, backends: Vec<StatelessBackend<BR, BW>>) -> Self {
        Self {
            client_read,
            client_write,
            backends,
        }
    }

    pub async fn serve(mut self) -> io::Result<()> {
        loop {
            let raw = match self.client_read.receive().await? {
                Some(raw) => raw,
                None => break,
            };
            let request = decode_request(&raw)?;
            let response = self.handle(request).await?;
            self.client_write.send(&encode_response(&response)).await?;
        }
        for backend in &mut self.backends {
            backend.close().await?;
        }
        Ok(())
    }

    async fn handle(&mut self, request: StatelessRequest) -> io::Result<StatelessResponse> {
        // Each stateless request is self-describing: negotiate its declared
        // protocol version before routing (SEP-2575). A version the proxy
        // cannot serve is rejected here rather than forwarded, since
        // statelessness has no handshake to fall back on.
        let requested = request.meta.get(PROTOCOL_VERSION_META_KEY).and_then(Value::as_str);
        let negotiated = match version::negotiate(requested, version::STATELESS_PROTOCOL_VERSION) {
            Some(negotiated) => negotiated,
            None => {
                return Ok(StatelessResponse {
                    meta: proxy_meta(),
                    body: error_body_with_data(
                        UNSUPPORTED_PROTOCOL_VERSION,
                        format!("unsupported protocol version: {}", requested.unwrap_or("")),
                        version::unsupported_error_data(requested),
                    ),
                });
            }
        };
        match request.method.as_str() {
            "server/discover" => self.discover().await,
            "tools/call" => self.call(request, negotiated).await,
            other => Ok(StatelessResponse {
                meta: proxy_meta(),
                body: error_body(METHOD_NOT_FOUND, format!("method not routable: {other}")),
            }),
        }
    }

    async fn discover(&mut self) -> io::Result<StatelessResponse> {
        let mut tools = Vec::new();
        for backend in &mut self.backends {
            let request = StatelessRequest::new("server/discover", None, proxy_meta(), "");
            let response = backend.exchange(&request).await?;
            let parsed: Value = serde_json::from_str(&response.body)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            if let Some(listed) = parsed.get("tools").and_then(Value::as_array) {
                for tool in listed {
                    let mut entry = tool.clone();
                    if let (Some(object), Some(name)) =
                        (entry.as_object_mut(), tool.get("name").and_then(Value::as_str))
                    {
                        object.insert("name".to_string(), Value::String(namespace::prefix(&backend.id, name)));
                    }
                    tools.push(entry);
                }
            }
        }
        Ok(StatelessResponse {
            meta: proxy_meta(),
            body: json!({ "tools": tools }).to_string(),
        })
    }

    async fn call(
        &mut self,
        request: StatelessRequest,
        negotiated: &str,
    ) -> io::Result<StatelessResponse> {
        // Route on the name header only; the body is never parsed here.
        let name = request.name.clone().unwrap_or_default();
        let resolved = namespace::split(&name);
        let index = resolved.and_then(|(bid, _)| self.backends.iter().position(|b| b.id == bid));
        let (index, original) = match (index, resolved) {
            (Some(index), Some((_, original))) => (index, original.to_string()),
            _ => {
                return Ok(StatelessResponse {
                    meta: proxy_meta(),
                    body: error_body(INVALID_PARAMS, format!("unknown tool: {name}")),
                });
            }
        };

        // Carry the client's _meta forward, inject the proxy identity, and pin
        // the negotiated version so the backend sees a self-describing request
        // (SEP-2575).
        let mut meta = request.meta.clone();
        if meta.as_object().is_none() {
            meta = Value::Object(Map::new());
        }
        if let Some(object) = meta.as_object_mut() {
            object.insert(CLIENT_INFO_META_KEY.to_string(), proxy_server_info());
            object.insert(
                PROTOCOL_VERSION_META_KEY.to_string(),
                Value::String(negotiated.to_string()),
            );
        }
        let forwarded = StatelessRequest::new("tools/call", Some(original), meta, request.body);
        self.backends[index].exchange(&forwarded).await
    }
}
