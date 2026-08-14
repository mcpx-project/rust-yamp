//! Transparent mode, Level 2 (SEP §10.3/§10.4 L2, §10.5, §7.1).
//!
//! Protocol-aware: parses bodies, augments `_meta` with proxy-hop tracing
//! (appending, not replacing), may filter the capability surface on
//! `server/discover`, namespaces across backends, and optionally performs a
//! dual handshake in stateful mode. Reuses the earlier layers: the stateless
//! envelope and backend (δ3), the namespace (δ2), the forward handshake (δ1),
//! and the Level 1 passthrough (δ4).

use std::io;

use serde_json::{json, Value};

use crate::errors::HEADER_MISMATCH;
use crate::forward::ForwardProxy;
use crate::jsonrpc::{INVALID_PARAMS, METHOD_NOT_FOUND};
use crate::namespace;
// Hop tracing helpers live in `observability`; re-exported for this layer's
// existing importers.
pub use crate::observability::{append_hop, proxy_hop, PROXY_HOPS_KEY};
use crate::stateless::{
    decode_request, encode_response, StatelessBackend, StatelessRequest, StatelessResponse,
};
use crate::transparent::{AllowAll, TransparentL1};
use crate::transport::{MessageRead, MessageWrite};

type ToolFilter = Box<dyn Fn(&str) -> bool>;

/// The field that disagrees, or `None` if header and body agree.
///
/// SEP-2243: a Level 2 intermediary parses the body, so it MUST verify the
/// transport headers (`Mcp-Method` / `Mcp-Name`, modeled here as the envelope
/// `method` / `name`) agree with the body. Otherwise header-based routing or
/// policy can be bypassed by a divergent body. An empty or opaque body carries
/// nothing to check.
pub fn header_body_mismatch(request: &StatelessRequest) -> Option<&'static str> {
    if request.body.is_empty() {
        return None;
    }
    let message: Value = match serde_json::from_str(&request.body) {
        Ok(value) => value,
        Err(_) => return None, // opaque body: nothing to validate against
    };
    let object = message.as_object()?; // opaque non-object body: nothing to validate
    if let Some(body_method) = object.get("method").and_then(Value::as_str) {
        if body_method != request.method {
            return Some("method");
        }
    }
    if request.method == "tools/call" {
        let body_name = object.get("params").and_then(|p| p.get("name")).and_then(Value::as_str);
        if let Some(body_name) = body_name {
            if Some(body_name) != request.name.as_deref() {
                return Some("name");
            }
        }
    }
    None
}

pub struct TransparentL2Stateless<CR, CW, BR, BW> {
    client_read: CR,
    client_write: CW,
    backends: Vec<StatelessBackend<BR, BW>>,
    tool_filter: Option<ToolFilter>,
}

impl<CR, CW, BR, BW> TransparentL2Stateless<CR, CW, BR, BW>
where
    CR: MessageRead,
    CW: MessageWrite,
    BR: MessageRead,
    BW: MessageWrite,
{
    pub fn new(
        client_read: CR,
        client_write: CW,
        backends: Vec<StatelessBackend<BR, BW>>,
        tool_filter: Option<ToolFilter>,
    ) -> Self {
        Self {
            client_read,
            client_write,
            backends,
            tool_filter,
        }
    }

    pub async fn serve(mut self) -> io::Result<()> {
        loop {
            let raw = match self.client_read.receive().await? {
                Some(raw) => raw,
                None => break,
            };
            let mut request = decode_request(&raw)?;
            request.meta = append_hop(&request.meta, "transparent"); // §7.1, append to existing hops
            let response = self.handle(request).await?;
            self.client_write.send(&encode_response(&response)).await?;
        }
        for backend in &mut self.backends {
            backend.close().await?;
        }
        Ok(())
    }

    async fn handle(&mut self, request: StatelessRequest) -> io::Result<StatelessResponse> {
        if let Some(field) = header_body_mismatch(&request) {
            return Ok(StatelessResponse {
                meta: request.meta.clone(),
                body: json!({ "error": { "code": HEADER_MISMATCH, "message": format!("header/body {field} mismatch") } })
                    .to_string(),
            });
        }
        match request.method.as_str() {
            "server/discover" => self.discover(request).await,
            "tools/call" => self.call(request).await,
            other => Ok(StatelessResponse {
                meta: request.meta.clone(),
                body: json!({ "error": { "code": METHOD_NOT_FOUND, "message": other } }).to_string(),
            }),
        }
    }

    async fn discover(&mut self, request: StatelessRequest) -> io::Result<StatelessResponse> {
        let filter = self.tool_filter.as_ref();
        let mut tools = Vec::new();
        for backend in &mut self.backends {
            let response = backend
                .exchange(&StatelessRequest::new("server/discover", None, request.meta.clone(), ""))
                .await?;
            let parsed: Value = serde_json::from_str(&response.body)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            if let Some(listed) = parsed.get("tools").and_then(Value::as_array) {
                for tool in listed {
                    let name = tool.get("name").and_then(Value::as_str).unwrap_or("");
                    if let Some(filter) = filter {
                        if !filter(name) {
                            continue; // L2 may filter the capability surface
                        }
                    }
                    let mut entry = tool.clone();
                    if let Some(object) = entry.as_object_mut() {
                        object.insert("name".to_string(), Value::String(namespace::prefix(backend.id(), name)));
                    }
                    tools.push(entry);
                }
            }
        }
        Ok(StatelessResponse {
            meta: request.meta,
            body: json!({ "tools": tools }).to_string(),
        })
    }

    async fn call(&mut self, request: StatelessRequest) -> io::Result<StatelessResponse> {
        let name = request.name.clone().unwrap_or_default();
        let resolved = namespace::split(&name);
        let index = resolved.and_then(|(bid, _)| self.backends.iter().position(|b| b.id() == bid));
        let (index, original) = match (index, resolved) {
            (Some(index), Some((_, original))) => (index, original.to_string()),
            _ => {
                return Ok(StatelessResponse {
                    meta: request.meta.clone(),
                    body: json!({ "error": { "code": INVALID_PARAMS, "message": format!("unknown tool: {name}") } })
                        .to_string(),
                });
            }
        };
        let forwarded = StatelessRequest::new("tools/call", Some(original), request.meta.clone(), request.body);
        self.backends[index].exchange(&forwarded).await
    }
}

/// Stateful Level 2: a per-connection choice between a dual handshake (follows
/// §2.2 via the δ1 forward proxy) and Level 1 passthrough (δ4). `serve` returns
/// whether the dual handshake was performed.
pub struct TransparentL2Stateful<CR, CW, BR, BW> {
    client_read: CR,
    client_write: CW,
    backend_read: BR,
    backend_write: BW,
    dual_handshake: bool,
}

impl<CR, CW, BR, BW> TransparentL2Stateful<CR, CW, BR, BW>
where
    CR: MessageRead,
    CW: MessageWrite,
    BR: MessageRead,
    BW: MessageWrite,
{
    pub fn new(client_read: CR, client_write: CW, backend_read: BR, backend_write: BW, dual_handshake: bool) -> Self {
        Self {
            client_read,
            client_write,
            backend_read,
            backend_write,
            dual_handshake,
        }
    }

    pub async fn serve(self) -> io::Result<bool> {
        if self.dual_handshake {
            ForwardProxy::new(self.client_read, self.client_write, self.backend_read, self.backend_write)
                .serve()
                .await?;
            Ok(true)
        } else {
            TransparentL1::run(
                self.client_read,
                self.client_write,
                self.backend_read,
                self.backend_write,
                AllowAll,
            )
            .await?;
            Ok(false)
        }
    }
}
