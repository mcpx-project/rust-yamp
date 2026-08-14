//! Forward proxy, stateful mode (SEP §2.2, draft §5.2).
//!
//! The proxy performs an independent initialize / initialized handshake with
//! the backend (dual handshake), then composes one client-facing initialize
//! response whose `serverInfo` identifies the proxy and whose
//! `protocolVersion` is the proxy's own highest. Backend identity is never
//! exposed to the client. After the handshake, messages are forwarded
//! unchanged (single backend; namespacing arrives in δ2).

use std::io;

use serde_json::{json, Value};

use crate::jsonrpc;
use crate::relay::Relay;
use crate::transport::{MessageRead, MessageWrite};
use crate::version::STATEFUL_PROTOCOL_VERSION;

/// The version the stateful served path advertises (SEP §2.2: the
/// intermediary's own highest). Sourced from the single version module; kept
/// here as the name every layer already imports.
pub const PROXY_PROTOCOL_VERSION: &str = STATEFUL_PROTOCOL_VERSION;

/// The proxy's identity, single-sourced here (the repo's "proxy identity in
/// forward" rule) so observability and the REST adapter do not re-derive it.
pub const PROXY_NAME: &str = "yamp";
pub const PROXY_VERSION: &str = "0.0.0";

/// The proxy's own identity, presented to clients and to backends. Single
/// definition reused by every layer.
pub fn proxy_server_info() -> Value {
    json!({ "name": PROXY_NAME, "version": PROXY_VERSION })
}

pub struct ForwardProxy<CR, CW, BR, BW> {
    client_read: CR,
    client_write: CW,
    backend_read: BR,
    backend_write: BW,
}

impl<CR, CW, BR, BW> ForwardProxy<CR, CW, BR, BW>
where
    CR: MessageRead,
    CW: MessageWrite,
    BR: MessageRead,
    BW: MessageWrite,
{
    pub fn new(client_read: CR, client_write: CW, backend_read: BR, backend_write: BW) -> Self {
        Self {
            client_read,
            client_write,
            backend_read,
            backend_write,
        }
    }

    /// Run the session. Returns the backend's `serverInfo` (held internally,
    /// never sent to the client) or `None` if the client closed first.
    pub async fn serve(mut self) -> io::Result<Option<Value>> {
        let raw = match self.client_read.receive().await? {
            Some(raw) => raw,
            None => return Ok(None),
        };
        let client_init = jsonrpc::decode(&raw)?;
        if jsonrpc::method_of(&client_init) != Some("initialize") {
            let id = client_init.get("id").cloned().unwrap_or(Value::Null);
            let reply = json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": jsonrpc::INVALID_REQUEST, "message": "expected initialize" },
            });
            self.client_write.send(&jsonrpc::encode(&reply)).await?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "first client message was not initialize",
            ));
        }

        let (backend_caps, backend_info) = self.backend_handshake().await?;

        let response = json!({
            "jsonrpc": "2.0",
            "id": client_init.get("id").cloned().unwrap_or(Value::Null),
            "result": {
                "protocolVersion": PROXY_PROTOCOL_VERSION,
                "capabilities": backend_caps,
                "serverInfo": proxy_server_info(),
            },
        });
        self.client_write.send(&jsonrpc::encode(&response)).await?;
        // Consume the client's notifications/initialized.
        self.client_read.receive().await?;

        Relay::run(
            self.client_read,
            self.client_write,
            self.backend_read,
            self.backend_write,
        )
        .await?;
        Ok(backend_info)
    }

    async fn backend_handshake(&mut self) -> io::Result<(Value, Option<Value>)> {
        let initialize = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROXY_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": proxy_server_info(),
            },
        });
        self.backend_write.send(&jsonrpc::encode(&initialize)).await?;

        let raw = self
            .backend_read
            .receive()
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "backend closed during initialize"))?;
        let backend_init = jsonrpc::decode(&raw)?;
        let result = backend_init.get("result");
        let caps = result
            .and_then(|r| r.get("capabilities"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let info = result.and_then(|r| r.get("serverInfo")).cloned();

        let initialized = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        self.backend_write
            .send(&jsonrpc::encode(&initialized))
            .await?;
        Ok((caps, info))
    }
}
