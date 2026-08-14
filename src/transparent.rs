//! Transparent mode, Level 1 (SEP §10, transport-aware).
//!
//! Byte-faithful interception. Inspects only the transport headers
//! (`Mcp-Method`, `Mcp-Name`) to observe, log, or block; never parses the
//! application body, performs no handshake, and does no namespacing. The
//! original destination recovered from the intercepted socket selects the
//! backend. Forwarding sends the original bytes unchanged.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::jsonrpc;
use crate::transport::{MessageRead, MessageWrite};

// Single source in the errors registry; re-exported for existing importers.
pub use crate::errors::POLICY_DENIED;

pub fn encode_envelope(headers: &Value, body: &str) -> Vec<u8> {
    jsonrpc::encode(&json!({ "headers": headers, "body": body }))
}

/// Read the transport headers without decoding the application body.
pub fn peek_headers(raw: &[u8]) -> io::Result<Value> {
    Ok(jsonrpc::decode(raw)?
        .get("headers")
        .cloned()
        .unwrap_or_else(|| json!({})))
}

/// Recover the pre-DNAT destination of an intercepted socket (SO_ORIGINAL_DST
/// / TPROXY). Integration-only: requires a real intercepted socket.
pub fn recover_original_destination() -> io::Result<(String, u16)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_ORIGINAL_DST recovery is integration-only",
    ))
}

/// Select the backend for a recovered original destination.
pub fn select_backend<'a, T>(
    destination: &(String, u16),
    table: &'a HashMap<(String, u16), T>,
) -> io::Result<&'a T> {
    table.get(destination).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("no backend for destination {destination:?}"))
    })
}

pub trait Policy {
    fn allow(&self, headers: &Value) -> bool;
}

pub struct AllowAll;

impl Policy for AllowAll {
    fn allow(&self, _headers: &Value) -> bool {
        true
    }
}

pub struct HeaderPolicy {
    blocked_methods: HashSet<String>,
    blocked_names: HashSet<String>,
}

impl HeaderPolicy {
    pub fn new<M, N>(blocked_methods: M, blocked_names: N) -> Self
    where
        M: IntoIterator<Item = String>,
        N: IntoIterator<Item = String>,
    {
        Self {
            blocked_methods: blocked_methods.into_iter().collect(),
            blocked_names: blocked_names.into_iter().collect(),
        }
    }
}

impl Policy for HeaderPolicy {
    fn allow(&self, headers: &Value) -> bool {
        if let Some(method) = headers.get("Mcp-Method").and_then(Value::as_str) {
            if self.blocked_methods.contains(method) {
                return false;
            }
        }
        if let Some(name) = headers.get("Mcp-Name").and_then(Value::as_str) {
            if self.blocked_names.contains(name) {
                return false;
            }
        }
        true
    }
}

fn blocked_response() -> Vec<u8> {
    encode_envelope(
        &json!({ "Mcp-Status": "blocked" }),
        &json!({ "error": { "code": POLICY_DENIED, "message": "blocked by policy" } }).to_string(),
    )
}

pub struct TransparentL1;

impl TransparentL1 {
    /// Pump both directions byte-faithfully, applying `policy` to client to
    /// backend messages by header. Returns the number of blocked messages.
    pub async fn run<CR, CW, BR, BW, P>(
        client_read: CR,
        client_write: CW,
        backend_read: BR,
        backend_write: BW,
        policy: P,
    ) -> io::Result<u64>
    where
        CR: MessageRead,
        CW: MessageWrite,
        BR: MessageRead,
        BW: MessageWrite,
        P: Policy,
    {
        // The client writer is shared: the forward path (backend to client) and
        // the block path (policy rejection) both write to it.
        let client_write = Arc::new(Mutex::new(client_write));
        let blocked = Arc::new(AtomicU64::new(0));

        let to_backend = {
            let client_write = client_write.clone();
            let blocked = blocked.clone();
            let mut client_read = client_read;
            let mut backend_write = backend_write;
            async move {
                loop {
                    match client_read.receive().await? {
                        None => {
                            backend_write.send_eof().await?;
                            return Ok::<(), io::Error>(());
                        }
                        Some(raw) => {
                            let headers = peek_headers(&raw)?;
                            if policy.allow(&headers) {
                                backend_write.send(&raw).await?; // unmodified
                            } else {
                                blocked.fetch_add(1, Ordering::Relaxed);
                                client_write.lock().await.send(&blocked_response()).await?;
                            }
                        }
                    }
                }
            }
        };

        let to_client = {
            let client_write = client_write.clone();
            let mut backend_read = backend_read;
            async move {
                loop {
                    match backend_read.receive().await? {
                        None => {
                            client_write.lock().await.send_eof().await?;
                            return Ok::<(), io::Error>(());
                        }
                        Some(raw) => {
                            client_write.lock().await.send(&raw).await?; // unmodified
                        }
                    }
                }
            }
        };

        tokio::try_join!(to_backend, to_client)?;
        Ok(blocked.load(Ordering::Relaxed))
    }
}
