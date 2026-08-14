//! Local request handlers and their registry (draft §5.3/§5.7).
//!
//! A [`Handler`] is a local source of tools that can originate a response,
//! rather than routing to a backend. It gives yamp a single dispatch seam: a
//! request whose namespaced name resolves to a handler is served locally
//! (server behavior), and one that resolves to a backend is routed (proxy
//! behavior). Handlers share the backend namespace discipline, so each carries a
//! reserved id and its tools are exposed as `id__tool`. This is the substrate
//! the drafts' Reverse and Conversion modes need: `RestToMcp` is a Conversion
//! handler, and meta-tools such as `yamp__backends` are built-in handlers.
//!
//! The trait is object-safe (the registry holds heterogeneous handlers), so the
//! async call returns a boxed future rather than using `impl Future`.

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::pin::Pin;

use serde_json::{json, Value};

use crate::namespace;

pub type CallFuture<'a> = Pin<Box<dyn Future<Output = Value> + Send + 'a>>;

/// A local, namespaced source of tools. `call_tool` receives the original
/// (un-prefixed) name; the registry maps the prefix to the handler.
pub trait Handler: Send + Sync {
    fn id(&self) -> &str;
    fn list_tools(&self) -> Vec<Value>;
    fn call_tool<'a>(&'a self, name: &'a str, arguments: &'a Value) -> CallFuture<'a>;
}

/// Maps a reserved id to its handler and namespaces the handlers' tools.
/// Consulted before the backend routing table: a tool name whose prefix is a
/// handler id is served locally.
pub struct Registry {
    handlers: Vec<Box<dyn Handler>>,
}

impl Registry {
    pub fn new(handlers: Vec<Box<dyn Handler>>) -> io::Result<Self> {
        let mut seen = HashSet::new();
        for handler in &handlers {
            let id = handler.id();
            if !namespace::valid_backend_id(id) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("invalid handler id: {id}")));
            }
            if !seen.insert(id.to_string()) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("duplicate handler id: {id}")));
            }
        }
        Ok(Self { handlers })
    }

    pub fn empty() -> Self {
        Self { handlers: Vec::new() }
    }

    pub fn ids(&self) -> HashSet<String> {
        self.handlers.iter().map(|h| h.id().to_string()).collect()
    }

    pub fn handler_for(&self, id: &str) -> Option<&dyn Handler> {
        self.handlers.iter().find(|h| h.id() == id).map(|h| h.as_ref())
    }

    /// Every handler's tools, namespaced under the handler id.
    pub fn list_tools(&self) -> Vec<Value> {
        let mut tools = Vec::new();
        for handler in &self.handlers {
            for tool in handler.list_tools() {
                let mut entry = tool.clone();
                if let (Some(object), Some(name)) =
                    (entry.as_object_mut(), tool.get("name").and_then(Value::as_str))
                {
                    object.insert("name".to_string(), Value::String(namespace::prefix(handler.id(), name)));
                }
                tools.push(entry);
            }
        }
        tools
    }
}

/// A built-in meta-tool (`yamp__backends`) that reports the proxy's own backends.
/// It originates its response entirely inside yamp, demonstrating the server
/// side of the dispatch seam.
pub struct BackendsHandler {
    id: String,
    provider: Box<dyn Fn() -> Value + Send + Sync>,
}

impl BackendsHandler {
    pub fn new(provider: impl Fn() -> Value + Send + Sync + 'static) -> Self {
        Self { id: "yamp".to_string(), provider: Box::new(provider) }
    }
}

impl Handler for BackendsHandler {
    fn id(&self) -> &str {
        &self.id
    }

    fn list_tools(&self) -> Vec<Value> {
        vec![json!({
            "name": "backends",
            "description": "List the backends this proxy fronts and their availability",
            "inputSchema": { "type": "object", "properties": {} },
        })]
    }

    fn call_tool<'a>(&'a self, _name: &'a str, _arguments: &'a Value) -> CallFuture<'a> {
        let backends = (self.provider)();
        Box::pin(async move { json!({ "content": [{ "type": "text", "text": backends.to_string() }] }) })
    }
}

/// Build a [`Registry`] from a `HandlerConfig` (δ17). Each configured REST
/// handler becomes a served `RestToMcp` (Conversion mode) with the default
/// HTTP client; `meta_tools` adds `yamp__backends`, reporting the proxy's
/// backends via `provider`.
pub fn build_registry(
    config: &crate::config::HandlerConfig,
    provider: impl Fn() -> Value + Send + Sync + 'static,
) -> io::Result<Registry> {
    let mut handlers: Vec<Box<dyn Handler>> = Vec::new();
    for spec in &config.rest {
        let manifest = json!({ "baseUrl": spec.base_url, "operations": spec.operations });
        handlers.push(Box::new(
            crate::rest::RestToMcp::new(&manifest, crate::rest::HttpTransport).with_id(spec.id.clone()),
        ));
    }
    if config.meta_tools {
        handlers.push(Box::new(BackendsHandler::new(provider)));
    }
    Registry::new(handlers)
}
