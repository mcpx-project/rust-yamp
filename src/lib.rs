//! Yet Another MCP Proxy, Rust arm.
//!
//! Increment δ0: Layer 1 transport substrate and a byte-faithful relay. This
//! arm mirrors the Python arm's δ0 design so the two form a paired experiment
//! (see ../EXPERIMENT.md): same intent stream and constraint clauses, the
//! language substrate varied.
//!
//! The transport traits return `Send` futures so a reader can run in a spawned
//! task (the bidirectional router spawns a demuxing reader per backend).

pub mod auth;
pub mod base64;
pub mod cache;
pub mod callout;
pub mod capability;
pub mod collision;
pub mod config;
pub mod content;
pub mod doctor;
pub mod errors;
pub mod filters;
pub mod handler;
pub mod icap;
pub mod media;
pub mod rest;
pub mod forward;
pub mod instrument;
pub mod jsonrpc;
pub mod namespace;
pub mod observability;
pub mod policy;
pub mod pool;
pub mod relay;
pub mod resilience;
pub mod router;
pub mod routing;
pub mod schema;
pub mod security;
pub mod server;
pub mod signing;
pub mod status;
pub mod subscriptions;
pub mod tap;
pub mod tasks;
pub mod stateless;
pub mod transparent;
pub mod transparent_l2;
pub mod transport;
pub mod variants;
pub mod version;

pub use cache::ListCache;
pub use forward::ForwardProxy;
pub use policy::PolicyLayer;
pub use relay::Relay;
pub use resilience::{
    BackendChannel, CircuitBreaker, CircuitState, ManagedBackend, ResilientRouter,
};
pub use router::{Backend, ForwardRouter};
pub use stateless::{StatelessBackend, StatelessForwarder};
pub use transparent::{HeaderPolicy, TransparentL1};
pub use transparent_l2::{TransparentL2Stateful, TransparentL2Stateless};
pub use transport::{
    parse_content_length, FramedReader, FramedWriter, LineReader, LineWriter, MessageRead,
    MessageWrite, SseReader, SseWriter,
};
