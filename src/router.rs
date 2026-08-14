//! Forward router, multi-backend (SEP §3, draft §5.2, §6.3).
//!
//! Aggregates several backends. List methods fan out and are namespaced;
//! `tools/call` reverse-resolves to one backend. Each backend runs a demuxing
//! reader: responses match pending requests by id, and backend-initiated
//! messages go to a sink. When a backend carries a circuit breaker the router
//! is resilient: it drops unavailable backends from `tools/list` (reporting the
//! omission), returns `-32003` for calls to them, announces surface changes with
//! `tools/list_changed`, applies a per-request timeout, and pings each backend
//! on a health interval.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio::task::AbortHandle;

use crate::cache::ListCache;
use crate::capability;
use crate::collision;
use crate::config::Namespacing;
use crate::filters::{self, FilterChain};
use crate::errors;
use crate::forward::{proxy_server_info, PROXY_PROTOCOL_VERSION};
use crate::handler::Registry;
use crate::jsonrpc::{self, INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND};
use crate::namespace;
use crate::observability;
use crate::pool::{self, InFlight};
use crate::resilience::{partial_meta, CircuitBreaker, SERVER_NOT_AVAILABLE};
use crate::routing;
use crate::schema;
use crate::server;
use crate::signing::{attestation_record, outcome_record, AuditLog};
use crate::subscriptions::{self, Subscriptions};
use crate::tasks;
use crate::transport::{MessageRead, MessageWrite};
use crate::variants;

type Pending = Arc<StdMutex<HashMap<String, oneshot::Sender<Value>>>>;
type Breaker = Arc<StdMutex<CircuitBreaker>>;
type SharedCache = Arc<StdMutex<ListCache>>;
// client-facing id -> (backend id, the backend's own request id). Correlates a
// backend-initiated request so the client's reply routes back (SEP §5.1).
type ServerRequests = Arc<StdMutex<HashMap<String, (String, Value)>>>;
// exposed tool name -> (backend id, original name). Reverse map for the collision
// strategies that do not keep a prefix (passthrough), so a tools/call by the
// exposed name still resolves to one backend (SEP §3.4).
type ReverseMap = Arc<StdMutex<HashMap<String, (String, String)>>>;

/// Backend notifications that invalidate a cached list for that backend (SEP §6.2).
const LIST_CHANGED_METHODS: [&str; 3] = [
    "notifications/tools/list_changed",
    "notifications/prompts/list_changed",
    "notifications/resources/list_changed",
];

fn clock() -> f64 {
    use std::sync::OnceLock;
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_secs_f64()
}

/// How a namespaced capability kind is named: tool/prompt names use the `__`
/// delimiter, resource URIs insert the id as a path segment.
enum Kind {
    Name,
    Uri,
}

struct Capability {
    list: &'static str,
    call: &'static str,
    collection: &'static str,
    field: &'static str,
    kind: Kind,
}

const CAPABILITIES: &[Capability] = &[
    Capability { list: "tools/list", call: "tools/call", collection: "tools", field: "name", kind: Kind::Name },
    Capability { list: "prompts/list", call: "prompts/get", collection: "prompts", field: "name", kind: Kind::Name },
    Capability { list: "resources/list", call: "resources/read", collection: "resources", field: "uri", kind: Kind::Uri },
];

fn label(kind: &Kind, id: &str, value: &str) -> String {
    match kind {
        Kind::Name => namespace::prefix(id, value),
        Kind::Uri => namespace::prefix_uri(id, value),
    }
}

fn resolve(kind: &Kind, value: &str) -> Option<(String, String)> {
    match kind {
        Kind::Name => namespace::split(value).map(|(a, b)| (a.to_string(), b.to_string())),
        Kind::Uri => namespace::split_uri(value),
    }
}

/// Process-unique trace and span ids. Non-cryptographic; a deployment that needs
/// unguessable ids supplies its own generator upstream.
fn trace_ids() -> (String, String) {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    (format!("{n:032x}"), format!("{n:016x}"))
}

/// The request-able state of a backend, shared so the route loop and the health
/// pinger can both send to it.
struct BackendHandle<BW> {
    id: String,
    writer: Arc<Mutex<BW>>,
    next_id: Arc<AtomicU64>,
    pending: Pending,
    breaker: Option<Breaker>,
    timeout: Option<Duration>,
    // The backend's own credential, injected into forwarded requests; the
    // client's is never forwarded (SEP §13.1, confused deputy).
    token: Option<String>,
}

impl<BW> Clone for BackendHandle<BW> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            writer: self.writer.clone(),
            next_id: self.next_id.clone(),
            pending: self.pending.clone(),
            breaker: self.breaker.clone(),
            timeout: self.timeout,
            token: self.token.clone(),
        }
    }
}

impl<BW: MessageWrite> BackendHandle<BW> {
    fn available(&self) -> bool {
        match &self.breaker {
            None => true,
            Some(breaker) => breaker.lock().unwrap().allow(clock()),
        }
    }

    async fn request(&self, method: &str, mut params: Value) -> io::Result<Value> {
        // Confused-deputy: strip any client credential and inject the backend's
        // own before forwarding (SEP §13.1). Skip when there is nothing to do so
        // an empty _meta is not added to every request.
        let has_client_cred = params
            .get("_meta")
            .and_then(Value::as_object)
            .map(|m| m.contains_key(crate::auth::AUTHORIZATION_META_KEY))
            .unwrap_or(false);
        if self.token.is_some() || has_client_cred {
            let meta = params.get("_meta").cloned().unwrap_or_else(|| json!({}));
            let forwarded = crate::auth::forward_meta(&meta, self.token.as_deref());
            if let Some(object) = params.as_object_mut() {
                object.insert("_meta".to_string(), forwarded);
            }
        }
        let n = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mid = format!("{}-{}", self.id, n);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(mid.clone(), tx);
        let request = json!({ "jsonrpc": "2.0", "id": mid, "method": method, "params": params });
        {
            let mut writer = self.writer.lock().await;
            writer.send(&jsonrpc::encode(&request)).await?;
        }
        let outcome = match self.timeout {
            Some(duration) => match tokio::time::timeout(duration, rx).await {
                Ok(result) => result.map_err(|_| closed(&self.id)),
                Err(_) => {
                    self.pending.lock().unwrap().remove(&mid);
                    Err(io::Error::new(io::ErrorKind::TimedOut, format!("backend {} timed out", self.id)))
                }
            },
            None => rx.await.map_err(|_| closed(&self.id)),
        };
        if let Some(breaker) = &self.breaker {
            match &outcome {
                Ok(_) => breaker.lock().unwrap().record_success(),
                Err(_) => breaker.lock().unwrap().record_failure(clock()),
            }
        }
        outcome
    }
}

fn closed(id: &str) -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, format!("backend {id} closed"))
}

/// Append the proxy's own hop to a result's `_meta` when tracing is on (SEP §7.1).
/// Free-standing so both the inline route path and a spawned pool task apply the
/// identical trace without borrowing the router.
fn apply_trace(trace: bool, mut result: Value) -> Value {
    if !trace {
        return result;
    }
    let meta = result.get("_meta").cloned().unwrap_or_else(|| json!({}));
    if let Some(object) = result.as_object_mut() {
        object.insert("_meta".to_string(), observability::append_hop(&meta, "forward"));
    }
    result
}

/// Serve a `tools/call` from a local handler (draft §5.3/§5.7): validate the
/// arguments against the tool's `inputSchema` (σ1), run the handler, validate the
/// result against `outputSchema`, and trace it. The single source both the inline
/// dispatch and the spawned worker-pool task use, so they cannot diverge. The
/// caller has already resolved `handler_id`/`original` and guarantees the handler
/// exists; a vanished handler (only reachable if a filter renamed the call) yields
/// a method-not-found error.
/// The server-role flags a local dispatch needs: schema validation, hop tracing,
/// and the σ5 output cap. Bundled so the dispatch helpers stay within a sane
/// argument count and cannot pass them in the wrong order.
#[derive(Clone, Copy)]
struct DispatchOpts {
    validate: bool,
    trace: bool,
    output_limit: usize,
}

async fn dispatch_local(registry: &Registry, opts: DispatchOpts, handler_id: &str, original: &str, arguments: &Value, id: &Value) -> Value {
    let validate_schemas = opts.validate;
    let trace = opts.trace;
    let handler = match registry.handler_for(handler_id) {
        Some(handler) => handler,
        None => {
            return json!({ "jsonrpc": "2.0", "id": id, "error": { "code": METHOD_NOT_FOUND, "message": format!("unknown tool: {handler_id}") } });
        }
    };
    let tool = validate_schemas
        .then(|| handler.list_tools().into_iter().find(|t| t.get("name").and_then(Value::as_str) == Some(original)))
        .flatten();
    if let Some(tool) = &tool {
        if let Some(err) = schema::validate_call_args(tool.get("inputSchema"), arguments) {
            return json!({ "jsonrpc": "2.0", "id": id, "error": err });
        }
    }
    let result = handler.call_tool(original, arguments).await;
    if let Some(tool) = &tool {
        if let Some(err) = schema::validate_call_result(tool.get("outputSchema"), &result) {
            return json!({ "jsonrpc": "2.0", "id": id, "error": err });
        }
    }
    // σ5: bound the server's own output. An oversize local result is the server's
    // fault, so it is a server-class error.
    if server::exceeds_output_cap(&result, opts.output_limit) {
        return json!({ "jsonrpc": "2.0", "id": id, "error": errors::error_object(errors::INTERNAL_ERROR, Some("server output exceeds size limit")) });
    }
    json!({ "jsonrpc": "2.0", "id": id, "result": apply_trace(trace, result) })
}

/// A monotonic-ish wall clock in milliseconds for the pool's idle bookkeeping.
/// Only compared within one process, so the epoch base is irrelevant.
fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Run a pooled server-originated call: the ε0 filter chain (a block returns the
/// `-32001` and records a failed audit outcome, as [`ForwardRouter::route_call`]
/// does) and then the shared local dispatch on the possibly-mutated request.
/// Mirrors the inline path so a pooled call behaves identically.
async fn run_pooled(
    registry: &Registry,
    filter_chain: &Option<Arc<FilterChain>>,
    audit: &Option<Arc<StdMutex<AuditLog>>>,
    opts: DispatchOpts,
    mut message: Value,
    id: &Value,
) -> Value {
    if let Some(chain) = filter_chain {
        let outcome = chain.run(filters::REQUEST, &message);
        if outcome["action"] == "block" {
            let name = message.get("params").and_then(|p| p.get("name")).and_then(Value::as_str);
            if let Some(log) = audit {
                if let Ok(mut guard) = log.lock() {
                    guard.append(outcome_record("tools/call", name, false));
                }
            }
            return outcome["response"].clone();
        }
        message = outcome["message"].clone();
    }
    let name = message.get("params").and_then(|p| p.get("name")).and_then(Value::as_str).unwrap_or("").to_string();
    let arguments = message.get("params").and_then(|p| p.get("arguments")).cloned().unwrap_or_else(|| json!({}));
    match namespace::split(&name) {
        Some((bid, original)) => dispatch_local(registry, opts, bid, original, &arguments, id).await,
        None => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": METHOD_NOT_FOUND, "message": format!("unknown tool: {name}") } }),
    }
}

pub struct Backend<BR, BW> {
    handle: BackendHandle<BW>,
    reader: Option<BR>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
    pub capabilities: Value,
    pub server_info: Option<Value>,
    // Keyword hints (SEP-2614) used to pre-select this backend on a filtered
    // list; empty means the backend is always queried.
    keywords: Vec<String>,
}

impl<BR, BW> Backend<BR, BW>
where
    BR: MessageRead + Send + 'static,
    BW: MessageWrite + Send + 'static,
{
    pub fn new(id: impl Into<String>, reader: BR, writer: BW) -> io::Result<Self> {
        Self::build(id, reader, writer, None, None)
    }

    pub fn resilient(
        id: impl Into<String>,
        reader: BR,
        writer: BW,
        breaker: CircuitBreaker,
        timeout: Option<Duration>,
    ) -> io::Result<Self> {
        Self::build(id, reader, writer, Some(Arc::new(StdMutex::new(breaker))), timeout)
    }

    fn build(
        id: impl Into<String>,
        reader: BR,
        writer: BW,
        breaker: Option<Breaker>,
        timeout: Option<Duration>,
    ) -> io::Result<Self> {
        let id = id.into();
        if !namespace::valid_backend_id(&id) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("invalid backend id: {id}")));
        }
        Ok(Self {
            handle: BackendHandle {
                id,
                writer: Arc::new(Mutex::new(writer)),
                next_id: Arc::new(AtomicU64::new(0)),
                pending: Arc::new(StdMutex::new(HashMap::new())),
                breaker,
                timeout,
                token: None,
            },
            reader: Some(reader),
            reader_task: None,
            capabilities: json!({}),
            server_info: None,
            keywords: Vec::new(),
        })
    }

    /// Declare keyword hints for keyword pre-selection (SEP-2614).
    pub fn with_keywords(mut self, keywords: Vec<String>) -> Self {
        self.keywords = keywords;
        self
    }

    /// Set the backend's own credential, injected into forwarded requests
    /// (SEP §13.1); the client's credential is never forwarded.
    pub fn with_token(mut self, token: String) -> Self {
        self.handle.token = Some(token);
        self
    }

    fn id(&self) -> &str {
        &self.handle.id
    }

    fn available(&self) -> bool {
        self.handle.available()
    }

    fn handle(&self) -> BackendHandle<BW> {
        self.handle.clone()
    }

    fn start(
        &mut self,
        sink: mpsc::Sender<Value>,
        cache: Option<SharedCache>,
        server_requests: ServerRequests,
        server_req_seq: Arc<AtomicU64>,
        single: bool,
    ) {
        let mut reader = self.reader.take().expect("start called once");
        let pending = self.handle.pending.clone();
        let id = self.handle.id.clone();
        self.reader_task = Some(tokio::spawn(async move {
            while let Ok(Some(raw)) = reader.receive().await {
                let message: Value = match jsonrpc::decode(&raw) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let key = message.get("id").and_then(Value::as_str).map(str::to_string);
                let responder = key.and_then(|k| pending.lock().unwrap().remove(&k));
                match responder {
                    Some(tx) => {
                        let _ = tx.send(message);
                    }
                    None => {
                        let is_request =
                            message.get("id").is_some() && jsonrpc::method_of(&message).is_some();
                        if is_request {
                            // A backend-initiated request: mint a unique
                            // client-facing id and remember the backend's own id
                            // so the client's reply routes back (SEP §5.1).
                            let n = server_req_seq.fetch_add(1, Ordering::SeqCst) + 1;
                            let client_id = format!("srv-{n}");
                            let original = message.get("id").cloned().unwrap_or(Value::Null);
                            server_requests
                                .lock()
                                .unwrap()
                                .insert(client_id.clone(), (id.clone(), original));
                            let mut rewritten = message;
                            if let Some(object) = rewritten.as_object_mut() {
                                object.insert("id".to_string(), Value::String(client_id));
                            }
                            if sink.send(rewritten).await.is_err() {
                                break;
                            }
                        } else {
                            // A backend's own list_changed invalidates its cached
                            // lists (SEP §6.2) before the notification is relayed.
                            if let Some(cache) = &cache {
                                if let Some(method) = jsonrpc::method_of(&message) {
                                    if LIST_CHANGED_METHODS.contains(&method) {
                                        cache.lock().unwrap().invalidate_backend(&id);
                                    }
                                }
                            }
                            // A task event carries the backend's own taskId;
                            // re-namespace it to the backend__taskId the client
                            // holds before relaying (SEP-2694).
                            let message = if jsonrpc::method_of(&message) == Some(tasks::TASK_EVENT_METHOD) {
                                tasks::namespace_event(&message, &id)
                            } else if !single && jsonrpc::method_of(&message) == Some(subscriptions::UPDATED_METHOD) {
                                // A backend's resources/updated carries the backend's
                                // own uri; re-namespace it to the backend__uri the
                                // client subscribed with before relaying (σ4). Single
                                // backend passes names through, so its uri is not
                                // rewritten (matching subscribe passthrough).
                                subscriptions::namespace_updated(&message, &id)
                            } else {
                                message
                            };
                            if sink.send(message).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            // On EOF or error, drop every pending sender so awaiting requests
            // fail instead of hanging forever.
            pending.lock().unwrap().clear();
        }));
    }

    async fn request(&self, method: &str, params: Value) -> io::Result<Value> {
        self.handle.request(method, params).await
    }

    /// Send a raw message to the backend without tracking a response. Used to
    /// relay a client's reply to a backend-initiated request and to forward
    /// client notifications (SEP §5.1).
    async fn send_message(&self, message: &Value) -> io::Result<()> {
        self.handle.writer.lock().await.send(&jsonrpc::encode(message)).await
    }

    async fn handshake(&mut self) -> io::Result<()> {
        let init = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": PROXY_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": proxy_server_info(),
                }),
            )
            .await?;
        if let Some(result) = init.get("result") {
            self.capabilities = result.get("capabilities").cloned().unwrap_or_else(|| json!({}));
            self.server_info = result.get("serverInfo").cloned();
        }
        let initialized = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        self.handle.writer.lock().await.send(&jsonrpc::encode(&initialized)).await
    }

    async fn close(&mut self) -> io::Result<()> {
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        self.handle.writer.lock().await.send_eof().await
    }
}

/// Watches which backends are available and announces changes to the client.
struct SurfaceWatcher<CW> {
    backends: Vec<(String, Option<Breaker>)>,
    surface: Arc<Mutex<HashSet<String>>>,
    client_write: Arc<Mutex<CW>>,
    cache: Option<SharedCache>,
}

impl<CW> Clone for SurfaceWatcher<CW> {
    fn clone(&self) -> Self {
        Self {
            backends: self.backends.clone(),
            surface: self.surface.clone(),
            client_write: self.client_write.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<CW: MessageWrite> SurfaceWatcher<CW> {
    fn available(&self) -> HashSet<String> {
        self.backends
            .iter()
            .filter(|(_, breaker)| match breaker {
                None => true,
                Some(cb) => cb.lock().unwrap().allow(clock()),
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    async fn reset(&self) {
        *self.surface.lock().await = self.available();
    }

    async fn emit_if_changed(&self) {
        let current = self.available();
        let mut surface = self.surface.lock().await;
        if current != *surface {
            // A backend that left the available set has an open breaker; drop
            // its cached lists so a recovered backend is re-fetched (SEP §6.2).
            if let Some(cache) = &self.cache {
                for departed in surface.difference(&current) {
                    cache.lock().unwrap().invalidate_backend(departed);
                }
            }
            *surface = current;
            let notification = json!({ "jsonrpc": "2.0", "method": "notifications/tools/list_changed" });
            let _ = self.client_write.lock().await.send(&jsonrpc::encode(&notification)).await;
        }
    }
}

pub struct ForwardRouter<CR, CW, BR, BW> {
    client_read: CR,
    client_write: Arc<Mutex<CW>>,
    backends: Vec<Backend<BR, BW>>,
    server_tx: Option<mpsc::Sender<Value>>,
    server_rx: Option<mpsc::Receiver<Value>>,
    sink: Option<mpsc::Sender<Value>>,
    resilient: bool,
    single: bool,
    trace: bool,
    disclose: bool,
    disclose_threshold: usize,
    health_interval: Option<Duration>,
    watcher: SurfaceWatcher<CW>,
    // Optional shared list cache (SEP §6). When several client connections share
    // one cache, repeated list fetches collapse to O(backends). The principal
    // scopes private entries so they never cross users.
    cache: Option<SharedCache>,
    principal: Option<String>,
    // Correlation for backend-initiated requests (SEP §5.1).
    server_requests: ServerRequests,
    server_req_seq: Arc<AtomicU64>,
    // Reverse map for the passthrough collision strategy (SEP §3.4), populated
    // as tools are listed.
    reverse: ReverseMap,
    // Optional accountability log (corpus SEP-2828/2787). When set, each routed
    // call appends a pre-call attestation and a post-call outcome, best-effort:
    // a lock failure is swallowed so audit never blocks or fails traffic.
    audit: Option<Arc<StdMutex<AuditLog>>>,
    // Collision resolution strategy (SEP §3.4). Default prefix; priority drops
    // lower-priority duplicates of the same original tool name.
    namespacing: Namespacing,
    // Local handlers that originate responses (draft §5.3/§5.7). Their tools
    // merge into tools/list and a tools/call whose prefix names a handler is
    // served locally rather than routed. Held in an Arc so a pooled call task can
    // share it (σ2).
    registry: Arc<Registry>,
    // Expected token claims (SEP-2468). When set, the client's initialize must
    // carry matching iss/aud claims or the handshake is rejected.
    issuer: Option<String>,
    audience: Option<String>,
    // Composed server variants (SEP-2053), filled at handshake by intersecting the
    // backends' offerings. The first is the default; empty means no backend
    // supports variants, so a selected variant is rejected.
    variants: Vec<String>,
    // Extension filter chain run on the client request before it is routed (ε0).
    // None means no filters, so the seam costs nothing. Held in
    // an Arc so a pooled call task can share it (σ2).
    filter_chain: Option<Arc<FilterChain>>,
    // Cache directives (ttlMs, cacheScope) the server attaches to its served list
    // results (σ0). None means no directives, so nothing is added.
    list_directives: Option<(u64, String)>,
    // Validate a local handler's tools/call arguments against its inputSchema and
    // its result against outputSchema (σ1). A server-role act, off by
    // default so existing flows stay byte-identical.
    validate_schemas: bool,
    // Worker pool for server-originated calls (σ2). Off by default, so
    // the route loop stays serial and existing flows byte-identical. When on, a
    // tools/call resolving to a local handler runs as a bounded, cancellable
    // spawned task; the shared state below is the bookkeeping.
    pool_enabled: bool,
    pool_idle_ms: u64,
    // A permit-bounded semaphore caps concurrent pooled calls; None is unbounded.
    pool_sem: Option<Arc<Semaphore>>,
    // The in-flight registry (deadlines) and the abort handles by request-id key,
    // so a notifications/cancelled can stop a running call.
    inflight: Arc<StdMutex<InFlight>>,
    pool_tasks: Arc<StdMutex<HashMap<String, AbortHandle>>>,
    // progressToken key -> in-flight call id, so a progress notification finds the
    // call whose idle deadline it resets.
    pool_tokens: Arc<StdMutex<HashMap<String, Value>>>,
    // Server-side task origination (σ3). Off by default; when on, a
    // task-augmented local call returns a working handle and runs in the
    // background, and the store serves the later tasks/get and tasks/cancel.
    server_tasks: bool,
    tasks_store: Arc<StdMutex<tasks::ServerTasks>>,
    task_handles: Arc<StdMutex<HashMap<String, AbortHandle>>>,
    // Server-side resource subscriptions (σ4). Off by default; when on,
    // a subscribe whose URI does not resolve to a backend is registered in this
    // per-connection registry, and a `ResourcePublisher` (cloned before `serve`
    // consumes the router) fans out `notifications/resources/updated` only to the
    // subscribed URIs. Proxy-side routing of subscribe/unsubscribe needs no toggle.
    subscriptions_enabled: bool,
    subscriptions: Arc<StdMutex<Subscriptions>>,
    // Deterministic output/memory bound (σ5): a server-originated result
    // whose encoded form exceeds this cap is rejected with a server-class error
    // rather than emitted. Defaults to the frame ceiling, so it never trips in
    // normal use and existing flows stay byte-identical.
    output_limit: usize,
    // Graceful drain window in ms (σ5): on shutdown, in-flight server-originated
    // work is given this long to finish before it is aborted. 0 means abort
    // immediately, the σ2/σ3 behavior, so existing flows are unchanged.
    drain_ms: u64,
}

/// A cloneable handle a resource source holds to publish updates to the client
/// (σ4). Cloned from the router with [`ForwardRouter::resource_publisher`] before
/// `serve` consumes the router; it shares the router's subscription registry and
/// client transport, so `publish` fans out only to subscribed URIs.
pub struct ResourcePublisher<CW> {
    subscriptions: Arc<StdMutex<Subscriptions>>,
    client_write: Arc<Mutex<CW>>,
}

impl<CW: MessageWrite> ResourcePublisher<CW> {
    /// Notify the client of a resource change, but only if it subscribed to `uri`.
    /// Returns whether a notification was sent, so an unsubscribed resource costs
    /// nothing (the efficient fan-out).
    pub async fn publish(&self, uri: &str) -> io::Result<bool> {
        if !self.subscriptions.lock().unwrap().contains(uri) {
            return Ok(false);
        }
        self.client_write.lock().await.send(&jsonrpc::encode(&subscriptions::updated_notification(uri))).await?;
        Ok(true)
    }
}

impl<CR, CW, BR, BW> ForwardRouter<CR, CW, BR, BW>
where
    CR: MessageRead,
    CW: MessageWrite + Send + 'static,
    BR: MessageRead + Send + 'static,
    BW: MessageWrite + Send + 'static,
{
    pub fn new(client_read: CR, client_write: CW, backends: Vec<Backend<BR, BW>>) -> Self {
        Self::configure(client_read, client_write, backends, None, None)
    }

    pub fn with_server_sink(
        client_read: CR,
        client_write: CW,
        backends: Vec<Backend<BR, BW>>,
        sink: Option<mpsc::Sender<Value>>,
    ) -> Self {
        Self::configure(client_read, client_write, backends, sink, None)
    }

    /// `sink` receives backend-initiated messages (default: the client
    /// transport). `health_interval` enables active health pings when any
    /// backend carries a breaker.
    pub fn configure(
        client_read: CR,
        client_write: CW,
        backends: Vec<Backend<BR, BW>>,
        sink: Option<mpsc::Sender<Value>>,
        health_interval: Option<Duration>,
    ) -> Self {
        let (server_tx, server_rx) = mpsc::channel(256);
        let resilient = backends.iter().any(|b| b.handle.breaker.is_some());
        // A single-backend intermediary MUST NOT modify names (SEP §5.3).
        let single = backends.len() == 1;
        let client_write = Arc::new(Mutex::new(client_write));
        let watcher = SurfaceWatcher {
            backends: backends.iter().map(|b| (b.id().to_string(), b.handle.breaker.clone())).collect(),
            surface: Arc::new(Mutex::new(backends.iter().map(|b| b.id().to_string()).collect())),
            client_write: client_write.clone(),
            cache: None,
        };
        Self {
            client_read,
            client_write,
            backends,
            server_tx: Some(server_tx),
            server_rx: Some(server_rx),
            sink,
            resilient,
            single,
            trace: true, // an intermediary records its hop by default (SEP §7.1)
            disclose: false,
            disclose_threshold: capability::DEFAULT_TOOL_THRESHOLD,
            health_interval,
            watcher,
            cache: None,
            principal: None,
            server_requests: Arc::new(StdMutex::new(HashMap::new())),
            server_req_seq: Arc::new(AtomicU64::new(0)),
            reverse: Arc::new(StdMutex::new(HashMap::new())),
            audit: None,
            namespacing: Namespacing::default(),
            registry: Arc::new(Registry::empty()),
            issuer: None,
            audience: None,
            variants: Vec::new(),
            filter_chain: None,
            list_directives: None,
            validate_schemas: false,
            pool_enabled: false,
            pool_idle_ms: 0,
            pool_sem: None,
            inflight: Arc::new(StdMutex::new(InFlight::new())),
            pool_tasks: Arc::new(StdMutex::new(HashMap::new())),
            pool_tokens: Arc::new(StdMutex::new(HashMap::new())),
            server_tasks: false,
            tasks_store: Arc::new(StdMutex::new(tasks::ServerTasks::new())),
            task_handles: Arc::new(StdMutex::new(HashMap::new())),
            subscriptions_enabled: false,
            subscriptions: Arc::new(StdMutex::new(Subscriptions::new())),
            output_limit: server::MAX_OUTPUT_BYTES,
            drain_ms: 0,
        }
    }

    /// Require the client's token claims to match `issuer`/`audience` (SEP-2468).
    pub fn set_auth(mut self, issuer: Option<String>, audience: Option<String>) -> Self {
        self.issuer = issuer;
        self.audience = audience;
        self
    }

    /// Set the collision resolution strategy (SEP §3.4). Default is prefix.
    pub fn set_namespacing(mut self, namespacing: Namespacing) -> Self {
        self.namespacing = namespacing;
        self
    }

    /// Attach an extension filter chain run on each client call request before
    /// routing (ε0). A block returns a `-32001`; a forward may
    /// carry a mutated request onward.
    pub fn set_filters(mut self, chain: FilterChain) -> Self {
        self.filter_chain = Some(Arc::new(chain));
        self
    }

    /// Serve server-originated calls from a bounded worker pool (σ2, §5). A
    /// `tools/call` that resolves to a local handler then runs concurrently under
    /// a per-connection cap (`cap` simultaneous calls; `0` is unbounded), is
    /// cancellable by `notifications/cancelled`, and is bounded by an idle
    /// deadline (`idle_ms`; `0` means no deadline). Off by default, so the route
    /// loop stays serial and existing flows byte-identical.
    pub fn set_worker_pool(mut self, cap: u64, idle_ms: u64) -> Self {
        self.pool_enabled = true;
        self.pool_idle_ms = idle_ms;
        self.pool_sem = (cap > 0).then(|| Arc::new(Semaphore::new(cap as usize)));
        self
    }

    /// Originate server-side task handles for task-augmented local calls (σ3, §5).
    /// A `tools/call` that resolves to a local handler and carries the task
    /// augmentation then returns a `working` handle at once, runs in the
    /// background, and its later `tasks/get`/`tasks/cancel` are served from the
    /// store. Off by default; when off a task-augmented call is answered
    /// synchronously (the augmentation is ignored).
    pub fn set_server_tasks(mut self, on: bool) -> Self {
        self.server_tasks = on;
        self
    }

    /// Originate server-side resource subscriptions (σ4, §5). A
    /// `resources/subscribe` whose URI does not resolve to a backend is then
    /// registered in a per-connection registry (an empty result is returned), and
    /// a [`ResourcePublisher`] sends `notifications/resources/updated` only to the
    /// subscribed URIs. Off by default; when off such a subscribe is rejected as
    /// an unknown resource. Proxy-side subscribe/unsubscribe to a backend routes
    /// regardless of this toggle.
    pub fn set_resource_subscriptions(mut self, on: bool) -> Self {
        self.subscriptions_enabled = on;
        self
    }

    /// Cap the encoded size of a server-originated result (σ5, §5). A local
    /// handler's result over `max_bytes` is rejected with a server-class error
    /// (`-32603`) instead of emitted, so a runaway handler cannot force an
    /// unbounded frame. `0` disables the cap. Defaults to the frame ceiling
    /// (`MAX_OUTPUT_BYTES`), which never trips in normal use. The proxy role never
    /// caps a routed backend response (the decoder that read it already bounded it).
    pub fn set_output_limit(mut self, max_bytes: usize) -> Self {
        self.output_limit = max_bytes;
        self
    }

    /// Drain in-flight server-originated work gracefully on shutdown (σ5, §5). When
    /// the client disconnects, running pooled calls and server tasks are given up
    /// to `drain_ms` to finish (and send their responses) before being aborted.
    /// `0` (the default) aborts immediately, the σ2/σ3 behavior.
    pub fn set_drain_timeout(mut self, drain_ms: u64) -> Self {
        self.drain_ms = drain_ms;
        self
    }

    /// A handle for a resource source to publish updates to this connection's
    /// client (σ4). Clone it before `serve` consumes the router; it shares the
    /// subscription registry and client transport.
    pub fn resource_publisher(&self) -> ResourcePublisher<CW> {
        ResourcePublisher {
            subscriptions: self.subscriptions.clone(),
            client_write: self.client_write.clone(),
        }
    }

    /// Advertise SEP-2549 cache directives on served list results (σ0, §5), so a
    /// downstream cache can cache yamp's own composed surface.
    pub fn set_list_directives(mut self, ttl_ms: u64, cache_scope: impl Into<String>) -> Self {
        self.list_directives = Some((ttl_ms, cache_scope.into()));
        self
    }

    /// Validate local-handler calls against their declared schemas (σ1, §5): a
    /// `tools/call`'s arguments against the tool's `inputSchema` before the
    /// handler runs, and the result against `outputSchema` before it leaves. Off
    /// by default; a routed backend's calls are never validated (the proxy role
    /// does not assume a schema it did not author).
    pub fn set_validate_schemas(mut self, on: bool) -> Self {
        self.validate_schemas = on;
        self
    }

    /// Attach a shared accountability log (corpus SEP-2828/2787). Each routed
    /// call then appends a pre-call attestation and a post-call outcome. The log
    /// may be shared across connections.
    pub fn set_audit(mut self, audit: Arc<StdMutex<AuditLog>>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Append a record to the audit log, best-effort: a poisoned lock is
    /// swallowed so accountability never blocks or fails the served path.
    fn record_audit(&self, record: Value) {
        if let Some(log) = &self.audit {
            if let Ok(mut guard) = log.lock() {
                guard.append(record);
            }
        }
    }

    /// Attach local handlers (draft §5.3/§5.7). A registry adds a second name
    /// source, so single-backend passthrough is disabled when it is non-empty.
    pub fn set_registry(mut self, registry: Registry) -> Self {
        self.single = self.backends.len() == 1 && registry.ids().is_empty();
        self.registry = Arc::new(registry);
        self
    }

    /// Attach a shared list cache and the principal that scopes its private
    /// entries (SEP §6). The same cache handle may be shared across connections.
    pub fn set_cache(mut self, cache: SharedCache, principal: Option<String>) -> Self {
        self.watcher.cache = Some(cache.clone());
        self.cache = Some(cache);
        self.principal = principal;
        self
    }

    /// Turn hop tracing off (used in tests).
    pub fn set_trace(mut self, trace: bool) -> Self {
        self.trace = trace;
        self
    }

    /// Enable progressive disclosure over `threshold` tools (SEP §6).
    pub fn set_disclose(mut self, threshold: usize) -> Self {
        self.disclose = true;
        self.disclose_threshold = threshold;
        self
    }

    fn trace_request(&self, mut params: Value) -> Value {
        if !self.trace {
            return params;
        }
        let meta = params.get("_meta").cloned().unwrap_or_else(|| json!({}));
        let meta = observability::ensure_trace_context(&observability::append_hop(&meta, "forward"), trace_ids);
        if let Some(object) = params.as_object_mut() {
            object.insert("_meta".to_string(), meta);
        }
        params
    }

    fn trace_result(&self, result: Value) -> Value {
        apply_trace(self.trace, result)
    }

    pub async fn serve(mut self) -> io::Result<()> {
        if !self.handshake().await? {
            return Ok(());
        }
        let mut rx = self.server_rx.take().expect("serve called once");
        let client_write = self.client_write.clone();
        let sink = self.sink.clone();
        let drain = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                match &sink {
                    Some(tx) => {
                        let _ = tx.send(message).await;
                    }
                    None => {
                        let _ = client_write.lock().await.send(&jsonrpc::encode(&message)).await;
                    }
                }
            }
        });

        let mut health = Vec::new();
        if self.resilient {
            if let Some(interval) = self.health_interval {
                for backend in &self.backends {
                    let handle = backend.handle();
                    let watcher = self.watcher.clone();
                    health.push(tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(interval).await;
                            let _ = handle.request("ping", json!({})).await;
                            watcher.emit_if_changed().await;
                        }
                    }));
                }
            }
        }

        self.route_loop().await?;
        self.drain().await;
        for task in health {
            task.abort();
        }
        for backend in &mut self.backends {
            backend.close().await?;
        }
        self.server_tx = None;
        let _ = drain.await;
        Ok(())
    }

    /// Drain the worker pool and any running server tasks at shutdown (σ2/σ3/σ5).
    /// With a drain window (σ5) the in-flight work is first given up to `drain_ms`
    /// to finish and send its response: since a completed task removes its own
    /// handle from the maps, an empty pair of maps means every call drained, so we
    /// poll them empty (bounded by the deadline). Whatever is still running past the
    /// window is then aborted. A window of 0 aborts immediately, the σ2/σ3 behavior.
    async fn drain(&self) {
        if self.drain_ms > 0 {
            let deadline = pool::deadline(now_ms(), self.drain_ms);
            loop {
                let in_flight = !self.pool_tasks.lock().unwrap().is_empty() || !self.task_handles.lock().unwrap().is_empty();
                if !in_flight || pool::expired(deadline, now_ms()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        for (_, handle) in self.pool_tasks.lock().unwrap().drain() {
            handle.abort();
        }
        for (_, handle) in self.task_handles.lock().unwrap().drain() {
            handle.abort();
        }
    }

    async fn send_client(&self, payload: &[u8]) -> io::Result<()> {
        self.client_write.lock().await.send(payload).await
    }

    async fn handshake(&mut self) -> io::Result<bool> {
        let raw = match self.client_read.receive().await? {
            Some(raw) => raw,
            None => return Ok(false),
        };
        let client_init = jsonrpc::decode(&raw)?;
        if jsonrpc::method_of(&client_init) != Some("initialize") {
            let id = client_init.get("id").cloned().unwrap_or(Value::Null);
            let reply = json!({ "jsonrpc": "2.0", "id": id, "error": { "code": INVALID_REQUEST, "message": "expected initialize" } });
            self.send_client(&jsonrpc::encode(&reply)).await?;
            return Err(io::Error::new(io::ErrorKind::InvalidData, "first client message was not initialize"));
        }

        // Validate the client's token claims before trusting it (SEP-2468,
        // confused deputy). Claims travel in the initialize _meta.
        if self.issuer.is_some() || self.audience.is_some() {
            let claims = client_init
                .get("params")
                .and_then(|p| p.get("_meta"))
                .and_then(|m| m.get(crate::auth::CLAIMS_META_KEY))
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !crate::auth::claims_valid(&claims, self.issuer.as_deref(), self.audience.as_deref()) {
                let id = client_init.get("id").cloned().unwrap_or(Value::Null);
                let reply = json!({ "jsonrpc": "2.0", "id": id, "error": { "code": crate::errors::UNAUTHORIZED, "message": "invalid token claims" } });
                self.send_client(&jsonrpc::encode(&reply)).await?;
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "client token claims failed iss/aud validation"));
            }
        }

        let server_tx = self.server_tx.clone().expect("server_tx");
        let client_caps = client_init.get("params").and_then(|p| p.get("capabilities")).cloned();
        let mut backend_caps: Vec<Value> = Vec::new();
        let single = self.single;
        for backend in &mut self.backends {
            backend.start(
                server_tx.clone(),
                self.cache.clone(),
                self.server_requests.clone(),
                self.server_req_seq.clone(),
                single,
            );
            match backend.handshake().await {
                Ok(()) => backend_caps.push(backend.capabilities.clone()),
                Err(e) => {
                    // In resilient mode a backend down at startup opens its
                    // breaker and is left out; otherwise the router fails.
                    if !self.resilient {
                        return Err(e);
                    }
                    if let Some(breaker) = &backend.handle.breaker {
                        breaker.lock().unwrap().record_failure(clock());
                    }
                }
            }
        }
        self.watcher.reset().await;

        // Compose per SEP §2.3 instead of last-writer-wins: sampling/logging if
        // any backend, elicitation if the client, extensions unioned.
        let mut capabilities = capability::compose_capabilities(&backend_caps, client_caps.as_ref());
        // Local handlers contribute tools too, so advertise the tools capability.
        if !self.registry.ids().is_empty() {
            if let Some(object) = capabilities.as_object_mut() {
                object.entry("tools").or_insert_with(|| json!({}));
            }
        }
        // Compose server variants across backends (SEP-2053): only variants every
        // variant-supporting backend offers are exposed, since the proxy cannot
        // honestly serve one a backend cannot. The naive extension union replaces
        // the payload with a single backend's copy, so overwrite it here.
        let composed_variants = variants::compose_variants(&backend_caps);
        if composed_variants.is_empty() {
            self.variants = Vec::new();
            if let Some(extensions) = capabilities.get_mut("extensions").and_then(Value::as_object_mut) {
                extensions.remove(variants::EXTENSION_ID);
                if extensions.is_empty() {
                    capabilities.as_object_mut().unwrap().remove("extensions");
                }
            }
        } else {
            self.variants = composed_variants
                .iter()
                .filter_map(|v| v.get("id").and_then(Value::as_str).map(str::to_string))
                .collect();
            let object = capabilities.as_object_mut().unwrap();
            let extensions = object.entry("extensions").or_insert_with(|| json!({}));
            extensions
                .as_object_mut()
                .unwrap()
                .insert(variants::EXTENSION_ID.to_string(), json!({ "availableVariants": composed_variants }));
        }
        let response = json!({
            "jsonrpc": "2.0",
            "id": client_init.get("id").cloned().unwrap_or(Value::Null),
            "result": {
                "protocolVersion": PROXY_PROTOCOL_VERSION,
                "capabilities": capabilities,
                "serverInfo": proxy_server_info(),
            },
        });
        self.send_client(&jsonrpc::encode(&response)).await?;
        self.client_read.receive().await?;
        Ok(true)
    }

    async fn route_loop(&mut self) -> io::Result<()> {
        loop {
            let raw = match self.client_read.receive().await? {
                Some(raw) => raw,
                None => return Ok(()),
            };
            let message = jsonrpc::decode(&raw)?;
            let method = jsonrpc::method_of(&message);
            if method.is_none() && message.get("id").is_some() {
                // A response with no method: the client replying to a
                // backend-initiated request. Route it back to the backend.
                self.route_client_reply(&message).await;
                continue;
            }
            let id = match message.get("id") {
                Some(id) => id.clone(),
                None => {
                    // A client notification: forward it onward (SEP §5.1).
                    self.forward_client_notification(&message).await;
                    continue;
                }
            };
            let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
            let list_filter = params.get("filter").cloned().unwrap_or_else(|| json!({}));
            // Per-request server variant (SEP-2053): selected via _meta, forwarded
            // to backends, and rejected here if the composed set does not offer it.
            let variant = variants::selected_variant(&params);
            let cursor = params.get("cursor").cloned().unwrap_or(Value::Null);
            // A task-augmented local call originates a server task (σ3), and a
            // plain local call runs in the worker pool (σ2). Both keep the loop
            // reading (a later cancellation can then reach them) and send their own
            // response, so both continue past the serial dispatch below.
            if self.server_tasks || self.pool_enabled {
                if let Some(cap) = CAPABILITIES.iter().find(|c| Some(c.call) == method) {
                    if self.is_local_call(&message, cap) {
                        if self.server_tasks && tasks::is_task_augmented(&params) {
                            let response = self.originate_task(message.clone(), id.clone());
                            self.send_client(&jsonrpc::encode(&response)).await?;
                            continue;
                        }
                        if self.pool_enabled {
                            self.spawn_pooled_call(message.clone(), id.clone());
                            continue;
                        }
                    }
                }
            }
            let response = if method.map(tasks::is_task_method).unwrap_or(false) {
                self.route_task(&message, id, method.unwrap()).await?
            } else if method.map(subscriptions::is_subscribe_method).unwrap_or(false) {
                self.route_subscription(&message, id, method.unwrap()).await?
            } else if let Some(err) = self.variant_error(&id, variant.as_deref()) {
                err
            } else if method == Some("server/discover") {
                // SEP §2.1: answer server/discover by composing the same
                // namespaced tool surface as tools/list, from all healthy
                // backends. It reuses the tools capability's fan-out.
                self.aggregate(id, &CAPABILITIES[0], &list_filter, &cursor, variant.as_deref()).await?
            } else if let Some(cap) = CAPABILITIES.iter().find(|c| Some(c.list) == method) {
                self.aggregate(id, cap, &list_filter, &cursor, variant.as_deref()).await?
            } else if let Some(cap) = CAPABILITIES.iter().find(|c| Some(c.call) == method) {
                self.route_call(&message, id, cap).await?
            } else {
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": METHOD_NOT_FOUND, "message": format!("method not routable: {}", method.unwrap_or("<none>")) },
                })
            };
            self.send_client(&jsonrpc::encode(&response)).await?;
            if self.resilient {
                self.watcher.emit_if_changed().await;
            }
        }
    }

    // --- Worker pool for server-originated calls (σ2) ---

    /// Whether a call resolves to a local handler, the only calls the worker pool
    /// serves (a routed call stays on the serial path). Only tools/call has local
    /// handlers; the pre-filter name decides.
    fn is_local_call(&self, message: &Value, cap: &Capability) -> bool {
        if cap.call != "tools/call" {
            return false;
        }
        let name = message.get("params").and_then(|p| p.get(cap.field)).and_then(Value::as_str).unwrap_or("");
        resolve(&cap.kind, name).map(|(bid, _)| self.registry.handler_for(&bid).is_some()).unwrap_or(false)
    }

    /// Spawn a pooled call: bound by the semaphore (a cap of 0 is unbounded),
    /// registered so a cancellation can abort it, run under its idle deadline, and
    /// its response sent when it finishes. An aborted task drops here and sends
    /// nothing (MCP cancellation semantics).
    fn spawn_pooled_call(&self, message: Value, id: Value) {
        let registry = self.registry.clone();
        let filter_chain = self.filter_chain.clone();
        let audit = self.audit.clone();
        let client_write = self.client_write.clone();
        let sem = self.pool_sem.clone();
        let inflight = self.inflight.clone();
        let pool_tasks = self.pool_tasks.clone();
        let pool_tokens = self.pool_tokens.clone();
        let opts = DispatchOpts { validate: self.validate_schemas, trace: self.trace, output_limit: self.output_limit };
        let idle_ms = self.pool_idle_ms;
        let id_key = pool::id_key(&id);
        let task_id_key = id_key.clone();

        // Map this call's progressToken (if any) so a progress notification resets
        // its idle deadline.
        let token = message.get("params").and_then(|p| p.get("_meta")).and_then(|m| m.get("progressToken")).map(pool::id_key);
        if let Some(tok) = &token {
            pool_tokens.lock().unwrap().insert(tok.clone(), id.clone());
        }

        let handle = tokio::spawn(async move {
            let _permit = match &sem {
                Some(sem) => sem.clone().acquire_owned().await.ok(),
                None => None,
            };
            inflight.lock().unwrap().register(&id, pool::deadline(now_ms(), idle_ms));
            let work = run_pooled(registry.as_ref(), &filter_chain, &audit, opts, message, &id);
            let response = if idle_ms > 0 {
                match tokio::time::timeout(Duration::from_millis(idle_ms), work).await {
                    Ok(response) => response,
                    Err(_) => json!({ "jsonrpc": "2.0", "id": &id, "error": errors::error_object(errors::INTERNAL_ERROR, Some("call exceeded idle deadline")) }),
                }
            } else {
                work.await
            };
            inflight.lock().unwrap().remove(&id);
            pool_tasks.lock().unwrap().remove(&task_id_key);
            if let Some(tok) = &token {
                pool_tokens.lock().unwrap().remove(tok);
            }
            let _ = client_write.lock().await.send(&jsonrpc::encode(&response)).await;
        });
        // spawn_pooled_call is synchronous, so this insert completes before the
        // route loop next awaits and the task can run: no insert/remove race.
        self.pool_tasks.lock().unwrap().insert(id_key, handle.abort_handle());
    }

    /// A progress notification for a tracked call resets that call's idle deadline
    /// (σ2). An unknown or token-less progress is a no-op.
    fn touch_progress(&self, message: &Value) {
        if let Some(token) = pool::progress_token(message) {
            let tok_key = pool::id_key(token);
            let id = self.pool_tokens.lock().unwrap().get(&tok_key).cloned();
            if let Some(id) = id {
                self.inflight.lock().unwrap().touch(&id, pool::deadline(now_ms(), self.pool_idle_ms));
            }
        }
    }

    // --- Server-side task origination (σ3) ---

    /// Create a task, start its background execution, and return the working
    /// handle at once. The client polls tasks/get for the outcome.
    fn originate_task(&self, message: Value, id: Value) -> Value {
        let task_id = self.tasks_store.lock().unwrap().create();
        self.spawn_task_execution(message, id.clone(), task_id.clone());
        json!({ "jsonrpc": "2.0", "id": id, "result": apply_trace(self.trace, tasks::task_handle(&task_id, tasks::STATUS_WORKING, None, None)) })
    }

    /// Run a task's call to completion in the background and record its outcome in
    /// the store. A cancellation (tasks/cancel) aborts this task; the store is
    /// marked cancelled by the handler, so the abort only stops the work.
    fn spawn_task_execution(&self, message: Value, id: Value, task_id: String) {
        let registry = self.registry.clone();
        let filter_chain = self.filter_chain.clone();
        let audit = self.audit.clone();
        let tasks_store = self.tasks_store.clone();
        let task_handles = self.task_handles.clone();
        let opts = DispatchOpts { validate: self.validate_schemas, trace: self.trace, output_limit: self.output_limit };
        let task_id_task = task_id.clone();
        let handle = tokio::spawn(async move {
            let response = run_pooled(registry.as_ref(), &filter_chain, &audit, opts, message, &id).await;
            let mut store = tasks_store.lock().unwrap();
            if let Some(result) = response.get("result") {
                store.complete(&task_id_task, result.clone());
            } else {
                let error = response.get("error").cloned().unwrap_or_else(|| errors::error_object(errors::INTERNAL_ERROR, Some("task execution failed")));
                store.fail(&task_id_task, error);
            }
            drop(store);
            task_handles.lock().unwrap().remove(&task_id_task);
        });
        self.task_handles.lock().unwrap().insert(task_id, handle.abort_handle());
    }

    /// Serve a server-originated task from the store (σ3). tasks/cancel aborts the
    /// running execution and marks the task cancelled; every method returns the
    /// task's current handle (status plus result or error).
    fn serve_local_task(&self, id: Value, method: &str, task_id: &str) -> Value {
        if method == "tasks/cancel" {
            if let Some(handle) = self.task_handles.lock().unwrap().remove(task_id) {
                handle.abort();
            }
            self.tasks_store.lock().unwrap().cancel(task_id);
        }
        let record = self.tasks_store.lock().unwrap().get(task_id).unwrap_or_else(|| json!({ "status": tasks::STATUS_FAILED }));
        let status = record.get("status").and_then(Value::as_str).unwrap_or(tasks::STATUS_FAILED);
        let result = record.get("result");
        let error = record.get("error");
        json!({ "jsonrpc": "2.0", "id": id, "result": apply_trace(self.trace, tasks::task_handle(task_id, status, result, error)) })
    }

    async fn route_client_reply(&self, message: &Value) {
        // Restore the backend's own id and send the reply back to it (SEP §5.1).
        // An id with no known correlation is a stray response and is dropped.
        let client_id = match message.get("id").and_then(Value::as_str) {
            Some(id) => id.to_string(),
            None => return,
        };
        let routed = self.server_requests.lock().unwrap().remove(&client_id);
        if let Some((backend_id, original)) = routed {
            let mut reply = message.clone();
            if let Some(object) = reply.as_object_mut() {
                object.insert("id".to_string(), original);
            }
            if let Some(backend) = self.backends.iter().find(|b| b.id() == backend_id) {
                let _ = backend.send_message(&reply).await;
            }
        }
    }

    async fn forward_client_notification(&self, message: &Value) {
        // A cancellation names one in-flight request, so it is delivered to the
        // single backend holding it rather than broadcast (SEP §5.1, corpus
        // SEP-2260/2322).
        if jsonrpc::method_of(message) == Some("notifications/cancelled") {
            self.route_client_cancellation(message).await;
            return;
        }
        // A progress notification for a pooled call resets that call's idle
        // deadline (σ2) before the notification is also broadcast onward.
        if jsonrpc::method_of(message) == Some("notifications/progress") {
            self.touch_progress(message);
        }
        // A generic client notification has no routing key, so it is broadcast
        // onward instead of dropped (SEP §5.1). A backend that has died must not
        // take the router down, so send errors are swallowed.
        for backend in &self.backends {
            let _ = backend.send_message(message).await;
        }
    }

    async fn route_client_cancellation(&self, message: &Value) {
        let request_id = match message.get("params").and_then(|p| p.get("requestId")) {
            Some(id) => id.clone(),
            None => return,
        };
        // A pooled server-originated call the client abandoned: abort the running
        // task (σ2). Per MCP the receiver stops and sends no response.
        let pool_handle = self.pool_tasks.lock().unwrap().remove(&pool::id_key(&request_id));
        if let Some(handle) = pool_handle {
            handle.abort();
            return;
        }
        // Otherwise the cancelled requestId is a client-facing id the proxy minted
        // for a backend-initiated request (SEP §5.1). Restore the backend's own id
        // and deliver the cancellation only to that backend. An id the proxy is not
        // holding (for example the client cancelling its own already-completed
        // call, whose id no backend ever saw) is dropped rather than broadcast to
        // every backend, since a stray requestId names nothing a backend can act on.
        let key = match request_id.as_str() {
            Some(key) => key.to_string(),
            None => return, // proxy-minted ids are strings; a non-string names nothing held
        };
        let routed = self.server_requests.lock().unwrap().remove(&key);
        if let Some((backend_id, original)) = routed {
            let mut forwarded = message.clone();
            if let Some(params) = forwarded.get_mut("params").and_then(Value::as_object_mut) {
                params.insert("requestId".to_string(), original);
            }
            if let Some(backend) = self.backends.iter().find(|b| b.id() == backend_id) {
                let _ = backend.send_message(&forwarded).await;
            }
        }
    }

    fn variant_error(&self, id: &Value, variant: Option<&str>) -> Option<Value> {
        // Reject a per-request variant the composed set cannot serve (SEP-2053).
        // No selection is always fine (the default variant applies).
        let variant = variant?;
        if self.variants.is_empty() {
            return Some(json!({
                "jsonrpc": "2.0", "id": id,
                "error": { "code": INVALID_PARAMS, "message": "Server variants not supported" },
            }));
        }
        if !self.variants.iter().any(|v| v == variant) {
            return Some(json!({
                "jsonrpc": "2.0", "id": id,
                "error": {
                    "code": INVALID_PARAMS,
                    "message": format!("unknown variant: {variant}"),
                    "data": { "availableVariants": self.variants },
                },
            }));
        }
        None
    }

    fn effective_variant(&self, variant: Option<&str>) -> Option<String> {
        // The variant that actually applies: the selection, or the default (first
        // composed) when the request omits one (SEP-2053 default rule).
        match variant {
            Some(v) => Some(v.to_string()),
            None => self.variants.first().cloned(),
        }
    }

    async fn collect(
        &self,
        cap: &Capability,
        list_filter: &Value,
        cursor: &Value,
        variant: Option<&str>,
    ) -> io::Result<(Vec<Value>, Vec<String>, BTreeMap<String, String>)> {
        let resilient = self.resilient;
        let single = self.single;
        let empty: Vec<Value> = Vec::new();
        let keywords = list_filter.get("keywords").and_then(Value::as_array).unwrap_or(&empty);
        let patterns = list_filter.get("namePatterns").and_then(Value::as_array).unwrap_or(&empty);
        let has_filter = list_filter.as_object().map(|o| !o.is_empty()).unwrap_or(false);
        // A proxy composite cursor (SEP-2053) restricts the continuation to the
        // backends that still have pages, each with its own backend cursor.
        let resolved_cursor = variants::resolve_cursor(cursor);
        let restrict = resolved_cursor.as_ref().map(|(_, c)| c);
        let effective_variant = self.effective_variant(variant);
        let has_cursor = !cursor.is_null();
        // Keyword pre-select (SEP-2614): skip backends that cannot match, cutting
        // fan-out. A filtered list is pushed down to the backend (SEP-2564).
        let available: Vec<&Backend<BR, BW>> = self
            .backends
            .iter()
            .filter(|b| {
                (!resilient || b.available())
                    && routing::backend_selected(&b.keywords, keywords)
                    && restrict.map(|r| r.contains_key(b.id())).unwrap_or(true)
            })
            .collect();
        // A filter, a cursor, or an active variant makes the fetch specific, so
        // only reuse the cache for the plain unfiltered default surface.
        let use_cache = !has_filter && !has_cursor && effective_variant.is_none();
        // Per-backend forwarded params: the shared filter, the active variant in
        // _meta (SEP-2053), and a continuation cursor: each backend's own cursor on
        // a composite continuation, or a raw cursor passed through in single mode.
        let params_for = |backend: &Backend<BR, BW>| -> Value {
            let mut params = Map::new();
            if has_filter {
                params.insert("filter".to_string(), list_filter.clone());
            }
            if let Some(v) = &effective_variant {
                params.insert("_meta".to_string(), json!({ variants::SERVER_VARIANT_META_KEY: v }));
            }
            if let Some(r) = restrict {
                params.insert("cursor".to_string(), Value::String(r[backend.id()].clone()));
            } else if has_cursor && single {
                params.insert("cursor".to_string(), cursor.clone());
            }
            Value::Object(params)
        };

        // A fresh cache hit skips the backend request entirely (SEP §6). Decide
        // hits before issuing the concurrent fetches for the misses.
        let mut per_backend: HashMap<String, Value> = HashMap::new();
        let mut to_fetch: Vec<&Backend<BR, BW>> = Vec::new();
        for backend in &available {
            let cached = self.cache.as_ref().filter(|_| use_cache).and_then(|cache| {
                cache.lock().unwrap().get(backend.id(), cap.list, self.principal.as_deref(), clock())
            });
            match cached {
                Some(result) => {
                    per_backend.insert(backend.id().to_string(), result);
                }
                None => to_fetch.push(backend),
            }
        }

        let queries = to_fetch.iter().map(|backend| {
            let bid = backend.id().to_string();
            let params = params_for(backend);
            async move { (bid, backend.request(cap.list, params).await) }
        });
        let results = futures::future::join_all(queries).await;

        let mut unavailable: Vec<String> = if resilient {
            self.backends.iter().filter(|b| !b.available()).map(|b| b.id().to_string()).collect()
        } else {
            Vec::new()
        };
        for (bid, response) in results {
            let response = match response {
                Ok(value) => value,
                Err(e) => {
                    if !resilient {
                        return Err(e);
                    }
                    unavailable.push(bid);
                    continue;
                }
            };
            let result = response.get("result").cloned().unwrap_or_else(|| json!({}));
            if use_cache {
                if let Some(cache) = &self.cache {
                    cache.lock().unwrap().put(&bid, cap.list, self.principal.as_deref(), result.clone(), clock());
                }
            }
            per_backend.insert(bid, result);
        }

        let mut items = Vec::new();
        for backend in &available {
            let result = match per_backend.get(backend.id()) {
                Some(result) => result,
                None => continue,
            };
            let listed = result.get(cap.collection).and_then(Value::as_array).cloned().unwrap_or_default();
            for item in listed {
                let value = match item.get(cap.field).and_then(Value::as_str) {
                    Some(value) => value,
                    None => continue,
                };
                // Collision labeling (SEP §3.4). prefix/priority/manual all label
                // with the backend prefix; manual is remapped to its exposed name
                // in `aggregate`, where the full name set is known and a collision
                // can be rejected. passthrough keeps the original name and records
                // a reverse map so a later tools/call still resolves.
                let passthrough = cap.collection == "tools" && self.namespacing.strategy == collision::PASSTHROUGH;
                let labeled = if single || passthrough {
                    value.to_string()
                } else {
                    label(&cap.kind, backend.id(), value)
                };
                if passthrough && !single {
                    self.reverse
                        .lock()
                        .unwrap()
                        .entry(labeled.clone())
                        .or_insert_with(|| (backend.id().to_string(), value.to_string()));
                }
                // Server-side name filtering on the composed surface (SEP-2564).
                if !routing::name_matches(&labeled, patterns) {
                    continue;
                }
                let mut entry = item.clone();
                if let Some(object) = entry.as_object_mut() {
                    object.insert(cap.field.to_string(), Value::String(labeled));
                }
                items.push(entry);
            }
        }
        // A backend that returned a nextCursor still has pages; carry each one so
        // the aggregator can mint a single variant-bound composite cursor.
        let mut next_cursors: BTreeMap<String, String> = BTreeMap::new();
        for backend in &available {
            if let Some(nc) = per_backend.get(backend.id()).and_then(|r| r.get("nextCursor")).and_then(Value::as_str) {
                next_cursors.insert(backend.id().to_string(), nc.to_string());
            }
        }
        Ok((items, unavailable, next_cursors))
    }

    async fn aggregate(
        &mut self,
        id: Value,
        cap: &Capability,
        list_filter: &Value,
        cursor: &Value,
        variant: Option<&str>,
    ) -> io::Result<Value> {
        // Variant-bound cursor validation (SEP-2053 rule 3): a proxy composite
        // cursor may only be used under the variant it was minted with; a cursor
        // the proxy did not mint cannot be routed by an aggregator.
        let effective_variant = self.effective_variant(variant);
        match variants::resolve_cursor(cursor) {
            Some((cursor_variant, _)) => {
                if cursor_variant.as_deref() != effective_variant.as_deref() {
                    return Ok(json!({
                        "jsonrpc": "2.0", "id": id,
                        "error": {
                            "code": INVALID_PARAMS,
                            "message": "Cursor invalid for requested variant",
                            "data": variants::mismatch_data(cursor_variant.as_deref(), effective_variant.as_deref()),
                        },
                    }));
                }
            }
            None => {
                if !cursor.is_null() && !self.single {
                    return Ok(json!({
                        "jsonrpc": "2.0", "id": id,
                        "error": { "code": INVALID_PARAMS, "message": "unknown cursor" },
                    }));
                }
            }
        }
        let (mut items, unavailable, next_cursors) = self.collect(cap, list_filter, cursor, variant).await?;
        if cap.collection == "tools" {
            // Merge local handler tools into the routed backend surface (dispatch
            // seam): the client sees one namespaced tools/list. Local tools honor
            // the same name filter.
            let empty: Vec<Value> = Vec::new();
            let patterns = list_filter.get("namePatterns").and_then(Value::as_array).unwrap_or(&empty);
            items.extend(
                self.registry
                    .list_tools()
                    .into_iter()
                    .filter(|t| routing::name_matches(t.get(cap.field).and_then(Value::as_str).unwrap_or(""), patterns)),
            );
        }
        if cap.collection == "tools" && self.namespacing.strategy == collision::MANUAL {
            // Apply explicit renames to the composed (prefixed) surface. The full
            // name set is known here, so an unresolved collision (two names
            // mapping to one exposed name) is rejected rather than served as a
            // silent duplicate (SEP §3.4).
            let names: Vec<String> = items
                .iter()
                .filter_map(|t| t.get(cap.field).and_then(Value::as_str).map(str::to_string))
                .collect();
            match collision::resolve_manual(&names, &self.namespacing.overrides) {
                Ok(mapping) => {
                    for item in &mut items {
                        let renamed = item.get(cap.field).and_then(Value::as_str).and_then(|n| mapping.get(n)).cloned();
                        if let (Some(exposed), Some(object)) = (renamed, item.as_object_mut()) {
                            object.insert(cap.field.to_string(), Value::String(exposed));
                        }
                    }
                }
                Err(message) => {
                    return Ok(json!({
                        "jsonrpc": "2.0", "id": id,
                        "error": { "code": INTERNAL_ERROR, "message": format!("manual collision: {message}") },
                    }));
                }
            }
        }
        if cap.collection == "tools" && self.namespacing.strategy == collision::PRIORITY {
            // Names stay prefixed; drop the lower-priority copy of a duplicated
            // original name (SEP §3.4). Reverse resolution is unaffected.
            let kind = &cap.kind;
            items = collision::apply_priority(
                items,
                |name| resolve(kind, name),
                &self.namespacing.priority,
                cap.field,
                |_| {},
            );
        }
        if self.disclose && cap.collection == "tools" {
            items = capability::disclose(&items, self.disclose_threshold).0;
        }
        let mut result = Map::new();
        result.insert(cap.collection.to_string(), Value::Array(items));
        // Mint one opaque composite cursor binding the active variant and every
        // paginating backend's own cursor (SEP-2053 rule 2), so the continuation
        // routes back to exactly those backends under the same variant.
        if !next_cursors.is_empty() {
            result.insert(
                "nextCursor".to_string(),
                Value::String(variants::bind_cursor(effective_variant.as_deref(), &next_cursors)),
            );
        }
        if !unavailable.is_empty() {
            result.insert("_meta".to_string(), partial_meta(unavailable, "backend_unavailable"));
        }
        // Attach the server's cache directives to the served list (σ0, §5).
        let body = match &self.list_directives {
            Some((ttl_ms, cache_scope)) => server::attach_directives(&Value::Object(result), *ttl_ms, cache_scope),
            None => Value::Object(result),
        };
        Ok(json!({ "jsonrpc": "2.0", "id": id, "result": self.trace_result(body) }))
    }

    async fn route_call(&mut self, message: &Value, id: Value, cap: &Capability) -> io::Result<Value> {
        // Extension filter chain (ε0): scan the request before routing. A block
        // returns a clean -32001 (plus a best-effort audit outcome); a forward
        // may carry a mutated request (substituted arguments, annotated _meta)
        // that flows onward. None means no filters, so the seam costs nothing.
        let filtered;
        let message = match &self.filter_chain {
            Some(chain) => {
                let outcome = chain.run(filters::REQUEST, message);
                if outcome.get("action").and_then(Value::as_str) == Some("block") {
                    let name = message
                        .get("params")
                        .and_then(|p| p.get(cap.field))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    self.record_audit(outcome_record(cap.call, Some(&name), false));
                    return Ok(outcome["response"].clone());
                }
                filtered = outcome.get("message").cloned().unwrap_or_else(|| message.clone());
                &filtered
            }
            None => message,
        };
        let value = message.get("params").and_then(|p| p.get(cap.field)).and_then(Value::as_str).unwrap_or("");
        if self.disclose && cap.call == "tools/call" && value == capability::PROXY_SEARCH_TOOL {
            // The search meta-tool is served by the proxy, not routed.
            let (tools, _, _) = self.collect(&CAPABILITIES[0], &json!({}), &Value::Null, None).await?;
            let query = message.get("params").and_then(|p| p.get("arguments")).and_then(|a| a.get("query")).and_then(Value::as_str).unwrap_or("");
            let matched = capability::search_tools(query, &tools);
            let names: Vec<&str> = matched.iter().filter_map(|t| t.get("name").and_then(Value::as_str)).collect();
            let text = serde_json::to_string(&names).unwrap_or_else(|_| "[]".to_string());
            let result = json!({ "content": [{ "type": "text", "text": text }] });
            return Ok(json!({ "jsonrpc": "2.0", "id": id, "result": self.trace_result(result) }));
        }
        // Dispatch seam: a tools/call whose prefix names a local handler is
        // served here (server behavior) instead of routed (draft §5.3/§5.7).
        if cap.call == "tools/call" {
            if let Some((bid, original)) = resolve(&cap.kind, value) {
                if self.registry.handler_for(&bid).is_some() {
                    let arguments = message
                        .get("params")
                        .and_then(|p| p.get("arguments"))
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    // Server-originated dispatch (σ1 schema validation inside the
                    // shared helper the worker pool also uses).
                    let opts = DispatchOpts { validate: self.validate_schemas, trace: self.trace, output_limit: self.output_limit };
                    return Ok(dispatch_local(self.registry.as_ref(), opts, &bid, &original, &arguments, &id).await);
                }
            }
        }
        let manual = cap.call == "tools/call" && self.namespacing.strategy == collision::MANUAL;
        let passthrough = cap.call == "tools/call" && self.namespacing.strategy == collision::PASSTHROUGH;
        let (index, original) = if self.single {
            (0usize, value.to_string())
        } else if manual {
            // Invert the explicit renames (exposed -> namespaced), then split.
            // A name that was not renamed is already its namespaced form.
            let namespaced = self
                .namespacing
                .overrides
                .iter()
                .find(|(_, exposed)| exposed.as_str() == value)
                .map(|(namespaced, _)| namespaced.as_str())
                .unwrap_or(value);
            let resolved = namespace::split(namespaced).map(|(a, b)| (a.to_string(), b.to_string()));
            let index = resolved.as_ref().and_then(|(bid, _)| self.backends.iter().position(|b| b.id() == bid));
            match (index, resolved) {
                (Some(index), Some((_, original))) => (index, original),
                _ => return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": INVALID_PARAMS, "message": format!("unknown {}: {value}", cap.field) } })),
            }
        } else if passthrough {
            // Resolve the original name through the reverse map, warming it with a
            // list fan-out if this is the first call before any list.
            let cold = { self.reverse.lock().unwrap().get(value).is_none() };
            if cold {
                let _ = self.collect(&CAPABILITIES[0], &json!({}), &Value::Null, None).await?;
            }
            let resolved = { self.reverse.lock().unwrap().get(value).cloned() };
            let index = resolved.as_ref().and_then(|(bid, _)| self.backends.iter().position(|b| b.id() == bid));
            match (index, resolved) {
                (Some(index), Some((_, original))) => (index, original),
                _ => return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": INVALID_PARAMS, "message": format!("unknown {}: {value}", cap.field) } })),
            }
        } else {
            let resolved = resolve(&cap.kind, value);
            let index = resolved.as_ref().and_then(|(bid, _)| self.backends.iter().position(|b| b.id() == bid));
            match (index, resolved) {
                (Some(index), Some((_, original))) => (index, original),
                _ => return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": INVALID_PARAMS, "message": format!("unknown {}: {value}", cap.field) } })),
            }
        };
        if self.resilient && !self.backends[index].available() {
            let bid = self.backends[index].id();
            return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": SERVER_NOT_AVAILABLE, "message": format!("backend unavailable: {bid}") } }));
        }

        let mut params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        if let Some(object) = params.as_object_mut() {
            object.insert(cap.field.to_string(), Value::String(original));
        }
        let params = self.trace_request(params);
        // Accountability (SEP-2787/2828): a pre-call attestation, then a paired
        // outcome once the call resolves. Best-effort; never affects the reply.
        let audit_name = value.to_string();
        let principal = self.principal.clone().unwrap_or_else(|| "anonymous".to_string());
        self.record_audit(attestation_record(&principal, cap.call, Some(&audit_name)));
        let response = match self.backends[index].request(cap.call, params).await {
            Ok(value) => value,
            Err(e) => {
                if !self.resilient {
                    return Err(e);
                }
                self.record_audit(outcome_record(cap.call, Some(&audit_name), false));
                let bid = self.backends[index].id();
                return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": SERVER_NOT_AVAILABLE, "message": format!("backend call failed: {bid}") } }));
            }
        };
        self.record_audit(outcome_record(cap.call, Some(&audit_name), response.get("result").is_some()));
        if let Some(result) = response.get("result") {
            // A task-augmented tools/call returns a task handle; namespace its id
            // so the client's later tasks/* requests route back here (SEP-2663).
            let result = if cap.call == "tools/call" && tasks::is_task_result(result) {
                tasks::namespace_task_id(result, self.backends[index].id())
            } else {
                result.clone()
            };
            Ok(json!({ "jsonrpc": "2.0", "id": id, "result": self.trace_result(result) }))
        } else {
            let error = response
                .get("error")
                .cloned()
                .unwrap_or_else(|| json!({ "code": INTERNAL_ERROR, "message": "backend error" }));
            Ok(json!({ "jsonrpc": "2.0", "id": id, "error": error }))
        }
    }

    async fn route_task(&mut self, message: &Value, id: Value, method: &str) -> io::Result<Value> {
        // Route a tasks/* request to the backend holding the task's state,
        // reverse-resolving the namespaced taskId (SEP-2663).
        let task_id = message.get("params").and_then(|p| p.get("taskId")).and_then(Value::as_str).unwrap_or("");
        // A server-originated task (σ3) is served from the local store; its id
        // carries no `__`, so it never collides with a routed backend__taskId.
        if self.server_tasks && self.tasks_store.lock().unwrap().contains(task_id) {
            return Ok(self.serve_local_task(id, method, task_id));
        }
        let resolved = tasks::resolve_task(task_id);
        let index = resolved.as_ref().and_then(|(bid, _)| self.backends.iter().position(|b| b.id() == bid));
        let (index, original) = match (index, resolved) {
            (Some(index), Some((_, original))) => (index, original),
            _ => return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": INVALID_PARAMS, "message": format!("unknown task: {task_id}") } })),
        };
        if self.resilient && !self.backends[index].available() {
            let bid = self.backends[index].id();
            return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": SERVER_NOT_AVAILABLE, "message": format!("backend unavailable: {bid}") } }));
        }
        let mut params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        if let Some(object) = params.as_object_mut() {
            object.insert("taskId".to_string(), Value::String(original)); // backend sees its own id
        }
        let params = self.trace_request(params);
        let bid = self.backends[index].id().to_string();
        let response = match self.backends[index].request(method, params).await {
            Ok(value) => value,
            Err(e) => {
                if !self.resilient {
                    return Err(e);
                }
                return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": SERVER_NOT_AVAILABLE, "message": format!("backend call failed: {bid}") } }));
            }
        };
        if let Some(result) = response.get("result") {
            // Re-namespace the taskId in the reply so the client sees one id.
            Ok(json!({ "jsonrpc": "2.0", "id": id, "result": self.trace_result(tasks::namespace_task_id(result, &bid)) }))
        } else {
            let error = response
                .get("error")
                .cloned()
                .unwrap_or_else(|| json!({ "code": INTERNAL_ERROR, "message": "backend error" }));
            Ok(json!({ "jsonrpc": "2.0", "id": id, "error": error }))
        }
    }

    async fn route_subscription(&mut self, message: &Value, id: Value, method: &str) -> io::Result<Value> {
        // Route resources/subscribe|unsubscribe (σ4). The URI namespaces exactly
        // like resources/read, so it reverse-resolves to the owning backend. A URI
        // that resolves to no backend is a local resource: with server
        // subscriptions on it is registered here (and served by a
        // ResourcePublisher), otherwise rejected.
        let cap = &CAPABILITIES[2]; // resources
        let uri = message.get("params").and_then(|p| p.get(cap.field)).and_then(Value::as_str).unwrap_or("");
        let (index, original) = if self.single {
            (0usize, uri.to_string())
        } else {
            let resolved = resolve(&cap.kind, uri);
            let index = resolved.as_ref().and_then(|(bid, _)| self.backends.iter().position(|b| b.id() == bid));
            match (index, resolved) {
                (Some(index), Some((_, original))) => (index, original),
                _ => {
                    if self.subscriptions_enabled {
                        if method == subscriptions::SUBSCRIBE_METHOD {
                            self.subscriptions.lock().unwrap().subscribe(uri);
                        } else {
                            self.subscriptions.lock().unwrap().unsubscribe(uri);
                        }
                        return Ok(json!({ "jsonrpc": "2.0", "id": id, "result": self.trace_result(json!({})) }));
                    }
                    return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": INVALID_PARAMS, "message": format!("unknown resource: {uri}") } }));
                }
            }
        };
        if self.resilient && !self.backends[index].available() {
            let bid = self.backends[index].id();
            return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": SERVER_NOT_AVAILABLE, "message": format!("backend unavailable: {bid}") } }));
        }
        let mut params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        if let Some(object) = params.as_object_mut() {
            object.insert(cap.field.to_string(), Value::String(original)); // backend sees its own uri
        }
        let params = self.trace_request(params);
        let bid = self.backends[index].id().to_string();
        let response = match self.backends[index].request(method, params).await {
            Ok(value) => value,
            Err(e) => {
                if !self.resilient {
                    return Err(e);
                }
                return Ok(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": SERVER_NOT_AVAILABLE, "message": format!("backend call failed: {bid}") } }));
            }
        };
        if let Some(result) = response.get("result") {
            Ok(json!({ "jsonrpc": "2.0", "id": id, "result": self.trace_result(result.clone()) }))
        } else {
            let error = response
                .get("error")
                .cloned()
                .unwrap_or_else(|| json!({ "code": INTERNAL_ERROR, "message": "backend error" }));
            Ok(json!({ "jsonrpc": "2.0", "id": id, "error": error }))
        }
    }
}
