//! Tasks-extension routing (corpus SEP-2663, with SEP-2694 and SEP-2848).
//!
//! A task-augmented `tools/call` returns a task handle (`resultType: "task"`)
//! with a server-generated `taskId` instead of an immediate result; the client
//! then polls `tasks/get` / `tasks/update` / `tasks/cancel` by that id. An
//! intermediary MUST route each same-task request to the replica that holds the
//! task's state (SEP-2663: `Mcp-Name` is set to the `taskId`).
//!
//! yamp routes tasks the same way it routes tools: the taskId is namespaced as
//! `backend__taskId` when the task is created, so a later `tasks/*` request
//! reverse-resolves to the originating backend by splitting the id. Task ids are
//! server-generated and carry no `__` delimiter, so this is stateless and needs
//! no correlation map.
//!
//! SEP-2694 (resumable task event streams) adds `tasks/stream`, which starts or
//! resumes a task's event stream and delivers `notifications/tasks/event` on the
//! same connection. It routes like any other `tasks/*` request (reverse-resolve
//! the taskId, forward the backend's own id, preserve the `after` resume cursor);
//! the events the backend then emits are re-namespaced so the client sees the same
//! `backend__taskId` it holds. SEP-2848 (asynchronous approval for tool calls)
//! needs no new routing: an approval-gated call returns an ordinary `working` task
//! handle, already namespaced and routed, and the optional
//! `net.openid.authzen/tool-approval` client extension composes through the
//! capability extensions union.

use std::collections::HashMap;

use serde_json::{json, Value};

use crate::namespace;

// tasks/stream (SEP-2694) routes like the other task methods and, like tasks/get,
// is a read (it observes events, it does not mutate task state).
pub const TASKS_METHODS: [&str; 4] = ["tasks/get", "tasks/update", "tasks/cancel", "tasks/stream"];
pub const TASK_READ_METHODS: [&str; 2] = ["tasks/get", "tasks/stream"]; // cacheable reads vs writes
pub const TASK_EVENT_METHOD: &str = "notifications/tasks/event"; // backend -> client event (SEP-2694)
pub const RESULT_TYPE_TASK: &str = "task";

// Server-side origination (σ3): the request-side augmentation marker
// and the task lifecycle statuses. A client opts a tools/call into task execution
// by carrying this key in the request _meta; yamp (as the server) then returns a
// working handle instead of blocking, runs the call in the background, and serves
// the later tasks/get and tasks/cancel from its own store.
pub const TASK_META_KEY: &str = "io.modelcontextprotocol/task";
pub const STATUS_WORKING: &str = "working";
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_CANCELLED: &str = "cancelled";
pub const STATUS_FAILED: &str = "failed";

pub fn is_task_method(method: &str) -> bool {
    TASKS_METHODS.contains(&method)
}

/// Whether a `tools/call` result is a task handle (SEP-2663).
pub fn is_task_result(result: &Value) -> bool {
    result.get("resultType").and_then(Value::as_str) == Some(RESULT_TYPE_TASK)
}

/// Whether a request opts into task execution (its `_meta` carries the task
/// augmentation key). A server that supports tasks then originates a handle
/// instead of blocking; one that does not may ignore this and answer directly.
pub fn is_task_augmented(params: &Value) -> bool {
    params.get("_meta").and_then(|m| m.get(TASK_META_KEY)).is_some()
}

/// A server-generated task id. It carries no `__` delimiter, so it never collides
/// with a routed `backend__taskId` and reverse resolution treats it as local.
pub fn new_task_id(seq: u64) -> String {
    format!("task-{seq}")
}

/// A task handle (`resultType: "task"`): the working handle returned at creation,
/// and the status object a later `tasks/get` returns. A completed task carries its
/// `result`; a failed one its `error`.
pub fn task_handle(task_id: &str, status: &str, result: Option<&Value>, error: Option<&Value>) -> Value {
    let mut handle = serde_json::Map::new();
    handle.insert("resultType".to_string(), Value::String(RESULT_TYPE_TASK.to_string()));
    handle.insert("taskId".to_string(), Value::String(task_id.to_string()));
    handle.insert("status".to_string(), Value::String(status.to_string()));
    if let Some(result) = result {
        handle.insert("result".to_string(), result.clone());
    }
    if let Some(error) = error {
        handle.insert("error".to_string(), error.clone());
    }
    Value::Object(handle)
}

/// Namespace `taskId` (and an embedded `task.taskId`) under the backend so the
/// client's later `tasks/*` requests reverse-resolve here.
pub fn namespace_task_id(result: &Value, backend_id: &str) -> Value {
    let mut out = result.clone();
    if let Some(object) = out.as_object_mut() {
        if let Some(id) = object.get("taskId").and_then(Value::as_str) {
            let namespaced = namespace::prefix(backend_id, id);
            object.insert("taskId".to_string(), Value::String(namespaced));
        }
        if let Some(task) = object.get_mut("task").and_then(Value::as_object_mut) {
            if let Some(id) = task.get("taskId").and_then(Value::as_str) {
                let namespaced = namespace::prefix(backend_id, id);
                task.insert("taskId".to_string(), Value::String(namespaced));
            }
        }
    }
    out
}

/// Reverse-resolve a namespaced task id to `(backend_id, original_id)`.
pub fn resolve_task(task_id: &str) -> Option<(String, String)> {
    namespace::split(task_id).map(|(a, b)| (a.to_string(), b.to_string()))
}

/// Re-namespace the `taskId` in a backend's `notifications/tasks/event` so the
/// client sees the same `backend__taskId` it holds (SEP-2694). A message without
/// a string `params.taskId` is returned unchanged.
pub fn namespace_event(message: &Value, backend_id: &str) -> Value {
    let mut out = message.clone();
    if let Some(params) = out.get_mut("params").and_then(Value::as_object_mut) {
        if let Some(id) = params.get("taskId").and_then(Value::as_str) {
            let namespaced = namespace::prefix(backend_id, id);
            params.insert("taskId".to_string(), Value::String(namespaced));
        }
    }
    out
}

/// The server's own task store (σ3): the state a server-originated task holds
/// between its working handle and its terminal outcome. Terminal states are final,
/// so `complete`/`fail`/`cancel` transition only from `working`; that makes
/// cancellation and completion racing on the same task deterministic (whichever
/// reaches `working` first wins). The background execution and its cancellation
/// live in the router; this is the bookkeeping they agree on.
#[derive(Default)]
pub struct ServerTasks {
    tasks: HashMap<String, Value>,
    seq: u64,
}

impl ServerTasks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new working task and return its server-generated id.
    pub fn create(&mut self) -> String {
        self.seq += 1;
        let task_id = new_task_id(self.seq);
        self.tasks.insert(task_id.clone(), json!({ "status": STATUS_WORKING }));
        task_id
    }

    pub fn contains(&self, task_id: &str) -> bool {
        self.tasks.contains_key(task_id)
    }

    pub fn get(&self, task_id: &str) -> Option<Value> {
        self.tasks.get(task_id).cloned()
    }

    fn is_working(&self, task_id: &str) -> bool {
        self.tasks.get(task_id).and_then(|r| r.get("status")).and_then(Value::as_str) == Some(STATUS_WORKING)
    }

    pub fn complete(&mut self, task_id: &str, result: Value) {
        if self.is_working(task_id) {
            self.tasks.insert(task_id.to_string(), json!({ "status": STATUS_COMPLETED, "result": result }));
        }
    }

    pub fn fail(&mut self, task_id: &str, error: Value) {
        if self.is_working(task_id) {
            self.tasks.insert(task_id.to_string(), json!({ "status": STATUS_FAILED, "error": error }));
        }
    }

    /// Cancel a working task. Returns whether it transitioned (a task already
    /// completed, failed, or cancelled is left as it was).
    pub fn cancel(&mut self, task_id: &str) -> bool {
        if self.is_working(task_id) {
            self.tasks.insert(task_id.to_string(), json!({ "status": STATUS_CANCELLED }));
            true
        } else {
            false
        }
    }
}
