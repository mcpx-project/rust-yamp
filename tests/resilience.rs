//! δ6 resilience + chaos tests (Rust arm). Mirrors the Python arm.
//! Deterministic: a shared fake clock and a scripted fault schedule.

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};

use yamp::instrument::within_budget;
use yamp::resilience::{
    BackendChannel, CircuitBreaker, CircuitState, ManagedBackend, ResilientRouter,
    PROXY_PARTIAL_KEY, SERVER_NOT_AVAILABLE,
};

struct ScriptedChannel {
    id: String,
    tools: Vec<&'static str>,
    script: Vec<&'static str>,
    calls: usize,
}

impl BackendChannel for ScriptedChannel {
    fn id(&self) -> &str {
        &self.id
    }

    async fn request(&mut self, method: &str, params: Value) -> io::Result<Value> {
        let behavior = self.script.get(self.calls).copied().unwrap_or("ok");
        self.calls += 1;
        match behavior {
            "fail" => Err(io::Error::new(io::ErrorKind::ConnectionAborted, "transport failure")),
            "err" => Ok(json!({ "error": { "code": -32000, "message": "tool error" } })),
            _ => match method {
                "tools/list" => {
                    let listed: Vec<Value> = self.tools.iter().map(|t| json!({ "name": t })).collect();
                    Ok(json!({ "result": { "tools": listed } }))
                }
                "tools/call" => Ok(json!({
                    "result": { "content": [ { "type": "text", "text": format!("{}:{}", self.id, params["name"].as_str().unwrap_or("")) } ] }
                })),
                _ => Ok(json!({ "result": {} })),
            },
        }
    }
}

fn build_router(
    specs: Vec<(&str, Vec<&'static str>, Vec<&'static str>)>,
    clock: Arc<Mutex<f64>>,
    threshold: u32,
    reset: f64,
) -> ResilientRouter<ScriptedChannel> {
    let backends = specs
        .into_iter()
        .map(|(name, tools, script)| {
            ManagedBackend::new(
                ScriptedChannel { id: name.to_string(), tools, script, calls: 0 },
                CircuitBreaker::new(threshold, reset),
            )
        })
        .collect();
    let c = clock.clone();
    ResilientRouter::new(backends, Box::new(move || *c.lock().unwrap()))
}

// ---- unit: circuit breaker state machine ----

#[test]
fn breaker_opens_after_threshold() {
    let mut breaker = CircuitBreaker::new(3, 10.0);
    assert_eq!(breaker.state(0.0), CircuitState::Closed);
    breaker.record_failure(0.0);
    breaker.record_failure(0.0);
    assert!(breaker.allow(0.0));
    breaker.record_failure(0.0);
    assert_eq!(breaker.state(0.0), CircuitState::Open);
    assert!(!breaker.allow(0.0));
}

#[test]
fn breaker_half_open_recovers_on_success() {
    let mut breaker = CircuitBreaker::new(1, 10.0);
    breaker.record_failure(0.0);
    assert_eq!(breaker.state(0.0), CircuitState::Open);
    assert_eq!(breaker.state(10.0), CircuitState::HalfOpen);
    assert!(breaker.allow(10.0));
    breaker.record_success();
    assert_eq!(breaker.state(10.0), CircuitState::Closed);
}

#[test]
fn breaker_half_open_reopens_on_failure() {
    let mut breaker = CircuitBreaker::new(1, 10.0);
    breaker.record_failure(0.0);
    assert_eq!(breaker.state(10.0), CircuitState::HalfOpen);
    breaker.record_failure(10.0);
    assert_eq!(breaker.state(12.0), CircuitState::Open);
    assert!(!breaker.allow(12.0));
}

#[test]
fn breaker_success_resets_count() {
    let mut breaker = CircuitBreaker::new(3, 10.0);
    breaker.record_failure(0.0);
    breaker.record_failure(0.0);
    breaker.record_success();
    breaker.record_failure(0.0);
    breaker.record_failure(0.0);
    assert!(breaker.allow(0.0));
}

// ---- chaos ----

#[tokio::test]
async fn chaos_partial_fanout_reports_unavailable() {
    let clock = Arc::new(Mutex::new(0.0));
    let mut router = build_router(
        vec![("gh", vec!["a"], vec![]), ("bad", vec!["b"], vec!["fail"]), ("gl", vec!["c"], vec![])],
        clock,
        2,
        10.0,
    );
    let response = router.tools_list(json!("l")).await.unwrap();
    let result = &response["result"];
    let names: BTreeSet<String> = result["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, ["gh__a", "gl__c"].iter().map(|s| s.to_string()).collect());
    let partial = &result["_meta"][PROXY_PARTIAL_KEY];
    assert_eq!(partial["unavailable_backends"], json!(["bad"]));
    assert_eq!(partial["reason"], "circuit_breaker_open");
}

#[tokio::test]
async fn chaos_open_breaker_removes_tools_and_blocks_calls() {
    let clock = Arc::new(Mutex::new(0.0));
    let mut router = build_router(vec![("bad", vec!["b"], vec!["fail", "fail"])], clock, 2, 10.0);
    router.tools_list(json!("1")).await.unwrap();
    router.tools_list(json!("2")).await.unwrap(); // breaker opens

    let listed = router.tools_list(json!("3")).await.unwrap();
    assert_eq!(listed["result"]["tools"], json!([])); // tools removed while open
    let call = router
        .tools_call(&json!({ "params": { "name": "bad__b" } }), json!("c"))
        .await
        .unwrap();
    assert_eq!(call["error"]["code"], SERVER_NOT_AVAILABLE);
    // Short-circuited by the open breaker, not attempted:
    assert!(call["error"]["message"].as_str().unwrap().contains("unavailable"));
}

#[tokio::test]
async fn chaos_recovery_after_reset_timeout() {
    let clock = Arc::new(Mutex::new(0.0));
    let mut router = build_router(vec![("b", vec!["t"], vec!["fail", "fail"])], clock.clone(), 2, 10.0);
    router.tools_list(json!("1")).await.unwrap();
    router.tools_list(json!("2")).await.unwrap(); // opens

    let opened = router
        .tools_call(&json!({ "params": { "name": "b__t" } }), json!("a"))
        .await
        .unwrap();
    assert_eq!(opened["error"]["code"], SERVER_NOT_AVAILABLE);

    *clock.lock().unwrap() = 10.0; // reset window elapses -> half-open

    let recovered = router
        .tools_call(&json!({ "params": { "name": "b__t" } }), json!("b"))
        .await
        .unwrap();
    assert_eq!(recovered["result"]["content"][0]["text"], "b:t");
}

#[tokio::test]
async fn chaos_no_retry_on_tools_call_failure() {
    let clock = Arc::new(Mutex::new(0.0));
    // If the router retried, it would consume "ok" and succeed. It must not.
    let mut router = build_router(vec![("b", vec!["t"], vec!["fail", "ok"])], clock, 5, 10.0);
    let result = router
        .tools_call(&json!({ "params": { "name": "b__t" } }), json!("a"))
        .await
        .unwrap();
    assert_eq!(result["error"]["code"], SERVER_NOT_AVAILABLE);
    assert!(result["error"]["message"].as_str().unwrap().contains("failed"));
}

#[tokio::test]
async fn chaos_backend_application_error_propagated() {
    let clock = Arc::new(Mutex::new(0.0));
    let mut router = build_router(vec![("b", vec!["t"], vec!["err"])], clock, 5, 10.0);
    let result = router
        .tools_call(&json!({ "params": { "name": "b__t" } }), json!("a"))
        .await
        .unwrap();
    assert_eq!(result["error"]["code"], -32000); // application error, not a breaker trip
}

#[tokio::test]
async fn unknown_backend_after_valid_split_rejected() {
    let clock = Arc::new(Mutex::new(0.0));
    let mut router = build_router(vec![("b", vec!["t"], vec![])], clock, 5, 10.0);
    let result = router
        .tools_call(&json!({ "params": { "name": "zz__x" } }), json!("a"))
        .await
        .unwrap();
    assert_ne!(result["error"]["code"], SERVER_NOT_AVAILABLE);
}

#[tokio::test]
async fn surface_change_detection_and_notification() {
    let clock = Arc::new(Mutex::new(0.0));
    let mut router = build_router(vec![("b", vec!["t"], vec!["fail", "fail"])], clock, 2, 10.0);
    assert!(!router.surface_changed());
    router.tools_list(json!("1")).await.unwrap();
    router.tools_list(json!("2")).await.unwrap(); // opens -> surface shrinks
    assert!(router.surface_changed());
    let note = ResilientRouter::<ScriptedChannel>::list_changed_notification();
    assert_eq!(note["method"], "notifications/tools/list_changed");
}

#[tokio::test]
async fn resilience_latency_within_budget() {
    let clock = Arc::new(Mutex::new(0.0));
    let mut router = build_router(vec![("b", vec!["t"], vec![])], clock, 5, 10.0);
    let message = json!({ "params": { "name": "b__t" } });
    for _ in 0..50 {
        router.tools_call(&message, json!("t")).await.unwrap();
    }
    let mut samples = Vec::new();
    for _ in 0..300 {
        let start = Instant::now();
        router.tools_call(&message, json!("t")).await.unwrap();
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = samples[samples.len() / 2];
    let under = samples.iter().filter(|&&x| within_budget(x)).count() as f64 / samples.len() as f64;
    println!("[latency δ6 resilience] median={median:.4}ms within={under:.3}");
    assert!(within_budget(median));
    assert!(under >= 0.99);
}
