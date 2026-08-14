//! Namespace collision resolution strategies (SEP §3.4).
//!
//! The drafts require an intermediary to implement at least one strategy and to
//! declare the active one. `prefix` (the required strategy) namespaces every
//! tool as `backend__tool` so distinct backends never collide, and is applied
//! by the router's labeling. This module adds the other three:
//!
//! - `priority`: names stay prefixed, but when the same original tool name is
//!   offered by several backends, only the highest-priority backend's version is
//!   kept; the rest are discarded.
//! - `manual`: explicit per-tool renames; two tools that would map to the same
//!   exposed name are an unresolved collision and are rejected at startup.
//! - `passthrough`: no resolution, original names, collisions kept as
//!   duplicates. NOT RECOMMENDED.

use std::collections::HashMap;

use serde_json::Value;

pub const PREFIX: &str = "prefix";
pub const PRIORITY: &str = "priority";
pub const MANUAL: &str = "manual";
pub const PASSTHROUGH: &str = "passthrough";
pub const STRATEGIES: [&str; 4] = [PREFIX, PRIORITY, MANUAL, PASSTHROUGH];

pub fn is_strategy(value: &str) -> bool {
    STRATEGIES.contains(&value)
}

/// Keep the highest-priority backend's copy of each original name.
///
/// `priority` lists backend ids highest-first; a backend not listed ranks below
/// every listed one. `split` reverse-resolves a labeled name to
/// `(backend_id, original)`. Discarded tools are passed to `on_discard`. Input
/// order is otherwise preserved.
pub fn apply_priority(
    tools: Vec<Value>,
    split: impl Fn(&str) -> Option<(String, String)>,
    priority: &[String],
    field: &str,
    mut on_discard: impl FnMut(&Value),
) -> Vec<Value> {
    let rank: HashMap<&str, usize> =
        priority.iter().enumerate().map(|(index, id)| (id.as_str(), index)).collect();
    let rank_of = |name: &str| -> usize {
        match split(name) {
            Some((backend, _)) => rank.get(backend.as_str()).copied().unwrap_or(priority.len()),
            None => priority.len(),
        }
    };

    let mut best: HashMap<String, usize> = HashMap::new(); // original -> kept rank
    let mut kept: HashMap<String, Value> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for tool in tools {
        let labeled = tool.get(field).and_then(Value::as_str).unwrap_or("").to_string();
        let original = split(&labeled).map(|(_, o)| o).unwrap_or_else(|| labeled.clone());
        let this_rank = rank_of(&labeled);
        match best.get(&original).copied() {
            None => {
                best.insert(original.clone(), this_rank);
                kept.insert(original.clone(), tool);
                order.push(original);
            }
            Some(current) if this_rank < current => {
                on_discard(&kept[&original]);
                best.insert(original.clone(), this_rank);
                kept.insert(original, tool);
            }
            Some(_) => on_discard(&tool),
        }
    }
    order.into_iter().map(|original| kept.remove(&original).unwrap()).collect()
}

/// Map each namespaced name to its exposed name, applying `overrides`.
///
/// Returns `Ok({namespaced: exposed})`, or `Err(message)` if two names map to
/// the same exposed name (SEP §3.4: manual MUST reject startup on unresolved
/// collisions).
pub fn resolve_manual(
    names: &[String],
    overrides: &HashMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    let mut exposed = HashMap::new();
    let mut seen: HashMap<String, String> = HashMap::new(); // exposed -> claiming name
    for name in names {
        let target = overrides.get(name).cloned().unwrap_or_else(|| name.clone());
        if let Some(other) = seen.get(&target) {
            if other != name {
                return Err(format!("{name:?} and {other:?} both map to {target:?}"));
            }
        }
        seen.insert(target.clone(), name.clone());
        exposed.insert(name.clone(), target);
    }
    Ok(exposed)
}
