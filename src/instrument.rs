//! Session record and the proxy latency budget.
//!
//! The session record is the append-only log the experiment protocol consumes
//! (EXPERIMENT.md). Every field is an index or a count, so no source or
//! payload content is written. `LATENCY_BUDGET_MS` is the hard proxy-overhead
//! budget per message.

use std::fs::{create_dir_all, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub const LATENCY_BUDGET_MS: f64 = 10.0;

/// True when a measured added latency respects the proxy budget.
pub fn within_budget(added_latency_ms: f64) -> bool {
    added_latency_ms <= LATENCY_BUDGET_MS
}

pub struct SessionRecord {
    path: PathBuf,
    arm: String,
}

impl SessionRecord {
    pub fn new(path: impl AsRef<Path>, arm: impl Into<String>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }
        Ok(Self {
            path,
            arm: arm.into(),
        })
    }

    /// Log one repair attempt: first-failing gate before and after.
    pub fn attempt(&self, gate_before: u32, gate_after: u32, kind: &str) -> io::Result<()> {
        let line = format!(
            "{{\"arm\":\"{}\",\"gate_after\":{},\"gate_before\":{},\"kind\":\"{}\"}}\n",
            self.arm, gate_after, gate_before, kind
        );
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())
    }
}
