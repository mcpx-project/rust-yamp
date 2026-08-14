//! Track U: the `yamp-doctor` CLI entrypoint (server-role config preflight).
//!
//! Spawns the real `yamp-doctor` binary over temp config files and asserts the
//! rendered report and the process exit code: 0 when servable (warnings advisory),
//! 2 when the config cannot be loaded. Mirrors the Python arm's test_doctor_cli.py.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::{json, Value};

fn write_config(name: &str, data: Value) -> PathBuf {
    let path = env::temp_dir().join(format!("yamp-doctor-{}-{name}.json", std::process::id()));
    fs::write(&path, serde_json::to_vec(&data).unwrap()).unwrap();
    path
}

fn run(path: &PathBuf, json_flag: bool) -> (i32, String) {
    run_mode(path, json_flag, None)
}

fn run_mode(path: &PathBuf, json_flag: bool, mode_flag: Option<&str>) -> (i32, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_yamp-doctor"));
    cmd.arg("--config").arg(path);
    if json_flag {
        cmd.arg("--json");
    }
    if let Some(flag) = mode_flag {
        cmd.arg(flag);
    }
    let out = cmd.output().unwrap();
    (out.status.code().unwrap(), String::from_utf8(out.stdout).unwrap())
}

#[test]
fn proxy_config_no_local_tools_is_servable() {
    // A pure forward-proxy config exposes no *local* handler tools, so the
    // server-role surface is empty: an advisory warning, but still servable.
    let path = write_config("proxy", json!({ "listen": "127.0.0.1:9100", "backends": { "b0": { "address": "127.0.0.1:9101" } } }));
    let (code, out) = run(&path, false);
    fs::remove_file(&path).ok();
    assert_eq!(code, 0, "stdout was: {out}");
    assert!(out.contains("warning: [no-tools] server exposes no tools"), "stdout was: {out}");
    assert!(out.trim_end().ends_with("config ok"), "stdout was: {out}");
}

#[test]
fn strict_mode_blocks_on_warning() {
    // --strict escalates the advisory no-tools warning into a blocking finding, so
    // the same proxy config that is servable by default is rejected under strict.
    let path = write_config("strict", json!({ "listen": "127.0.0.1:9100", "backends": { "b0": { "address": "127.0.0.1:9101" } } }));
    let (code, out) = run_mode(&path, false, Some("--strict"));
    fs::remove_file(&path).ok();
    assert_eq!(code, 1, "stdout was: {out}");
    assert!(out.trim_end().ends_with("config invalid"), "stdout was: {out}");
}

#[test]
fn lenient_mode_accepts_warning() {
    let path = write_config("lenient", json!({ "listen": "127.0.0.1:9100", "backends": { "b0": { "address": "127.0.0.1:9101" } } }));
    let (code, out) = run_mode(&path, false, Some("--lenient"));
    fs::remove_file(&path).ok();
    assert_eq!(code, 0, "stdout was: {out}");
    assert!(out.trim_end().ends_with("config ok"), "stdout was: {out}");
}

#[test]
fn unloadable_config_exits_two() {
    // Missing `listen` makes the config unloadable: a fatal config-file problem. Doctor
    // identifies the specific field-failure cause (U8), not a generic config-load error.
    let path = write_config("bad", json!({ "backends": { "b0": { "address": "127.0.0.1:9101" } } }));
    let (code, out) = run(&path, false);
    fs::remove_file(&path).ok();
    assert_eq!(code, 2, "stdout was: {out}");
    assert!(out.contains("error: [missing-listen]"), "stdout was: {out}");
    assert!(out.trim_end().ends_with("config invalid"), "stdout was: {out}");
}

#[test]
fn unloadable_config_json_exits_two() {
    let path = write_config("badjson", json!({ "backends": { "b0": { "address": "127.0.0.1:9101" } } }));
    let (code, out) = run(&path, true);
    fs::remove_file(&path).ok();
    assert_eq!(code, 2, "stdout was: {out}");
    let report: Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(report["ok"], json!(false));
    assert_eq!(report["findings"][0]["code"], json!("missing-listen"));
}
