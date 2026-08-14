//! yamp config: inspect a --config file's effective values and their provenance (Track U).
//!
//! Five read-only subcommands over config documents:
//!
//!   yamp-config validate --config file.json [--json]
//!   yamp-config explain --config file.json <key> [--json]
//!   yamp-config effective --config file.json [--json]
//!   yamp-config diff --config a.json --to b.json [--json]
//!   yamp-config adapt --config human.json
//!
//! `validate` is the `nginx -t` for the config document: it reports whether the file
//! loads and satisfies the schema (a narrower check than `yamp-doctor`, which also
//! runs the server-surface preflight on top of loading). `explain` reports one key's
//! effective value and whether it came from the config document (`config`), a built-in
//! default (`default`), or is unrecognized (`unknown`). `effective` reports every
//! known key in that form, the fully resolved view. `diff` reports every key whose
//! effective value differs between two documents. `adapt` normalizes a human-friendly
//! document to canonical JSON. Exit code is 0 on success (`diff`: no differences), 1
//! when `validate` finds an invalid config or `diff` finds a difference, and 2 when a
//! config cannot be read or parsed (or `explain` of an unknown key).

use std::fs;
use std::process::exit;

use serde_json::{json, Value};

use yamp::config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(String::as_str);
    let mut config_path: Option<String> = None;
    let mut to_path: Option<String> = None;
    let mut key: Option<String> = None;
    let mut as_json = false;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--config" if i + 1 < args.len() => {
                config_path = Some(args[i + 1].clone());
                i += 1;
            }
            "--to" if i + 1 < args.len() => {
                to_path = Some(args[i + 1].clone());
                i += 1;
            }
            "--json" => as_json = true,
            other if !other.starts_with("--") => key = Some(other.to_string()),
            _ => {}
        }
        i += 1;
    }
    let usage = || {
        eprintln!("usage: yamp-config (validate --config file.json | explain --config file.json <key> | effective --config file.json | diff --config a.json --to b.json | adapt --config file.json) [--json]");
    };
    let path = match config_path {
        Some(path) => path,
        None => {
            usage();
            exit(2);
        }
    };
    match command {
        Some("validate") => exit(run_validate(&path, as_json)),
        Some("explain") => match key {
            Some(key) => exit(run_explain(&path, &key, as_json)),
            None => {
                usage();
                exit(2);
            }
        },
        Some("effective") => exit(run_effective(&path, as_json)),
        Some("diff") => match to_path {
            Some(to) => exit(run_diff(&path, &to, as_json)),
            None => {
                usage();
                exit(2);
            }
        },
        Some("adapt") => exit(run_adapt(&path)),
        _ => {
            usage();
            exit(2);
        }
    }
}

fn load_raw(path: &str) -> Result<Value, i32> {
    let text = fs::read_to_string(path).map_err(|err| {
        eprintln!("error: {err}");
        2
    })?;
    serde_json::from_str(&text).map_err(|err| {
        eprintln!("error: {err}");
        2
    })
}

fn run_explain(path: &str, key: &str, as_json: bool) -> i32 {
    let raw = match load_raw(path) {
        Ok(raw) => raw,
        Err(code) => return code,
    };
    let entry = config::explain(&raw, key);
    if as_json {
        println!("{}", serde_json::to_string(&entry).expect("serialize entry"));
    } else {
        println!("{}", config::explain_line(&entry));
    }
    if entry["source"] == config::SOURCE_UNKNOWN {
        2
    } else {
        0
    }
}

fn run_effective(path: &str, as_json: bool) -> i32 {
    let raw = match load_raw(path) {
        Ok(raw) => raw,
        Err(code) => return code,
    };
    let entries = config::effective(&raw);
    if as_json {
        println!("{}", serde_json::to_string(&entries).expect("serialize entries"));
    } else {
        for entry in &entries {
            println!("{}", config::explain_line(entry));
        }
    }
    0
}

fn run_diff(path: &str, to: &str, as_json: bool) -> i32 {
    let left = match load_raw(path) {
        Ok(raw) => raw,
        Err(code) => return code,
    };
    let right = match load_raw(to) {
        Ok(raw) => raw,
        Err(code) => return code,
    };
    let changes = config::diff(&left, &right);
    if as_json {
        println!("{}", serde_json::to_string(&changes).expect("serialize changes"));
    } else if changes.is_empty() {
        println!("no differences");
    } else {
        for entry in &changes {
            println!("{}", config::diff_line(entry));
        }
    }
    if changes.is_empty() {
        0
    } else {
        1
    }
}

fn invalid_report(finding: Value, as_json: bool) -> i32 {
    if as_json {
        println!("{}", serde_json::to_string(&json!({ "valid": false, "error": finding })).expect("serialize"));
    } else {
        let location = finding
            .get("line")
            .map(|line| format!(" (line {line}, column {})", finding["column"]))
            .unwrap_or_default();
        println!("config invalid: {}{location}", finding["message"].as_str().unwrap_or(""));
        println!("  fix: {}", finding["hint"].as_str().unwrap_or(""));
        println!("  docs: {}", finding["docsUrl"].as_str().unwrap_or(""));
    }
    1
}

fn run_validate(path: &str, as_json: bool) -> i32 {
    // Config-document conformance only (the nginx -t analog): does it load and satisfy
    // the schema, with a line/column, fix hint, and docs URL per error (U4). The
    // server-surface preflight is yamp-doctor's job.
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("error: {err}");
            return 2;
        }
    };
    let raw: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(err) => return invalid_report(config::parse_error_finding(&err.to_string(), err.line(), err.column()), as_json),
    };
    match config::diagnose(&raw) {
        Some(finding) => invalid_report(finding, as_json),
        None => {
            println!("{}", if as_json { "{\"valid\":true}" } else { "config valid" });
            0
        }
    }
}

fn run_adapt(path: &str) -> i32 {
    let raw = match load_raw(path) {
        Ok(raw) => raw,
        Err(code) => return code,
    };
    println!("{}", serde_json::to_string(&config::adapt(&raw)).expect("serialize canonical"));
    0
}
