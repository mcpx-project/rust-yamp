//! yamp doctor: server-role config preflight over a --config file (Track U).
//!
//! The `nginx -t` analog. Loads the config, builds the local handler surface it
//! would serve, and runs the σ6 doctor check over that surface and the advertised
//! protocol version. Every finding is printed at once.
//!
//! The exit code follows a selectable strictness mode: `default` (warnings
//! advisory, only an error blocks, exit 1), `--strict` (any finding blocks), or
//! `--lenient` (surface findings never block). All three still exit 2 when the
//! config could not be loaded at all, since an unparseable file cannot be
//! preflighted.
//!
//! Usage:
//!   yamp-doctor --config file.json [--json] [--strict | --lenient]

use std::fs;
use std::process::exit;

use serde_json::{json, Value};

use yamp::config;
use yamp::handler::build_registry;
use yamp::{doctor, version};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path: Option<String> = None;
    let mut as_json = false;
    let mut mode = doctor::MODE_DEFAULT;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" if i + 1 < args.len() => {
                config_path = Some(args[i + 1].clone());
                i += 1;
            }
            "--json" => as_json = true,
            "--strict" => mode = doctor::MODE_STRICT,
            "--lenient" => mode = doctor::MODE_LENIENT,
            _ => {}
        }
        i += 1;
    }
    let path = match config_path {
        Some(path) => path,
        None => {
            eprintln!("usage: yamp-doctor --config file.json [--json] [--strict | --lenient]");
            exit(2);
        }
    };
    exit(run(&path, as_json, mode));
}

fn emit(findings: &[Value], as_json: bool, mode: &str) {
    if as_json {
        println!("{}", serde_json::to_string(&doctor::report(findings, mode)).expect("serialize report"));
    } else {
        println!("{}", doctor::render_text(findings, mode));
    }
}

fn config_finding(finding: &Value) -> Value {
    // Map a config diagnosis (U4/U8) to a doctor finding: the specific field-failure
    // slug as the code, and the message plus fix hint, so doctor identifies the exact
    // cause rather than a generic config-load error.
    let location = finding
        .get("line")
        .map(|line| format!(" (line {line}, column {})", finding["column"]))
        .unwrap_or_default();
    let message = format!("{}{location}; {}", finding["message"].as_str().unwrap_or(""), finding["hint"].as_str().unwrap_or(""));
    doctor::finding(doctor::LEVEL_ERROR, finding["slug"].as_str().unwrap_or(""), &message)
}

fn run(path: &str, as_json: bool, mode: &str) -> i32 {
    // A config that will not load is a fatal config-file problem (exit 2) in every
    // mode: an unparseable file cannot be preflighted. It is rendered under the default
    // mode so the verdict reads "config invalid", and the specific field-failure cause
    // is identified (U8) with a fix hint (U4).
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) => {
            emit(&[doctor::finding(doctor::LEVEL_ERROR, "config-load", &err.to_string())], as_json, doctor::MODE_DEFAULT);
            return 2;
        }
    };
    let raw: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(err) => {
            emit(&[config_finding(&config::parse_error_finding(&err.to_string(), err.line(), err.column()))], as_json, doctor::MODE_DEFAULT);
            return 2;
        }
    };
    if let Some(diagnosis) = config::diagnose(&raw) {
        emit(&[config_finding(&diagnosis)], as_json, doctor::MODE_DEFAULT);
        return 2;
    }
    let config = config::from_value(&raw).expect("diagnose passed, so from_value succeeds");
    let ids: Vec<String> = config.backends.iter().map(|b| b.id.clone()).collect();
    let provider = move || json!(ids.iter().map(|id| json!({ "id": id })).collect::<Vec<_>>());
    let registry = build_registry(&config.handlers, provider).expect("valid handler config");
    // The server-role preflight inspects the *local* handler surface (Conversion
    // handlers, meta-tools); live backend tools are not consulted, so the check
    // needs no backend connections, matching the pure σ6 doctor.
    let findings = doctor::check_registry(&registry, version::STATEFUL_PROTOCOL_VERSION);
    emit(&findings, as_json, mode);
    doctor::exit_code(&findings, mode)
}
