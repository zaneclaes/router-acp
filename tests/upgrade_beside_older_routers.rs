//! Workstation upgrades keep older routers serving their sessions. A newer
//! router must start and report accounts beside them, never wait for them.
use std::path::Path;
use std::process::Command;

use serde_json::{Value, json};

/// A fresh `serve` that reads EOF: it starts, migrates, and exits cleanly.
fn serve(home: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_router-acp"))
        .args(["serve", "--config"])
        .arg(home.join(".config/router-acp/router.yaml"))
        .current_dir(home)
        .env_clear()
        .env("HOME", home)
        .env("PATH", home.join("empty-bin"))
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "serve failed to start: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn status(home: &Path) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_router-acp"))
        .args(["account-status", "--config"])
        .arg(home.join(".config/router-acp/router.yaml"))
        .env_clear()
        .env("HOME", home)
        .env("PATH", home.join("empty-bin"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "account-status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn new_sessions_and_account_status_work_while_an_older_router_serves() {
    let home = tempfile::tempdir().unwrap();
    let config = home.path().join(".config/router-acp");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("router.yaml"),
        "agents:\n- name: grok@work\n- name: codex@work\n",
    )
    .unwrap();
    let grok = config.join("accounts/grok@work");
    std::fs::create_dir_all(&grok).unwrap();
    std::fs::write(
        grok.join("auth.json"),
        serde_json::to_vec(&json!({"https://accounts.x.ai/sign-in": {
            "key": "fixture-grok-access", "refresh_token": "fixture-grok-refresh",
            "email": "grok@example.test"
        }}))
        .unwrap(),
    )
    .unwrap();
    // The older router's live session: an unlocked runtime written just now.
    let runtime = grok.join(".router-acp-runtime/9bbeb4d9");
    std::fs::create_dir_all(runtime.join("sessions")).unwrap();
    std::fs::write(runtime.join("sessions/events.jsonl"), "turn").unwrap();
    let codex = config.join("accounts/codex@work");
    std::fs::create_dir_all(&codex).unwrap();
    std::fs::write(
        codex.join("auth.json"),
        serde_json::to_vec(&json!({"email": "codex@example.test", "tokens": {
            "access_token": "fixture-codex-access", "refresh_token": "fixture-codex-refresh"
        }}))
        .unwrap(),
    )
    .unwrap();
    // Another older router is refreshing this account right now.
    let refreshing = std::fs::File::create(codex.join(".router-acp-auth.lock")).unwrap();
    refreshing.lock().unwrap();

    serve(home.path());
    let accounts = status(home.path())["accounts"].as_array().unwrap().clone();
    assert_eq!(accounts.len(), 2);
    assert!(accounts.iter().all(|a| a["credentialPresent"] == true));
    // The older router's session and lock are untouched.
    assert!(runtime.join("sessions/events.jsonl").exists());
    assert!(codex.join(".router-acp-auth.lock").exists());
    drop(refreshing);
}
